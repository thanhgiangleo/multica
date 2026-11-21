//! Disposable PostgreSQL checks for the issue limit-usage read model.
//!
//! The Go handler degrades to 204 whenever the entitlement provider is absent,
//! the policy cannot be resolved, or the policy is not enforcing, and caps both
//! the scan and the reported usage at the limit. These cases pin that behavior
//! against a real workspace row count so a wrong count or a leaked tenant is a
//! failing test rather than an unnoticed drift.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{
        ApiState,
        issue_write::{IssueCountPolicy, IssueCreateRuntime, IssueCreatedEvent},
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

/// Entitlement provider stand-in: the policy is per selected workspace so a
/// cross-tenant answer is visible as a wrong count instead of a silent pass.
struct Policy {
    workspace: Uuid,
    result: Mutex<Result<IssueCountPolicy, ()>>,
}

#[async_trait::async_trait]
impl IssueCreateRuntime for Policy {
    async fn count_policy(&self, workspace: Uuid) -> Result<IssueCountPolicy, ()> {
        if workspace != self.workspace {
            return Err(());
        }
        // `IssueCountPolicy` is `Copy`, so copy out of the guard directly.
        *self.result.lock().unwrap()
    }

    fn created(&self, _: &IssueCreatedEvent) {}
}

fn jwt(user: Uuid) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),
        &EncodingKey::from_secret(b"queries-sprint-limit-usage"),
    )
    .unwrap()
}

fn app(pool: Arc<Pool>, policy: Option<Policy>) -> axum::Router {
    let mut state = ApiState::new(pool.clone(), b"queries-sprint-limit-usage".to_vec());
    if let Some(policy) = policy {
        state = state.with_issue_create_runtime(Arc::new(policy));
    }
    router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "queries-sprint-limit-usage".into(),
        )),
        state,
    )
}

async fn read(app: &axum::Router, ws: Uuid, user: Uuid) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/issues/limit-usage?workspace_id={ws}"))
                .header("authorization", format!("Bearer {}", jwt(user)))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

async fn policy_app(pool: Arc<Pool>, ws: Uuid, policy: IssueCountPolicy) -> axum::Router {
    app(
        pool,
        Some(Policy {
            workspace: ws,
            result: Mutex::new(Ok(policy)),
        }),
    )
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn limit_usage_degrades_to_204_and_caps_reported_usage() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|host| matches!(host,tokio_postgres::config::Host::Tcp(ip) if ip=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('limit-owner',$1) RETURNING id",
            &[&format!("queries-limit-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let mut workspaces = Vec::new();
    for (name, issues) in [
        ("Limit Full", 5_i64),
        ("Limit Small", 2),
        ("Limit Foreign", 7),
    ] {
        let ws: Uuid = db
            .query_one(
                "INSERT INTO workspace(name,slug) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("limit-{}-{suffix}", name.replace(' ', "-"))],
            )
            .await
            .unwrap()
            .get(0);
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
            &[&ws, &user],
        )
        .await
        .unwrap();
        for number in 1..=issues {
            db.execute(
                "INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,$2,'member',$3,'todo','medium',$4)",
                &[&ws, &format!("usage {number}"), &user, &(number as i32)],
            )
            .await
            .unwrap();
        }
        workspaces.push(ws);
    }
    drop(db);
    let (full, small, foreign) = (workspaces[0], workspaces[1], workspaces[2]);

    // Absent provider: Cloud outages and unconfigured entitlement answer 204.
    let (status, body) = read(&app(pool.clone(), None), full, user).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert!(body.is_empty(), "{body}");

    // Policy resolution failure also degrades to 204 rather than surfacing 5xx.
    let failing = app(
        pool.clone(),
        Some(Policy {
            workspace: full,
            result: Mutex::new(Err(())),
        }),
    );
    let (status, body) = read(&failing, full, user).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");

    // action=off, a non-positive limit, and an unenforced policy are all "off".
    for limit in [None, Some(0), Some(-5)] {
        let app = policy_app(pool.clone(), full, IssueCountPolicy { limit, revision: 7 }).await;
        let (status, body) = read(&app, full, user).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "limit {limit:?}: {body}");
        assert!(body.is_empty(), "limit {limit:?}: {body}");
    }

    // Under the limit: the exact count for the selected workspace only.
    let under = policy_app(
        pool.clone(),
        full,
        IssueCountPolicy {
            limit: Some(10),
            revision: 7,
        },
    )
    .await;
    let (status, body) = read(&under, full, user).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({"used": 5, "limit": 10})
    );

    // At and over the limit: usage is capped, and a foreign workspace's issues
    // never contribute even though they share the same owner.
    for (ws, issues) in [(full, 5), (small, 2), (foreign, 7)] {
        let capped = policy_app(
            pool.clone(),
            ws,
            IssueCountPolicy {
                limit: Some(3),
                revision: 7,
            },
        )
        .await;
        let (status, body) = read(&capped, ws, user).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            serde_json::from_str::<Value>(&body).unwrap(),
            json!({"used": issues.min(3), "limit": 3}),
            "workspace {ws}"
        );
    }

    // A saturated limit must not overflow the bounded scan.
    let saturated = policy_app(
        pool.clone(),
        full,
        IssueCountPolicy {
            limit: Some(i64::MAX),
            revision: 7,
        },
    )
    .await;
    let (status, body) = read(&saturated, full, user).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({"used": 5, "limit": i64::MAX})
    );

    // A policy answered for another workspace is not reused for this one.
    let mismatched: axum::Router = app(
        pool.clone(),
        Some(Policy {
            workspace: small,
            result: Mutex::new(Ok(IssueCountPolicy {
                limit: Some(3),
                revision: 7,
            })),
        }),
    );
    let (status, body) = read(&mismatched, full, user).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
}
