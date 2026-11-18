//! Synthetic PostgreSQL archived inbox faceting, including tenant and recipient isolation.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

async fn get(app: &axum::Router, path: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::builder().uri(path);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    (
        status,
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on a random loopback port"]
async fn archived_facets_retain_go_cross_filter_counts_and_isolation() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for name in ["owner", "other", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("facets-{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Facets A',$1) RETURNING id",
            &[&format!("facets-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Facets B',$1) RETURNING id",
            &[&format!("facets-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$5,'owner')",
        &[&ws,&users[0],&users[1],&foreign,&users[2]]).await.unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,'Facets issue','member',$2,'in_progress','high',977) RETURNING id",
        &[&ws,&users[0]]).await.unwrap().get(0);
    let completed: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,'Other issue','member',$2,'done','low',978) RETURNING id",
        &[&ws,&users[0]]).await.unwrap().get(0);
    let insert = "INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,issue_id,title,actor_type,actor_id,archived,read,created_at) VALUES ($1,'member',$2,'notice',$3,'Synthetic',$4,$5,$6,$7,now()+$8::text::interval)";
    db.execute(
        insert,
        &[
            &ws,
            &users[0],
            &Some(issue),
            &Some("system"),
            &Option::<Uuid>::None,
            &true,
            &false,
            &"-4 minutes",
        ],
    )
    .await
    .unwrap();
    db.execute(
        insert,
        &[
            &ws,
            &users[0],
            &Some(issue),
            &Some("member"),
            &Some(users[0]),
            &true,
            &true,
            &"-3 minutes",
        ],
    )
    .await
    .unwrap();
    db.execute(
        insert,
        &[
            &ws,
            &users[0],
            &Some(completed),
            &Some("system"),
            &Option::<Uuid>::None,
            &true,
            &false,
            &"-2 minutes",
        ],
    )
    .await
    .unwrap();
    // An active notification suppresses an archived issue group, even if the archived row is newer.
    db.execute(
        insert,
        &[
            &ws,
            &users[0],
            &Some(completed),
            &Some("system"),
            &Option::<Uuid>::None,
            &false,
            &false,
            &"-1 minute",
        ],
    )
    .await
    .unwrap();
    db.execute(
        insert,
        &[
            &ws,
            &users[1],
            &Some(issue),
            &Some("system"),
            &Option::<Uuid>::None,
            &true,
            &false,
            &"-1 minute",
        ],
    )
    .await
    .unwrap();
    db.execute(
        insert,
        &[
            &foreign,
            &users[0],
            &Option::<Uuid>::None,
            &Some("system"),
            &Option::<Uuid>::None,
            &true,
            &false,
            &"-1 minute",
        ],
    )
    .await
    .unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"facets-test-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),
        &EncodingKey::from_secret(b"facets-test-secret")).unwrap()
    };
    let path = format!("/api/inbox/archived/facets?workspace_id={ws}");
    assert_eq!(get(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        get(&app, &path, Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&app, &path, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, &path, Some(&jwt(users[0]))).await,
        (
            StatusCode::OK,
            json!({"statuses":{"in_progress":1},"priorities":{"high":1},
            "actors":{format!("member:{}",users[0]):1},"unread_count":0})
        )
    );
    let filtered = format!("{path}&statuses=done&actors=system");
    assert_eq!(
        get(&app, &filtered, Some(&jwt(users[0]))).await.1,
        json!({"statuses":{"in_progress":0},"priorities":{"high":0},
            "actors":{format!("member:{}",users[0]):0},"unread_count":0})
    );
    assert_eq!(
        get(&app, &path, Some(&jwt(users[1]))).await.1,
        json!({"statuses":{"in_progress":1},"priorities":{"high":1},"actors":{"system":1},"unread_count":1})
    );
    assert_eq!(
        get(
            &app,
            &format!("{path}&unread_only=true"),
            Some(&jwt(users[0]))
        )
        .await
        .1,
        json!({"statuses":{"in_progress":0},"priorities":{"high":0},"actors":{format!("member:{}",users[0]):0},"unread_count":0})
    );
    for invalid in ["statuses=a,,b", "priorities=x&unread_only=yes"] {
        assert_eq!(
            get(&app, &format!("{path}&{invalid}"), Some(&jwt(users[0])))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let pat = format!("mul_{}", Uuid::new_v4().simple());
    let hash = format!("{:x}", Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'facets test',$2,'mul_facets')", &[&users[0],&hash]).await.unwrap();
    assert_eq!(get(&app, &path, Some(&pat)).await.0, StatusCode::OK);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "ALTER TABLE inbox_item RENAME TO inbox_item_facets_test",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &path, Some(&jwt(users[0]))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to load archived inbox filters"})
        )
    );
    db.execute(
        "ALTER TABLE inbox_item_facets_test RENAME TO inbox_item",
        &[],
    )
    .await
    .unwrap();
}
