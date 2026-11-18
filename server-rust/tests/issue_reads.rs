//! Issue-bound reads on a migrated, disposable PostgreSQL database.
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

async fn read(app: &axum::Router, path: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(path);
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 on a random loopback port"]
async fn issue_bound_reads_obey_membership_identifier_and_token_revocation() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
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
    for name in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[
                    &name,
                    &format!("issue-read-{name}-{suffix}@invalid.example"),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Team A',$1,'ABC') RETURNING id",
            &[&format!("issue-read-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Team B',$1,'XYZ') RETURNING id",
            &[&format!("issue-read-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&other]).await.unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,metadata) VALUES ($1,'Visible','member',$2,42,$3) RETURNING id", &[&ws,&users[0],&json!({"note":"private A","count":2})]).await.unwrap().get(0);
    let foreign: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,metadata) VALUES ($1,'Foreign','member',$2,42,$3) RETURNING id", &[&other,&users[0],&json!({"note":"private B"})]).await.unwrap().get(0);
    let empty: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Empty','member',$2,43) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let label: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'alpha','#123456','issue') RETURNING id", &[&ws]).await.unwrap().get(0);
    let foreign_label: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'secret','#654321','issue') RETURNING id", &[&other]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO issue_to_label(issue_id,label_id) VALUES ($1,$2),($1,$3)",
        &[&issue, &label, &foreign_label],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason) VALUES ($1,'member',$2,'creator'),($1,'member',$3,'manual')", &[&issue,&users[0],&users[1]]).await.unwrap();
    db.execute(
        "UPDATE issue_subscriber SET unsubscribed_at=now() WHERE issue_id=$1 AND user_id=$2",
        &[&issue, &users[1]],
    )
    .await
    .unwrap();
    let pat = format!("mul_issue_{suffix}");
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'synthetic',$2,'mul_issue')", &[&users[0], &hex::encode(Sha256::digest(pat.as_bytes()))]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-issue-secret".to_vec()),
    );
    let auth = |id: Uuid| {
        encode(&Header::default(), &json!({"sub": id, "exp": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"synthetic-issue-secret")).unwrap()
    };
    let owner = auth(users[0]);
    let member = auth(users[1]);
    let outsider = auth(users[2]);
    let path = |id: &str, segment: &str, selected: Uuid| {
        format!("/api/issues/{id}/{segment}?workspace_id={selected}")
    };
    let labels = path(&issue.to_string(), "labels", ws);
    assert_eq!(read(&app, &labels, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &labels, Some(&outsider)).await.0,
        StatusCode::NOT_FOUND
    );
    let visible = read(&app, &labels, Some(&member)).await;
    assert_eq!(visible.0, StatusCode::OK);
    assert_eq!(visible.1["labels"].as_array().unwrap().len(), 1);
    assert_eq!(visible.1["labels"][0]["id"], label.to_string());
    assert_eq!(visible.1["labels"][0]["usage_count"], 0);
    assert_eq!(
        read(&app, &path("abc-042", "labels", ws), Some(&owner)).await,
        visible
    );
    assert_eq!(read(&app, &labels, Some(&pat)).await, visible);
    assert_eq!(
        read(&app, &path("XYZ-42", "labels", ws), Some(&member))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(
            &app,
            &path(&foreign.to_string(), "metadata", ws),
            Some(&member)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &path("ABC-42", "metadata", other), Some(&member))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(
            &app,
            &path(&issue.to_string(), "metadata", ws),
            Some(&member)
        )
        .await
        .1,
        json!({"metadata":{"note":"private A","count":2}})
    );
    assert_eq!(
        read(
            &app,
            &path(&empty.to_string(), "metadata", ws),
            Some(&member)
        )
        .await
        .1,
        json!({"metadata":{}})
    );
    assert_eq!(
        read(&app, &path(&empty.to_string(), "labels", ws), Some(&member))
            .await
            .1["labels"],
        json!([])
    );
    assert_eq!(
        read(
            &app,
            &path(&empty.to_string(), "subscribers", ws),
            Some(&member)
        )
        .await
        .1,
        json!([])
    );
    let subscribers = read(
        &app,
        &path(&issue.to_string(), "subscribers", ws),
        Some(&member),
    )
    .await;
    assert_eq!(subscribers.0, StatusCode::OK);
    assert_eq!(subscribers.1.as_array().unwrap().len(), 1);
    assert_eq!(subscribers.1[0]["user_id"], users[0].to_string());
    assert_eq!(subscribers.1[0]["reason"], "creator");
    assert_eq!(
        read(
            &app,
            &path(&foreign.to_string(), "subscribers", ws),
            Some(&member)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &path("not-an-issue", "metadata", ws), Some(&member))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &path("ABC-2147483648", "metadata", ws), Some(&member))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hex::encode(Sha256::digest(pat.as_bytes()))],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &labels, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, &labels, Some("invalid-token")).await.0,
        StatusCode::UNAUTHORIZED
    );
    let expired = encode(
        &Header::default(),
        &json!({"sub":users[0],"exp":1}),
        &EncodingKey::from_secret(b"synthetic-issue-secret"),
    )
    .unwrap();
    assert_eq!(
        read(&app, &labels, Some(&expired)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issues/{issue}/metadata"),
            Some(&member)
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    db.execute(
        "ALTER TABLE issue_to_label RENAME TO issue_to_label_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &labels, Some(&member)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list labels"})
        )
    );
    db.execute(
        "ALTER TABLE issue_to_label_unavailable RENAME TO issue_to_label",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "ALTER TABLE issue_subscriber RENAME TO issue_subscriber_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(
            &app,
            &path(&issue.to_string(), "subscribers", ws),
            Some(&member)
        )
        .await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list subscribers"})
        )
    );
    db.execute(
        "ALTER TABLE issue_subscriber_unavailable RENAME TO issue_subscriber",
        &[],
    )
    .await
    .unwrap();
    let saved: Value = db
        .query_one("SELECT metadata FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(saved, json!({"note":"private A","count":2}));
}
