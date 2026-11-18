//! Synthetic isolated PostgreSQL matrix for the assignee-frequency read.
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
    let resp = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    (
        status,
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on a random loopback port"]
async fn frequency_aggregates_only_actor_and_workspace_and_fails_closed() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
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
    for role in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&role, &format!("freq-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Frequency A',$1) RETURNING id",
            &[&format!("freq-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Frequency B',$1) RETURNING id",
            &[&format!("freq-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member')",
        &[&ws, &users[0], &users[1]],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
        &[&other, &users[1]],
    )
    .await
    .unwrap();
    let target = Uuid::new_v4();
    let foreign_target = Uuid::new_v4();
    for (workspace, actor, actor_type, to_type, to_id) in [
        (ws, users[1], "member", "member", target.to_string()),
        (ws, users[1], "member", "member", target.to_string()),
        (ws, users[1], "member", "agent", Uuid::new_v4().to_string()),
        (ws, users[0], "member", "member", foreign_target.to_string()),
        (ws, users[1], "agent", "member", foreign_target.to_string()),
        (
            other,
            users[1],
            "member",
            "member",
            foreign_target.to_string(),
        ),
        (ws, users[1], "member", "member", String::new()),
    ] {
        db.execute("INSERT INTO activity_log(workspace_id,actor_id,actor_type,action,details) VALUES ($1,$2,$3,'assignee_changed',$4)", &[&workspace,&actor,&actor_type,&json!({"to_type":to_type,"to_id":to_id})]).await.unwrap();
    }
    for (number, (workspace, creator, creator_type, assignee)) in [
        (ws, users[1], "member", target),
        (ws, users[1], "member", target),
        (ws, users[0], "member", foreign_target),
        (other, users[1], "member", foreign_target),
        (ws, users[1], "agent", foreign_target),
    ]
    .into_iter()
    .enumerate()
    {
        db.execute("INSERT INTO issue(workspace_id,title,creator_type,creator_id,assignee_type,assignee_id,number) VALUES ($1,'Synthetic',$2,$3,'member',$4,$5)", &[&workspace,&creator_type,&creator,&assignee,&(number as i32 + 1)]).await.unwrap();
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-freq-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(b"synthetic-freq-secret")).unwrap()
    };
    let path = format!("/api/assignee-frequency?workspace_id={ws}");
    assert_eq!(read(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &path, Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, &path, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/assignee-frequency?workspace_id={other}"),
            Some(&jwt(users[0]))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, "/api/assignee-frequency", Some(&jwt(users[1])))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let (status, rows) = read(&app, &path, Some(&jwt(users[1]))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rows.as_array().unwrap().len(), 2);
    assert_eq!(
        rows[0],
        json!({"assignee_type":"member","assignee_id":target,"frequency":4})
    );
    assert_eq!(rows[1]["assignee_type"], "agent");
    assert_eq!(rows[1]["frequency"], 1);
    assert!(!rows.to_string().contains(&foreign_target.to_string()));
    assert_eq!(
        read(&app, &path, Some(&jwt(users[0]))).await.1,
        json!([{"assignee_type":"member","assignee_id":foreign_target,"frequency":2}])
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/assignee-frequency?workspace_id={other}"),
            Some(&jwt(users[1]))
        )
        .await
        .1,
        json!([{"assignee_type":"member","assignee_id":foreign_target,"frequency":2}])
    );
    let pat = format!("mul_frequency_{suffix}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'Synthetic',$2,'mul_frequency')", &[&users[1],&hash]).await.unwrap();
    assert_eq!(read(&app, &path, Some(&pat)).await.1, rows);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "ALTER TABLE activity_log RENAME TO activity_log_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[1]))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to get assignee frequency"})
        )
    );
    db.execute(
        "ALTER TABLE activity_log_unavailable RENAME TO activity_log",
        &[],
    )
    .await
    .unwrap();
    db.execute("ALTER TABLE issue RENAME TO issue_unavailable", &[])
        .await
        .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[1]))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to get assignee frequency"})
        )
    );
    db.execute("ALTER TABLE issue_unavailable RENAME TO issue", &[])
        .await
        .unwrap();
}
