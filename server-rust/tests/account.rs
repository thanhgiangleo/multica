//! Account-level read regressions against a migrated disposable PostgreSQL.
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
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &str = "isolated-account-test-secret";

async fn read(app: &axum::Router, path: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(path);
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let res = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi63 database"]
async fn account_reads_match_identity_scopes_and_never_return_token_hash() {
    let url = std::env::var("RUST_API_TEST_DATABASE_URL").unwrap();
    let config = Config::from_str(&url).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(1)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let alice: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Alice', $1) RETURNING id",
            &[&format!("alice-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let bob_email = format!("bob-{suffix}@invalid.example");
    let bob: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Bob', $1) RETURNING id",
            &[&bob_email],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Invited workspace',$1,'INV') RETURNING id", &[&format!("invite-{suffix}")]).await.unwrap().get(0);
    let hash = format!("secret-hash-{suffix}");
    let token_id: Uuid = db.query_one("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'active',$2,'mul_demo') RETURNING id", &[&alice,&hash]).await.unwrap().get(0);
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,revoked) VALUES ($1,'revoked',$2,'mul_rev',true)", &[&alice,&format!("revoked-{suffix}")]).await.unwrap();
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,expires_at) VALUES ($1,'expired',$2,'mul_old',now()-interval '1 day')", &[&alice,&format!("expired-{suffix}")]).await.unwrap();
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'other',$2,'mul_other')", &[&bob,&format!("other-{suffix}")]).await.unwrap();
    let invite: Uuid = db.query_one("INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,role) VALUES ($1,$2,$3,'member') RETURNING id", &[&ws,&alice,&bob_email]).await.unwrap().get(0);
    let foreign: Uuid = db.query_one("INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,role) VALUES ($1,$2,$3,'admin') RETURNING id", &[&ws,&alice,&format!("foreign-{suffix}@invalid.example")]).await.unwrap().get(0);
    db.execute("INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,role,status) VALUES ($1,$2,$3,'member','declined')", &[&ws,&alice,&format!("old-{suffix}@invalid.example")]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec()),
    );
    drop(db);
    let exp = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600) as usize;
    let auth = |user: Uuid| {
        encode(
            &Header::default(),
            &json!({"sub":user.to_string(), "exp":exp}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let a = auth(alice);
    let b = auth(bob);
    for path in [
        "/api/tokens/",
        "/api/invitations",
        &format!("/api/invitations/{invite}"),
    ] {
        assert_eq!(
            read(&app, path, None).await,
            (
                StatusCode::UNAUTHORIZED,
                json!({"error":"missing authorization"})
            )
        );
    }
    let (status, tokens) = read(&app, "/api/tokens/", Some(&a)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tokens.as_array().unwrap().len(), 2); // Go lists expired but not revoked.
    assert!(
        tokens
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["id"] == token_id.to_string() && p["token_prefix"] == "mul_demo")
    );
    assert!(
        tokens
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p.get("token_hash").is_none()
                && p.get("revoked").is_none()
                && p.get("user_id").is_none()
                && p.get("token").is_none())
    );
    assert_eq!(
        read(&app, "/api/tokens/", Some(&b)).await.1[0]["name"],
        "other"
    );
    let (status, pending) = read(&app, "/api/invitations", Some(&b)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pending.as_array().unwrap().len(), 1);
    assert_eq!(pending[0]["id"], invite.to_string());
    assert_eq!(pending[0]["workspace_name"], "Invited workspace");
    assert_eq!(
        pending[0]["inviter_email"],
        format!("alice-{suffix}@invalid.example")
    );
    assert_eq!(read(&app, "/api/invitations", Some(&a)).await.1, json!([]));
    let (status, own) = read(&app, &format!("/api/invitations/{invite}"), Some(&b)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(own["id"], invite.to_string());
    assert_eq!(own["invitee_user_id"], Value::Null);
    assert_eq!(
        read(&app, &format!("/api/invitations/{invite}"), Some(&a)).await,
        (
            StatusCode::FORBIDDEN,
            json!({"error":"invitation does not belong to you"})
        )
    );
    assert_eq!(
        read(&app, &format!("/api/invitations/{foreign}"), Some(&b))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        read(&app, "/api/invitations/not-a-uuid", Some(&b)).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid invitation id"})
        )
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/invitations/{}", Uuid::new_v4()),
            Some(&b)
        )
        .await,
        (
            StatusCode::NOT_FOUND,
            json!({"error":"invitation not found"})
        )
    );
    // A user-ID-bound invitation is visible without matching the email; an
    // expired one is absent from the list but still readable by ID as in Go.
    let (db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let bound: Uuid = db.query_one("INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,invitee_user_id,role,expires_at) VALUES ($1,$2,$3,$4,'member',now()-interval '1 hour') RETURNING id", &[&ws,&alice,&format!("old-address-{suffix}@invalid.example"),&bob]).await.unwrap().get(0);
    assert_eq!(
        read(&app, "/api/invitations", Some(&b))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        read(&app, &format!("/api/invitations/{bound}"), Some(&b))
            .await
            .0,
        StatusCode::OK
    );
    db.batch_execute("CREATE SCHEMA zbsi63_account_partial; CREATE VIEW zbsi63_account_partial.\"user\" AS SELECT * FROM public.\"user\"").await.unwrap();
    let mut partial = Config::from_str(&url).unwrap();
    partial.options("-c search_path=zbsi63_account_partial");
    let partial_pool = Arc::new(
        Pool::builder(Manager::new(partial, NoTls))
            .max_size(1)
            .build()
            .unwrap(),
    );
    let partial_app = router_with_api(
        Arc::new(Health::new(
            Some(partial_pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(partial_pool, SECRET.as_bytes().to_vec()),
    );
    assert_eq!(
        read(&partial_app, "/api/tokens/", Some(&a)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list tokens"})
        )
    );
    assert_eq!(
        read(&partial_app, "/api/invitations", Some(&b)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list invitations"})
        )
    );
    assert_eq!(
        read(
            &partial_app,
            &format!("/api/invitations/{invite}"),
            Some(&b)
        )
        .await,
        (
            StatusCode::NOT_FOUND,
            json!({"error":"invitation not found"})
        )
    );
    // These GETs are read-only: neither token usage nor invitation status changes.
    let used: Option<chrono::DateTime<chrono::Utc>> = app_state_check_unused(&url, token_id).await;
    assert!(used.is_none());
}

async fn app_state_check_unused(url: &str, id: Uuid) -> Option<chrono::DateTime<chrono::Utc>> {
    let (db, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    db.query_one(
        "SELECT last_used_at FROM personal_access_token WHERE id=$1",
        &[&id],
    )
    .await
    .unwrap()
    .get(0)
}
