//! Real run-owned PostgreSQL. No Fleet/provider or installed agent account.
use axum::{
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
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

const SECRET: &[u8] = b"synthetic-daemon-auth-only";

fn jwt(user: Uuid, algorithm: Algorithm, extra: Value) -> String {
    let mut claims = json!({"sub":user});
    claims
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    encode(
        &Header::new(algorithm),
        &claims,
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

fn request(path: &str, token: Option<&str>) -> Request<Body> {
    let mut req = Request::builder().uri(path);
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    req.body(Body::empty()).unwrap()
}

async fn call(app: &axum::Router, req: Request<Body>) -> (StatusCode, HeaderMap, Value, Vec<u8>) {
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = res.into_body().collect().await.unwrap().to_bytes().to_vec();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, headers, body, bytes)
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi63 disposable database migrated to both ledgers"]
async fn daemon_discovery_is_bearer_only_and_workspace_bound() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|host| matches!(host, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(config.clone(), NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
    let (owner, outsider, shadow, a, b) = (ids[0], ids[1], ids[2], ids[3], ids[4]);
    for (id, is_shadow) in [(owner, false), (outsider, false), (shadow, true)] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email,zbs_is_shadow) VALUES ($1,'machine-test',$2,$3)",
            &[&id, &format!("{id}@machine.invalid"), &is_shadow],
        )
        .await
        .unwrap();
    }
    for (id, name) in [(a, "<&>\u{2028}\u{2029}"), (b, "Other")] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES ($1,$2,$3)",
            &[&id, &name, &format!("machine-{id}")],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
            &[&id, &owner],
        )
        .await
        .unwrap();
    }
    let daemon = format!("mdt_synthetic_{a}");
    let hash = hex::encode(Sha256::digest(daemon.as_bytes()));
    db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES ($1,$2,'synthetic-daemon',$3,now()+interval '1 hour')", &[&hash,&a,&owner]).await.unwrap();
    let pat = format!("mul_synthetic_{a}");
    let pat_hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'synthetic',$2,'mul_synth')", &[&owner,&pat_hash]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let path = "/api/daemon/workspaces";
    let user = jwt(owner, Algorithm::HS256, json!({}));
    for token in [&user, &pat] {
        let (status, headers, body, _) = call(&app, request(path, Some(token))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["cache-control"], "private, no-cache");
        assert_eq!(headers["content-type"], "application/json");
        assert!(!headers.contains_key("set-cookie"));
        assert_eq!(body.as_array().unwrap().len(), 2);
        assert!(
            body.as_array()
                .unwrap()
                .iter()
                .all(|row| row.as_object().unwrap().len() == 2)
        );
        let order: Vec<_> = body
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        assert!(order.windows(2).all(|w| w[0] < w[1]));
    }
    for alg in [Algorithm::HS256, Algorithm::HS384, Algorithm::HS512] {
        assert_eq!(
            call(
                &app,
                request(path, Some(&jwt(owner, alg, json!({"aud":"ignored-by-go"}))))
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    let snapshot: Value = db
        .query_one(
            "SELECT to_jsonb(t) FROM daemon_token t WHERE token_hash=$1",
            &[&hash],
        )
        .await
        .unwrap()
        .get(0);
    let mut spoofed = request(path, Some(&daemon));
    for (key, value) in [
        ("x-user-id", owner.to_string()),
        ("x-workspace-id", b.to_string()),
        ("x-workspace-slug", "other".into()),
        ("x-actor-source", "member".into()),
        ("x-agent-id", Uuid::new_v4().to_string()),
        ("x-task-id", Uuid::new_v4().to_string()),
    ] {
        spoofed.headers_mut().insert(
            axum::http::HeaderName::from_bytes(key.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    let (status, headers, body, _) = call(&app, spoofed).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([{"id":a,"name":"<&>\u{2028}\u{2029}"}]));
    let tag = headers["etag"].clone();
    let mut conditional = request(path, Some(&daemon));
    conditional
        .headers_mut()
        .insert("if-none-match", tag.clone());
    let (status, cached, _, bytes) = call(&app, conditional).await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(bytes.is_empty());
    assert_eq!(cached["etag"], tag);
    assert_eq!(cached["cache-control"], "private, no-cache");
    assert!(!cached.contains_key("set-cookie"));
    for value in [
        "*".to_owned(),
        format!("{}, W/\"other\"", tag.to_str().unwrap()),
    ] {
        let mut req = request(path, Some(&daemon));
        req.headers_mut()
            .insert("if-none-match", value.parse().unwrap());
        assert_eq!(call(&app, req).await.0, StatusCode::OK);
    }
    assert_eq!(
        snapshot,
        db.query_one(
            "SELECT to_jsonb(t) FROM daemon_token t WHERE token_hash=$1",
            &[&hash]
        )
        .await
        .unwrap()
        .get::<_, Value>(0)
    );
    assert_eq!(
        call(
            &app,
            request(path, Some(&jwt(outsider, Algorithm::HS256, json!({}))))
        )
        .await
        .2,
        json!([])
    );
    let mut cookie = request(path, None);
    cookie
        .headers_mut()
        .insert("cookie", format!("multica_auth={user}").parse().unwrap());
    let denied = call(&app, cookie).await;
    assert_eq!(denied.0, StatusCode::UNAUTHORIZED);
    assert_eq!(denied.2, json!({"error":"missing authorization header"}));
    for token in [
        "mat_synthetic_missing",
        "mdt_missing",
        "mcn_unconfigured",
        "invalid",
        &jwt(shadow, Algorithm::HS256, json!({})),
        &jwt(Uuid::new_v4(), Algorithm::HS256, json!({})),
        &jwt(owner, Algorithm::HS256, json!({"exp":1})),
        &jwt(owner, Algorithm::HS256, json!({"nbf":4102444800u64})),
    ] {
        assert_eq!(
            call(&app, request(path, Some(token))).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        call(&app, request("/api/me", Some(&daemon))).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&pat_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, request(path, Some(&pat))).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "UPDATE daemon_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, request(path, Some(&daemon))).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "UPDATE daemon_token SET expires_at=now()+interval '1 hour' WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM member WHERE user_id=$1", &[&owner])
        .await
        .unwrap();
    assert_eq!(call(&app, request(path, Some(&user))).await.2, json!([]));
    // Daemon token discovery is workspace-scoped, not user-membership-scoped.
    assert_eq!(
        call(&app, request(path, Some(&daemon))).await.0,
        StatusCode::OK
    );
    db.batch_execute("ALTER TABLE workspace RENAME TO machine_hidden_workspace")
        .await
        .unwrap();
    assert_eq!(
        call(&app, request(path, Some(&daemon))).await.0,
        StatusCode::NOT_FOUND
    );
    db.batch_execute("ALTER TABLE machine_hidden_workspace RENAME TO workspace")
        .await
        .unwrap();
    db.execute("DELETE FROM workspace WHERE id=$1", &[&a])
        .await
        .unwrap();
    // Workspace deletion removes its daemon tokens as part of the DB lifecycle.
    assert_eq!(
        call(&app, request(path, Some(&daemon))).await.0,
        StatusCode::UNAUTHORIZED
    );
    // Missing workspace is normally unreachable: FK deletion cascades token.
    // Exercise the handler's query-failure mapping without dropping data.
    db.batch_execute("ALTER TABLE workspace RENAME TO machine_hidden_workspace")
        .await
        .unwrap();
    assert_eq!(
        call(&app, request(path, Some(&user))).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.batch_execute("ALTER TABLE machine_hidden_workspace RENAME TO workspace")
        .await
        .unwrap();
    assert_eq!(
        call(&app, request(path, Some(&daemon))).await.0,
        StatusCode::UNAUTHORIZED
    );
    let mut missing = config;
    missing.options("-c search_path=zbsi63_missing");
    let absent = Arc::new(
        Pool::builder(Manager::new(missing, NoTls))
            .max_size(1)
            .build()
            .unwrap(),
    );
    let broken = router_with_api(
        Arc::new(Health::new(
            Some(absent.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(absent, SECRET.to_vec()),
    );
    assert_eq!(
        call(&broken, request(path, Some(&daemon))).await.2,
        json!({"error":"invalid daemon token"})
    );
    assert_eq!(
        call(&broken, request(path, Some(&user))).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}
