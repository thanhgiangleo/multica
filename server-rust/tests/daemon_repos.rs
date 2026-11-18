//! Workspace repository feed against a run-owned, fully migrated database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
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

const SECRET: &[u8] = b"synthetic-daemon-repos";

fn jwt(user: Uuid) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &json!({"sub":user}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

async fn get(app: &axum::Router, url: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(url);
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
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi63 disposable database migrated to both ledgers"]
async fn repo_feed_normalizes_urls_and_checks_live_workspace_binding() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|host| matches!(host, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(3)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let [owner, member, outsider, ws, other_ws]: [Uuid; 5] =
        std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, member, outsider] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES ($1,'repo-feed',$2)",
            &[&user, &format!("{user}@repo.invalid")],
        )
        .await
        .unwrap();
    }
    let initial = json!([
        {"url":" git@example:app.git ","description":"App","ref":"feature"},
        {"url":"git@example:app.git","description":"Duplicate"},
        {"url":"  ","description":"Blank"},
        {"url":"git@example:web.git","description":""}
    ]);
    db.execute(
        "INSERT INTO workspace(id,name,slug,repos,settings) VALUES ($1,'repo-feed',$2,$3,$4)",
        &[
            &ws,
            &format!("repo-{ws}"),
            &initial,
            &json!({"git":{"mode":"strict"}}),
        ],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO workspace(id,name,slug,repos) VALUES ($1,'foreign',$2,$3)",
        &[
            &other_ws,
            &format!("repo-{other_ws}"),
            &json!([{"url":"foreign-private"}]),
        ],
    )
    .await
    .unwrap();
    for (user, role) in [(owner, "owner"), (member, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    let daemon = format!("mdt_repo_{ws}");
    let hash = hex::encode(Sha256::digest(daemon.as_bytes()));
    db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES ($1,$2,'repo-daemon',$3,now()+interval '1 hour')", &[&hash,&ws,&Some(owner)]).await.unwrap();
    let qoder = format!("mdt_qoder_repo_{ws}");
    let qoder_hash = hex::encode(Sha256::digest(qoder.as_bytes()));
    db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES ($1,$2,'qoder-managed-repo',$3,now()+interval '1 hour')", &[&qoder_hash,&ws,&Some(owner)]).await.unwrap();
    db.execute("INSERT INTO cloud_agent_connection(workspace_id,provider,owner_id,token_id,config_encrypted) SELECT $1,'qoder_cloud',$2,id,'synthetic-only' FROM daemon_token WHERE token_hash=$3", &[&ws,&owner,&qoder_hash]).await.unwrap();
    let pat = format!("mul_repo_{ws}");
    let pat_hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'repo-feed',$2,'mul_repo')", &[&member,&pat_hash]).await.unwrap();
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let app_without_ca =
        router_with_api(health.clone(), ApiState::new(pool.clone(), SECRET.to_vec()));
    let app = router_with_api(
        health,
        ApiState::new(pool, SECRET.to_vec()).with_git_extra_ca_certs("synthetic PEM\n".into()),
    );
    let url = |id| format!("/api/daemon/workspaces/{id}/repos");
    let before: Value = db
        .query_one("SELECT to_jsonb(w) FROM workspace w WHERE id=$1", &[&ws])
        .await
        .unwrap()
        .get(0);
    let expected_version = hex::encode(Sha256::digest(b"git@example:app.git\ngit@example:web.git"));
    for token in [
        jwt(owner),
        jwt(member),
        pat.clone(),
        daemon.clone(),
        qoder.clone(),
    ] {
        assert_eq!(
            get(&app, &url(ws), Some(&token)).await,
            (
                StatusCode::OK,
                json!({
                    "workspace_id":ws,"repos":[{"url":"git@example:app.git","description":"App","ref":"feature"},{"url":"git@example:web.git"}],
                    "repos_version":expected_version,"settings":{"git":{"mode":"strict"}},"ca_certs":"synthetic PEM\n"
                })
            )
        );
    }
    let (status, without_ca) = get(&app_without_ca, &url(ws), Some(&jwt(owner))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(without_ca.get("ca_certs").is_none());
    assert_eq!(
        before,
        db.query_one("SELECT to_jsonb(w) FROM workspace w WHERE id=$1", &[&ws])
            .await
            .unwrap()
            .get::<_, Value>(0)
    );
    for (target, token) in [
        (ws, jwt(outsider)),
        (other_ws, jwt(member)),
        (other_ws, daemon.clone()),
        (other_ws, qoder.clone()),
    ] {
        assert_eq!(
            get(&app, &url(target), Some(&token)).await.0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(get(&app, &url(ws), None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        get(&app, "/api/daemon/workspaces/invalid/repos", Some(&daemon))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute("UPDATE workspace SET repos=$1 WHERE id=$2", &[&json!([{"url":"git@example:web.git","description":"changed"},{"url":"git@example:app.git"}]),&ws]).await.unwrap();
    let (_, changed) = get(&app, &url(ws), Some(&daemon)).await;
    assert_eq!(changed["repos_version"], expected_version);
    db.execute(
        "UPDATE workspace SET repos=$1 WHERE id=$2",
        &[&json!([{"url":9}]), &ws],
    )
    .await
    .unwrap();
    let (_, invalid) = get(&app, &url(ws), Some(&daemon)).await;
    assert_eq!(invalid["repos"], json!([]));
    assert_eq!(invalid["repos_version"], hex::encode(Sha256::digest(b"")));
    db.execute(
        "UPDATE workspace SET repos='[]',settings='{}' WHERE id=$1",
        &[&ws],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &url(ws), Some(&daemon)).await.1["settings"],
        json!({})
    );
    db.execute(
        "UPDATE cloud_agent_connection SET enabled=false WHERE workspace_id=$1",
        &[&ws],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &url(ws), Some(&qoder)).await,
        (
            StatusCode::FORBIDDEN,
            json!({"error":"runtime access revoked"})
        )
    );
    db.execute(
        "UPDATE cloud_agent_connection SET enabled=true WHERE workspace_id=$1",
        &[&ws],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE member SET role='member' WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &url(ws), Some(&qoder)).await.0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE member SET role='owner' WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&pat_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &url(ws), Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &member],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &url(ws), Some(&jwt(member))).await.0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE daemon_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &url(ws), Some(&daemon)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("ALTER TABLE workspace RENAME COLUMN repos TO repos_test_unavailable")
        .await
        .unwrap();
    assert_eq!(
        get(&app, &url(ws), Some(&jwt(owner))).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error":"workspace not found"})
        )
    );
    db.batch_execute("ALTER TABLE workspace RENAME COLUMN repos_test_unavailable TO repos")
        .await
        .unwrap();
}
