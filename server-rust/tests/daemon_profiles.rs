//! Daemon enabled-profile feed against a disposable, fully migrated database.
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

const SECRET: &[u8] = b"synthetic-profile-feed";

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
    let code = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi63 disposable database migrated to both ledgers"]
async fn daemon_profiles_require_workspace_binding_and_active_qoder_owner() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(3)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let [
        owner,
        member,
        outsider,
        ws,
        other_ws,
        enabled,
        disabled,
        foreign,
    ]: [Uuid; 8] = std::array::from_fn(|_| Uuid::new_v4());
    for id in [owner, member, outsider] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES ($1,'profile-feed',$2)",
            &[&id, &format!("{id}@profile.invalid")],
        )
        .await
        .unwrap();
    }
    for id in [ws, other_ws] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES ($1,'profile-feed',$2)",
            &[&id, &format!("profile-{id}")],
        )
        .await
        .unwrap();
    }
    for (user, role) in [(owner, "owner"), (member, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    for (id, workspace, enabled_flag, name) in [
        (enabled, ws, true, "Enabled"),
        (disabled, ws, false, "Disabled"),
        (foreign, other_ws, true, "Foreign"),
    ] {
        db.execute("INSERT INTO runtime_profile(id,workspace_id,display_name,protocol_family,runtime_type,command_name,fixed_args,visibility,created_by,enabled) VALUES ($1,$2,$3,'codex','','my-cli',$4,'workspace',$5,$6)", &[&id,&workspace,&name,&json!(["--flag"]),&Some(owner),&enabled_flag]).await.unwrap();
    }
    let daemon = format!("mdt_profile_{ws}");
    let qoder = format!("mdt_qoder_{ws}");
    for (token, name) in [
        (&daemon, "profile-daemon"),
        (&qoder, "qoder-managed-profile"),
    ] {
        let hash = hex::encode(Sha256::digest(token.as_bytes()));
        db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES ($1,$2,$3,$4,now()+interval '1 hour')", &[&hash,&ws,&name,&Some(owner)]).await.unwrap();
    }
    let qoder_hash = hex::encode(Sha256::digest(qoder.as_bytes()));
    db.execute("INSERT INTO cloud_agent_connection(workspace_id,provider,owner_id,token_id,config_encrypted) SELECT $1,'qoder_cloud',$2,id,'synthetic-only' FROM daemon_token WHERE token_hash=$3", &[&ws,&owner,&qoder_hash]).await.unwrap();
    let pat = format!("mul_profile_{ws}");
    let pat_hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'profile-feed',$2,'mul_prof')", &[&member,&pat_hash]).await.unwrap();
    let before: Value = db.query_one("SELECT jsonb_agg(to_jsonb(p) ORDER BY p.id) FROM runtime_profile p WHERE workspace_id=$1", &[&ws]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    );
    let url = |id| format!("/api/daemon/workspaces/{id}/runtime-profiles");
    let expected = json!({"workspace_id":ws.to_string(),"runtime_profiles":[{
        "id":enabled,"workspace_id":ws,"display_name":"Enabled","protocol_family":"codex", "runtime_type":"codex", "command_name":"my-cli", "description":null,"fixed_args":["--flag"],"visibility":"workspace","created_by":owner,"enabled":true,
        "created_at":"placeholder","updated_at":"placeholder"
    }]});
    for credential in [
        jwt(owner),
        jwt(member),
        pat.clone(),
        daemon.clone(),
        qoder.clone(),
    ] {
        let (status, body) = get(&app, &url(ws), Some(&credential)).await;
        assert_eq!(status, StatusCode::OK, "{credential}");
        assert_eq!(body["workspace_id"], expected["workspace_id"]);
        assert_eq!(body["runtime_profiles"].as_array().unwrap().len(), 1);
        let mut profile = body["runtime_profiles"][0].clone();
        assert!(profile["created_at"].is_string() && profile["updated_at"].is_string());
        profile["created_at"] = json!("placeholder");
        profile["updated_at"] = json!("placeholder");
        assert_eq!(profile, expected["runtime_profiles"][0]);
    }
    let after: Value = db.query_one("SELECT jsonb_agg(to_jsonb(p) ORDER BY p.id) FROM runtime_profile p WHERE workspace_id=$1", &[&ws]).await.unwrap().get(0);
    assert_eq!(before, after, "GET changed profile rows");
    for (target, credential, status) in [
        (ws, jwt(outsider), StatusCode::NOT_FOUND),
        (other_ws, jwt(member), StatusCode::NOT_FOUND),
        (other_ws, daemon.clone(), StatusCode::NOT_FOUND),
        (other_ws, qoder.clone(), StatusCode::NOT_FOUND),
    ] {
        assert_eq!(get(&app, &url(target), Some(&credential)).await.0, status);
    }
    assert_eq!(get(&app, &url(ws), None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        get(&app, &url(ws), Some("mat_invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(
            &app,
            "/api/daemon/workspaces/invalid/runtime-profiles",
            Some(&daemon)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
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
        &[&hex::encode(Sha256::digest(daemon.as_bytes()))],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &url(ws), Some(&daemon)).await.0,
        StatusCode::UNAUTHORIZED
    );
    // A missing data relation must fail closed, never return an empty list.
    db.batch_execute("ALTER TABLE runtime_profile RENAME TO runtime_profile_test_unavailable")
        .await
        .unwrap();
    assert_eq!(
        get(&app, &url(ws), Some(&jwt(owner))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list runtime profiles"})
        )
    );
    db.batch_execute("ALTER TABLE runtime_profile_test_unavailable RENAME TO runtime_profile")
        .await
        .unwrap();
    assert_eq!(
        get(&app, &url(ws), Some(&jwt(owner))).await.0,
        StatusCode::OK
    );
}
