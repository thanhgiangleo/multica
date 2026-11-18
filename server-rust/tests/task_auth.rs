//! Real PostgreSQL task credentials; no installed agent, provider or live data.
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
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

const SECRET: &str = "synthetic-task-auth-secret";

fn request(path: &str, token: &str, headers: &[(&str, String)]) -> Request<Body> {
    let mut req = Request::builder()
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("x-user-id", Uuid::new_v4().to_string())
        .header("x-agent-id", Uuid::new_v4().to_string())
        .header("x-task-id", Uuid::new_v4().to_string())
        .header("x-actor-source", "member");
    for (key, value) in headers {
        req = req.header(*key, value);
    }
    req.body(Body::empty()).unwrap()
}

async fn call(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn expect_error(app: &axum::Router, req: Request<Body>, status: StatusCode, message: &str) {
    let uri = req.uri().to_string();
    assert_eq!(
        call(app, req).await,
        (status, json!({"error":message})),
        "{uri}"
    );
}

#[tokio::test]
#[ignore = "requires an owned disposable multica_zbsi63 database with both migration streams"]
async fn task_credentials_preserve_binding_actor_guards_and_guest_generation_fences() {
    let config = Config::from_str(
        &std::env::var("RUST_API_TEST_DATABASE_URL").expect("RUST_API_TEST_DATABASE_URL"),
    )
    .unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(!config.get_hosts().is_empty());
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
    let owner: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Task Owner',$1) RETURNING id",
            &[&format!("task-owner-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let shadow: Uuid = db.query_one("INSERT INTO \"user\"(name,email,zbs_is_shadow) VALUES ('Task Guest',$1,true) RETURNING id", &[&format!("task-guest-{suffix}@invalid.example")]).await.unwrap().get(0);
    let ws_slug = format!("task-a-{suffix}");
    let other_slug = format!("task-b-{suffix}");
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Task A',$1) RETURNING id",
            &[&ws_slug],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Task B',$1) RETURNING id",
            &[&other_slug],
        )
        .await
        .unwrap()
        .get(0);
    // Owner can access BOTH workspaces. A membership check alone would leak B.
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$3,'owner'),($2,$3,'owner'),($1,$4,'member')", &[&ws,&other,&owner,&shadow]).await.unwrap();
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Synthetic runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'Synthetic task actor','cloud',$2,$3) RETURNING id", &[&ws,&owner,&runtime]).await.unwrap().get(0);
    let task: Uuid = db
        .query_one(
            "INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id",
            &[&agent,&runtime],
        )
        .await
        .unwrap()
        .get(0);
    let raw = format!("mat_synthetic_{suffix}");
    let hash = hex::encode(Sha256::digest(raw.as_bytes()));
    let token_id: Uuid = db.query_one("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour') RETURNING id", &[&hash,&task,&agent,&ws,&owner]).await.unwrap().get(0);
    let token_before: Value = db
        .query_one(
            "SELECT to_jsonb(t) FROM task_token t WHERE id=$1",
            &[&token_id],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO issue_label(workspace_id,name,color) VALUES ($1,'task-bound-a','#112233'),($2,'secret-label-b','#334455')", &[&ws,&other]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "task-auth-test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.as_bytes().to_vec()),
    );
    let (status, data) = call(&app, request("/api/me", &raw, &[])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(data["id"], owner.to_string());
    let (status, labels) = call(&app, request("/api/labels/", &raw, &[])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(labels["labels"].as_array().unwrap().len(), 1);
    assert_eq!(labels["labels"][0]["name"], "task-bound-a");
    for path in [
        format!("/api/workspaces/{ws}/"),
        format!("/api/workspaces/{ws}/members"),
    ] {
        assert_eq!(
            call(&app, request(&path, &raw, &[])).await.0,
            StatusCode::OK
        );
    }
    // Go user-scoped list is intentionally not narrowed to the task binding.
    let (status, workspaces) = call(&app, request("/api/workspaces/", &raw, &[])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(workspaces.as_array().unwrap().len(), 2);
    expect_error(
        &app,
        request("/api/tokens/", &raw, &[]),
        StatusCode::FORBIDDEN,
        "this endpoint is only available to human actors",
    )
    .await;
    assert_eq!(
        call(&app, request("/api/invitations", &raw, &[])).await,
        (StatusCode::OK, json!([]))
    );
    for path in [
        format!("/api/workspaces/{other}/"),
        format!("/api/workspaces/{other}/members"),
        "/api/workspaces/not-a-uuid/".into(),
    ] {
        expect_error(
            &app,
            request(&path, &raw, &[]),
            StatusCode::FORBIDDEN,
            "task token is bound to a different workspace",
        )
        .await;
    }
    for headers in [
        vec![("x-workspace-id", other.to_string())],
        vec![("x-workspace-slug", other_slug.clone())],
        vec![
            ("x-workspace-id", ws.to_string()),
            ("x-workspace-id", other.to_string()),
        ],
        vec![
            ("x-workspace-slug", ws_slug.clone()),
            ("x-workspace-slug", other_slug.clone()),
        ],
    ] {
        expect_error(
            &app,
            request("/api/me", &raw, &headers),
            StatusCode::FORBIDDEN,
            "task token is bound to a different workspace",
        )
        .await;
    }
    for query in [
        format!("workspace_id={other}"),
        format!("workspace_slug={other_slug}"),
        format!("workspace_id={ws}&workspace_id={other}"),
        format!("workspace_slug={ws_slug}&workspace_slug={other_slug}"),
        format!("workspace_slug={ws_slug}&workspace_id={other}"),
        format!("workspace_id=&workspace_id={other}"),
    ] {
        expect_error(
            &app,
            request(&format!("/api/labels/?{query}"), &raw, &[]),
            StatusCode::FORBIDDEN,
            "task token is bound to a different workspace",
        )
        .await;
    }
    for query in ["workspace_id=%zz", "unrelated=%", "x=a;b"] {
        expect_error(
            &app,
            request(&format!("/api/me?{query}"), &raw, &[]),
            StatusCode::BAD_REQUEST,
            "malformed query string",
        )
        .await;
    }
    expect_error(
        &app,
        request("/api/me?workspace_id=invalid", &raw, &[]),
        StatusCode::BAD_REQUEST,
        "invalid workspace_id",
    )
    .await;
    expect_error(
        &app,
        request(
            "/api/me?workspace_slug=unknown-synthetic-workspace",
            &raw,
            &[],
        ),
        StatusCode::NOT_FOUND,
        "workspace not found",
    )
    .await;
    expect_error(
        &app,
        request("/api/me?workspace_id=%FF", &raw, &[]),
        StatusCode::BAD_REQUEST,
        "invalid workspace_id",
    )
    .await;
    expect_error(
        &app,
        request("/api/me?workspace_slug=%FF", &raw, &[]),
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to resolve workspace",
    )
    .await;
    for query in [
        format!(
            "workspace_id={}&workspace_slug={ws_slug}",
            ws.to_string().to_uppercase()
        ),
        "workspace_id=&workspace_slug=&x=a%3Bb".into(),
        "unrelated=%FF".into(),
    ] {
        assert_eq!(
            call(&app, request(&format!("/api/labels/?{query}"), &raw, &[]))
                .await
                .0,
            StatusCode::OK
        );
    }
    let cookie_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/me")
                .header("cookie", format!("multica_auth={raw}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cookie_response.status(), StatusCode::OK);
    assert_eq!(
        cookie_response
            .headers()
            .get_all("set-cookie")
            .iter()
            .count(),
        0
    );
    expect_error(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/api/me")
            .header("cookie", format!("multica_auth={raw}"))
            .body(Body::empty())
            .unwrap(),
        StatusCode::FORBIDDEN,
        "CSRF validation failed",
    )
    .await;
    // A forged machine source on a real human session cannot change its actor.
    let human = encode(
        &Header::default(),
        &json!({"sub":owner,"exp":chrono::Utc::now().timestamp()+3600}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();
    assert_eq!(
        call(
            &app,
            request(
                "/api/tokens/",
                &human,
                &[("x-actor-source", "task_token".into())]
            )
        )
        .await
        .0,
        StatusCode::OK
    );
    let token_after: Value = db
        .query_one(
            "SELECT to_jsonb(t) FROM task_token t WHERE id=$1",
            &[&token_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        token_before, token_after,
        "task authentication must not renew or modify token rows"
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    expect_error(
        &app,
        request("/api/labels/", &raw, &[]),
        StatusCode::NOT_FOUND,
        "workspace not found",
    )
    .await;
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 second' WHERE id=$1",
        &[&token_id],
    )
    .await
    .unwrap();
    expect_error(
        &app,
        request("/api/me", &raw, &[]),
        StatusCode::UNAUTHORIZED,
        "invalid token",
    )
    .await;
    db.execute(
        "UPDATE task_token SET expires_at=now()+interval '1 hour', user_id=$2 WHERE id=$1",
        &[&token_id, &shadow],
    )
    .await
    .unwrap();
    expect_error(
        &app,
        request("/api/me", &raw, &[]),
        StatusCode::UNAUTHORIZED,
        "invalid token",
    )
    .await;

    // Both guest systems must fence the runtime OWNER's token, not only a
    // shadow-owned token. Each change is observed immediately (no auth cache).
    for channel in ["telegram", "zalo"] {
        let installation: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id) VALUES ($1,$2,$3,$4) RETURNING id", &[&ws,&agent,&channel,&owner]).await.unwrap().get(0);
        db.execute("INSERT INTO channel_task_delivery(task_id,binding_id,installation_id,channel_type,channel_chat_id,chat_type,route_revision,zbs_guest_shadow_user_id,zbs_guest_grant_generation) VALUES ($1,$2,$3,$4,'synthetic-chat','p2p',1,$5,1)", &[&task,&Uuid::new_v4(),&installation,&channel,&shadow]).await.unwrap();
        let grant_sql = format!(
            "INSERT INTO zbs_{channel}_guest_grant(installation_id,workspace_id,agent_id,{channel}_user_id,shadow_user_id,alias,invite_id) VALUES ($1,$2,$3,'synthetic-guest',$4,'Guest',$5)"
        );
        db.execute(
            &grant_sql,
            &[&installation, &ws, &agent, &shadow, &Uuid::new_v4()],
        )
        .await
        .unwrap();
        for token_owner in [owner, shadow] {
            db.execute(
                "UPDATE task_token SET user_id=$2 WHERE id=$1",
                &[&token_id, &token_owner],
            )
            .await
            .unwrap();
            assert_eq!(
                call(&app, request("/api/me", &raw, &[])).await.0,
                StatusCode::OK,
                "active {channel}"
            );
            let sql = format!(
                "UPDATE zbs_{channel}_guest_grant SET revoked_at=now(),revoked_by_user_id=$2 WHERE installation_id=$1"
            );
            db.execute(&sql, &[&installation, &owner]).await.unwrap();
            expect_error(
                &app,
                request("/api/me", &raw, &[]),
                StatusCode::UNAUTHORIZED,
                "invalid token",
            )
            .await;
            let sql = format!(
                "UPDATE zbs_{channel}_guest_grant SET revoked_at=NULL,revoked_by_user_id=NULL,grant_generation=grant_generation+1 WHERE installation_id=$1"
            );
            db.execute(&sql, &[&installation]).await.unwrap();
            expect_error(
                &app,
                request("/api/me", &raw, &[]),
                StatusCode::UNAUTHORIZED,
                "invalid token",
            )
            .await;
            let sql = format!(
                "UPDATE channel_task_delivery d SET zbs_guest_grant_generation=g.grant_generation FROM zbs_{channel}_guest_grant g WHERE d.installation_id=g.installation_id AND g.installation_id=$1"
            );
            db.execute(&sql, &[&installation]).await.unwrap();
            assert_eq!(
                call(&app, request("/api/me", &raw, &[])).await.0,
                StatusCode::OK,
                "re-paired {channel}"
            );
            for (change, restore) in [
                ("status='revoked'", "status='active'"),
                (
                    &format!("workspace_id='{other}'"),
                    &format!("workspace_id='{ws}'"),
                ),
                (
                    &format!("agent_id='{}'", Uuid::new_v4()),
                    &format!("agent_id='{agent}'"),
                ),
                (
                    "channel_type='feishu'",
                    &format!("channel_type='{channel}'"),
                ),
            ] {
                db.execute(
                    &format!("UPDATE channel_installation SET {change} WHERE id=$1"),
                    &[&installation],
                )
                .await
                .unwrap();
                expect_error(
                    &app,
                    request("/api/me", &raw, &[]),
                    StatusCode::UNAUTHORIZED,
                    "invalid token",
                )
                .await;
                db.execute(
                    &format!("UPDATE channel_installation SET {restore} WHERE id=$1"),
                    &[&installation],
                )
                .await
                .unwrap();
            }
            db.execute(
                "UPDATE \"user\" SET zbs_is_shadow=false WHERE id=$1",
                &[&shadow],
            )
            .await
            .unwrap();
            expect_error(
                &app,
                request("/api/me", &raw, &[]),
                StatusCode::UNAUTHORIZED,
                "invalid token",
            )
            .await;
            db.execute(
                "UPDATE \"user\" SET zbs_is_shadow=true WHERE id=$1",
                &[&shadow],
            )
            .await
            .unwrap();
        }
        db.execute(
            &format!("DELETE FROM zbs_{channel}_guest_grant WHERE installation_id=$1"),
            &[&installation],
        )
        .await
        .unwrap();
        expect_error(
            &app,
            request("/api/me", &raw, &[]),
            StatusCode::UNAUTHORIZED,
            "invalid token",
        )
        .await;
        db.execute(
            "DELETE FROM channel_task_delivery WHERE installation_id=$1",
            &[&installation],
        )
        .await
        .unwrap();
    }
    db.execute(
        "UPDATE task_token SET user_id=$2 WHERE id=$1",
        &[&token_id, &owner],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, request("/api/me", &raw, &[])).await.0,
        StatusCode::OK
    );
    db.execute("DELETE FROM task_token WHERE id=$1", &[&token_id])
        .await
        .unwrap();
    expect_error(
        &app,
        request("/api/me", &raw, &[]),
        StatusCode::UNAUTHORIZED,
        "invalid token",
    )
    .await;
    for invalid in ["mat_unknown", "mdt_unknown", "mcn_unknown"] {
        expect_error(
            &app,
            request("/api/me", invalid, &[]),
            StatusCode::UNAUTHORIZED,
            "invalid token",
        )
        .await;
    }
    // SQL failure also fails closed rather than admitting an unverified actor.
    db.batch_execute("ALTER TABLE task_token RENAME TO task_token_unavailable")
        .await
        .unwrap();
    let failure = call(&app, request("/api/me", &raw, &[])).await;
    db.batch_execute("ALTER TABLE task_token_unavailable RENAME TO task_token")
        .await
        .unwrap();
    assert_eq!(
        failure,
        (StatusCode::UNAUTHORIZED, json!({"error":"invalid token"}))
    );
}
