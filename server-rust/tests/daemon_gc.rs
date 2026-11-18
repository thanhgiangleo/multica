//! Synthetic daemon GC reads on a disposable, fully migrated PostgreSQL instance.
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

const SECRET: &[u8] = b"synthetic-gc-reads";

fn jwt(id: Uuid) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &json!({"sub":id}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

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
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi63 disposable database migrated to both ledgers"]
async fn gc_reads_enforce_workspace_binding_and_keep_go_time_shape() {
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
    let [
        owner,
        member,
        outsider,
        ws,
        foreign_ws,
        agent,
        chat,
        foreign_chat,
        autopilot,
        foreign_autopilot,
        run,
        foreign_run,
        issue,
        foreign_issue,
        custom_issue,
        unknown_issue,
    ]: [Uuid; 16] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, member, outsider] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES ($1,'gc-test',$2)",
            &[&user, &format!("{user}@gc.invalid")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, foreign_ws] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES ($1,'gc-test',$2)",
            &[&workspace, &format!("gc-{workspace}")],
        )
        .await
        .unwrap();
    }
    for user in [owner, member] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
            &[&ws, &user],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id) VALUES ($1,$2,'gc-agent','cloud',$3)", &[&agent,&ws,&owner]).await.unwrap();
    for (id, workspace, status) in [(chat, ws, "archived"), (foreign_chat, foreign_ws, "active")] {
        db.execute("INSERT INTO chat_session(id,workspace_id,agent_id,creator_id,status,updated_at) VALUES ($1,$2,$3,$4,$5,'2026-11-01T01:02:03.123456Z')", &[&id,&workspace,&agent,&owner,&status]).await.unwrap();
    }
    for (id, workspace) in [(autopilot, ws), (foreign_autopilot, foreign_ws)] {
        db.execute("INSERT INTO autopilot(id,workspace_id,title,assignee_id,created_by_type,created_by_id) VALUES ($1,$2,'gc-test',$3,'member',$4)", &[&id,&workspace,&agent,&owner]).await.unwrap();
    }
    for (id, parent, status) in [
        (run, autopilot, "running"),
        (foreign_run, foreign_autopilot, "completed"),
    ] {
        db.execute(
            "INSERT INTO autopilot_run(id,autopilot_id,source,status) VALUES ($1,$2,'manual',$3)",
            &[&id, &parent, &status],
        )
        .await
        .unwrap();
    }
    for (number, (id, workspace, status)) in [
        (issue, ws, "in_review"),
        (foreign_issue, foreign_ws, "done"),
        (custom_issue, ws, "shipped_gc"),
        (unknown_issue, ws, "missing_gc"),
    ]
    .into_iter()
    .enumerate()
    {
        let number = number as i32 + 1;
        db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,status,updated_at,number) VALUES ($1,$2,'gc-test','member',$3,$4,'2026-11-01T01:02:03.123456Z',$5)", &[&id,&workspace,&owner,&status,&number]).await.unwrap();
    }
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,position,archived_at) VALUES ($1,'shipped_gc','Shipped','done','#00aa00',42,now())", &[&ws]).await.unwrap();
    let token = format!("mdt_gc_{ws}");
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES ($1,$2,'gc-daemon',$3,now()+interval '1 hour')", &[&hash,&ws,&Some(owner)]).await.unwrap();
    let qoder = format!("mdt_gc_qoder_{ws}");
    let qoder_hash = hex::encode(Sha256::digest(qoder.as_bytes()));
    db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES ($1,$2,'qoder-managed-gc',$3,now()+interval '1 hour')", &[&qoder_hash,&ws,&Some(owner)]).await.unwrap();
    db.execute("INSERT INTO cloud_agent_connection(workspace_id,provider,owner_id,token_id,config_encrypted) SELECT $1,'qoder_cloud',$2,id,'synthetic-only' FROM daemon_token WHERE token_hash=$3", &[&ws,&owner,&qoder_hash]).await.unwrap();
    // Qoder owner must have an active owner/admin membership, not merely member.
    let pat = format!("mul_gc_{ws}");
    let pat_hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'gc-test',$2,'mul_gc')", &[&member,&pat_hash]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    );
    let issue_url = |id| format!("/api/daemon/issues/{id}/gc-check");
    let chat_url = |id| format!("/api/daemon/chat-sessions/{id}/gc-check");
    let run_url = |id| format!("/api/daemon/autopilot-runs/{id}/gc-check");
    let snapshot: Value = db
        .query_one(
            "SELECT to_jsonb(c) FROM chat_session c WHERE id=$1",
            &[&chat],
        )
        .await
        .unwrap()
        .get(0);
    for credential in [jwt(owner), jwt(member), pat.clone(), token.clone()] {
        assert_eq!(
            get(&app, &issue_url(issue), Some(&credential)).await,
            (
                StatusCode::OK,
                json!({"status":"in_review","category":"started","updated_at":"2026-11-01T01:02:03.123456Z"})
            )
        );
        assert_eq!(
            get(&app, &issue_url(custom_issue), Some(&credential)).await,
            (
                StatusCode::OK,
                json!({"status":"done","category":"done","updated_at":"2026-11-01T01:02:03.123456Z"})
            )
        );
        assert_eq!(
            get(&app, &issue_url(unknown_issue), Some(&credential)).await,
            (
                StatusCode::OK,
                json!({"status":"missing_gc","category":"","updated_at":"2026-11-01T01:02:03.123456Z"})
            )
        );
        assert_eq!(
            get(&app, &issue_url(foreign_issue), Some(&credential)).await,
            (StatusCode::NOT_FOUND, json!({"error":"not found"}))
        );

        assert_eq!(
            get(&app, &chat_url(chat), Some(&credential)).await,
            (
                StatusCode::OK,
                json!({"status":"archived","updated_at":"2026-11-01T01:02:03.123456Z"})
            )
        );
        assert_eq!(
            get(&app, &run_url(run), Some(&credential)).await,
            (
                StatusCode::OK,
                json!({"status":"running","completed_at":"0001-01-01T00:00:00Z"})
            )
        );
        assert_eq!(
            get(&app, &chat_url(foreign_chat), Some(&credential)).await,
            (StatusCode::NOT_FOUND, json!({"error":"not found"}))
        );
        assert_eq!(
            get(&app, &run_url(foreign_run), Some(&credential)).await,
            (StatusCode::NOT_FOUND, json!({"error":"not found"}))
        );
    }
    assert_eq!(
        get(&app, &issue_url(issue), Some(&jwt(outsider))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, &issue_url(issue), None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(
            &app,
            "/api/daemon/issues/not-a-uuid/gc-check",
            Some(&jwt(owner))
        )
        .await,
        (StatusCode::BAD_REQUEST, json!({"error":"invalid issue_id"}))
    );
    assert_eq!(
        get(&app, &issue_url(Uuid::new_v4()), Some(&jwt(owner))).await,
        (StatusCode::NOT_FOUND, json!({"error":"issue not found"}))
    );
    assert_eq!(
        get(&app, &issue_url(issue), Some(&qoder)).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        snapshot,
        db.query_one(
            "SELECT to_jsonb(c) FROM chat_session c WHERE id=$1",
            &[&chat]
        )
        .await
        .unwrap()
        .get::<_, Value>(0)
    );
    assert_eq!(
        get(&app, &chat_url(chat), Some(&jwt(outsider))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, &run_url(run), Some(&jwt(outsider))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, &chat_url(chat), Some(&qoder)).await,
        (
            StatusCode::FORBIDDEN,
            json!({"error":"runtime access revoked"})
        )
    );
    db.execute(
        "UPDATE member SET role='owner' WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &chat_url(chat), Some(&qoder)).await.0,
        StatusCode::OK
    );
    assert_eq!(
        get(&app, &issue_url(issue), Some(&qoder)).await.0,
        StatusCode::OK
    );
    db.execute(
        "UPDATE cloud_agent_connection SET enabled=false WHERE workspace_id=$1",
        &[&ws],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &run_url(run), Some(&qoder)).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        get(&app, &issue_url(issue), Some(&qoder)).await.0,
        StatusCode::FORBIDDEN
    );
    db.execute("UPDATE autopilot_run SET status='completed',completed_at='2026-11-01T05:06:07Z' WHERE id=$1", &[&run]).await.unwrap();
    assert_eq!(
        get(&app, &run_url(run), Some(&token)).await,
        (
            StatusCode::OK,
            json!({"status":"completed","completed_at":"2026-11-01T05:06:07Z"})
        )
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&pat_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &chat_url(chat), Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&app, &issue_url(issue), Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "UPDATE daemon_token SET expires_at=now()-interval '1 minute' WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &run_url(run), Some(&token)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&app, &issue_url(issue), Some(&token)).await.0,
        StatusCode::UNAUTHORIZED
    );
    for path in [chat_url(Uuid::new_v4()), run_url(Uuid::new_v4())] {
        assert_eq!(
            get(&app, &path, Some(&jwt(owner))).await.0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        get(&app, &chat_url(chat), None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&app, &run_url(run), None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(
            &app,
            "/api/daemon/chat-sessions/not-a-uuid/gc-check",
            Some(&jwt(owner))
        )
        .await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid session_id"})
        )
    );
    assert_eq!(
        get(
            &app,
            "/api/daemon/autopilot-runs/not-a-uuid/gc-check",
            Some(&jwt(owner))
        )
        .await,
        (StatusCode::BAD_REQUEST, json!({"error":"invalid run_id"}))
    );
    // A catalog failure must never promote a custom status to a terminal GC key.
    db.batch_execute("ALTER TABLE issue_status RENAME TO gc_status_unavailable")
        .await
        .unwrap();
    assert_eq!(
        get(&app, &issue_url(custom_issue), Some(&jwt(owner)))
            .await
            .1["status"],
        "shipped_gc"
    );
    assert_eq!(
        get(&app, &issue_url(custom_issue), Some(&jwt(owner)))
            .await
            .1["category"],
        ""
    );
    db.batch_execute("ALTER TABLE gc_status_unavailable RENAME TO issue_status")
        .await
        .unwrap();
    let issue_before: Value = db
        .query_one("SELECT to_jsonb(i) FROM issue i WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        get(&app, &issue_url(issue), Some(&jwt(owner))).await.0,
        StatusCode::OK
    );
    let issue_after: Value = db
        .query_one("SELECT to_jsonb(i) FROM issue i WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(issue_before, issue_after);
    db.batch_execute("ALTER TABLE chat_session RENAME TO gc_chat_unavailable")
        .await
        .unwrap();
    assert_eq!(
        get(&app, &chat_url(chat), Some(&jwt(owner))).await.0,
        StatusCode::NOT_FOUND
    );
    db.batch_execute("ALTER TABLE gc_chat_unavailable RENAME TO chat_session")
        .await
        .unwrap();
}
