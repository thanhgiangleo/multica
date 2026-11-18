//! Synthetic workspace runtime catalog: governance list is not runtime access.
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

async fn read(app: &axum::Router, path: &str, auth: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::builder().uri(path);
    if let Some(auth) = auth {
        request = request.header("authorization", format!("Bearer {auth}"));
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
#[ignore = "requires run-owned PostgreSQL 16 on random loopback port"]
async fn list_runtime_honors_owner_role_grants_and_workspace() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|host| matches!(host, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for name in ["owner", "admin", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("runtime-{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let mut workspaces = Vec::new();
    for name in ["Runtimes", "Foreign"] {
        workspaces.push(
            db.query_one(
                "INSERT INTO workspace(name,slug) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("runtime-{name}-{suffix}")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let (ws, foreign) = (workspaces[0], workspaces[1]);
    for (user, role, workspace) in [
        (users[0], "owner", ws),
        (users[1], "admin", ws),
        (users[2], "member", ws),
        (users[0], "owner", foreign),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&workspace, &user, &role],
        )
        .await
        .unwrap();
    }
    let mut runtimes = Vec::new();
    for (name, workspace, owner, visibility, shared, provider) in [
        ("Private", ws, users[0], "private", vec![], "codex"),
        ("Public", ws, users[0], "public", vec![], "omp"),
        (
            "Shared",
            ws,
            users[0],
            "private",
            vec![users[2]],
            "synthetic",
        ),
        ("Mine", ws, users[2], "private", vec![], "codex"),
        ("Foreign", foreign, users[0], "public", vec![], "codex"),
    ] {
        runtimes.push(db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,owner_id,visibility,shared_with,metadata,custom_name) VALUES ($1,$2,'local',$3,$4,$5,$6,$7::jsonb,$8) RETURNING id", &[&workspace,&format!("{name} {suffix}"),&provider,&owner,&visibility,&shared,&json!({"fixture":name}),&Some(format!("{name} custom"))]).await.unwrap().get::<_,Uuid>(0));
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"runtime-test-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(b"runtime-test-secret")).unwrap()
    };
    let tokens: Vec<_> = users.iter().copied().map(jwt).collect();
    let path = format!("/api/runtimes/?workspace_id={ws}");
    assert_eq!(read(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &path, Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, &path, Some(&tokens[3])).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/runtimes/?workspace_id={foreign}"),
            Some(&tokens[2])
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for i in [0, 1] {
        let (status, body) = read(&app, &path, Some(&tokens[i])).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_array().unwrap().len(), 4);
        assert!(body.as_array().unwrap().iter().all(
            |row| row["workspace_id"] == ws.to_string() && row["id"] != runtimes[4].to_string()
        ));
    }
    let (status, body) = read(&app, &path, Some(&tokens[2])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        runtimes[1..4]
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(body[0]["launch_header"], "omp (json mode)");
    assert_eq!(body[1]["launch_header"], "");
    assert_eq!(body[2]["launch_header"], "codex app-server");
    assert_eq!(body[1]["metadata"], json!({"fixture":"Shared"}));
    assert_eq!(body[1]["shared_with"], json!([users[2].to_string()]));
    assert_eq!(body[1]["custom_name"], format!("Shared custom"));
    let (_, mine) = read(&app, &format!("{path}&owner=me"), Some(&tokens[2])).await;
    assert_eq!(mine.as_array().unwrap().len(), 1);
    assert_eq!(mine[0]["id"], runtimes[3].to_string());
    let (_, owner_mine) = read(&app, &format!("{path}&owner=me"), Some(&tokens[0])).await;
    assert_eq!(owner_mine.as_array().unwrap().len(), 3);
    let (_, ignored) = read(&app, &format!("{path}&owner=everyone"), Some(&tokens[2])).await;
    assert_eq!(ignored, body);
    assert_eq!(
        read(&app, &format!("{path}&owner=me"), Some(&tokens[1]))
            .await
            .1,
        json!([])
    );
    assert_eq!(
        read(
            &app,
            &format!("{path}&owner=me&owner=everyone"),
            Some(&tokens[2])
        )
        .await
        .1,
        mine
    );
    let agent:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'runtime-test-agent','local',$2,$3) RETURNING id", &[&ws,&users[2],&runtimes[3]]).await.unwrap().get(0);
    let task:Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id", &[&agent,&runtimes[3]]).await.unwrap().get(0);
    let task_token = format!("mat_runtime_{suffix}");
    let task_hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&task_hash,&task,&agent,&ws,&users[2]]).await.unwrap();
    // Task credentials retain the frozen read contract, but do not acquire
    // another user's role or escape their verified workspace binding.
    assert_eq!(read(&app, &path, Some(&task_token)).await.1, body);
    assert_eq!(
        read(
            &app,
            &format!("/api/runtimes/?workspace_id={foreign}"),
            Some(&task_token)
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&task_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&task_token)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE agent_runtime SET metadata='null'::jsonb,custom_name=NULL,daemon_id=NULL WHERE id=$1", &[&runtimes[0]]).await.unwrap();
    let (_, nulls) = read(&app, &path, Some(&tokens[0])).await;
    assert_eq!(nulls[0]["metadata"], json!({}));
    for field in ["daemon_id", "custom_name", "profile_id", "last_seen_at"] {
        assert!(nulls[0][field].is_null());
    }
    assert_eq!(nulls[0]["shared_with"], json!([]));
    let pat = format!("mul_runtime_{suffix}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    let pat_id:Uuid = db.query_one("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'runtime synthetic',$2,'mul_runtime') RETURNING id", &[&users[2],&hash]).await.unwrap().get(0);
    assert_eq!(read(&app, &path, Some(&pat)).await.0, StatusCode::OK);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE id=$1",
        &[&pat_id],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &users[2]],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&tokens[2])).await.0,
        StatusCode::NOT_FOUND
    );
    let before: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_runtime WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, 4);
    db.execute(
        "ALTER TABLE agent_runtime RENAME TO agent_runtime_broken",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&tokens[0])).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list runtimes"})
        )
    );
    db.execute(
        "ALTER TABLE agent_runtime_broken RENAME TO agent_runtime",
        &[],
    )
    .await
    .unwrap();
    let after: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_runtime WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
}
