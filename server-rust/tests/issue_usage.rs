//! Synthetic database coverage for the frozen-Go issue usage aggregate.
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
async fn issue_usage_preserves_coverage_and_workspace_boundary() {
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
                    &format!("issue-usage-{name}-{suffix}@invalid.example"),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Usage A',$1,'USA') RETURNING id", &[&format!("usage-a-{suffix}")]).await.unwrap().get(0);
    let other: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Usage B',$1,'USB') RETURNING id", &[&format!("usage-b-{suffix}")]).await.unwrap().get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&other]).await.unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Metered','member',$2,42) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let empty: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Empty','member',$2,43) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let foreign: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Private','member',$2,42) RETURNING id", &[&other,&users[0]]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Usage A',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let foreign_agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Usage B',$1,$2,'user','private','cloud') RETURNING id", &[&other,&users[0]]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&ws,&format!("Usage runtime {suffix}")]).await.unwrap().get(0);
    let foreign_runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&other,&format!("Foreign runtime {suffix}")]).await.unwrap().get(0);
    let mut tasks = Vec::new();
    for (status, started, completed) in [
        ("completed", true, true),
        ("failed", true, true),
        ("cancelled", true, true),
        ("queued", false, false),
        ("completed", false, true),
    ] {
        tasks.push(db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,started_at,completed_at) VALUES ($1,$2,$3,$4,CASE WHEN $5 THEN now() ELSE NULL END,CASE WHEN $6 THEN now() ELSE NULL END) RETURNING id", &[&agent,&runtime,&issue,&status,&started,&completed]).await.unwrap().get::<_, Uuid>(0));
    }
    let cross_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,started_at,completed_at) VALUES ($1,$2,$3,'completed',now(),now()) RETURNING id", &[&foreign_agent,&foreign_runtime,&foreign]).await.unwrap().get(0);
    for (model, (id, cost, input)) in [
        (tasks[0], Some(120_i64), 10_i64),
        (tasks[0], None, 3),
        (tasks[1], None, 7),
        (tasks[3], Some(0), 2),
        (cross_task, Some(9999), 9999),
    ]
    .into_iter()
    .enumerate()
    {
        let model = format!("synthetic-{model}");
        db.execute("INSERT INTO task_usage(task_id,provider,model,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,cost_usd_ticks) VALUES ($1,'synthetic',$4,$2,4,2,1,$3)", &[&id,&input,&cost,&model]).await.unwrap();
    }
    let pat = format!("mul_usage_{suffix}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'synthetic',$2,'mul_usage')", &[&users[0],&hash]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-usage-secret".to_vec()),
    );
    let auth = |id: Uuid| {
        encode(&Header::default(), &json!({"sub":id,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"synthetic-usage-secret")).unwrap()
    };
    let member = auth(users[1]);
    let outsider = auth(users[2]);
    let path = |id: &str, selected: Uuid| format!("/api/issues/{id}/usage?workspace_id={selected}");
    let target = path(&issue.to_string(), ws);
    assert_eq!(read(&app, &target, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &target, Some(&outsider)).await.0,
        StatusCode::NOT_FOUND
    );
    let usage = read(&app, &target, Some(&member)).await;
    assert_eq!(
        usage,
        (
            StatusCode::OK,
            json!({
                "total_input_tokens":22,"total_output_tokens":16,"total_cache_read_tokens":8,"total_cache_write_tokens":4,
                "cost_usd_ticks":120,"uncosted_input_tokens":10,"uncosted_output_tokens":8,
                "uncosted_cache_read_tokens":4,"uncosted_cache_write_tokens":2,"task_count":3,
                "terminal_task_count":3,"metered_task_count":2,"unreported_task_count":1
            })
        )
    );
    assert_eq!(read(&app, &path("usa-42", ws), Some(&member)).await, usage);
    assert_eq!(read(&app, &target, Some(&pat)).await, usage);
    assert_eq!(
        read(&app, &path(&empty.to_string(), ws), Some(&member))
            .await
            .1,
        json!({"total_input_tokens":0,"total_output_tokens":0,"total_cache_read_tokens":0,"total_cache_write_tokens":0,"cost_usd_ticks":0,"uncosted_input_tokens":0,"uncosted_output_tokens":0,"uncosted_cache_read_tokens":0,"uncosted_cache_write_tokens":0,"task_count":0,"terminal_task_count":0,"metered_task_count":0,"unreported_task_count":0})
    );
    assert_eq!(
        read(&app, &path(&foreign.to_string(), ws), Some(&member))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &path("USB-42", ws), Some(&member)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &path(&issue.to_string(), other), Some(&member))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &path("bogus", ws), Some(&member)).await.0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &target, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, &target, Some("invalid-token")).await.0,
        StatusCode::UNAUTHORIZED
    );
    let before: i64 = db
        .query_one(
            "SELECT count(*) FROM task_usage WHERE task_id=$1",
            &[&tasks[0]],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "ALTER TABLE task_usage RENAME TO task_usage_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &target, Some(&member)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to get issue usage"})
        )
    );
    db.execute(
        "ALTER TABLE task_usage_unavailable RENAME TO task_usage",
        &[],
    )
    .await
    .unwrap();
    let after: i64 = db
        .query_one(
            "SELECT count(*) FROM task_usage WHERE task_id=$1",
            &[&tasks[0]],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
}
