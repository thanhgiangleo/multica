//! Synthetic PostgreSQL coverage for frozen Go workspace daily dashboard reads.
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
    let mut req = Request::builder().uri(path);
    if let Some(auth) = auth {
        req = req.header("authorization", format!("Bearer {auth}"));
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
#[ignore = "requires run-owned PostgreSQL 16 on a random loopback port"]
async fn daily_aggregates_preserve_workspace_project_and_local_day_boundary() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(config.get_hosts().iter().all(
        |host| matches!(host, tokio_postgres::config::Host::Tcp(name) if name == "127.0.0.1")
    ));
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
    for name in ["owner", "member", "outsider"] {
        users.push(db.query_one("INSERT INTO \"user\"(name,email,timezone) VALUES ($1,$2,'Asia/Ho_Chi_Minh') RETURNING id", &[&name, &format!("daily-{name}-{suffix}@invalid.example")]).await.unwrap().get::<_, Uuid>(0));
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Daily',$1) RETURNING id",
            &[&format!("daily-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Other',$1) RETURNING id",
            &[&format!("other-daily-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&foreign]).await.unwrap();
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(title,workspace_id) VALUES ('Daily', $1) RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Daily Agent',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let other_agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Other Agent',$1,$2,'user','private','cloud') RETURNING id", &[&foreign,&users[0]]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&ws,&format!("Daily runtime {suffix}")]).await.unwrap().get(0);
    let other_runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&foreign,&format!("Other runtime {suffix}")]).await.unwrap().get(0);
    let day: chrono::NaiveDate = db
        .query_one("SELECT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date", &[])
        .await
        .unwrap()
        .get(0);
    let yesterday = day.pred_opt().unwrap();
    // Yesterday at 23:00 local must be included by Go's days=1 N+1 window.
    let late: chrono::DateTime<chrono::Utc> = db
        .query_one(
            "SELECT ((($1::date - 1)::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')) + interval '23 hours'",
            &[&day],
        )
        .await
        .unwrap()
        .get(0);
    let early: chrono::DateTime<chrono::Utc> = db
        .query_one(
            "SELECT ($1::date::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh') + interval '1 hour'",
            &[&day],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO task_usage_hourly(bucket_hour,workspace_id,runtime_id,agent_id,project_id,provider,model,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,cost_usd_ticks,uncosted_input_tokens,task_count) VALUES ($1,$2,$3,$4,$5,'Synthetic','m',10,2,3,1,90,0,1), ($6,$2,$3,$4,NULL,'synthetic','m',5,1,2,0,0,NULL,2), ($1,$7,$8,$9,NULL,'synthetic','m',999,0,0,0,0,NULL,1)", &[&late,&ws,&runtime,&agent,&project,&early,&foreign,&other_runtime,&other_agent]).await.unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,project_id,number) VALUES ($1,'Daily','member',$2,$3,1) RETURNING id", &[&ws,&users[0],&project]).await.unwrap().get(0);
    for (agent_id, runtime_id, issue_id, status, completed, reason) in [
        (agent, runtime, Some(issue), "failed", late, None),
        (agent, runtime, None, "completed", early, None),
        (
            other_agent,
            other_runtime,
            None,
            "failed",
            early,
            Some("synthetic"),
        ),
    ] {
        db.execute("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,completed_at,failure_reason) VALUES ($1,$2,$3,$4,$5,$6)", &[&agent_id,&runtime_id,&issue_id,&status,&completed,&reason]).await.unwrap();
    }
    let pat = format!("mul_daily_{suffix}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'synthetic',$2,'mul_daily')", &[&users[1],&hash]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"daily-test-secret".to_vec()),
    );
    let auth = |id: Uuid| {
        encode(&Header::default(), &json!({"sub":id,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"daily-test-secret")).unwrap()
    };
    let owner = auth(users[0]);
    let member = auth(users[1]);
    let outsider = auth(users[2]);
    let usage = format!("/api/dashboard/usage/daily?workspace_id={ws}&days=1");
    let failure = format!("/api/dashboard/failures/daily?workspace_id={ws}&days=1");
    assert_eq!(read(&app, &usage, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &usage, Some(&outsider)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &usage, Some(&member)).await,
        (
            StatusCode::OK,
            json!([
                {"date":day.to_string(),"provider":"synthetic","model":"m","input_tokens":5,"output_tokens":1,"cache_read_tokens":2,"cache_write_tokens":0,"cost_usd_ticks":0,"uncosted_input_tokens":5,"uncosted_output_tokens":1,"uncosted_cache_read_tokens":2,"uncosted_cache_write_tokens":0,"task_count":2},
                {"date":yesterday.to_string(),"provider":"synthetic","model":"m","input_tokens":10,"output_tokens":2,"cache_read_tokens":3,"cache_write_tokens":1,"cost_usd_ticks":90,"uncosted_input_tokens":0,"uncosted_output_tokens":2,"uncosted_cache_read_tokens":3,"uncosted_cache_write_tokens":1,"task_count":1}
            ])
        )
    );
    assert_eq!(
        read(&app, &failure, Some(&member)).await,
        (
            StatusCode::OK,
            json!([
                {"date":day.to_string(),"failure_reason":"","task_count":1},
                {"date":yesterday.to_string(),"failure_reason":"unclassified","task_count":1}
            ])
        )
    );
    assert_eq!(
        read(&app, &format!("{usage}&project_id={project}"), Some(&pat))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        read(
            &app,
            &format!("{failure}&project_id={project}"),
            Some(&member)
        )
        .await
        .1
        .as_array()
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        read(
            &app,
            &format!("{usage}&project_id={}", Uuid::new_v4()),
            Some(&member)
        )
        .await
        .1,
        json!([])
    );
    assert_eq!(
        read(&app, &format!("{failure}&project_id=bad"), Some(&member)).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid project_id"})
        )
    );
    let other_usage = format!("/api/dashboard/usage/daily?workspace_id={foreign}&days=1");
    assert_eq!(
        read(&app, &other_usage, Some(&member)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &other_usage, Some(&owner))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        1
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &usage, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, &usage, Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "ALTER TABLE task_usage_hourly RENAME TO task_usage_hourly_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &usage, Some(&member)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list usage"})
        )
    );
    db.execute(
        "ALTER TABLE task_usage_hourly_unavailable RENAME TO task_usage_hourly",
        &[],
    )
    .await
    .unwrap();
}
