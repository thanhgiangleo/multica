//! Frozen-Go runtime usage and private machine access against disposable PostgreSQL.
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
use std::{
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"synthetic-runtime-usage";
fn jwt(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user,"exp":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
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
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 migrated on random loopback port"]
async fn runtime_reports_enforce_owner_consent_and_frozen_aggregates() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let ids: [Uuid; 8] = std::array::from_fn(|_| Uuid::new_v4());
    let [
        owner,
        admin,
        shared,
        outsider,
        ws,
        foreign,
        private,
        foreign_runtime,
    ] = ids;
    for id in [owner, admin, shared, outsider] {
        db.execute("INSERT INTO \"user\"(id,name,email,timezone) VALUES ($1,'Runtime user',$2,'Asia/Ho_Chi_Minh')", &[&id,&format!("{id}@runtime.invalid")]).await.unwrap();
    }
    for id in [ws, foreign] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES ($1,'Runtime usage',$2)",
            &[&id, &format!("usage-{id}")],
        )
        .await
        .unwrap();
    }
    for (user, role) in [(owner, "owner"), (admin, "admin"), (shared, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
        &[&foreign, &shared],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_runtime(id,workspace_id,name,runtime_mode,provider,status,owner_id,visibility,shared_with) VALUES ($1,$2,'Private runtime','local','synthetic','online',$3,'private',ARRAY[$4]::uuid[]),($5,$6,'Other runtime','local','synthetic','online',$3,'public',ARRAY[]::uuid[])", &[&private,&ws,&owner,&shared,&foreign_runtime,&foreign]).await.unwrap();
    let agent:Uuid=db.query_one("INSERT INTO agent(name,workspace_id,owner_id,runtime_id,kind,permission_mode,runtime_mode) VALUES ('Runtime agent',$1,$2,$3,'user','private','local') RETURNING id", &[&ws,&owner,&private]).await.unwrap().get(0);
    let other:Uuid=db.query_one("INSERT INTO agent(name,workspace_id,owner_id,runtime_id,kind,permission_mode,runtime_mode) VALUES ('Foreign agent',$1,$2,$3,'user','private','local') RETURNING id", &[&foreign,&owner,&foreign_runtime]).await.unwrap().get(0);
    let today: chrono::NaiveDate = db
        .query_one("SELECT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date", &[])
        .await
        .unwrap()
        .get(0);
    let local:chrono::DateTime<chrono::Utc>=db.query_one("SELECT ((now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh') + interval '1 hour'",&[]).await.unwrap().get(0);
    db.execute("INSERT INTO task_usage_hourly(bucket_hour,workspace_id,runtime_id,agent_id,provider,model,input_tokens,output_tokens,cost_usd_ticks,uncosted_input_tokens) VALUES ($1,$2,$3,$4,'Synthetic','model-a',10,2,100,0),($1,$2,$3,$4,'synthetic','model-a',5,1,0,NULL),($1,$5,$6,$7,'synthetic','model-a',500,1,0,NULL)", &[&local,&ws,&private,&agent,&foreign,&foreign_runtime,&other]).await.unwrap();
    for (runtime, agent_id) in [(private, agent), (foreign_runtime, other)] {
        let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,started_at) VALUES ($1,$2,'running',$3) RETURNING id", &[&agent_id,&runtime,&local]).await.unwrap().get(0);
        db.execute("INSERT INTO task_usage(task_id,provider,model,input_tokens,output_tokens,cost_usd_ticks,created_at) VALUES ($1,'Synthetic','model-a',7,3,60,$2),($1,'synthetic','model-b',2,1,NULL,$2)", &[&task,&local]).await.unwrap();
    }
    let pat = format!("mul_runtime_{private}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'runtime test',$2,'mul_runt')", &[&shared,&hash]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let daily = format!("/api/runtimes/{private}/usage?tz=Asia%2FHo_Chi_Minh&days=1");
    let agent_url = format!("/api/runtimes/{private}/usage/by-agent?days=1");
    let hour = format!("/api/runtimes/{private}/usage/by-hour?days=1");
    let activity = format!("/api/runtimes/{private}/activity?tz=Asia%2FHo_Chi_Minh");
    let before: i64 = db
        .query_one("SELECT count(*) FROM task_usage", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        get(&app, &daily, Some(&jwt(owner))).await,
        (
            StatusCode::OK,
            json!([{"runtime_id":private,"date":today.to_string(),"provider":"synthetic","model":"model-a","input_tokens":15,"output_tokens":3,"cache_read_tokens":0,"cache_write_tokens":0,"cost_usd_ticks":100,"uncosted_input_tokens":5,"uncosted_output_tokens":3,"uncosted_cache_read_tokens":0,"uncosted_cache_write_tokens":0}])
        )
    );
    assert_eq!(
        get(&app, &agent_url, Some(&pat)).await,
        (
            StatusCode::OK,
            json!([{"agent_id":agent,"provider":"synthetic","model":"model-a","input_tokens":7,"output_tokens":3,"cache_read_tokens":0,"cache_write_tokens":0,"cost_usd_ticks":60,"uncosted_input_tokens":0,"uncosted_output_tokens":0,"uncosted_cache_read_tokens":0,"uncosted_cache_write_tokens":0,"task_count":1},{"agent_id":agent,"provider":"synthetic","model":"model-b","input_tokens":2,"output_tokens":1,"cache_read_tokens":0,"cache_write_tokens":0,"cost_usd_ticks":0,"uncosted_input_tokens":2,"uncosted_output_tokens":1,"uncosted_cache_read_tokens":0,"uncosted_cache_write_tokens":0,"task_count":1}])
        )
    );
    let (hour_status, hours) = get(&app, &hour, Some(&jwt(shared))).await;
    assert_eq!(hour_status, StatusCode::OK);
    assert_eq!(hours.as_array().unwrap().len(), 2);
    assert!(
        hours
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["hour"] == 1 && row["task_count"] == 1)
    );
    assert_eq!(
        get(&app, &activity, Some(&jwt(shared))).await,
        (StatusCode::OK, json!([{"hour":1,"count":1}]))
    );
    for url in [&daily, &agent_url, &hour, &activity] {
        assert_eq!(get(&app, url, None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            get(&app, url, Some(&jwt(admin))).await,
            (StatusCode::NOT_FOUND, json!({"error":"runtime not found"}))
        );
        assert_eq!(
            get(&app, url, Some(&jwt(outsider))).await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(get(&app, url, Some(&jwt(owner))).await.0, StatusCode::OK);
        assert_eq!(
            get(
                &app,
                &url.replace(&private.to_string(), &foreign_runtime.to_string()),
                Some(&jwt(owner))
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        get(&app, "/api/runtimes/invalid/usage", Some(&jwt(owner))).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid runtime_id"})
        )
    );
    assert_eq!(
        get(
            &app,
            &format!("/api/runtimes/{private}/usage?tz=Not%2FAZone&days=bogus"),
            Some(&jwt(shared))
        )
        .await
        .1,
        get(
            &app,
            &format!("/api/runtimes/{private}/usage?tz=Asia%2FHo_Chi_Minh&days=90"),
            Some(&jwt(shared))
        )
        .await
        .1,
        "invalid timezone and days must fall back to user timezone/default days"
    );
    db.execute(
        "UPDATE agent_runtime SET visibility='public', shared_with=ARRAY[]::uuid[] WHERE id=$1",
        &[&private],
    )
    .await
    .unwrap();
    assert_eq!(get(&app, &daily, Some(&jwt(admin))).await.0, StatusCode::OK);
    db.execute(
        "UPDATE agent_runtime SET owner_id=NULL WHERE id=$1",
        &[&private],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &daily, Some(&jwt(owner))).await.0,
        StatusCode::NOT_FOUND
    );
    db.execute("UPDATE agent_runtime SET owner_id=$2,visibility='private',shared_with=ARRAY[$3]::uuid[] WHERE id=$1", &[&private,&owner,&shared]).await.unwrap();
    let task: Uuid = db
        .query_one(
            "SELECT id FROM agent_task_queue WHERE runtime_id=$1 LIMIT 1",
            &[&private],
        )
        .await
        .unwrap()
        .get(0);
    let mat = format!("mat_runtime_{private}");
    let mat_hash = hex::encode(Sha256::digest(mat.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&mat_hash,&task,&agent,&ws,&shared]).await.unwrap();
    assert_eq!(get(&app, &daily, Some(&mat)).await.0, StatusCode::OK);
    assert_eq!(
        get(&app, &format!("{daily}&workspace_id={foreign}"), Some(&mat))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &agent_url, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &shared],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &daily, Some(&jwt(shared))).await.0,
        StatusCode::NOT_FOUND
    );
    let after: i64 = db
        .query_one("SELECT count(*) FROM task_usage", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after, "read routes mutated source usage");
    db.execute(
        "ALTER TABLE task_usage_hourly RENAME TO runtime_usage_fault",
        &[],
    )
    .await
    .unwrap();
    let failure = get(&app, &daily, Some(&jwt(owner))).await;
    db.execute(
        "ALTER TABLE runtime_usage_fault RENAME TO task_usage_hourly",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        failure,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list usage"})
        )
    );
}
