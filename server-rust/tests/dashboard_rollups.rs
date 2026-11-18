//! Synthetic DB coverage for all six frozen dashboard aggregates and actor visibility.
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
    if let Some(token) = auth {
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
#[ignore = "requires run-owned PostgreSQL 16 on random loopback port"]
async fn dashboards_fold_live_hidden_agents_without_losing_totals() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(name) if name == "127.0.0.1"))
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
    for role in ["owner", "member", "outsider"] {
        users.push(db.query_one("INSERT INTO \"user\"(name,email,timezone) VALUES ($1,$2,'Asia/Ho_Chi_Minh') RETURNING id", &[&role,&format!("rollup-{role}-{suffix}@invalid.example")]).await.unwrap().get::<_,Uuid>(0));
    }
    let mut workspaces = Vec::new();
    for name in ["Rollup", "Foreign"] {
        workspaces.push(
            db.query_one(
                "INSERT INTO workspace(name,slug) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("{name}-{suffix}")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let (ws, foreign) = (workspaces[0], workspaces[1]);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&foreign]).await.unwrap();
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(title,workspace_id) VALUES ('Rollup',$1) RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let mut agents = Vec::new();
    for (name, workspace, owner, kind, mode) in [
        ("Private", ws, users[0], "user", "private"),
        ("Allowed", ws, users[0], "user", "public_to"),
        ("Own", ws, users[1], "user", "private"),
        ("System", ws, users[0], "system", "public_to"),
        ("Foreign", foreign, users[0], "user", "public_to"),
    ] {
        agents.push(db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ($1,$2,$3,$4,$5,'cloud') RETURNING id", &[&name,&workspace,&owner,&kind,&mode]).await.unwrap().get::<_,Uuid>(0));
    }
    let runtime:Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&ws,&format!("Rollup {suffix}")]).await.unwrap().get(0);
    let other_runtime:Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&foreign,&format!("Foreign {suffix}")]).await.unwrap().get(0);
    let today: chrono::NaiveDate = db
        .query_one("SELECT (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date", &[])
        .await
        .unwrap()
        .get(0);
    let yesterday: chrono::DateTime<chrono::Utc> = db.query_one("SELECT (($1::date - 1)::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh') + interval '23 hours'", &[&today]).await.unwrap().get(0);
    let recent: chrono::DateTime<chrono::Utc> = db
        .query_one(
            "SELECT ($1::date::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh') + interval '1 hour'",
            &[&today],
        )
        .await
        .unwrap()
        .get(0);
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,project_id,number) VALUES ($1,'Rollup','member',$2,$3,1) RETURNING id", &[&ws,&users[0],&project]).await.unwrap().get(0);
    // Two hidden agents share provider/model and failure reason: they must merge.
    let mut tasks = Vec::new();
    for (idx, agent, rt, when, started, status, reason, project_issue) in [
        (
            0,
            agents[0],
            runtime,
            recent,
            true,
            "failed",
            Some("provider_error"),
            Some(issue),
        ),
        (
            1,
            agents[3],
            runtime,
            recent,
            true,
            "failed",
            Some("provider_error"),
            Some(issue),
        ),
        (2, agents[2], runtime, recent, true, "completed", None, None),
        (3, agents[1], runtime, recent, true, "completed", None, None),
        (
            4,
            agents[0],
            runtime,
            yesterday,
            true,
            "failed",
            None,
            Some(issue),
        ),
        (
            5,
            agents[4],
            other_runtime,
            recent,
            true,
            "failed",
            Some("foreign"),
            None,
        ),
        (
            6,
            agents[2],
            runtime,
            recent,
            false,
            "failed",
            Some("queued_expired"),
            None,
        ),
        (7, agents[2], runtime, recent, true, "cancelled", None, None),
    ] {
        let start = started.then_some(when - chrono::Duration::seconds(60 + idx));
        tasks.push(db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,started_at,completed_at,failure_reason) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id", &[&agent,&rt,&project_issue,&status,&start,&when,&reason]).await.unwrap().get::<_,Uuid>(0));
    }
    for (idx, agent, workspace, rt, when, project_id) in [
        (0, agents[0], ws, runtime, recent, Some(project)),
        (1, agents[3], ws, runtime, recent, Some(project)),
        (2, agents[2], ws, runtime, recent, None),
        (3, agents[1], ws, runtime, recent, None),
        (4, agents[0], ws, runtime, yesterday, Some(project)),
        (5, agents[4], foreign, other_runtime, recent, None),
    ] {
        db.execute("INSERT INTO task_usage_hourly(bucket_hour,workspace_id,runtime_id,agent_id,project_id,provider,model,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,cost_usd_ticks,task_count) VALUES ($1,$2,$3,$4,$5,'Synthetic','model',10,1,0,0,0,1)", &[&when,&workspace,&rt,&agent,&project_id]).await.unwrap();
        if idx == 0 {
            // Zero provider usage still counts as metered task.
            db.execute("INSERT INTO task_usage(task_id,provider,model,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,cost_usd_ticks) VALUES ($1,'synthetic','model',0,0,0,0,0)", &[&tasks[idx]]).await.unwrap();
        }
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"rollup-test-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(b"rollup-test-secret")).unwrap()
    };
    let member = jwt(users[1]);
    let owner = jwt(users[0]);
    let outsider = jwt(users[2]);
    let path = |suffix: &str| format!("/api/dashboard/{suffix}?workspace_id={ws}&days=1");
    let usage = path("usage/by-agent");
    let agent_runtime = path("agent-runtime");
    let runtime_daily = path("runtime/daily");
    let failures = path("failures/by-agent");
    for route in [&usage, &agent_runtime, &runtime_daily, &failures] {
        assert_eq!(read(&app, route, None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            read(&app, route, Some(&outsider)).await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            read(&app, &format!("{route}&project_id=invalid"), Some(&member)).await,
            (
                StatusCode::BAD_REQUEST,
                json!({"error":"invalid project_id"})
            )
        );
        assert_eq!(
            read(
                &app,
                &format!("{route}&project_id={}", Uuid::new_v4()),
                Some(&member)
            )
            .await,
            (StatusCode::OK, json!([]))
        );
        assert_eq!(
            read(
                &app,
                &route.replace(&ws.to_string(), &foreign.to_string()),
                Some(&member)
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }
    let usage_rows = read(&app, &usage, Some(&member)).await.1;
    assert_eq!(usage_rows.as_array().unwrap().len(), 2);
    assert_eq!(
        usage_rows
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["input_tokens"].as_i64().unwrap())
            .sum::<i64>(),
        40
    );
    assert_eq!(
        usage_rows
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["agent_id"] == "__restricted_agents__")
            .count(),
        1
    );
    assert_eq!(
        usage_rows
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["agent_id"] == "__restricted_agents__")
            .unwrap()["input_tokens"],
        30
    );
    assert!(!usage_rows.to_string().contains(&agents[0].to_string()));
    assert!(!usage_rows.to_string().contains(&agents[3].to_string()));
    assert!(!usage_rows.to_string().contains(&agents[4].to_string()));
    let runtime_rows = read(&app, &agent_runtime, Some(&member)).await.1;
    assert_eq!(runtime_rows.as_array().unwrap().len(), 2); // allowed agent and restricted bucket; queued/previous day excluded
    assert_eq!(
        runtime_rows
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["agent_id"] == "__restricted_agents__")
            .unwrap()["metered_task_count"],
        1
    );
    assert_eq!(
        runtime_rows
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["task_count"].as_i64().unwrap())
            .sum::<i64>(),
        5
    );
    let daily = read(&app, &runtime_daily, Some(&member)).await.1;
    assert_eq!(daily.as_array().unwrap().len(), 2); // N+1 day bucketing unlike exact agent cutoffs
    assert_eq!(
        daily[0],
        json!({"date":today.to_string(),"total_seconds":60+61+62+63+67,"task_count":5,"failed_count":2,"cancelled_count":1})
    );
    assert_eq!(daily[1]["failed_count"], 1);
    let failure_rows = read(&app, &failures, Some(&member)).await.1;
    assert_eq!(
        failure_rows
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["task_count"].as_i64().unwrap())
            .sum::<i64>(),
        5
    ); // queued failure included, cancellation excluded
    assert_eq!(
        failure_rows
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["agent_id"] == "__restricted_agents__"
                && r["failure_reason"] == "provider_error")
            .unwrap()["task_count"],
        2
    );
    assert!(!failure_rows.to_string().contains(&agents[3].to_string()));
    let project_usage = read(
        &app,
        &format!("{usage}&project_id={project}"),
        Some(&member),
    )
    .await
    .1;
    assert_eq!(project_usage.as_array().unwrap().len(), 1);
    assert_eq!(project_usage[0]["input_tokens"], 20);
    assert_eq!(
        read(
            &app,
            &format!("{agent_runtime}&project_id={project}"),
            Some(&member)
        )
        .await
        .1[0]["task_count"],
        2
    );
    assert_eq!(
        read(
            &app,
            &format!("{runtime_daily}&project_id={project}"),
            Some(&member)
        )
        .await
        .1[0]["task_count"],
        2
    );
    let owned = read(&app, &usage, Some(&owner)).await.1;
    assert_eq!(owned.as_array().unwrap().len(), 4); // owner sees every user-kind agent, never system
    assert_eq!(
        owned
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["agent_id"] == "__restricted_agents__")
            .unwrap()["input_tokens"],
        10
    );
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'member',$2)", &[&agents[1],&users[1]]).await.unwrap();
    assert_eq!(
        read(&app, &usage, Some(&member))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        3
    );
    db.execute(
        "UPDATE agent SET archived_at=now() WHERE id=$1",
        &[&agents[1]],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &usage, Some(&member))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        3
    );
    let pat = format!("mul_rollup_{suffix}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'Synthetic',$2,'mul_rollup')", &[&users[1],&hash]).await.unwrap();
    assert_eq!(read(&app, &usage, Some(&pat)).await.0, StatusCode::OK);
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
    // Agent actor sees all user agents, but cannot select a second workspace
    // even when its authenticating user belongs to both.
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
        &[&foreign, &users[1]],
    )
    .await
    .unwrap();
    let active: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id", &[&agents[2],&runtime]).await.unwrap().get(0);
    let task_token = format!("mat_rollup_{suffix}");
    let task_hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&task_hash,&active,&agents[2],&ws,&users[1]]).await.unwrap();
    assert_eq!(
        read(&app, &usage, Some(&task_token)).await.1,
        read(&app, &usage, Some(&owner)).await.1
    );
    for route in [&usage, &agent_runtime, &runtime_daily, &failures] {
        assert_eq!(
            read(
                &app,
                &route.replace(&ws.to_string(), &foreign.to_string()),
                Some(&task_token)
            )
            .await,
            (
                StatusCode::FORBIDDEN,
                json!({"error":"task token is bound to a different workspace"})
            )
        );
    }
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&task_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &usage, Some(&task_token)).await.0,
        StatusCode::UNAUTHORIZED
    );
    // Hard-deleted agents remain distinct from restricted live agents.
    let deleted = Uuid::new_v4();
    db.execute("INSERT INTO task_usage_hourly(bucket_hour,workspace_id,runtime_id,agent_id,provider,model,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,cost_usd_ticks,task_count) VALUES ($1,$2,$3,$4,'synthetic','model',7,0,0,0,0,1)", &[&recent,&ws,&runtime,&deleted]).await.unwrap();
    let deleted_usage = read(&app, &usage, Some(&member)).await.1;
    assert_eq!(
        deleted_usage
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["agent_id"] == deleted.to_string())
            .unwrap()["input_tokens"],
        7
    );
    // Permission-lookup failure must never degrade to unfiltered per-agent rows.
    db.execute(
        "ALTER TABLE agent_invocation_target RENAME TO agent_invocation_target_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &usage, Some(&member)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to resolve agent access"})
        )
    );
    db.execute(
        "ALTER TABLE agent_invocation_target_unavailable RENAME TO agent_invocation_target",
        &[],
    )
    .await
    .unwrap();
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
            json!({"error":"failed to list usage by agent"})
        )
    );
    db.execute(
        "ALTER TABLE task_usage_hourly_unavailable RENAME TO task_usage_hourly",
        &[],
    )
    .await
    .unwrap();
    // Guard cannot leak other workspace even when same owner has both memberships.
    assert_eq!(
        read(
            &app,
            &usage.replace(&ws.to_string(), &foreign.to_string()),
            Some(&owner)
        )
        .await
        .0,
        StatusCode::OK
    );
}
