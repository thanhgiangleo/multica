//! Agent aggregates on an owned migrated PostgreSQL 16 database.
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
    let resp = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    (
        status,
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 on a random loopback port"]
async fn agent_aggregates_filter_private_system_foreign_and_old_tasks() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
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
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&role, &format!("activity-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Activity A',$1) RETURNING id",
            &[&format!("activity-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Activity B',$1) RETURNING id",
            &[&format!("activity-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role) in users.iter().zip(["owner", "member"]) {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, user, &role],
        )
        .await
        .unwrap();
    }
    let mut agents = Vec::new();
    for (name, workspace, owner, kind, mode) in [
        ("Private", ws, users[0], "user", "private"),
        ("Allowed", ws, users[0], "user", "public_to"),
        ("Mine", ws, users[1], "user", "private"),
        ("System", ws, users[0], "system", "public_to"),
        ("Foreign", foreign, users[0], "user", "public_to"),
    ] {
        agents.push(db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ($1,$2,$3,$4,$5,'cloud') RETURNING id", &[&name,&workspace,&owner,&kind,&mode]).await.unwrap().get::<_,Uuid>(0));
    }
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&ws,&format!("Activity runtime {suffix}")]).await.unwrap().get(0);
    let other_runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&foreign,&format!("Activity foreign {suffix}")]).await.unwrap().get(0);
    for (i, agent) in agents.iter().enumerate() {
        let rt = if i == 4 { other_runtime } else { runtime };
        db.execute("INSERT INTO agent_task_queue(agent_id,runtime_id,status,started_at,completed_at) VALUES ($1,$2,$3,now()-interval '2 hours',now()-interval '1 hour')", &[agent,&rt,&if i==2 {"failed"} else {"completed"}]).await.unwrap();
    }
    db.execute("INSERT INTO agent_task_queue(agent_id,runtime_id,status,created_at,completed_at) VALUES ($1,$2,'cancelled',now()-interval '40 days',now()-interval '40 days')", &[&agents[2],&runtime]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-activity-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(b"synthetic-activity-secret")).unwrap()
    };
    let counts = format!("/api/agent-run-counts?workspace_id={ws}");
    let activity = format!("/api/agent-activity-30d?workspace_id={ws}");
    for path in [&counts, &activity] {
        assert_eq!(read(&app, path, None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            read(&app, path, Some("invalid")).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            read(&app, path, Some(&jwt(users[2]))).await.0,
            StatusCode::NOT_FOUND
        );
        let (status, rows) = read(&app, path, Some(&jwt(users[1]))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["agent_id"], json!(agents[2]));
        let owner = read(&app, path, Some(&jwt(users[0]))).await.1;
        assert_eq!(owner.as_array().unwrap().len(), 3);
        assert!(!owner.to_string().contains(&agents[3].to_string()));
        assert!(!owner.to_string().contains(&agents[4].to_string()));
    }
    assert_eq!(
        read(&app, &counts, Some(&jwt(users[1]))).await.1[0]["run_count"],
        1
    );
    let bucket = read(&app, &activity, Some(&jwt(users[1]))).await.1;
    assert_eq!(bucket[0]["failed_count"], 1);
    assert_eq!(bucket[0]["task_count"], 1);
    assert_eq!(bucket[0]["duration_count"], 1);
    assert_eq!(bucket[0]["duration_ms"], 3_600_000.0);
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'member',$2)", &[&agents[1],&users[1]]).await.unwrap();
    assert_eq!(
        read(&app, &counts, Some(&jwt(users[1])))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        2
    );
    db.execute(
        "UPDATE agent SET archived_at=now() WHERE id=$1",
        &[&agents[1]],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &activity, Some(&jwt(users[1])))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        2
    );
    // A SQL failure in the aggregate has its own Go error; a failure in the
    // access lookup must fail closed instead of returning unfiltered totals.
    let pat = format!("mul_activity_{suffix}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'Synthetic',$2,'mul_activity')", &[&users[1],&hash]).await.unwrap();
    assert_eq!(
        read(&app, &counts, Some(&pat))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        2
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &counts, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "ALTER TABLE agent_task_queue RENAME TO agent_task_queue_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &counts, Some(&jwt(users[1]))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to get agent run counts"})
        )
    );
    assert_eq!(
        read(&app, &activity, Some(&jwt(users[1]))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to get agent activity"})
        )
    );
    db.execute(
        "ALTER TABLE agent_task_queue_unavailable RENAME TO agent_task_queue",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "ALTER TABLE agent_invocation_target RENAME TO agent_invocation_target_unavailable",
        &[],
    )
    .await
    .unwrap();
    for path in [&counts, &activity] {
        assert_eq!(
            read(&app, path, Some(&jwt(users[1]))).await,
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error":"failed to resolve agent access"})
            )
        );
    }
    db.execute(
        "ALTER TABLE agent_invocation_target_unavailable RENAME TO agent_invocation_target",
        &[],
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 on a random loopback port"]
async fn working_agents_enforce_visibility_filters_scope_and_errors() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
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
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&role, &format!("working-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Working A',$1) RETURNING id",
            &[&format!("working-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Working B',$1) RETURNING id",
            &[&format!("working-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member')",
        &[&ws, &users[0], &users[1]],
    )
    .await
    .unwrap();
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&ws,&format!("Working {suffix}")]).await.unwrap().get(0);
    let foreign_runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&foreign,&format!("Foreign {suffix}")]).await.unwrap().get(0);
    let mut agents = Vec::new();
    for (name, workspace, owner, kind, mode, archived) in [
        ("Private", ws, users[0], "user", "private", false),
        ("Allowed", ws, users[0], "user", "public_to", false),
        ("Mine", ws, users[1], "user", "private", false),
        ("System", ws, users[0], "system", "public_to", false),
        ("Foreign", foreign, users[0], "user", "public_to", false),
        ("Archived", ws, users[0], "user", "public_to", true),
    ] {
        agents.push(db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode,archived_at) VALUES ($1,$2,$3,$4,$5,'cloud',CASE WHEN $6 THEN now() ELSE NULL END) RETURNING id", &[&name,&workspace,&owner,&kind,&mode,&archived]).await.unwrap().get::<_,Uuid>(0));
    }
    let parent: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Parent','member',$2,101) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let child: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,parent_issue_id,assignee_type,assignee_id) VALUES ($1,'Child','member',$2,102,$3,'member',$4) RETURNING id", &[&ws,&users[1],&parent,&users[1]]).await.unwrap().get(0);
    let other: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Other','member',$2,103) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    for (i, agent) in agents.iter().enumerate() {
        let rt = if i == 4 { foreign_runtime } else { runtime };
        db.execute("INSERT INTO agent_task_queue(agent_id,runtime_id,status,issue_id) VALUES ($1,$2,'running',$3)", &[agent,&rt,&child]).await.unwrap();
    }
    db.execute("INSERT INTO agent_task_queue(agent_id,runtime_id,status,issue_id) VALUES ($1,$2,'running',$3)", &[&agents[2],&runtime,&other]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(agent_id,runtime_id,status,issue_id) VALUES ($1,$2,'queued',$3)", &[&agents[2],&runtime,&child]).await.unwrap();
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'member',$2)", &[&agents[1],&users[1]]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-working-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(b"synthetic-working-secret")).unwrap()
    };
    let path = format!("/api/working-agents?workspace_id={ws}");
    assert_eq!(read(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &path, Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, &path, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    let (status, rows) = read(&app, &path, Some(&jwt(users[1]))).await;
    assert_eq!(status, StatusCode::OK);
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|r| r["id"] == json!(agents[1])));
    let mine = rows.iter().find(|r| r["id"] == json!(agents[2])).unwrap();
    assert_eq!(mine["running_task_count"], 2);
    assert_eq!(mine["issue_ids"].as_array().unwrap().len(), 2);
    assert!(
        mine["issue_ids"]
            .as_array()
            .unwrap()
            .contains(&json!(child))
    );
    assert!(
        mine["issue_ids"]
            .as_array()
            .unwrap()
            .contains(&json!(other))
    );
    assert!(mine["avatar_url"].is_null());
    assert_eq!(
        read(&app, &path, Some(&jwt(users[0])))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        3
    );
    let scoped = format!("{path}&type=issue&scope=mine&relation=assigned");
    let scoped_rows = read(&app, &scoped, Some(&jwt(users[1]))).await.1;
    assert_eq!(scoped_rows.as_array().unwrap().len(), 2);
    assert_eq!(
        scoped_rows
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == json!(agents[2]))
            .unwrap()["running_task_count"],
        1
    );
    assert_eq!(
        read(
            &app,
            &format!("{path}&type=issue&scope=mine&relation=created"),
            Some(&jwt(users[0]))
        )
        .await
        .1[0]["issue_ids"],
        json!([other])
    );
    let narrowed = read(
        &app,
        &format!("{path}&type=issue&parent={parent}"),
        Some(&jwt(users[1])),
    )
    .await
    .1;
    assert_eq!(
        narrowed
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == json!(agents[2]))
            .unwrap()["issue_ids"],
        json!([child])
    );
    for (suffix, msg) in [
        (
            "type=invalid",
            "invalid type: must be issue, autopilot, or chat",
        ),
        ("relation=any", "relation requires scope=mine"),
        ("type=chat&scope=mine", "scope=mine requires type=issue"),
        (
            "type=issue&scope=mine&relation=wrong",
            "invalid relation: must be assigned, created, involved, or any",
        ),
        ("scope=invalid", "invalid scope: must be mine"),
        ("parent=not-uuid", "parent requires type=issue"),
        ("type=issue&parent=not-uuid", "invalid parent"),
        (
            "type=issue&scope=mine&parent=not-uuid",
            "parent cannot be combined with scope",
        ),
    ] {
        assert_eq!(
            read(&app, &format!("{path}&{suffix}"), Some(&jwt(users[0]))).await,
            (StatusCode::BAD_REQUEST, json!({"error":msg}))
        );
    }
    let pat = format!("mul_working_{suffix}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'Synthetic',$2,'mul_working')", &[&users[1],&hash]).await.unwrap();
    assert_eq!(read(&app, &path, Some(&pat)).await.0, StatusCode::OK);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "ALTER TABLE agent_task_queue RENAME TO agent_task_queue_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[1]))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list workspace working agents"})
        )
    );
    db.execute(
        "ALTER TABLE agent_task_queue_unavailable RENAME TO agent_task_queue",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "ALTER TABLE agent_invocation_target RENAME TO agent_invocation_target_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[1]))).await,
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
}
