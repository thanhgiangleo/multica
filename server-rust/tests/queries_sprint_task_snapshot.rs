//! Task snapshot on owned PostgreSQL: visibility, latest outcomes and unconditional chat redaction.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{ApiState, queries_sprint_timeline::TimelineAvatarUrlPolicy},
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"queries-task-snapshot-synthetic";
struct SyntheticAvatars;
impl TimelineAvatarUrlPolicy for SyntheticAvatars {
    fn resolve(&self, stored: &str) -> Option<String> {
        if stored == "private-avatar-key" {
            Some("https://safe.invalid/avatars/signed".into())
        } else {
            Some(stored.to_string())
        }
    }
}
fn jwt(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
}
async fn read(app: &axum::Router, ws: Uuid, token: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::builder().uri(format!("/api/agent-task-snapshot?workspace_id={ws}"));
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    (
        response.status(),
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn snapshot_visibility_outcomes_attribution_and_chat_privacy() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let mut users = Vec::new();
    for name in ["owner", "member", "outsider"] {
        users.push(db.query_one("INSERT INTO \"user\"(name,email,avatar_url) VALUES ($1,$2,'https://invalid.example/avatar') RETURNING id", &[&name,&format!("snapshot-{name}-{suffix}@invalid.example")]).await.unwrap().get::<_,Uuid>(0));
    }
    let shadow: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Channel identity',$1) RETURNING id",
            &[&format!("snapshot-{suffix}@channel.invalid")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Snapshot A',$1) RETURNING id",
            &[&format!("snapshot-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Snapshot B',$1) RETURNING id",
            &[&format!("snapshot-b-{suffix}")],
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
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Snapshot synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let other_runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Snapshot foreign','cloud','synthetic','online') RETURNING id", &[&foreign]).await.unwrap().get(0);
    let mut agents = Vec::new();
    for (name, workspace, owner, kind, mode) in [
        ("Hidden", ws, users[0], "user", "private"),
        ("Allowed", ws, users[0], "user", "public_to"),
        ("Mine", ws, users[1], "user", "private"),
        ("System", ws, users[0], "system", "public_to"),
        ("Foreign", foreign, users[0], "user", "public_to"),
    ] {
        agents.push(db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ($1,$2,$3,$4,$5,'cloud') RETURNING id", &[&name,&workspace,&owner,&kind,&mode]).await.unwrap().get::<_,Uuid>(0));
    }
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'member',$2)", &[&agents[1],&users[1]]).await.unwrap();
    let chat: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title) VALUES ($1,$2,$3,'Private chat') RETURNING id", &[&ws,&agents[1],&users[0]]).await.unwrap().get(0);
    let chat_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status,result,error,trigger_summary,handoff_note,work_dir,durable_work_dir,branch_name,context,originator_user_id,accountable_user_id,originator_source) VALUES ($1,$2,$3,'running','{\"reply\":\"CHATSECRET\"}','CHATSECRET','CHATSECRET','CHATSECRET','/home/CHATSECRET','/home/CHATSECRET','CHATSECRET','{\"wakeup_joined\":[{\"note\":\"CHATSECRET\"}]}',$4,$4,'direct_human') RETURNING id", &[&agents[1],&runtime,&chat,&users[0]]).await.unwrap().get(0);
    let mut active = Vec::new();
    for (index, status) in ["queued", "dispatched", "running", "waiting_local_directory"]
        .iter()
        .enumerate()
    {
        let principal = if index == 0 { users[1] } else { shadow };
        active.push(db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,originator_user_id,accountable_user_id,originator_source) VALUES ($1,$2,$3,$4,$4,'direct_human') RETURNING id", &[&agents[2],&runtime,&status,&principal]).await.unwrap().get::<_,Uuid>(0));
    }
    let wakeup_context = json!({"wakeup_id":Uuid::new_v4()});
    let deferred: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,context) VALUES ($1,$2,'deferred',$3::jsonb) RETURNING id", &[&agents[2],&runtime,&wakeup_context]).await.unwrap().get(0);
    let excluded_deferred: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'deferred') RETURNING id", &[&agents[2],&runtime]).await.unwrap().get(0);
    let latest_failed: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,created_at,completed_at,result) VALUES ($1,$2,'failed',now()-interval '2 hours',now()-interval '1 hour','{\"reason\":\"ordinary issue outcome\"}') RETURNING id", &[&agents[2],&runtime]).await.unwrap().get(0);
    let mut excluded = vec![excluded_deferred];
    for (status, completed) in [
        ("completed", Some("2001-01-01T00:00:00Z")),
        ("completed", None),
        ("cancelled", Some("2099-01-01T00:00:00Z")),
    ] {
        excluded.push(db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,completed_at) VALUES ($1,$2,$3,$4::text::timestamptz) RETURNING id", &[&agents[2],&runtime,&status,&completed]).await.unwrap().get::<_,Uuid>(0));
    }
    for index in [0usize, 3, 4] {
        let rt = if index == 4 { other_runtime } else { runtime };
        excluded.push(db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,trigger_summary) VALUES ($1,$2,'queued','HIDDENSECRET') RETURNING id", &[&agents[index],&rt]).await.unwrap().get::<_,Uuid>(0));
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "queries-snapshot".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec())
            .with_timeline_avatar_url_policy(Arc::new(SyntheticAvatars)),
    );
    assert_eq!(read(&app, ws, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, ws, Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, ws, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    let (status, response) = read(&app, ws, Some(&jwt(users[1]))).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let rows = response.as_array().unwrap();
    assert_eq!(rows.len(), 7, "{response}");
    for id in active.iter().chain([&chat_task, &deferred, &latest_failed]) {
        assert!(
            rows.iter().any(|row| row["id"] == id.to_string()),
            "missing {id}: {response}"
        );
    }
    for id in &excluded {
        assert!(
            !rows.iter().any(|row| row["id"] == id.to_string()),
            "unexpected {id}: {response}"
        );
    }
    assert!(!response.to_string().contains("HIDDENSECRET"));
    assert!(!response.to_string().contains("CHATSECRET"));
    assert!(!response.to_string().contains("@channel.invalid"));
    let chat_row = rows
        .iter()
        .find(|row| row["id"] == chat_task.to_string())
        .unwrap();
    assert_eq!(chat_row["kind"], "chat");
    assert_eq!(chat_row["chat_session_id"], chat.to_string());
    assert!(chat_row["result"].is_null());
    assert!(chat_row["error"].is_null());
    assert!(chat_row.get("attribution").is_none());
    let ordinary = rows
        .iter()
        .find(|row| row["id"] == active[0].to_string())
        .unwrap();
    assert_eq!(ordinary["attribution"]["initiator"]["name"], "member");
    assert_eq!(
        ordinary["attribution"]["initiator"]["avatar_url"],
        "https://invalid.example/avatar"
    );
    assert_eq!(ordinary["attribution"]["originator"]["name"], "member");
    let channel_task = rows
        .iter()
        .find(|row| row["id"] == active[1].to_string())
        .unwrap();
    assert_eq!(
        channel_task["attribution"]["originator"]["name"],
        "Channel identity"
    );
    assert!(
        channel_task["attribution"]["originator"]
            .get("email")
            .is_none()
    );
    db.execute(
        "UPDATE \"user\" SET avatar_url='private-avatar-key' WHERE id=$1",
        &[&users[1]],
    )
    .await
    .unwrap();
    let (status, no_policy) = read(
        &router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "snapshot-no-avatar-policy".into(),
            )),
            ApiState::new(pool.clone(), SECRET.to_vec()),
        ),
        ws,
        Some(&jwt(users[1])),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{no_policy}");
    assert!(!no_policy.to_string().contains("private-avatar-key"));
    let (status, signed) = read(&app, ws, Some(&jwt(users[1]))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!signed.to_string().contains("private-avatar-key"));
    assert!(
        signed
            .to_string()
            .contains("https://safe.invalid/avatars/signed")
    );
    let owner = read(&app, ws, Some(&jwt(users[0]))).await.1;
    assert_eq!(owner.as_array().unwrap().len(), 8);
    assert!(!owner.to_string().contains("CHATSECRET"));
    assert!(!owner.to_string().contains(&agents[3].to_string()));
    assert!(!owner.to_string().contains(&agents[4].to_string()));
    db.execute(
        "UPDATE agent SET archived_at=now() WHERE id=$1",
        &[&agents[1]],
    )
    .await
    .unwrap();
    assert!(
        read(&app, ws, Some(&jwt(users[1])))
            .await
            .1
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["id"] == chat_task.to_string())
    );
    db.execute(
        "DELETE FROM agent_invocation_target WHERE agent_id=$1",
        &[&agents[1]],
    )
    .await
    .unwrap();
    assert!(
        !read(&app, ws, Some(&jwt(users[1])))
            .await
            .1
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["id"] == chat_task.to_string())
    );
}
