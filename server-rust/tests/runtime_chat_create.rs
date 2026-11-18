//! Chat creation and authorization on a disposable migrated database.
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
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"isolated-zbsi67-chat-secret";

async fn call(
    app: &axum::Router,
    ws: Uuid,
    credential: &str,
    body: Value,
    cookie: bool,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(format!("/api/chat/sessions?workspace_id={ws}"))
        .header("content-type", "application/json");
    request = if cookie {
        request.header("cookie", format!("multica_auth={credential}"))
    } else {
        request.header("authorization", format!("Bearer {credential}"))
    };
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let code = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&bytes).unwrap())
}

async fn list(app: &axum::Router, ws: Uuid, credential: &str, status: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .uri(format!("/api/chat/sessions?workspace_id={ws}{status}"))
        .header("authorization", format!("Bearer {credential}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let code = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&bytes).unwrap())
}

async fn patch(
    app: &axum::Router,
    ws: Uuid,
    id: Uuid,
    credential: &str,
    endpoint: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(Method::PATCH)
        .uri(format!(
            "/api/chat/sessions/{id}{endpoint}?workspace_id={ws}"
        ))
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let code = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&bytes).unwrap())
}

async fn detail(app: &axum::Router, ws: Uuid, id: Uuid, credential: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .uri(format!("/api/chat/sessions/{id}?workspace_id={ws}"))
        .header("authorization", format!("Bearer {credential}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let code = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&bytes).unwrap())
}

async fn transcript(
    app: &axum::Router,
    ws: Uuid,
    id: Uuid,
    credential: &str,
    page: &str,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .uri(format!(
            "/api/chat/sessions/{id}/messages{page}{}workspace_id={ws}",
            if page.contains('?') { "&" } else { "?" }
        ))
        .header("authorization", format!("Bearer {credential}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let code = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&bytes).unwrap())
}

async fn read_session(
    app: &axum::Router,
    ws: Uuid,
    id: &str,
    credential: &str,
    cookie: bool,
) -> StatusCode {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(format!("/api/chat/sessions/{id}/read?workspace_id={ws}"));
    request = if cookie {
        request.header("cookie", format!("multica_auth={credential}"))
    } else {
        request.header("authorization", format!("Bearer {credential}"))
    };
    app.clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn create_is_transactional_and_invocation_scoped() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi67"));
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
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for label in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\" (name,email) VALUES ($1,$2) RETURNING id",
                &[&label, &format!("chat-{label}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace (name,slug) VALUES ('Chat',$1) RETURNING id",
            &[&format!("zbsi67-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace (name,slug) VALUES ('Other',$1) RETURNING id",
            &[&format!("zbsi67-other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for user in &users[..2] {
        db.execute(
            "INSERT INTO member (workspace_id,user_id,role) VALUES ($1,$2,'member')",
            &[&ws, user],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member (workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&other, &users[0]],
    )
    .await
    .unwrap();
    let agent: Uuid = db.query_one("INSERT INTO agent (name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Private',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let public: Uuid = db.query_one("INSERT INTO agent (name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Public',$1,$2,'user','public_to','cloud') RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let project: Uuid = db
        .query_one(
            "INSERT INTO project (workspace_id,title) VALUES ($1,'context') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_project: Uuid = db
        .query_one(
            "INSERT INTO project (workspace_id,title) VALUES ($1,'foreign') RETURNING id",
            &[&other],
        )
        .await
        .unwrap()
        .get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(), &json!({"sub": user, "exp": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
    };
    let private = json!({"agent_id":agent,"title":"hello", "project_id":project});
    assert_eq!(
        call(&app, ws, &jwt(users[1]), private.clone(), false)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, ws, &jwt(users[2]), private.clone(), false)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, other, &jwt(users[0]), private.clone(), false)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, ws, &jwt(users[0]), private.clone(), true)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, ws, &jwt(users[0]), json!({"title":"missing"}), false)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            ws,
            &jwt(users[0]),
            json!({"agent_id":agent,"project_id":foreign_project}),
            false
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (code, response) = call(&app, ws, &jwt(users[0]), private, false).await;
    assert_eq!(code, StatusCode::CREATED, "{response}");
    assert_eq!(response["creator_id"], json!(users[0]));
    assert_eq!(response["project_id"], json!(project));
    let session = Uuid::parse_str(response["id"].as_str().unwrap()).unwrap();
    assert_eq!(session.get_version_num(), 7);
    let pending = |who: Uuid, workspace: Uuid| {
        let app = app.clone();
        let credential = jwt(who);
        async move {
            let response = app
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/api/chat/sessions/{session}/pending-task?workspace_id={workspace}"
                        ))
                        .header("authorization", format!("Bearer {credential}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            (status, serde_json::from_slice::<Value>(&bytes).unwrap())
        }
    };
    assert_eq!(pending(users[1], ws).await.0, StatusCode::FORBIDDEN);
    assert_eq!(pending(users[0], other).await.0, StatusCode::NOT_FOUND);
    assert_eq!(
        pending(users[0], ws).await.1,
        json!({"supports_queue":true})
    );
    let runtime: Uuid = db
        .query_one(
            "INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) \
         VALUES ($1,'ZBSI67 synthetic','cloud','synthetic','online') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let head: Uuid = db
        .query_one(
            "INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status,wait_reason) \
         VALUES ($1,$2,$3,'waiting_local_directory','holding directory') RETURNING id",
            &[&agent, &runtime, &session],
        )
        .await
        .unwrap()
        .get(0);
    let queued: Uuid = db
        .query_one(
            "INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) \
         VALUES ($1,$2,$3,'queued') RETURNING id",
            &[&agent, &runtime, &session],
        )
        .await
        .unwrap()
        .get(0);
    let input: Uuid = db
        .query_one(
            "INSERT INTO chat_message(chat_session_id,role,content,task_id) \
         VALUES ($1,'user','follow-up',$2) RETURNING id",
            &[&session, &queued],
        )
        .await
        .unwrap()
        .get(0);
    let (status, queue) = pending(users[0], ws).await;
    assert_eq!(status, StatusCode::OK, "{queue}");
    assert_eq!(queue["task_id"], json!(head));
    assert_eq!(queue["wait_reason"], "holding directory");
    assert_eq!(queue["queued_tasks"][0]["task_id"], json!(queued));
    assert_eq!(queue["queued_tasks"][0]["message_id"], json!(input));
    assert_eq!(queue["queued_tasks"][0]["content"], "follow-up");
    db.execute(
        "UPDATE agent_task_queue SET status='running' WHERE id=$1",
        &[&head],
    )
    .await
    .unwrap();
    assert!(pending(users[0], ws).await.1.get("wait_reason").is_none());
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&head],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE agent_task_queue SET status='cancelled' WHERE id=$1",
        &[&queued],
    )
    .await
    .unwrap();
    assert_eq!(
        pending(users[0], ws).await.1,
        json!({"supports_queue":true})
    );
    db.execute("DELETE FROM chat_message WHERE id=$1", &[&input])
        .await
        .unwrap();
    assert_eq!(
        patch(
            &app,
            ws,
            session,
            &jwt(users[1]),
            "",
            json!({"title":"stolen"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        patch(&app, ws, session, &jwt(users[0]), "", json!({"title":"  "}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        patch(
            &app,
            ws,
            session,
            &jwt(users[0]),
            "",
            json!({"title":"renamed", "project_id":null})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        patch(
            &app,
            ws,
            session,
            &jwt(users[0]),
            "",
            json!({"project_id":foreign_project})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (status, renamed) = patch(
        &app,
        ws,
        session,
        &jwt(users[0]),
        "",
        json!({"title":"  renamed  "}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{renamed}");
    assert_eq!(renamed["title"], "renamed");
    let (status, moved) = patch(
        &app,
        ws,
        session,
        &jwt(users[0]),
        "",
        json!({"project_id":null}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{moved}");
    assert!(moved["project_id"].is_null());
    assert_eq!(
        patch(
            &app,
            ws,
            session,
            &jwt(users[0]),
            "",
            json!({"project_id":project})
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, pinned) = patch(
        &app,
        ws,
        session,
        &jwt(users[0]),
        "/pin",
        json!({"pinned":true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{pinned}");
    assert_eq!(pinned["pinned"], true);
    assert_eq!(
        patch(
            &app,
            ws,
            session,
            &jwt(users[1]),
            "/pin",
            json!({"pinned":false})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        patch(
            &app,
            ws,
            session,
            &jwt(users[0]),
            "/pin",
            json!({"pinned":false})
        )
        .await
        .1["pinned"],
        false
    );
    let (status, fetched) = detail(&app, ws, session, &jwt(users[0])).await;
    assert_eq!(status, StatusCode::OK, "{fetched}");
    assert_eq!(fetched["id"], json!(session));
    assert_eq!(
        detail(&app, ws, session, &jwt(users[1])).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        detail(&app, other, session, &jwt(users[0])).await.0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(
        detail(&app, ws, session, &jwt(users[0])).await.0,
        StatusCode::OK
    );
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &users[1]],
    )
    .await
    .unwrap();
    assert_eq!(
        detail(&app, ws, session, &jwt(users[0])).await.0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE chat_session SET zbs_source_channel_type='telegram' WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    let (status, owner_view) = detail(&app, ws, session, &jwt(users[1])).await;
    assert_eq!(status, StatusCode::OK, "{owner_view}");
    assert_eq!(owner_view["access_mode"], "viewer");
    assert_eq!(owner_view["read_only"], true);
    db.execute(
        "UPDATE chat_session SET zbs_source_channel_type=NULL WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(
        detail(&app, ws, session, &jwt(users[1])).await.0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &users[0]],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=false WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE chat_session SET pinned_at=now() WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO chat_message (chat_session_id,role,content) VALUES ($1,'user','first question')", &[&session]).await.unwrap();
    db.execute(
        "INSERT INTO chat_message (chat_session_id,role,content) VALUES ($1,'assistant','answer')",
        &[&session],
    )
    .await
    .unwrap();
    // Frozen Go hides onboarding kickoff and pages newest-first with an
    // id tie-breaker; a viewer cannot see another member's transcript.
    let kickoff: Uuid = db.query_one("INSERT INTO chat_message (chat_session_id,role,content,message_kind) VALUES ($1,'user','private instructions','onboarding_kickoff') RETURNING id", &[&session]).await.unwrap().get(0);
    db.execute(
        "UPDATE chat_message SET created_at='2025-01-01T00:00:00Z' WHERE id=$1",
        &[&kickoff],
    )
    .await
    .unwrap();
    let ids: Vec<Uuid> = db.query("SELECT id FROM chat_message WHERE chat_session_id=$1 AND message_kind='message' ORDER BY content", &[&session]).await.unwrap().iter().map(|r| r.get(0)).collect();
    for id in &ids {
        db.execute(
            "UPDATE chat_message SET created_at=CASE WHEN content='answer' THEN '2025-01-02T00:00:00Z'::timestamptz ELSE '2025-01-01T00:00:00Z'::timestamptz END WHERE id=$1",
            &[id],
        ).await.unwrap();
    }
    db.execute(
        "UPDATE chat_session SET last_read_at='2025-01-01T12:00:00Z' WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(
        transcript(&app, ws, session, &jwt(users[1]), "").await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        transcript(&app, other, session, &jwt(users[0]), "").await.0,
        StatusCode::NOT_FOUND
    );
    let (status, all_messages) = transcript(&app, ws, session, &jwt(users[0]), "").await;
    assert_eq!(status, StatusCode::OK, "{all_messages}");
    assert_eq!(all_messages.as_array().unwrap().len(), 2);
    assert!(
        all_messages
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["quick_actions"] == json!([]))
    );
    let (status, first_page) = transcript(&app, ws, session, &jwt(users[0]), "/page?limit=1").await;
    assert_eq!(status, StatusCode::OK, "{first_page}");
    assert_eq!(first_page["messages"].as_array().unwrap().len(), 1);
    assert_eq!(first_page["has_more"], true);
    let cursor = &first_page["next_cursor"];
    let (status, older) = transcript(
        &app,
        ws,
        session,
        &jwt(users[0]),
        &format!(
            "/page?limit=1&before_created_at={}&before_id={}",
            cursor["created_at"].as_str().unwrap().replace('+', "%2B"),
            cursor["id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{older}");
    assert_eq!(older["has_more"], false);
    assert_ne!(older["messages"][0]["id"], first_page["messages"][0]["id"]);
    assert_eq!(
        transcript(&app, ws, session, &jwt(users[0]), "/page?limit=0")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        transcript(&app, ws, session, &jwt(users[0]), "/page?before_id=oops")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let restore = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    db.execute("INSERT INTO chat_draft_restore(id,chat_session_id,task_id,content) VALUES ($1,$2,$3,'recover')", &[&restore,&session,&task_id]).await.unwrap();
    let attachment: Uuid = db.query_one(
        "INSERT INTO attachment (workspace_id,chat_session_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) \
         VALUES ($1,$2,'member',$3,'draft.txt','https://files.invalid/draft.txt','text/plain',7) RETURNING id",
        &[&ws, &session, &users[0]],
    ).await.unwrap().get(0);
    let unrelated: Uuid = db.query_one(
        "INSERT INTO attachment (workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) \
         VALUES ($1,'member',$2,'private.txt','https://files.invalid/private.txt','text/plain',3) RETURNING id",
        &[&other, &users[0]],
    ).await.unwrap().get(0);
    db.execute(
        "UPDATE chat_draft_restore SET attachment_ids=$2 WHERE id=$1",
        &[&restore, &vec![attachment, unrelated]],
    )
    .await
    .unwrap();
    let drafts = |who: Uuid, workspace: Uuid, id: String| {
        let app = app.clone();
        let credential = jwt(who);
        async move {
            let response = app
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/api/chat/sessions/{id}/draft-restores?workspace_id={workspace}"
                        ))
                        .header("authorization", format!("Bearer {credential}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            (status, serde_json::from_slice::<Value>(&bytes).unwrap())
        }
    };
    assert_eq!(
        drafts(users[1], ws, session.to_string()).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        drafts(users[0], other, session.to_string()).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        drafts(users[0], ws, "bad".into()).await.0,
        StatusCode::BAD_REQUEST
    );
    let (status, before) = drafts(users[0], ws, session.to_string()).await;
    assert_eq!(status, StatusCode::OK, "{before}");
    assert_eq!(before["restores"][0]["content"], "recover");
    assert_eq!(before["restores"][0]["task_id"], json!(task_id));
    assert_eq!(
        before["restores"][0]["attachments"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        before["restores"][0]["attachments"][0]["id"],
        json!(attachment)
    );
    assert_eq!(
        before["restores"][0]["attachments"][0]["filename"],
        "draft.txt"
    );
    let consume = |who: Uuid, session: Uuid, restore: String| {
        let app = app.clone();
        let credential = jwt(who);
        async move {
            let response = app.oneshot(Request::builder().method(Method::DELETE)
                .uri(format!("/api/chat/sessions/{session}/draft-restores/{restore}?workspace_id={ws}"))
                .header("authorization", format!("Bearer {credential}"))
                .body(Body::empty()).unwrap()).await.unwrap();
            response.status()
        }
    };
    assert_eq!(
        consume(users[1], session, restore.to_string()).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        consume(users[0], session, "invalid".into()).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        consume(users[0], session, restore.to_string()).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        consume(users[0], session, restore.to_string()).await,
        StatusCode::NO_CONTENT
    );
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM chat_draft_restore WHERE id=$1",
            &[&restore],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let (status, after) = drafts(users[0], ws, session.to_string()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(after["restores"], json!([]));
    let (status, listed) = list(&app, ws, &jwt(users[0]), "").await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    let rows = listed.as_array().unwrap();
    assert_eq!(rows.len(), 1, "{listed}");
    assert_eq!(rows[0]["id"], json!(session));
    assert_eq!(rows[0]["pinned"], true);
    assert_eq!(rows[0]["unread_count"], 1);
    assert_eq!(rows[0]["has_unread"], true);
    assert_eq!(rows[0]["last_message"]["content"], "answer");
    assert_eq!(rows[0]["last_message"]["role"], "assistant");
    assert_eq!(rows[0]["access_mode"], "owner");
    assert_eq!(
        read_session(&app, ws, "bad", &jwt(users[0]), false).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        read_session(&app, ws, &session.to_string(), &jwt(users[1]), false).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        read_session(&app, other, &session.to_string(), &jwt(users[0]), false).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read_session(&app, ws, &session.to_string(), &jwt(users[0]), true).await,
        StatusCode::FORBIDDEN
    );
    let hidden: Uuid = db.query_one(
        "INSERT INTO chat_session(workspace_id,agent_id,creator_id,title) VALUES ($1,$2,$3,'command-only') RETURNING id",
        &[&ws,&agent,&users[0]],
    ).await.unwrap().get(0);
    assert_eq!(
        read_session(&app, ws, &hidden.to_string(), &jwt(users[0]), false).await,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(
        read_session(&app, ws, &session.to_string(), &jwt(users[0]), false).await,
        StatusCode::NO_CONTENT
    );
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=false WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(
        read_session(&app, ws, &session.to_string(), &jwt(users[0]), false).await,
        StatusCode::NO_CONTENT
    );
    let read_at: Option<chrono::DateTime<chrono::Utc>> = db
        .query_one(
            "SELECT last_read_at FROM chat_session WHERE id=$1",
            &[&session],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        read_at.unwrap()
            > "2025-01-01T12:00:00Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
    );
    let (status, read_list) = list(&app, ws, &jwt(users[0]), "").await;
    assert_eq!(status, StatusCode::OK, "{read_list}");
    assert_eq!(read_list[0]["unread_count"], 0);
    assert_eq!(read_list[0]["has_unread"], false);
    // Revocation blocks writes even when the creator still owns the session.
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &users[1]],
    )
    .await
    .unwrap();
    assert_eq!(
        read_session(&app, ws, &session.to_string(), &jwt(users[0]), false).await,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &users[0]],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &users[1]],
    )
    .await
    .unwrap();
    assert_eq!(
        read_session(&app, ws, &session.to_string(), &jwt(users[0]), false).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        read_session(&app, ws, &session.to_string(), &jwt(users[1]), false).await,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &users[0]],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=false WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    // The agent owner sees only its own member's sessions, and the session
    // disappears from the active list once archived unless status=all asks.
    assert_eq!(list(&app, ws, &jwt(users[1]), "").await.1, json!([]));
    db.execute(
        "UPDATE chat_session SET status='archived' WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(list(&app, ws, &jwt(users[0]), "").await.1, json!([]));
    let (status, archived) = list(&app, ws, &jwt(users[0]), "&status=all").await;
    assert_eq!(status, StatusCode::OK, "{archived}");
    assert_eq!(archived[0]["status"], "archived");
    assert_eq!(archived[0]["unread_count"], 0);
    db.execute(
        "UPDATE chat_session SET status='active' WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    let row = db.query_one("SELECT explicitly_created_at IS NOT NULL, agent_id, creator_id, project_id FROM chat_session WHERE id=$1 AND workspace_id=$2", &[&session,&ws]).await.unwrap();
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, Uuid>(1), agent);
    assert_eq!(row.get::<_, Uuid>(2), users[0]);
    assert_eq!(row.get::<_, Option<Uuid>>(3), Some(project));
    db.execute("INSERT INTO agent_invocation_target (agent_id,target_type,target_id) VALUES ($1,'member',$2)", &[&public,&users[1]]).await.unwrap();
    assert_eq!(
        call(&app, ws, &jwt(users[1]), json!({"agent_id":public}), false)
            .await
            .0,
        StatusCode::CREATED
    );
    db.execute("UPDATE agent SET archived_at=now() WHERE id=$1", &[&public])
        .await
        .unwrap();
    assert_eq!(
        call(&app, ws, &jwt(users[1]), json!({"agent_id":public}), false)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
}
