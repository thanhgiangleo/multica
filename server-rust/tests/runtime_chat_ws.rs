//! Real websocket upgrade and committed-event delivery against disposable PostgreSQL.
use axum::http::StatusCode;
use deadpool_postgres::{Manager, Pool};
use futures_util::{SinkExt, StreamExt};
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
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Config, NoTls};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

const SECRET: &[u8] = b"zbsi67-websocket-synthetic-secret";
fn token(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user,"exp":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
}
async fn next(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = message {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn scoped_event_delivery_and_revoked_membership() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi67"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(6)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let owner: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('WS owner',$1) RETURNING id",
            &[&format!("ws-owner-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let stranger: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('WS stranger',$1) RETURNING id",
            &[&format!("ws-stranger-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('WS',$1) RETURNING id",
            &[&format!("ws-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for user in [owner, stranger] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
            &[&workspace, &user],
        )
        .await
        .unwrap();
    }
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,status,runtime_mode) VALUES ('WS',$1,$2,'user','private','working','cloud') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let session: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'Private',now()) RETURNING id", &[&workspace,&agent,&owner]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'WS runtime','cloud','synthetic','online') RETURNING id", &[&workspace]).await.unwrap().get(0);
    let chat_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'queued') RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES ($1,'WS issue','member',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let issue_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES ($1,$2,$3,'queued') RETURNING id", &[&agent,&runtime,&issue]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let url = format!("ws://127.0.0.1:{port}/ws?workspace_id={workspace}");
    let mut wrong_origin = url.as_str().into_client_request().unwrap();
    wrong_origin
        .headers_mut()
        .insert("origin", "https://untrusted.invalid".parse().unwrap());
    let denied = connect_async(wrong_origin).await.unwrap_err();
    assert!(
        matches!(denied, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == StatusCode::FORBIDDEN)
    );
    let mut wrong_cookie = url.as_str().into_client_request().unwrap();
    wrong_cookie
        .headers_mut()
        .insert("cookie", "multica_auth=invalid".parse().unwrap());
    let denied = connect_async(wrong_cookie).await.unwrap_err();
    assert!(
        matches!(denied, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == StatusCode::UNAUTHORIZED)
    );
    let recently_expired = encode(
        &Header::default(),
        &json!({"sub": owner, "exp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() - 1}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap();
    let mut expired_cookie = url.as_str().into_client_request().unwrap();
    expired_cookie.headers_mut().insert(
        "cookie",
        format!("multica_auth={recently_expired}").parse().unwrap(),
    );
    let denied = connect_async(expired_cookie).await.unwrap_err();
    assert!(
        matches!(denied, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == StatusCode::UNAUTHORIZED)
    );
    let (mut expired_bearer, _) = connect_async(&url).await.unwrap();
    expired_bearer
        .send(Message::Text(
            json!({"type":"auth","payload":{"token":recently_expired}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut expired_bearer).await["error"], "invalid token");
    let pat = format!("mul_ws_{suffix}");
    let pat_hash = hex::encode(Sha256::digest(pat.as_bytes()));
    let pat_id: Uuid = db.query_one("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'WS',$2,'mul_ws') RETURNING id", &[&owner,&pat_hash]).await.unwrap().get(0);
    let (mut pat_ws, _) = connect_async(&url).await.unwrap();
    pat_ws
        .send(Message::Text(
            json!({"type":"auth","payload":{"token":pat}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut pat_ws).await["type"], "auth_ack");
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE id=$1",
        &[&pat_id],
    )
    .await
    .unwrap();
    // Revoke after handshake: a queued owner-only event must never reach the
    // previously authenticated socket.
    let read_endpoint = format!(
        "http://127.0.0.1:{port}/api/chat/sessions/{session}/read?workspace_id={workspace}"
    );
    assert_eq!(
        reqwest::Client::new()
            .post(&read_endpoint)
            .bearer_auth(token(owner))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    let closed = tokio::time::timeout(Duration::from_secs(3), pat_ws.next())
        .await
        .unwrap();
    assert!(
        matches!(closed, Some(Ok(Message::Close(_))) | None),
        "revoked PAT socket received private event: {closed:?}"
    );
    let (mut revoked_ws, _) = connect_async(&url).await.unwrap();
    revoked_ws
        .send(Message::Text(
            json!({"type":"auth","payload":{"token":pat}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut revoked_ws).await["error"], "invalid token");
    let (mut machine_ws, _) = connect_async(&url).await.unwrap();
    machine_ws
        .send(Message::Text(
            json!({"type":"auth","payload":{"token":"mdt_machine"}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut machine_ws).await["error"], "invalid token");
    let mut cookie_request = url.as_str().into_client_request().unwrap();
    cookie_request.headers_mut().insert(
        "cookie",
        format!("multica_auth={}", token(owner)).parse().unwrap(),
    );
    let (mut cookie_ws, _) = connect_async(cookie_request).await.unwrap();
    cookie_ws
        .send(Message::Text(json!({"type":"ping"}).to_string().into()))
        .await
        .unwrap();
    assert_eq!(
        next(&mut cookie_ws).await["type"],
        "pong",
        "cookie-auth clients do not wait for auth_ack"
    );
    let (mut stranger_ws, _) = connect_async(&url).await.unwrap();
    stranger_ws
        .send(Message::Text(
            json!({"type":"auth","payload":{"token":token(stranger)}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut stranger_ws).await["type"], "auth_ack");
    stranger_ws
        .send(Message::Text(
            json!({"type":"subscribe","payload":{"scope":"chat","id":session}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        next(&mut stranger_ws).await["payload"]["error"],
        "forbidden"
    );
    stranger_ws
        .send(Message::Text(
            json!({"type":"subscribe","payload":{"scope":"task","id":chat_task}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        next(&mut stranger_ws).await["payload"]["error"],
        "forbidden"
    );
    stranger_ws
        .send(Message::Text(
            json!({"type":"subscribe","payload":{"scope":"task","id":issue_task}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut stranger_ws).await["type"], "subscribe_ack");
    let (mut owner_ws, _) = connect_async(&url).await.unwrap();
    owner_ws
        .send(Message::Text(
            json!({"type":"auth","payload":{"token":token(owner)}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut owner_ws).await["type"], "auth_ack");
    owner_ws
        .send(Message::Text(
            json!({"type":"subscribe","payload":{"scope":"chat","id":session}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut owner_ws).await["type"], "subscribe_ack");
    owner_ws
        .send(Message::Text(
            json!({"type":"subscribe","payload":{"scope":"task","id":chat_task}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut owner_ws).await["type"], "subscribe_ack");
    let client = reqwest::Client::new();
    let endpoint = format!(
        "http://127.0.0.1:{port}/api/chat/sessions/{session}/read?workspace_id={workspace}"
    );
    let response = client
        .post(&endpoint)
        .bearer_auth(token(owner))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let event = next(&mut owner_ws).await;
    assert_eq!(event["type"], "chat:session_read");
    assert_eq!(event["payload"]["chat_session_id"], session.to_string());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), stranger_ws.next())
            .await
            .is_err(),
        "foreign workspace member received private event"
    );
    db.execute(
        "UPDATE agent SET runtime_id=$2 WHERE id=$1",
        &[&agent, &runtime],
    )
    .await
    .unwrap();
    let send_endpoint = format!(
        "http://127.0.0.1:{port}/api/chat/sessions/{session}/messages?workspace_id={workspace}"
    );
    let sent = client
        .post(&send_endpoint)
        .bearer_auth(token(owner))
        .header("content-type", "application/json")
        .body(json!({"content":"committed private turn"}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(sent.status(), StatusCode::CREATED);
    let sent: Value = serde_json::from_str(&sent.text().await.unwrap()).unwrap();
    let queued = next(&mut owner_ws).await;
    assert_eq!(queued["type"], "task:queued");
    assert_eq!(queued["payload"]["task_id"], sent["task_id"]);
    assert_eq!(queued["actor_type"], "system");
    let message = next(&mut owner_ws).await;
    assert_eq!(message["type"], "chat:message");
    assert_eq!(message["payload"]["message_id"], sent["message_id"]);
    assert_eq!(message["payload"]["content"], "committed private turn");
    assert_eq!(message["actor_id"], owner.to_string());
    assert!(message["payload"].get("attachment_ids").is_none());
    let message_id = Uuid::parse_str(sent["message_id"].as_str().unwrap()).unwrap();
    let committed = db.query_one("SELECT m.task_id,q.chat_input_task_id FROM chat_message m JOIN agent_task_queue q ON q.id=m.task_id WHERE m.id=$1", &[&message_id]).await.unwrap();
    assert_eq!(
        committed.get::<_, Option<Uuid>>("task_id"),
        committed.get::<_, Option<Uuid>>("chat_input_task_id")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), stranger_ws.next())
            .await
            .is_err()
    );
    let denied = client
        .post(&send_endpoint)
        .bearer_auth(token(owner))
        .header("content-type", "application/json")
        .body(json!({"content":"invalid", "attachment_ids":["bad"]}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), owner_ws.next())
            .await
            .is_err(),
        "failed send published an event"
    );
    // A channel chat may be read by both its creator and its agent's current
    // owner. Send from the creator; neither recipient subscribed to this new
    // chat scope, matching Go's user-scope broadcasts.
    let channel_agent: Uuid = db.query_one(
        "INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,status,runtime_mode,runtime_id) VALUES ('Channel',$1,$2,'user','public_to','working','cloud',$3) RETURNING id",
        &[&workspace, &stranger, &runtime],
    ).await.unwrap().get(0);
    db.execute(
        "INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'workspace',$2)",
        &[&channel_agent, &workspace],
    ).await.unwrap();
    let channel_session: Uuid = db.query_one(
        "INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at,zbs_source_channel_type) VALUES ($1,$2,$3,'Channel',now(),'telegram') RETURNING id",
        &[&workspace, &channel_agent, &owner],
    ).await.unwrap().get(0);
    // The creator may edit a channel conversation before guest history begins.
    // Go broadcasts each committed metadata mutation to the current agent owner
    // as well as the creator without changing actor attribution.
    let metadata = format!(
        "http://127.0.0.1:{port}/api/chat/sessions/{channel_session}?workspace_id={workspace}"
    );
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'WS channel') RETURNING id",
            &[&workspace],
        )
        .await
        .unwrap()
        .get(0);
    for (path, body, field, expected) in [
        (
            metadata.clone(),
            json!({"title":"Renamed channel"}),
            "title",
            json!("Renamed channel"),
        ),
        (
            format!(
                "http://127.0.0.1:{port}/api/chat/sessions/{channel_session}/pin?workspace_id={workspace}"
            ),
            json!({"pinned":true}),
            "pinned",
            json!(true),
        ),
        (
            metadata.clone(),
            json!({"project_id":project}),
            "project_id",
            json!(project),
        ),
    ] {
        let response = client
            .patch(path)
            .bearer_auth(token(owner))
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{}",
            response.text().await.unwrap()
        );
        for ws in [&mut owner_ws, &mut stranger_ws] {
            let event = next(ws).await;
            assert_eq!(event["type"], "chat:session_updated");
            assert_eq!(
                event["payload"]["chat_session_id"],
                channel_session.to_string()
            );
            assert_eq!(event["payload"][field], expected);
            assert_eq!(event["actor_id"], owner.to_string());
        }
    }
    let persisted = db
        .query_one(
            "SELECT title,project_id,pinned_at IS NOT NULL AS pinned FROM chat_session WHERE id=$1",
            &[&channel_session],
        )
        .await
        .unwrap();
    assert_eq!(persisted.get::<_, String>("title"), "Renamed channel");
    assert_eq!(
        persisted.get::<_, Option<Uuid>>("project_id"),
        Some(project)
    );
    assert!(persisted.get::<_, bool>("pinned"));
    let invalid = client
        .patch(&metadata)
        .bearer_auth(token(owner))
        .header("content-type", "application/json")
        .body(json!({"project_id":Uuid::new_v4()}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::NOT_FOUND);
    for ws in [&mut owner_ws, &mut stranger_ws] {
        assert!(
            tokio::time::timeout(Duration::from_millis(100), ws.next())
                .await
                .is_err(),
            "rejected metadata update published an event"
        );
    }
    let sent = client.post(format!(
        "http://127.0.0.1:{port}/api/chat/sessions/{channel_session}/messages?workspace_id={workspace}"
    )).bearer_auth(token(owner))
         .header("content-type", "application/json").body(json!({"content":"two viewers"}).to_string()).send().await.unwrap();
    assert_eq!(
        sent.status(),
        StatusCode::CREATED,
        "{}",
        sent.text().await.unwrap()
    );
    for ws in [&mut owner_ws, &mut stranger_ws] {
        let queued = next(ws).await;
        assert_eq!(queued["type"], "task:queued");
        assert_eq!(
            queued["payload"]["chat_session_id"],
            channel_session.to_string()
        );
        let message = next(ws).await;
        assert_eq!(message["type"], "chat:message");
        assert_eq!(message["payload"]["content"], "two viewers");
        assert_eq!(message["actor_id"], owner.to_string());
    }
    // Queue mutations and archive use the same two-viewer channel audience as send.
    let active_task: Uuid = db
        .query_one(
            "SELECT id FROM agent_task_queue WHERE chat_session_id=$1 ORDER BY created_at LIMIT 1",
            &[&channel_session],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "UPDATE agent_task_queue SET status='running' WHERE id=$1",
        &[&active_task],
    )
    .await
    .unwrap();
    let follow_up: Uuid = db
        .query_one(
            "INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status,priority) VALUES ($1,$2,$3,'queued',0) RETURNING id",
            &[&channel_agent, &runtime, &channel_session],
        )
        .await
        .unwrap()
        .get(0);
    let base = format!("http://127.0.0.1:{port}/api/chat/sessions/{channel_session}/queued-tasks");
    let priority = client
        .post(format!(
            "{base}/{follow_up}/prioritize?workspace_id={workspace}"
        ))
        .bearer_auth(token(owner))
        .send()
        .await
        .unwrap();
    assert_eq!(priority.status(), StatusCode::OK);
    assert_eq!(
        db.query_one(
            "SELECT priority FROM agent_task_queue WHERE id=$1",
            &[&follow_up]
        )
        .await
        .unwrap()
        .get::<_, i32>(0),
        4
    );
    for ws in [&mut owner_ws, &mut stranger_ws] {
        let queued = next(ws).await;
        assert_eq!(queued["type"], "task:queued");
        assert_eq!(queued["payload"]["task_id"], follow_up.to_string());
    }
    let cleared = client
        .delete(format!("{base}?workspace_id={workspace}"))
        .bearer_auth(token(owner))
        .send()
        .await
        .unwrap();
    assert_eq!(cleared.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&follow_up]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "cancelled"
    );
    for ws in [&mut owner_ws, &mut stranger_ws] {
        let cancelled = next(ws).await;
        assert_eq!(cancelled["type"], "task:cancelled");
        assert_eq!(cancelled["payload"]["task_id"], follow_up.to_string());
    }
    let archived = client
        .patch(format!(
            "http://127.0.0.1:{port}/api/chat/sessions/{channel_session}/archive?workspace_id={workspace}"
        ))
        .bearer_auth(token(owner))
        .header("content-type", "application/json")
        .body(json!({"archived":true}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(archived.status(), StatusCode::OK);
    for ws in [&mut owner_ws, &mut stranger_ws] {
        let update = next(ws).await;
        assert_eq!(update["type"], "chat:session_updated");
        assert_eq!(
            update["payload"]["chat_session_id"],
            channel_session.to_string()
        );
        assert_eq!(update["payload"]["status"], "archived");
        assert_eq!(update["actor_id"], owner.to_string());
    }
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&channel_session],
    )
    .await
    .unwrap();
    let denied = client
        .patch(&metadata)
        .bearer_auth(token(owner))
        .header("content-type", "application/json")
        .body(json!({"title":"private guest history"}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    for ws in [&mut owner_ws, &mut stranger_ws] {
        assert!(
            tokio::time::timeout(Duration::from_millis(100), ws.next())
                .await
                .is_err(),
            "guest-history mutation published an event"
        );
    }
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&workspace, &owner],
    )
    .await
    .unwrap();
    // A revoked member cannot trigger another mutation or receive a response frame.
    let admin = client
        .post(&endpoint)
        .bearer_auth(token(owner))
        .send()
        .await
        .unwrap();
    assert_eq!(admin.status(), StatusCode::NOT_FOUND);
    owner_ws
        .send(Message::Text(json!({"type":"ping"}).to_string().into()))
        .await
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(3), owner_ws.next())
        .await
        .unwrap();
    assert!(
        matches!(closed, Some(Ok(Message::Close(_))) | None),
        "revoked socket received content: {closed:?}"
    );
    server.abort();
}
