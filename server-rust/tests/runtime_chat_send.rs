//! Direct sends against child-owned PostgreSQL: no provider or installed agent.
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
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"zbsi67-send-synthetic-secret";
fn token(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
}
async fn send(
    app: &axum::Router,
    ws: Uuid,
    session: Uuid,
    user: Uuid,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/chat/sessions/{session}/messages?workspace_id={ws}"
                ))
                .header("authorization", format!("Bearer {}", token(user)))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn direct_send_atomic_queue_attribution_access_and_rebind() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi67"));
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
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for name in ["Owner", "Other", "Viewer"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("send-{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let (owner, other, viewer) = (users[0], users[1], users[2]);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Send',$1) RETURNING id",
            &[&format!("send-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Foreign',$1) RETURNING id",
            &[&format!("send-foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role) in [(owner, "admin"), (other, "member"), (viewer, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&foreign, &owner],
    )
    .await
    .unwrap();
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Synthetic send','cloud','synthetic','offline') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent:Uuid=db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode,runtime_id) VALUES ('Send',$1,$2,'user','private','cloud',$3) RETURNING id", &[&ws,&owner,&runtime]).await.unwrap().get(0);
    let session:Uuid=db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'',now()) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let kickoff:Uuid=db.query_one("INSERT INTO chat_message(chat_session_id,role,content,message_kind) VALUES ($1,'user','onboarding','onboarding_kickoff') RETURNING id", &[&session]).await.unwrap().get(0);
    assert_eq!(
        send(&app, ws, session, owner, Value::Null).await.1["error"],
        "content is required"
    );
    assert_eq!(
        send(&app, ws, session, owner, json!({"content":null}))
            .await
            .1["error"],
        "content is required"
    );
    let path = format!("/api/chat/sessions/{session}/messages?workspace_id={ws}");
    let anonymous = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&path)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"content":"denied"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
    let expired = encode(
        &Header::default(),
        &json!({"sub":owner,"exp":1}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap();
    let old = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&path)
                .header("authorization", format!("Bearer {expired}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"content":"denied"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(old.status(), StatusCode::UNAUTHORIZED);
    let cookie = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&path)
                .header("cookie", format!("multica_auth={}", token(owner)))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"content":"denied"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        cookie.status(),
        StatusCode::FORBIDDEN,
        "cookie send requires CSRF"
    );
    assert_eq!(
        send(&app, ws, session, owner, json!({"content":""}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            &app,
            ws,
            session,
            owner,
            json!({"content":"hi","attachment_ids":["broken"]})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(&app, foreign, session, owner, json!({"content":"hi"}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(&app, ws, session, other, json!({"content":"hi"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send(&app, ws, session, viewer, json!({"content":"hi"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let first = send(
        &app,
        ws,
        session,
        owner,
        json!({"content":"# First **turn**"}),
    )
    .await;
    assert_eq!(first.0, StatusCode::CREATED, "{}", first.1);
    assert_eq!(first.1["queued"], false);
    assert_eq!(first.1["supports_queue"], true);
    assert!(first.1["attachment_ids"].is_null());
    let first_task = Uuid::parse_str(first.1["task_id"].as_str().unwrap()).unwrap();
    let row=db.query_one("SELECT q.*,m.content,m.message_kind FROM agent_task_queue q JOIN chat_message m ON m.task_id=q.id WHERE q.id=$1 AND m.message_kind='message'", &[&first_task]).await.unwrap();
    assert_eq!(row.get::<_, String>("status"), "queued");
    assert_eq!(row.get::<_, i32>("priority"), 2);
    for column in [
        "initiator_user_id",
        "originator_user_id",
        "accountable_user_id",
    ] {
        assert_eq!(row.get::<_, Option<Uuid>>(column), Some(owner));
    }
    assert_eq!(
        row.get::<_, Option<String>>("originator_source").as_deref(),
        Some("direct_human")
    );
    assert_eq!(
        row.get::<_, Option<String>>("trigger_evidence_kind")
            .as_deref(),
        Some("chat")
    );
    assert_eq!(
        row.get::<_, Option<Uuid>>("trigger_evidence_ref_id"),
        Some(session)
    );
    assert_eq!(
        row.get::<_, Option<Uuid>>("chat_input_task_id"),
        Some(first_task)
    );
    assert_eq!(
        db.query_one("SELECT task_id FROM chat_message WHERE id=$1", &[&kickoff])
            .await
            .unwrap()
            .get::<_, Option<Uuid>>(0),
        Some(first_task)
    );
    assert_eq!(
        db.query_one("SELECT title FROM chat_session WHERE id=$1", &[&session])
            .await
            .unwrap()
            .get::<_, String>(0),
        ""
    );
    let mut attachments = Vec::new();
    for (upload_ws, uploader) in [(ws, owner), (ws, other), (foreign, owner)] {
        attachments.push(db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,'member',$2,'Report.pdf','/synthetic','application/pdf',5) RETURNING id", &[&upload_ws,&uploader]).await.unwrap().get::<_,Uuid>(0));
    }
    let second = send(
        &app,
        ws,
        session,
        owner,
        json!({"content":"follow up","attachment_ids":attachments}),
    )
    .await;
    assert_eq!(second.0, StatusCode::CREATED, "{}", second.1);
    assert_eq!(second.1["queued"], true);
    assert_eq!(second.1["attachment_ids"], json!([attachments[0]]));
    let third = send(
        &app,
        ws,
        session,
        owner,
        json!({"content":"next","attachment_ids":attachments}),
    )
    .await;
    assert_eq!(third.1["attachment_ids"], json!([]));
    let current = db
        .query_one(
            "SELECT chat_message_id,chat_session_id FROM attachment WHERE id=$1",
            &[&attachments[0]],
        )
        .await
        .unwrap();
    assert_eq!(
        current
            .get::<_, Option<Uuid>>("chat_message_id")
            .unwrap()
            .to_string(),
        second.1["message_id"].as_str().unwrap()
    );
    assert_eq!(
        current.get::<_, Option<Uuid>>("chat_session_id"),
        Some(session)
    );
    // Concurrent first sends have exactly one visible head and independent immutable inputs.
    let concurrent_session:Uuid=db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'Concurrent',now()) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let (left, right) = tokio::join!(
        send(
            &app,
            ws,
            concurrent_session,
            owner,
            json!({"content":"left"})
        ),
        send(
            &app,
            ws,
            concurrent_session,
            owner,
            json!({"content":"right"})
        )
    );
    assert_eq!(left.0, StatusCode::CREATED, "{}", left.1);
    assert_eq!(right.0, StatusCode::CREATED, "{}", right.1);
    assert_ne!(left.1["queued"], right.1["queued"]);
    assert_ne!(left.1["task_id"], right.1["task_id"]);
    assert_eq!(db.query_one("SELECT count(*) FROM chat_message m JOIN agent_task_queue q ON q.id=m.task_id WHERE m.chat_session_id=$1 AND q.chat_input_task_id=q.id", &[&concurrent_session]).await.unwrap().get::<_,i64>(0),2);
    // Admin view permission cannot bypass revoked invocation.
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &other],
    )
    .await
    .unwrap();
    let denied = send(&app, ws, session, owner, json!({"content":"denied"})).await;
    assert_eq!(denied.0, StatusCode::FORBIDDEN);
    assert_eq!(denied.1["reason_code"], "invocation_not_allowed");
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(
        send(&app, ws, session, owner, json!({"content":"guest denied"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=false WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &owner],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE chat_session SET status='archived' WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(
        send(&app, ws, session, owner, json!({"content":"denied"}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    db.execute(
        "UPDATE chat_session SET status='active' WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    db.execute("UPDATE agent SET archived_at=now() WHERE id=$1", &[&agent])
        .await
        .unwrap();
    assert_eq!(
        send(&app, ws, session, owner, json!({"content":"denied"}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    db.execute(
        "UPDATE agent SET archived_at=NULL,runtime_id=NULL WHERE id=$1",
        &[&agent],
    )
    .await
    .unwrap();
    let missing = send(&app, ws, session, owner, json!({"content":"denied"})).await;
    assert_eq!(missing.0, StatusCode::CONFLICT);
    assert_eq!(missing.1["reason_code"], "agent_runtime_required");
    db.execute(
        "UPDATE agent SET runtime_id=$2 WHERE id=$1",
        &[&agent, &runtime],
    )
    .await
    .unwrap();
    for (metadata, code) in [
        (
            json!({"offline_reason":{"code":"not_executable"}}),
            "runtime_unusable",
        ),
        (
            json!({"offline_reason":{"code":"dsh_profile"}}),
            "runtime_profile_missing",
        ),
    ] {
        db.execute(
            "UPDATE agent_runtime SET metadata=$2 WHERE id=$1",
            &[&runtime, &metadata],
        )
        .await
        .unwrap();
        let denied = send(&app, ws, session, owner, json!({"content":"denied"})).await;
        assert_eq!(denied.0, StatusCode::CONFLICT);
        assert_eq!(denied.1["reason_code"], code);
    }
    db.execute(
        "UPDATE agent_runtime SET metadata='{}'::jsonb,visibility='private',owner_id=$2 WHERE id=$1",
        &[&runtime, &other],
    )
    .await
    .unwrap();
    assert_eq!(
        send(&app, ws, session, owner, json!({"content":"denied"}))
            .await
            .1["reason_code"],
        "runtime_access_denied"
    );
    db.execute(
        "UPDATE agent_runtime SET visibility='public' WHERE id=$1",
        &[&runtime],
    )
    .await
    .unwrap();
    // Only successful sends exist; no denied input/task leaked through preflight.
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_task_queue WHERE chat_session_id=$1",
            &[&session]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        3
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM chat_message WHERE chat_session_id=$1",
            &[&session]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        4
    );
    // Lock the session as a rebind would; request must use the new runtime after waiting.
    let next_runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Synthetic rebound','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let mut binder = pool.get().await.unwrap();
    let tx = binder.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM chat_session WHERE id=$1 FOR UPDATE",
        &[&session],
    )
    .await
    .unwrap();
    let request = send(&app, ws, session, owner, json!({"content":"rebound"}));
    tokio::pin!(request);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut request)
            .await
            .is_err()
    );
    tx.execute(
        "UPDATE agent SET runtime_id=$2 WHERE id=$1",
        &[&agent, &next_runtime],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let rebound = request.await;
    assert_eq!(rebound.0, StatusCode::CREATED, "{}", rebound.1);
    let rebound_task = Uuid::parse_str(rebound.1["task_id"].as_str().unwrap()).unwrap();
    assert_eq!(
        db.query_one(
            "SELECT runtime_id FROM agent_task_queue WHERE id=$1",
            &[&rebound_task]
        )
        .await
        .unwrap()
        .get::<_, Option<Uuid>>(0),
        Some(next_runtime)
    );
    // An attachment-only first turn names an empty chat from the bound filename.
    let media_session:Uuid=db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'',now()) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let report:Uuid=db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,'member',$2,'Quarterly report final.pdf','/synthetic','application/pdf',5) RETURNING id", &[&ws,&owner]).await.unwrap().get(0);
    let media = send(
        &app,
        ws,
        media_session,
        owner,
        json!({"content":"   ","attachment_ids":[report]}),
    )
    .await;
    assert_eq!(media.0, StatusCode::CREATED, "{}", media.1);
    assert_eq!(media.1["attachment_ids"], json!([report]));
    assert_eq!(
        db.query_one(
            "SELECT title FROM chat_session WHERE id=$1",
            &[&media_session]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "Quarterly report final.pdf"
    );
    // Quick-action regeneration is not a visible turn, so it cannot make a send look queued.
    let regen_session:Uuid=db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'Regen',now()) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let regen:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status,priority) VALUES ($1,$2,$3,'queued',2) RETURNING id", &[&agent,&next_runtime,&regen_session]).await.unwrap().get(0);
    db.execute(
        "UPDATE agent_task_queue SET regenerate_quick_actions_for=$2 WHERE id=$1",
        &[&regen, &first_task],
    )
    .await
    .unwrap();
    assert_eq!(
        send(&app, ws, regen_session, owner, json!({"content":"fresh"}))
            .await
            .1["queued"],
        false
    );
    db.execute(
        "UPDATE agent_task_queue SET fire_at=now() WHERE id=$1",
        &[&regen],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE agent_task_queue SET status='deferred' WHERE id=$1",
        &[&regen],
    )
    .await
    .unwrap();
    assert_eq!(
        send(
            &app,
            ws,
            regen_session,
            owner,
            json!({"content":"deferred head"})
        )
        .await
        .1["queued"],
        true
    );
    // Fail after task/message/title writes: attachment trigger must roll back the whole turn.
    let failed_session:Uuid=db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'',now()) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let fail_attachment:Uuid=db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,'member',$2,'rollback','/synthetic','text/plain',1) RETURNING id", &[&ws,&owner]).await.unwrap().get(0);
    let function = format!("send_fail_{}", suffix);
    db.batch_execute(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='{fail_attachment}'::uuid THEN RAISE EXCEPTION 'synthetic attachment failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER {function} BEFORE UPDATE ON attachment FOR EACH ROW EXECUTE FUNCTION {function}()" )).await.unwrap();
    let failed = send(
        &app,
        ws,
        failed_session,
        owner,
        json!({"content":"rollback title","attachment_ids":[fail_attachment]}),
    )
    .await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{}", failed.1);
    db.batch_execute(&format!(
        "DROP TRIGGER {function} ON attachment; DROP FUNCTION {function}()"
    ))
    .await
    .unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_task_queue WHERE chat_session_id=$1",
            &[&failed_session]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM chat_message WHERE chat_session_id=$1",
            &[&failed_session]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT title FROM chat_session WHERE id=$1",
            &[&failed_session]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        ""
    );
}
