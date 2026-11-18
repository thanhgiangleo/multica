//! Archive semantics against child-owned PostgreSQL: channel cancellation versus web-only preservation.
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

const SECRET: &[u8] = b"zbsi67-archive-test-secret";

async fn request(
    app: &axum::Router,
    ws: Uuid,
    session: &str,
    user: Uuid,
    archived: bool,
    cookie: bool,
) -> (StatusCode, Value) {
    let token = encode(&Header::default(), &json!({"sub":user,
        "exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600,
    }), &EncodingKey::from_secret(SECRET)).unwrap();
    let mut req = Request::builder()
        .method(Method::PATCH)
        .uri(format!(
            "/api/chat/sessions/{session}/archive?workspace_id={ws}"
        ))
        .header("content-type", "application/json");
    req = if cookie {
        req.header("cookie", format!("multica_auth={token}"))
    } else {
        req.header("authorization", format!("Bearer {token}"))
    };
    let res = app
        .clone()
        .oneshot(
            req.body(Body::from(json!({"archived": archived}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    (
        status,
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap(),
    )
}

async fn delete(
    app: &axum::Router,
    ws: Uuid,
    session: &str,
    user: Uuid,
    cookie: bool,
) -> StatusCode {
    let token = encode(&Header::default(), &json!({"sub":user,
        "exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600,
    }), &EncodingKey::from_secret(SECRET)).unwrap();
    let mut req = Request::builder()
        .method(Method::DELETE)
        .uri(format!("/api/chat/sessions/{session}?workspace_id={ws}"));
    req = if cookie {
        req.header("cookie", format!("multica_auth={token}"))
    } else {
        req.header("authorization", format!("Bearer {token}"))
    };
    app.clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn archive_cancels_only_bound_chat_tasks_and_retires_binding() {
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
    let owner: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Owner',$1) RETURNING id",
            &[&format!("archive-owner-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Other',$1) RETURNING id",
            &[&format!("archive-other-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Archive',$1) RETURNING id",
            &[&format!("archive-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Other',$1) RETURNING id",
            &[&format!("archive-foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for who in [owner, other] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
            &[&ws, &who],
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
    let agent:Uuid=db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,status,runtime_mode) VALUES ('Archive',$1,$2,'user','private','working','cloud') RETURNING id", &[&ws,&owner]).await.unwrap().get(0);
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'synthetic archive','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let bound:Uuid=db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'Bound',now()) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let web:Uuid=db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'Web',now()) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let queued:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'queued') RETURNING id", &[&agent,&runtime,&bound]).await.unwrap().get(0);
    let running:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[&agent,&runtime,&bound]).await.unwrap().get(0);
    let web_task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[&agent,&runtime,&web]).await.unwrap().get(0);
    for task in [running, web_task] {
        db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&format!("test-archive-{task}"),&task,&agent,&ws,&owner]).await.unwrap();
    }
    let installation:Uuid=db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,config,status,installer_user_id) VALUES ($1,$2,'wecom','{}'::jsonb,'active',$3) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    db.execute("INSERT INTO channel_chat_session_binding(chat_session_id,installation_id,channel_type,channel_chat_id,chat_type) VALUES ($1,$2,'wecom',$3,'group')", &[&bound,&installation,&format!("room-{suffix}")]).await.unwrap();
    let hidden: Uuid = db.query_one(
        "INSERT INTO chat_session(workspace_id,agent_id,creator_id,title) VALUES ($1,$2,$3,'Command-only') RETURNING id",
        &[&ws, &agent, &owner],
    ).await.unwrap().get(0);
    let guest: Uuid = db.query_one(
        "INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at,zbs_has_guest_history) VALUES ($1,$2,$3,'Guest',now(),true) RETURNING id",
        &[&ws, &agent, &owner],
    ).await.unwrap().get(0);
    let foreign_guest: Uuid = db.query_one(
        "INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at,zbs_has_guest_history) VALUES ($1,$2,$3,'Guest-other',now(),true) RETURNING id",
        &[&ws, &agent, &other],
    ).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    assert_eq!(
        request(&app, ws, "invalid", owner, true, false).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, foreign, &bound.to_string(), owner, true, false)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, ws, &bound.to_string(), other, true, false)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(&app, ws, &bound.to_string(), owner, true, true)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(&app, ws, &foreign_guest.to_string(), other, true, false)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        delete(&app, ws, &foreign_guest.to_string(), other, false).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(&app, ws, &guest.to_string(), owner, true, false)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        request(&app, ws, &hidden.to_string(), owner, true, false)
            .await
            .0,
        StatusCode::OK
    );
    let (status, response) = request(&app, ws, &bound.to_string(), owner, true, false).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["status"], "archived");
    assert_eq!(
        db.query_one(
            "SELECT count(*)::int FROM channel_chat_session_binding WHERE chat_session_id=$1",
            &[&bound]
        )
        .await
        .unwrap()
        .get::<_, i32>(0),
        0
    );
    for task in [queued, running] {
        let row=db.query_one("SELECT status,cancelled_by_type,completed_at IS NOT NULL AS completed FROM agent_task_queue WHERE id=$1", &[&task]).await.unwrap();
        assert_eq!(row.get::<_, String>("status"), "cancelled");
        assert_eq!(
            row.get::<_, Option<String>>("cancelled_by_type"),
            Some("system".into())
        );
        assert!(row.get::<_, bool>("completed"));
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*)::int FROM task_token WHERE task_id=$1",
            &[&running]
        )
        .await
        .unwrap()
        .get::<_, i32>(0),
        0
    );
    assert_eq!(
        request(&app, ws, &web.to_string(), owner, true, false)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&web_task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "running"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*)::int FROM task_token WHERE task_id=$1",
            &[&web_task]
        )
        .await
        .unwrap()
        .get::<_, i32>(0),
        1
    );
    assert_eq!(
        request(&app, ws, &bound.to_string(), owner, false, false)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one("SELECT status FROM chat_session WHERE id=$1", &[&bound])
            .await
            .unwrap()
            .get::<_, String>(0),
        "active"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*)::int FROM channel_chat_session_binding WHERE chat_session_id=$1",
            &[&bound]
        )
        .await
        .unwrap()
        .get::<_, i32>(0),
        0
    );
    let protected: Uuid = db.query_one(
        "INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at,zbs_source_channel_type) VALUES ($1,$2,$3,'Zalo',now(),'zalo') RETURNING id",
        &[&ws,&agent,&owner],
    ).await.unwrap().get(0);
    assert_eq!(
        delete(&app, ws, &protected.to_string(), owner, false).await,
        StatusCode::CONFLICT
    );
    assert_eq!(
        delete(&app, ws, &bound.to_string(), other, false).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        delete(&app, foreign, &bound.to_string(), owner, false).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        delete(&app, ws, &bound.to_string(), owner, true).await,
        StatusCode::FORBIDDEN
    );
    let doomed: Uuid = db.query_one(
        "INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'Delete',now()) RETURNING id",
        &[&ws,&agent,&owner],
    ).await.unwrap().get(0);
    let task: Uuid = db.query_one(
        "INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'running') RETURNING id",
        &[&agent,&runtime,&doomed],
    ).await.unwrap().get(0);
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&format!("test-archive-{task}"),&task,&agent,&ws,&owner]).await.unwrap();
    db.execute("INSERT INTO chat_draft_restore(id,chat_session_id,task_id,content) VALUES ($1,$2,$3,'recover')", &[&Uuid::new_v4(),&doomed,&task]).await.unwrap();
    db.execute(
        "INSERT INTO agent_builder_draft(chat_session_id,workspace_id) VALUES ($1,$2)",
        &[&doomed, &ws],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO channel_outbound_card_message(chat_session_id,channel_type,channel_chat_id,channel_card_message_id) VALUES ($1,'wecom','room','card')", &[&doomed]).await.unwrap();
    assert_eq!(
        delete(&app, ws, &doomed.to_string(), owner, false).await,
        StatusCode::NO_CONTENT
    );
    for (sql, expected) in [
        ("SELECT count(*)::int FROM chat_session WHERE id=$1", 0),
        (
            "SELECT count(*)::int FROM chat_draft_restore WHERE chat_session_id=$1",
            0,
        ),
        (
            "SELECT count(*)::int FROM agent_builder_draft WHERE chat_session_id=$1",
            0,
        ),
        (
            "SELECT count(*)::int FROM channel_outbound_card_message WHERE chat_session_id=$1",
            0,
        ),
    ] {
        assert_eq!(
            db.query_one(sql, &[&doomed])
                .await
                .unwrap()
                .get::<_, i32>(0),
            expected,
            "{sql}"
        );
    }
    assert_eq!(
        db.query_one(
            "SELECT status,chat_session_id FROM agent_task_queue WHERE id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "cancelled"
    );
    assert_eq!(
        db.query_one(
            "SELECT chat_session_id FROM agent_task_queue WHERE id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, Option<Uuid>>(0),
        None
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*)::int FROM task_token WHERE task_id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, i32>(0),
        0
    );
    assert_eq!(
        delete(&app, ws, &doomed.to_string(), owner, false).await,
        StatusCode::NOT_FOUND
    );
}
