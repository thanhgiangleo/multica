//! Quick-agent pin mutations against a disposable migrated database.
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

async fn request(
    app: &axum::Router,
    method: Method,
    ws: Uuid,
    credential: &str,
    agent: &str,
    body: Option<Value>,
) -> (StatusCode, Option<Value>) {
    let path = if method == Method::DELETE {
        format!("/api/chat/pinned-agents/{agent}?workspace_id={ws}")
    } else {
        format!("/api/chat/pinned-agents?workspace_id={ws}")
    };
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {credential}"));
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let response = app
        .clone()
        .oneshot(
            builder
                .body(Body::from(body.map(|b| b.to_string()).unwrap_or_default()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let payload = (!bytes.is_empty()).then(|| serde_json::from_slice(&bytes).unwrap());
    (status, payload)
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn pin_is_scoped_idempotent_and_concurrency_capped() {
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
            .max_size(12)
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
                &[&label, &format!("pin-{label}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace (name,slug) VALUES ('Chat',$1) RETURNING id",
            &[&format!("zbsi67-pin-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace (name,slug) VALUES ('Other',$1) RETURNING id",
            &[&format!("zbsi67-pin-other-{suffix}")],
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
    let mut agents = Vec::new();
    for n in 0..7 {
        agents.push(db.query_one("INSERT INTO agent (name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ($3,$1,$2,'user','public_to','cloud') RETURNING id", &[&ws,&users[0],&format!("Public {n}")]).await.unwrap().get::<_,Uuid>(0));
    }
    let private: Uuid = db.query_one("INSERT INTO agent (name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Private',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let system: Uuid = db.query_one("INSERT INTO agent (name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Carrier',$1,$2,'system','public_to','cloud') RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let foreign: Uuid = db.query_one("INSERT INTO agent (name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Foreign',$1,$2,'user','public_to','cloud') RETURNING id", &[&other,&users[0]]).await.unwrap().get(0);
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
    let owner = jwt(users[0]);
    let member = jwt(users[1]);
    let outsider = jwt(users[2]);
    let pin = |agent: Uuid| Some(json!({"agent_id":agent}));
    assert_eq!(
        request(
            &app,
            Method::POST,
            ws,
            &owner,
            "",
            Some(json!({"agent_id":"bad"}))
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, Method::POST, ws, &owner, "", Some(json!({})))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, Method::POST, ws, &outsider, "", pin(agents[0]))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, Method::POST, ws, &owner, "", pin(foreign))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, Method::POST, ws, &owner, "", pin(system))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, Method::POST, ws, &member, "", pin(private))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, Method::POST, ws, &member, "", pin(agents[0]))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute("INSERT INTO agent_invocation_target (agent_id,target_type,target_id) VALUES ($1,'workspace',$2)", &[&agents[0],&ws]).await.unwrap();
    assert_eq!(
        request(&app, Method::GET, ws, &member, "", None).await,
        (StatusCode::OK, Some(json!([])))
    );
    let (status, first) = request(&app, Method::POST, ws, &member, "", pin(agents[0])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first.unwrap()["position"], json!(1.0));
    assert_eq!(
        request(&app, Method::POST, ws, &owner, "", pin(agents[0]))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        request(&app, Method::GET, ws, &member, "", None)
            .await
            .1
            .unwrap()[0]["agent_id"],
        json!(agents[0])
    );
    assert_eq!(
        request(&app, Method::GET, other, &owner, "", None).await,
        (StatusCode::OK, Some(json!([])))
    );
    db.execute(
        "DELETE FROM agent_invocation_target WHERE agent_id=$1",
        &[&agents[0]],
    )
    .await
    .unwrap();
    assert_eq!(
        request(&app, Method::GET, ws, &member, "", None).await,
        (StatusCode::OK, Some(json!([])))
    );
    assert_eq!(
        request(&app, Method::POST, ws, &member, "", pin(agents[0]))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (status, again) = request(&app, Method::POST, ws, &owner, "", pin(agents[0])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again.unwrap()["position"], json!(1.0));
    assert_eq!(
        request(&app, Method::DELETE, ws, &owner, "bad", None)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(
            &app,
            Method::DELETE,
            other,
            &owner,
            &agents[0].to_string(),
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        request(
            &app,
            Method::DELETE,
            ws,
            &outsider,
            &agents[0].to_string(),
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(
            &app,
            Method::DELETE,
            ws,
            &member,
            &agents[0].to_string(),
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let remaining: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM chat_pinned_agent WHERE workspace_id=$1 AND agent_id=$2",
            &[&ws, &agents[0]],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 1);

    // Multiple device requests for the same user must not exceed the cap.
    let results = tokio::join!(
        request(&app, Method::POST, ws, &owner, "", pin(agents[1])),
        request(&app, Method::POST, ws, &owner, "", pin(agents[2])),
        request(&app, Method::POST, ws, &owner, "", pin(agents[3])),
        request(&app, Method::POST, ws, &owner, "", pin(agents[4])),
        request(&app, Method::POST, ws, &owner, "", pin(agents[5])),
        request(&app, Method::POST, ws, &owner, "", pin(agents[6])),
    );
    let results = [
        results.0, results.1, results.2, results.3, results.4, results.5,
    ];
    assert_eq!(
        results.iter().filter(|r| r.0 == StatusCode::OK).count(),
        4,
        "{results:?}"
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| r.0 == StatusCode::BAD_REQUEST)
            .count(),
        2,
        "{results:?}"
    );
    let rows = db.query("SELECT agent_id,position FROM chat_pinned_agent WHERE workspace_id=$1 AND user_id=$2 ORDER BY position", &[&ws,&users[0]]).await.unwrap();
    assert_eq!(rows.len(), 5);
    let mut positions = rows
        .iter()
        .map(|r| r.get::<_, f64>("position"))
        .collect::<Vec<_>>();
    positions.sort_by(f64::total_cmp);
    assert_eq!(positions, [1., 2., 3., 4., 5.]);
    let (status, again) = request(&app, Method::POST, ws, &owner, "", pin(agents[0])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again.unwrap()["position"], json!(1.0));
    assert_eq!(
        request(
            &app,
            Method::DELETE,
            ws,
            &owner,
            &agents[0].to_string(),
            None
        )
        .await,
        (StatusCode::NO_CONTENT, None)
    );
    assert_eq!(
        request(
            &app,
            Method::DELETE,
            ws,
            &owner,
            &agents[0].to_string(),
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM chat_pinned_agent WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &users[0]],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 4);
}
