//! Committed inbox writes reach only the authenticated recipient's workspace socket.
use deadpool_postgres::{Manager, Pool};
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio_postgres::{Config, NoTls};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

fn token(user: Uuid) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":exp}),
        &EncodingKey::from_secret(b"inbox-bridge-test"),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires owned loopback PostgreSQL database multica_zbsi69"]
async fn committed_inbox_event_is_private_and_membership_is_rechecked() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(6)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let mut users = Vec::new();
    for name in ["recipient", "peer"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[
                    &name,
                    &format!("inbox-bridge-{name}-{suffix}@invalid.example"),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Inbox Bridge',$1) RETURNING id",
            &[&format!("inbox-bridge-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Other Inbox Bridge',$1) RETURNING id",
            &[&format!("inbox-bridge-other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member'),($1,$3,'owner'),($4,$2,'member')", &[&ws,&users[0],&users[1],&foreign]).await.unwrap();
    let item: Uuid = db.query_one("INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,title,body) VALUES ($1,'member',$2,'new_comment','Private notice','Synthetic') RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let pinned_issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,'Private pin','member',$2,'todo','medium',101) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, b"inbox-bridge-test".to_vec())
            .with_local_inbox_events()
            .with_local_pin_events(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let request = |workspace: Uuid, user: Uuid| {
        let mut req = format!("ws://127.0.0.1:{port}/ws?workspace_id={workspace}")
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            "cookie",
            format!("multica_auth={}", token(user)).parse().unwrap(),
        );
        req
    };
    let (mut own, _) = connect_async(request(ws, users[0])).await.unwrap();
    let (mut peer, _) = connect_async(request(ws, users[1])).await.unwrap();
    let (mut other, _) = connect_async(request(foreign, users[0])).await.unwrap();
    for socket in [&mut own, &mut peer, &mut other] {
        socket
            .send(Message::Text(json!({"type":"ping"}).to_string().into()))
            .await
            .unwrap();
        let ack = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(ack.to_text().unwrap()).unwrap()["type"],
            "pong"
        );
    }
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/api/inbox/{item}/read?workspace_id={ws}");
    let response = client
        .post(&url)
        .bearer_auth(token(users[0]))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let message = tokio::time::timeout(Duration::from_secs(3), own.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(frame["type"], "inbox:read");
    assert_eq!(
        frame["payload"],
        json!({"item_id":item,"recipient_id":users[0]})
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), peer.next())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), other.next())
            .await
            .is_err()
    );
    let denied = client
        .post(&url)
        .bearer_auth(token(users[1]))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::NOT_FOUND);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), own.next())
            .await
            .is_err()
    );
    let response = client
        .post(format!(
            "http://127.0.0.1:{port}/api/pins/?workspace_id={ws}"
        ))
        .bearer_auth(token(users[0]))
        .header("content-type", "application/json")
        .body(json!({"item_type":"issue", "item_id":pinned_issue}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let pin: Value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    let message = tokio::time::timeout(Duration::from_secs(3), own.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(frame["type"], "pin:created");
    assert_eq!(frame["payload"], json!({"pin":pin}));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM pinned_item WHERE workspace_id=$1 AND user_id=$2 AND item_id=$3",
            &[&ws, &users[0], &pinned_issue]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), peer.next())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), other.next())
            .await
            .is_err()
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &users[0]],
    )
    .await
    .unwrap();
    own.send(Message::Text(json!({"type":"ping"}).to_string().into()))
        .await
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(3), own.next())
        .await
        .unwrap();
    assert!(matches!(closed, None | Some(Ok(Message::Close(_)))));
    server.abort();
    server.await.unwrap_err();
}
