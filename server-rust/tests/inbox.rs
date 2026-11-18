//! Synthetic PostgreSQL inbox contract: own/member isolation and grouped unread semantics.
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
    (
        status,
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on a random loopback port"]
async fn inbox_list_raw_count_and_grouped_summary() {
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
                &[&role, &format!("inbox-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Inbox A',$1) RETURNING id",
            &[&format!("inbox-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Inbox B',$1) RETURNING id",
            &[&format!("inbox-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'member'),($4,$5,'owner')", &[&ws,&users[0],&users[1],&other,&users[2]]).await.unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,'Inbox issue','member',$2,'in_progress','high',987) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let preview = "🧪".repeat(201);
    let id: Uuid = db.query_one("INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,issue_id,title,body,details,created_at) VALUES ($1,'member',$2,'new_comment',$3,'Older',$4,$5,now()-interval '1 hour') RETURNING id", &[&ws,&users[0],&issue,&preview,&json!({"comment_id":"synthetic"})]).await.unwrap().get(0);
    db.execute("INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,issue_id,title,read,created_at) VALUES ($1,'member',$2,'new_comment',$3,'Newest',true,now())", &[&ws,&users[0],&issue]).await.unwrap();
    db.execute("INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,title,body) VALUES ($1,'member',$2,'notice','Independent','Full text')", &[&ws,&users[0]]).await.unwrap();
    db.execute("INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,title,body) VALUES ($1,'member',$2,'notice','Different recipient','Secret')", &[&ws,&users[1]]).await.unwrap();
    db.execute("INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,title) VALUES ($1,'agent',$2,'notice','Agent notification')", &[&ws,&users[0]]).await.unwrap();
    db.execute("INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,title,archived) VALUES ($1,'member',$2,'notice','Archived',true)", &[&ws,&users[0]]).await.unwrap();
    db.execute("INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,title) VALUES ($1,'member',$2,'notice','Cross workspace')", &[&other,&users[0]]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"inbox-test-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"inbox-test-secret")).unwrap()
    };
    let list_path = format!("/api/inbox/?workspace_id={ws}");
    assert_eq!(
        get(
            &app,
            &format!("/api/inbox/?workspace_id={other}"),
            Some(&jwt(users[2]))
        )
        .await,
        (StatusCode::OK, json!([]))
    );
    assert_eq!(
        get(
            &app,
            &format!("/api/inbox/unread-count?workspace_id={other}"),
            Some(&jwt(users[2]))
        )
        .await
        .1,
        json!({"count":0})
    );
    assert_eq!(
        get(
            &app,
            &format!("/api/inbox/unread-summary?workspace_id={other}"),
            Some(&jwt(users[2]))
        )
        .await
        .1,
        json!([])
    );
    assert_eq!(
        get(&app, &list_path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&app, &list_path, Some("bad")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&app, &list_path, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, "/api/inbox/unread-summary", Some(&jwt(users[0])))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let (status, rows) = get(&app, &list_path, Some(&jwt(users[0]))).await;
    assert_eq!(status, StatusCode::OK);
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 3);
    let older = rows.iter().find(|r| r["id"] == id.to_string()).unwrap();
    assert_eq!(older["body"], format!("{}…", "🧪".repeat(199)));
    assert_eq!(older["issue_status"], "in_progress");
    assert_eq!(older["issue_priority"], "high");
    assert_eq!(older["details"], json!({"comment_id":"synthetic"}));
    assert_eq!(
        get(
            &app,
            &format!("/api/inbox/unread-count?workspace_id={ws}"),
            Some(&jwt(users[0]))
        )
        .await
        .1,
        json!({"count":2})
    );
    let summary = get(
        &app,
        &format!("/api/inbox/unread-summary?workspace_id={ws}"),
        Some(&jwt(users[0])),
    )
    .await
    .1;
    let mut actual = summary.as_array().unwrap().clone();
    actual.sort_by_key(|x| x["workspace_id"].as_str().unwrap().to_owned());
    let mut expected = json!([{"workspace_id":ws,"count":1},{"workspace_id":other,"count":1}])
        .as_array()
        .unwrap()
        .clone();
    expected.sort_by_key(|x| x["workspace_id"].as_str().unwrap().to_owned());
    assert_eq!(actual, expected);
    assert_eq!(
        get(&app, &list_path, Some(&jwt(users[1])))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        1
    );
    // A revoked membership must immediately remove the other workspace from summary.
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&other, &users[0]],
    )
    .await
    .unwrap();
    assert_eq!(
        get(
            &app,
            &format!("/api/inbox/unread-summary?workspace_id={ws}"),
            Some(&jwt(users[0]))
        )
        .await
        .1,
        json!([{"workspace_id":ws,"count":1}])
    );
    let pat = format!("mul_{}", Uuid::new_v4().simple());
    let hash = format!("{:x}", Sha256::digest(pat.as_bytes()));
    db.execute(
        "INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'inbox test',$2,'mul_inbox')",
        &[&users[0], &hash],
    )
    .await
    .unwrap();
    assert_eq!(get(&app, &list_path, Some(&pat)).await.0, StatusCode::OK);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &list_path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    // SQL faults must not leak the member's notifications through response bodies.
    db.execute(
        "ALTER TABLE inbox_item RENAME TO inbox_item_inbox_test",
        &[],
    )
    .await
    .unwrap();
    for (path, message) in [
        (list_path.clone(), "failed to list inbox"),
        (
            format!("/api/inbox/unread-count?workspace_id={ws}"),
            "failed to count unread inbox",
        ),
        (
            format!("/api/inbox/unread-summary?workspace_id={ws}"),
            "failed to summarize unread inbox",
        ),
    ] {
        assert_eq!(
            get(&app, &path, Some(&jwt(users[0]))).await,
            (StatusCode::INTERNAL_SERVER_ERROR, json!({"error":message}))
        );
    }
    db.execute(
        "ALTER TABLE inbox_item_inbox_test RENAME TO inbox_item",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "DELETE FROM workspace WHERE id=ANY($1::uuid[])",
        &[&vec![ws, other]],
    )
    .await
    .unwrap();
    for user in users {
        db.execute("DELETE FROM \"user\" WHERE id=$1", &[&user])
            .await
            .unwrap();
    }
}
