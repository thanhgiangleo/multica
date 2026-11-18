//! Synthetic archived page: group selection, cursor stability and cross-tenant isolation.
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
    (
        response.status(),
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on a random loopback port"]
async fn archived_page_uses_latest_group_before_filter_and_stable_cursor() {
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
    for name in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("archive-{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
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
            &[&format!("other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$5,'owner')", &[&ws,&users[0],&users[1],&foreign,&users[2]]).await.unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,'Archived','member',$2,'in_review','high',987) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let hidden: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,'Active','member',$2,'done','low',988) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let actor = Uuid::new_v4();
    let comment = Uuid::new_v4();
    let insert = "INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,issue_id,title,actor_type,actor_id,archived,read,created_at,details,body) VALUES ($1,'member',$2,$3,$4,'Archive',$5,$6,$7,$8,now()+$9::text::interval,$10::jsonb,$11) RETURNING id";
    let add = |recipient,
               issue_id,
               kind,
               actor_type,
               actor_id,
               archived,
               read,
               delay: &'static str,
               details: Value,
               body: Option<String>| {
        let db = &db;
        async move {
            db.query_one(
                insert,
                &[
                    &ws,
                    &recipient,
                    &kind,
                    &issue_id,
                    &actor_type,
                    &actor_id,
                    &archived,
                    &read,
                    &delay,
                    &details,
                    &body,
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0)
        }
    };
    add(
        users[0],
        Some(issue),
        "new_comment",
        Some("agent"),
        Some(actor),
        true,
        false,
        "-10 minutes",
        json!({"comment_id":comment}),
        Some("A".repeat(250)),
    )
    .await;
    let newest = add(
        users[0],
        Some(issue),
        "mentioned",
        Some("system"),
        None,
        true,
        true,
        "-9 minutes",
        json!({}),
        None,
    )
    .await;
    add(
        users[0],
        Some(hidden),
        "mentioned",
        Some("system"),
        None,
        true,
        false,
        "-8 minutes",
        json!({}),
        None,
    )
    .await;
    add(
        users[0],
        Some(hidden),
        "mentioned",
        Some("system"),
        None,
        false,
        false,
        "-7 minutes",
        json!({}),
        None,
    )
    .await;
    add(
        users[1],
        Some(issue),
        "mentioned",
        Some("system"),
        None,
        true,
        false,
        "-6 minutes",
        json!({}),
        None,
    )
    .await;
    let base = chrono::Utc::now();
    let mut count = 0;
    for n in 0..103 {
        let id = Uuid::new_v4();
        db.execute("INSERT INTO inbox_item(id,workspace_id,recipient_type,recipient_id,type,title,actor_type,archived,read,created_at) VALUES ($1,$2,'member',$3,'mentioned','Other','system',true,false,$4)", &[&id,&ws,&users[0],&base]).await.unwrap();
        count += 1;
        assert_eq!(n + 1, count);
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"archive-page-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"archive-page-secret")).unwrap()
    };
    let path = format!("/api/inbox/archived/page?workspace_id={ws}");
    assert_eq!(get(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        get(&app, &path, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(
            &app,
            &format!("{path}&group_id={issue}&actors=system&statuses=in_review&priorities=high"),
            Some(&jwt(users[0]))
        )
        .await
        .1["items"][0]["id"],
        newest.to_string()
    );
    let detail = get(
        &app,
        &format!("{path}&group_id={issue}"),
        Some(&jwt(users[0])),
    )
    .await
    .1;
    assert_eq!(
        detail["items"][0]["details"]["comment_id"],
        comment.to_string()
    );
    assert_eq!(detail["items"][0]["issue_status"], "in_review");
    assert_eq!(
        get(
            &app,
            &format!("{path}&group_id={issue}&unread_only=true"),
            Some(&jwt(users[0]))
        )
        .await
        .1["items"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        get(
            &app,
            &format!("{path}&group_id={hidden}"),
            Some(&jwt(users[0]))
        )
        .await
        .1["items"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        get(
            &app,
            &format!("{path}&group_id={issue}"),
            Some(&jwt(users[1]))
        )
        .await
        .1["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        get(
            &app,
            &format!("/api/inbox/archived/page?workspace_id={foreign}&group_id={issue}"),
            Some(&jwt(users[2]))
        )
        .await
        .1["items"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    let mut cursor = String::new();
    let mut seen = std::collections::HashSet::new();
    let mut pages = 0;
    loop {
        let result = get(
            &app,
            &format!("{path}&limit=50&cursor={cursor}"),
            Some(&jwt(users[0])),
        )
        .await;
        assert_eq!(result.0, StatusCode::OK, "{result:?}");
        pages += 1;
        for item in result.1["items"].as_array().unwrap() {
            assert!(seen.insert(item["id"].as_str().unwrap().to_owned()));
        }
        if !result.1["has_more"].as_bool().unwrap() {
            assert!(result.1["next_cursor"].is_null());
            break;
        }
        cursor = result.1["next_cursor"].as_str().unwrap().to_owned();
        assert!(pages < 4);
    }
    assert_eq!(seen.len(), 104);
    assert_eq!(pages, 3);
    for query in [
        "limit=0",
        "limit=101",
        "group_id=bad",
        "cursor=broken",
        "actors=system,,agent",
    ] {
        assert_eq!(
            get(&app, &format!("{path}&{query}"), Some(&jwt(users[0])))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        get(
            &app,
            &format!("{path}&cursor={cursor}&unread_only=true"),
            Some(&jwt(users[0]))
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let pat = format!("mul_{}", Uuid::new_v4().simple());
    let hash = format!("{:x}", Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'page test',$2,'mul_page')", &[&users[0],&hash]).await.unwrap();
    assert_eq!(get(&app, &path, Some(&pat)).await.0, StatusCode::OK);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("ALTER TABLE inbox_item RENAME TO inbox_item_page_test", &[])
        .await
        .unwrap();
    assert_eq!(
        get(&app, &path, Some(&jwt(users[0]))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to load archived inbox page"})
        )
    );
    db.execute("ALTER TABLE inbox_item_page_test RENAME TO inbox_item", &[])
        .await
        .unwrap();
}
