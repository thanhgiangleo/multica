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
async fn legacy_archive_limits_groups_preserves_anchor_and_isolates_recipient() {
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
                &[
                    &name,
                    &format!("archive-legacy-{name}-{suffix}@invalid.example"),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Legacy Archive',$1) RETURNING id",
            &[&format!("archive-legacy-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Other Archive',$1) RETURNING id",
            &[&format!("archive-other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$5,'owner')", &[&ws,&users[0],&users[1],&other,&users[2]]).await.unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,'Archived','member',$2,'in_review','high',998) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let hidden: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,'Hidden','member',$2,'done','low',999) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let insert = "INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,issue_id,title,archived,created_at,body,details) VALUES ($1,'member',$2,$3,$4,$5,$6,now()+$7::text::interval,$8,$9) RETURNING id";
    let comment: Uuid = db
        .query_one(
            insert,
            &[
                &ws,
                &users[0],
                &"new_comment",
                &Some(issue),
                &"Comment",
                &true,
                &"-10 minutes",
                &Some("🧪".repeat(201)),
                &json!({"comment_id":"anchor"}),
            ],
        )
        .await
        .unwrap()
        .get(0);
    let newest: Uuid = db
        .query_one(
            insert,
            &[
                &ws,
                &users[0],
                &"notice",
                &Some(issue),
                &"Newest",
                &true,
                &"-9 minutes",
                &None::<String>,
                &json!({}),
            ],
        )
        .await
        .unwrap()
        .get(0);
    db.query_one(
        insert,
        &[
            &ws,
            &users[0],
            &"notice",
            &Some(hidden),
            &"Hidden old",
            &true,
            &"-8 minutes",
            &None::<String>,
            &json!({}),
        ],
    )
    .await
    .unwrap();
    db.query_one(
        insert,
        &[
            &ws,
            &users[0],
            &"notice",
            &Some(hidden),
            &"Active",
            &false,
            &"-7 minutes",
            &None::<String>,
            &json!({}),
        ],
    )
    .await
    .unwrap();
    db.query_one(
        insert,
        &[
            &ws,
            &users[1],
            &"notice",
            &None::<Uuid>,
            &"Other member secret",
            &true,
            &"-6 minutes",
            &None::<String>,
            &json!({}),
        ],
    )
    .await
    .unwrap();
    db.query_one(
        insert,
        &[
            &other,
            &users[2],
            &"notice",
            &None::<Uuid>,
            &"Other workspace secret",
            &true,
            &"-5 minutes",
            &None::<String>,
            &json!({}),
        ],
    )
    .await
    .unwrap();
    for n in 0..205 {
        db.query_one(
            insert,
            &[
                &ws,
                &users[0],
                &"notice",
                &None::<Uuid>,
                &format!("Group {n}"),
                &true,
                &format!("{} seconds", n + 1),
                &None::<String>,
                &json!({}),
            ],
        )
        .await
        .unwrap();
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"legacy-archive-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"legacy-archive-secret")).unwrap()
    };
    let path = format!("/api/inbox/archived?workspace_id={ws}");
    assert_eq!(get(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        get(&app, &path, Some("bad")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&app, &path, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    let (status, rows) = get(&app, &path, Some(&jwt(users[0]))).await;
    assert_eq!(status, StatusCode::OK);
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 200);
    assert!(!rows.iter().any(|r| r["issue_id"] == hidden.to_string()));
    assert!(
        !rows
            .iter()
            .any(|r| r["title"] == "Other member secret" || r["title"] == "Other workspace secret")
    );
    // The latest 200 groups must be bounded before choosing both rows per group.
    assert!(
        !rows
            .iter()
            .any(|r| r["id"] == comment.to_string() || r["id"] == newest.to_string())
    );
    db.execute("DELETE FROM inbox_item WHERE title LIKE 'Group %'", &[])
        .await
        .unwrap();
    let rows = get(&app, &path, Some(&jwt(users[0]))).await.1;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], newest.to_string());
    assert_eq!(rows[1]["id"], comment.to_string());
    assert_eq!(rows[1]["body"], format!("{}…", "🧪".repeat(199)));
    assert_eq!(rows[1]["issue_status"], "in_review");
    assert_eq!(rows[1]["issue_priority"], "high");
    assert_eq!(rows[1]["details"], json!({"comment_id":"anchor"}));
    let foreign_rows = get(
        &app,
        &format!("/api/inbox/archived?workspace_id={other}"),
        Some(&jwt(users[2])),
    )
    .await
    .1;
    assert_eq!(foreign_rows.as_array().unwrap().len(), 1);
    let pat = format!("mul_{}", Uuid::new_v4().simple());
    let hash = format!("{:x}", Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'legacy archive test',$2,'mul_arch')", &[&users[0],&hash]).await.unwrap();
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
    db.execute(
        "ALTER TABLE inbox_item RENAME TO inbox_item_archived_legacy_test",
        &[],
    )
    .await
    .unwrap();
    let (status, err) = get(&app, &path, Some(&jwt(users[0]))).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(err, json!({"error":"failed to list archived inbox"}));
    db.execute(
        "ALTER TABLE inbox_item_archived_legacy_test RENAME TO inbox_item",
        &[],
    )
    .await
    .unwrap();
}
