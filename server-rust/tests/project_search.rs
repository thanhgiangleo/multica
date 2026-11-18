//! Synthetic workspace-isolated PostgreSQL contract for project search.
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

async fn read(app: &axum::Router, path: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(path);
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on a random loopback port"]
async fn project_search_ranks_and_isolates_real_rows() {
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
                &[&role, &format!("search-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Search A',$1) RETURNING id",
            &[&format!("search-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Search B',$1) RETURNING id",
            &[&format!("search-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member')",
        &[&ws, &users[0], &users[1]],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&other, &users[2]],
    )
    .await
    .unwrap();
    let mut ids = Vec::new();
    for (workspace, title, description, status) in [
        (ws, "blue star", None, "in_progress"),
        (ws, "blue starlight", None, "in_progress"),
        (ws, "a blue star trail", None, "in_progress"),
        (ws, "star blue", None, "in_progress"),
        (
            ws,
            "description match",
            Some("🌟 blue star near Tokyo"),
            "in_progress",
        ),
        (ws, "blue star cancelled", None, "cancelled"),
        (ws, "blue star finished", None, "completed"),
        (other, "blue star private", None, "in_progress"),
    ] {
        ids.push(db.query_one("INSERT INTO project(workspace_id,title,description,status) VALUES ($1,$2,$3,$4) RETURNING id", &[&workspace,&title,&description,&status]).await.unwrap().get::<_, Uuid>(0));
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-project-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"synthetic-project-secret")).unwrap()
    };
    let path = format!("/api/projects/search?workspace_id={ws}&q=blue%20star");
    assert_eq!(read(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &path, Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, &path, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, "/api/projects/search?q=x", Some(&jwt(users[1])))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/projects/search?workspace_id={ws}"),
            Some(&jwt(users[1]))
        )
        .await
        .1,
        json!({"error":"q parameter is required"})
    );
    let (status, response) = read(&app, &path, Some(&jwt(users[1]))).await;
    assert_eq!(status, StatusCode::OK);
    let rows = response["projects"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(
        rows.iter()
            .map(|r| r["id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
        ids[..5].iter().map(ToString::to_string).collect::<Vec<_>>()
    );
    assert_eq!(rows[4]["match_source"], "description");
    assert_eq!(rows[4]["matched_snippet"], "🌟 blue star near Tokyo");
    assert!(rows[..4].iter().all(|r| r.get("matched_snippet").is_none()));
    assert_eq!(rows[0]["workflow_id"], Value::Null);
    assert_eq!(rows[0]["issue_count"], 0);
    assert_eq!(rows[0]["resource_count"], 0);
    assert_eq!(
        read(
            &app,
            &format!("{path}&limit=2&offset=2"),
            Some(&jwt(users[0]))
        )
        .await
        .1["projects"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let closed = read(
        &app,
        &format!("{path}&include_closed=true"),
        Some(&jwt(users[1])),
    )
    .await
    .1;
    assert_eq!(closed["projects"].as_array().unwrap().len(), 7);
    assert_ne!(closed["projects"][0]["id"], ids[5].to_string());
    assert_eq!(
        read(
            &app,
            &format!("/api/projects/search?workspace_id={other}&q=blue"),
            Some(&jwt(users[2]))
        )
        .await
        .1["projects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/projects/search?workspace_id={ws}&q=nonexistent"),
            Some(&jwt(users[0]))
        )
        .await
        .1,
        json!({"projects":[]})
    );
    let mixed: Uuid = db.query_one(
        "INSERT INTO project(workspace_id,title,description) VALUES ($1,'crimson archive','saffron log') RETURNING id",
        &[&ws],
    ).await.unwrap().get(0);
    let mixed_path = format!("/api/projects/search?workspace_id={ws}&q=saffron%20crimson");
    let mixed_result = read(&app, &mixed_path, Some(&jwt(users[1]))).await.1;
    assert_eq!(mixed_result["projects"][0]["id"], mixed.to_string());
    assert_eq!(mixed_result["projects"][0]["match_source"], "description");
    assert_eq!(
        mixed_result["projects"][0]["matched_snippet"],
        "saffron log"
    );
    let wildcard: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'search_100%') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO project(workspace_id,title) VALUES ($1,'searchX1000')",
        &[&ws],
    )
    .await
    .unwrap();
    assert_eq!(
        read(
            &app,
            &format!("/api/projects/search?workspace_id={ws}&q=search_100%25"),
            Some(&jwt(users[1]))
        )
        .await
        .1["projects"][0]["id"],
        wildcard.to_string()
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/projects/search?workspace_id={ws}&q=search_100%25"),
            Some(&jwt(users[1]))
        )
        .await
        .1["projects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        read(
            &app,
            &format!("{path}&limit=garbage&offset=-1"),
            Some(&jwt(users[1]))
        )
        .await
        .1,
        response
    );
    let pat = format!("mul_search_{suffix}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'Synthetic',$2,'mul_search')", &[&users[1],&hash]).await.unwrap();
    assert_eq!(read(&app, &path, Some(&pat)).await.1, response);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "ALTER TABLE project RENAME TO project_search_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[1]))).await.1,
        json!({"error":"failed to search projects"})
    );
    db.execute(
        "ALTER TABLE project_search_unavailable RENAME TO project",
        &[],
    )
    .await
    .unwrap();
}
