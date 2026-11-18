//! Workspace-isolated preference mutations against a disposable migrated database.
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
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &str = "isolated-zbsi65-preference-secret";

async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    credential: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn account_catalog_preferences_scope_validation_merge_and_replace() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi65"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|host| matches!(host, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(6)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Prefs test',$1) RETURNING id",
            &[&format!("prefs-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Other',$1) RETURNING id",
            &[&format!("other-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Prefs ws',$1) RETURNING id",
            &[&format!("prefs-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Foreign ws',$1) RETURNING id",
            &[&format!("foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&ws, &other],
    )
    .await
    .unwrap();
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Prefs project') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Foreign project') RETURNING id",
            &[&foreign],
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
        ApiState::new(pool, SECRET.as_bytes().to_vec()),
    );
    let jwt = |u: Uuid| {
        encode(
            &Header::default(),
            &json!({"sub":u.to_string(),"exp":chrono::Utc::now().timestamp()+3600}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let token = jwt(user);
    let path = format!("/api/notification-preferences/?workspace_id={ws}");
    let view = format!("/api/issue-view-preferences?workspace_id={ws}");
    assert_eq!(
        call(
            &app,
            "PUT",
            &path,
            &token,
            json!({"preferences":{"assignments":"all"}})
        )
        .await,
        (
            StatusCode::OK,
            json!({"workspace_id":ws,"preferences":{"assignments":"all"}})
        )
    );
    let (first, second) = tokio::join!(
        call(
            &app,
            "PATCH",
            &path,
            &token,
            json!({"preferences":{"comments":"muted"}})
        ),
        call(
            &app,
            "PATCH",
            &path,
            &token,
            json!({"preferences":{"mentions":"all"}})
        )
    );
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::OK);
    assert_eq!(
        call(&app, "GET", &path, &token, Value::Null).await.1["preferences"],
        json!({"assignments":"all","comments":"muted","mentions":"all"})
    );
    assert_eq!(
        call(&app, "PUT", &path, &token, json!({"preferences":{}}))
            .await
            .1["preferences"],
        json!({})
    );
    assert_eq!(
        call(&app, "PUT", &path, &token, json!({"preferences":null}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &path,
            &token,
            json!({"preferences":{"comments":"invalid"}})
        )
        .await
        .1,
        json!({"error":"invalid preference value: invalid"})
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &path,
            &token,
            json!({"preferences":{"unknown":"all"}})
        )
        .await
        .1,
        json!({"error":"invalid preference group: unknown"})
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &view,
            &token,
            json!({"scope_type":"workspace","prefs":{"hidden":["builtin:all"]}})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{view}&scope_type=workspace"),
            &token,
            Value::Null
        )
        .await
        .1["prefs"],
        json!({"hidden":["builtin:all"]})
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &view,
            &token,
            json!({"scope_type":"my","prefs":{}})
        )
        .await
        .1["scope_id"],
        user.to_string()
    );
    assert_eq!(call(&app,"PUT",&view,&token,json!({"scope_type":"project","scope_id":project.to_string(),"prefs":{"order":["builtin:all"]}})).await.0, StatusCode::OK);
    assert_eq!(
        call(
            &app,
            "PUT",
            &view,
            &token,
            json!({"scope_type":"project","scope_id":foreign_project.to_string(),"prefs":{}})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &view,
            &token,
            json!({"scope_type":"project","scope_id":"wrong","prefs":{}})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &view,
            &token,
            json!({"scope_type":"project","prefs":{}})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &view,
            &token,
            json!({"scope_type":"workspace","prefs":[]})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("/api/notification-preferences/?workspace_id={foreign}"),
            &token,
            json!({"preferences":{}})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &path,
            &jwt(Uuid::new_v4()),
            json!({"preferences":{}})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "PUT", &path, "mul_invalid", json!({"preferences":{}}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "GET", &path, &jwt(other), Value::Null).await.1["preferences"],
        json!({})
    );
    let cookie = Request::builder()
        .method("PUT")
        .uri(&path)
        .header("cookie", format!("multica_auth={token}"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"preferences":{}}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_view_preference WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 3);
    let prefs: Value = db
        .query_one(
            "SELECT preferences FROM notification_preference WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(prefs, json!({}));

    // Each case begins without a preference row, proving failed validation has no DB effect.
    let cases: Vec<Value> = serde_json::from_str(include_str!(
        "account_catalog_write_notification_oracle.json"
    ))
    .unwrap();
    for method in ["PUT", "PATCH"] {
        for case in &cases {
            db.execute(
                "DELETE FROM notification_preference WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &user],
            )
            .await
            .unwrap();
            let raw = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                case["body"].as_str().unwrap(),
            )
            .unwrap();
            let request = Request::builder()
                .method(method)
                .uri(&path)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(raw))
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            let status = response.status();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            let label = case["label"].as_str().unwrap();
            let expected_error = if case["decode_error"] == true {
                Some("invalid request body")
            } else if case["preferences"].is_null() {
                Some("preferences field is required")
            } else {
                case["validation_error"].as_str()
            };
            if let Some(message) = expected_error {
                assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {label}");
                assert_eq!(body, json!({"error":message}), "{method} {label}");
                assert!(db.query_opt("SELECT 1 FROM notification_preference WHERE workspace_id=$1 AND user_id=$2", &[&ws, &user]).await.unwrap().is_none(), "{method} {label}");
            } else {
                assert_eq!(status, StatusCode::OK, "{method} {label}: {body}");
                assert_eq!(body["preferences"], case["preferences"], "{method} {label}");
                let saved: Value = db.query_one("SELECT preferences FROM notification_preference WHERE workspace_id=$1 AND user_id=$2", &[&ws, &user]).await.unwrap().get(0);
                assert_eq!(saved, case["preferences"], "{method} {label}");
            }
        }
    }

    // Raw request bytes exercise Go's duplicate keys, RawMessage, 128 KiB cap
    // and trailing-data semantics through the actual HTTP/DB boundary.
    let view_cases: Vec<Value> = serde_json::from_str(include_str!(
        "account_catalog_write_view_preference_oracle.json"
    ))
    .unwrap();
    #[derive(serde::Deserialize)]
    struct ViewReply {
        scope_type: String,
        scope_id: Uuid,
        prefs: Box<serde_json::value::RawValue>,
        updated_at: String,
    }
    for case in &view_cases {
        db.execute(
            "DELETE FROM issue_view_preference WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .unwrap();
        let label = case["label"].as_str().unwrap();
        let raw = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            case["body"].as_str().unwrap(),
        )
        .unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(&view)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(raw))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let scope = case["scope_type"].as_str().unwrap_or("");
        let prefs = case["prefs_is_object"] == true;
        let expected_error = if case["decode_error"] == true {
            Some((StatusCode::BAD_REQUEST, "invalid request body"))
        } else if !matches!(scope, "workspace" | "my" | "project") {
            Some((StatusCode::BAD_REQUEST, "invalid scope_type"))
        } else if scope == "project" && case["scope_id"].as_str().unwrap_or("").is_empty() {
            Some((
                StatusCode::BAD_REQUEST,
                "scope_id is required for project scope",
            ))
        } else if scope == "project" {
            Some((StatusCode::BAD_REQUEST, "invalid scope_id"))
        } else if !prefs {
            Some((StatusCode::BAD_REQUEST, "prefs must be a JSON object"))
        } else if matches!(
            label,
            "surrogate-raw-prefs"
                | "invalid-utf8-raw-prefs"
                | "nul-raw-prefs"
                | "too-large-postgres-number"
        ) {
            Some((
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to save preference",
            ))
        } else {
            None
        };
        if let Some((expected_status, message)) = expected_error {
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(status, expected_status, "{label}: {body}");
            assert_eq!(body, json!({"error":message}), "{label}");
            assert!(
                db.query_opt(
                    "SELECT 1 FROM issue_view_preference WHERE workspace_id=$1 AND user_id=$2",
                    &[&ws, &user]
                )
                .await
                .unwrap()
                .is_none(),
                "{label}"
            );
        } else {
            let reply: ViewReply =
                serde_json::from_slice(&bytes).unwrap_or_else(|error| panic!("{label}: {error}"));
            assert_eq!(status, StatusCode::OK, "{label}: {}", reply.scope_type);
            let id = if scope == "workspace" { ws } else { user };
            assert_eq!(reply.scope_type, scope, "{label}");
            assert_eq!(reply.scope_id, id, "{label}");
            assert!(!reply.updated_at.is_empty(), "{label}");
            let saved: String = db
                .query_one(
                    "SELECT prefs::text FROM issue_view_preference WHERE workspace_id=$1 AND user_id=$2 AND scope_type=$3 AND scope_id=$4",
                    &[&ws, &user, &scope, &id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(reply.prefs.get(), saved, "{label}");
            let oracle_prefs = String::from_utf8(
                base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    case["prefs_base64"].as_str().unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
            let expected: String = db
                .query_one("SELECT $1::text::jsonb::text", &[&oracle_prefs])
                .await
                .unwrap()
                .get(0);
            assert_eq!(saved, expected, "{label}");
        }
    }

    let workspace_body = json!({"scope_type":"workspace","prefs":{"survives":true}});
    assert_eq!(
        call(&app, "PUT", &view, &token, workspace_body).await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &view,
            &token,
            json!({"scope_type":"workspace","prefs":null})
        )
        .await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"prefs must be a JSON object"})
        )
    );
    let saved: Value = db.query_one(
        "SELECT prefs FROM issue_view_preference WHERE workspace_id=$1 AND user_id=$2 AND scope_type='workspace'",
        &[&ws,&user],
    ).await.unwrap().get(0);
    assert_eq!(saved, json!({"survives":true}));
    assert_eq!(
        call(
            &app,
            "PUT",
            &view,
            &jwt(Uuid::new_v4()),
            json!({"scope_type":"workspace"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &view,
            "mul_invalid",
            json!({"scope_type":"workspace"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("/api/issue-view-preferences?workspace_id={foreign}"),
            &token,
            json!({"scope_type":"workspace"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let alternate_project = project.to_string().replace('-', "x");
    let (status, reply) = call(
        &app,
        "PUT",
        &view,
        &token,
        json!({"scope_type":"project","scope_id":alternate_project,"prefs":{"uuid":true}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reply["scope_id"], json!(project));
    assert_eq!(
        db.query_one("SELECT prefs FROM issue_view_preference WHERE workspace_id=$1 AND user_id=$2 AND scope_type='project' AND scope_id=$3", &[&ws, &user, &project])
            .await.unwrap().get::<_, Value>(0),
        json!({"uuid":true})
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &view,
            &token,
            json!({"scope_type":"project","scope_id":format!("urn:uuid:{project}"),"prefs":{}})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let cookie = Request::builder()
        .method("PUT")
        .uri(&view)
        .header("cookie", format!("multica_auth={token}"))
        .body(Body::from(r#"{"scope_type":"workspace"}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let (first, second) = tokio::join!(
        call(
            &app,
            "PUT",
            &view,
            &token,
            json!({"scope_type":"workspace","prefs":{"first":true}})
        ),
        call(
            &app,
            "PUT",
            &view,
            &token,
            json!({"scope_type":"workspace","prefs":{"second":true}})
        ),
    );
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::OK);
    let saved: Value = db.query_one(
        "SELECT prefs FROM issue_view_preference WHERE workspace_id=$1 AND user_id=$2 AND scope_type='workspace'",
        &[&ws,&user],
    ).await.unwrap().get(0);
    assert!(saved == json!({"first":true}) || saved == json!({"second":true}));
    let large_tail = format!(
        r#"{{"scope_type":"workspace","prefs":{{"tail":true}}}}{}"#,
        "x".repeat(3_000_000)
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&view)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(large_tail))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let saved: Value = db.query_one(
        "SELECT prefs FROM issue_view_preference WHERE workspace_id=$1 AND user_id=$2 AND scope_type='workspace'",
        &[&ws,&user],
    ).await.unwrap().get(0);
    assert_eq!(saved, json!({"tail":true}));
}
