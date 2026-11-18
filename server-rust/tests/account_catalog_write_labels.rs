//! HTTP and database checks for workspace label definitions.
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

const SECRET: &str = "zbsi65-labels-synthetic-secret";

async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    call_raw(app, method, path, token, body.to_string().as_bytes(), true).await
}

async fn call_raw(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: &[u8],
    content_type: bool,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"));
    if content_type {
        request = request.header("content-type", "application/json");
    }
    let request = request.body(Body::from(body.to_owned())).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn label_definitions_scope_uniqueness_and_cleanup() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(6)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Labels', $1) RETURNING id",
            &[&format!("labels-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let stranger: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Stranger', $1) RETURNING id",
            &[&format!("stranger-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Labels', $1) RETURNING id",
            &[&format!("labels-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Other', $1) RETURNING id",
            &[&format!("other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec()),
    );
    let jwt = |id: Uuid| {
        encode(
            &Header::default(),
            &json!({"sub":id.to_string(),"exp":chrono::Utc::now().timestamp()+3600}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let owner = jwt(user);
    let outsider = jwt(stranger);
    let collection = format!("/api/labels/?workspace_id={ws}");
    let expired = encode(
        &Header::default(),
        &json!({"sub": user.to_string(), "exp": chrono::Utc::now().timestamp() - 3600}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &collection,
            &expired,
            json!({"name":"No","color":"000000"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let cookie = Request::builder()
        .method("POST")
        .uri(collection.as_str())
        .header("cookie", format!("multica_auth={owner}"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"name":"No","color":"000000"}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let payload = json!({"name":" Bug ","color":"ABCDef", "resource_type":"agent", "description":"  triage  "});
    assert_eq!(
        call(&app, "POST", &collection, &outsider, payload.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/labels/?workspace_id={other}"),
            &owner,
            payload.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for bad in [
        json!({"name":"Hi\n!", "color":"#aaaaaa"}),
        json!({"name":"Valid", "color":"red"}),
        json!({"name":"Valid", "color":"#ffffff", "resource_type":"other"}),
    ] {
        assert_eq!(
            call(&app, "POST", &collection, &owner, bad).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let (created, label) = call(&app, "POST", &collection, &owner, payload.clone()).await;
    assert_eq!(created, StatusCode::CREATED, "{label:?}");
    assert_eq!(label["name"], "Bug");
    assert_eq!(label["description"], "triage");
    assert_eq!(label["color"], "#abcdef");
    assert_eq!(label["resource_type"], "agent");
    assert_eq!(label["usage_count"], 0);
    assert_eq!(
        call(
            &app,
            "POST",
            &collection,
            &owner,
            json!({"name":"bug", "color":"000000", "resource_type":"agent"})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &collection,
            &owner,
            json!({"name":"Bug", "color":"000000", "resource_type":"issue"})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    let race = json!({"name":"Race", "color":"#123456"});
    let (first, second) = tokio::join!(
        call(&app, "POST", &collection, &owner, race.clone()),
        call(&app, "POST", &collection, &owner, race)
    );
    assert!([first.0, second.0].contains(&StatusCode::CREATED));
    assert!([first.0, second.0].contains(&StatusCode::CONFLICT));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_label WHERE workspace_id=$1 AND name='Race'",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    let id = Uuid::parse_str(label["id"].as_str().unwrap()).unwrap();
    let path = format!("/api/labels/{id}/?workspace_id={ws}");
    let foreign = format!("/api/labels/{id}/?workspace_id={other}");
    assert_eq!(
        call(&app, "PUT", &path, &outsider, json!({"name":"Stolen"}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "PUT", &foreign, &owner, json!({"name":"Stolen"}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("/api/labels/nope/?workspace_id={ws}"),
            &owner,
            json!({})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (updated, label) = call(
        &app,
        "PUT",
        &path,
        &owner,
        json!({"description":" new ", "color":"0f0F0f"}),
    )
    .await;
    assert_eq!(updated, StatusCode::OK);
    assert_eq!(label["description"], "new");
    assert_eq!(label["color"], "#0f0f0f");
    assert_eq!(
        db.query_one("SELECT color FROM issue_label WHERE id=$1", &[&id])
            .await
            .unwrap()
            .get::<_, String>(0),
        "#0f0f0f"
    );
    let agent = Uuid::new_v4();
    db.execute(
        "INSERT INTO agent_to_label(agent_id,label_id) VALUES($1,$2)",
        &[&agent, &id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "DELETE", &path, &outsider, Value::Null).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "DELETE", &path, &owner, Value::Null).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_to_label WHERE label_id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        call(&app, "DELETE", &path, &owner, Value::Null).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "GET", &path, &owner, Value::Null).await.0,
        StatusCode::NOT_FOUND
    );

    use base64::Engine;
    let (status, stable) = call(
        &app,
        "POST",
        &collection,
        &owner,
        json!({"name":"Stored","color":"654321","description":"original"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let stable_id: Uuid = serde_json::from_value(stable["id"].clone()).unwrap();
    let stable_path = format!("/api/labels/{stable_id}/?workspace_id={ws}");
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("account_catalog_write_labels_oracle.json")).unwrap();
    for case in cases {
        let method = if case["method"] == "create" {
            "POST"
        } else {
            "PUT"
        };
        let path = if method == "POST" {
            &collection
        } else {
            &stable_path
        };
        db.execute(
            "UPDATE issue_label SET name='Stored',description='original',color='#654321' WHERE id=$1",
            &[&stable_id],
        ).await.unwrap();
        let snapshot: String = db
            .query_one(
                "SELECT row_to_json(l)::text FROM issue_label l WHERE id=$1",
                &[&stable_id],
            )
            .await
            .unwrap()
            .get(0);
        let count: i64 = db
            .query_one(
                "SELECT count(*) FROM issue_label WHERE workspace_id=$1",
                &[&ws],
            )
            .await
            .unwrap()
            .get(0);
        let body = base64::engine::general_purpose::STANDARD
            .decode(case["body"].as_str().unwrap())
            .unwrap();
        let (status, response) = call_raw(&app, method, path, &owner, &body, false).await;
        let label = format!("{} {}", method, case["label"]);
        if let Some(error) = case["error"].as_str() {
            assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {response}");
            assert_eq!(response, json!({"error":error}), "{label}");
            assert_eq!(
                db.query_one(
                    "SELECT row_to_json(l)::text FROM issue_label l WHERE id=$1",
                    &[&stable_id],
                )
                .await
                .unwrap()
                .get::<_, String>(0),
                snapshot,
                "{label}: rejected request changed row"
            );
            assert_eq!(
                db.query_one(
                    "SELECT count(*) FROM issue_label WHERE workspace_id=$1",
                    &[&ws],
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
                count,
                "{label}: rejected request inserted row"
            );
        } else {
            assert_eq!(
                status,
                if method == "POST" {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
                "{label}: {response}"
            );
            let saved: Uuid = serde_json::from_value(response["id"].clone()).unwrap();
            let row = db
                .query_one(
                    "SELECT resource_type,name,description,color FROM issue_label WHERE id=$1",
                    &[&saved],
                )
                .await
                .unwrap();
            for (key, fallback) in [
                ("resource_type", "issue"),
                ("name", "Stored"),
                ("description", "original"),
                ("color", "#654321"),
            ] {
                let expected = case["validated"][key].as_str().unwrap_or(fallback);
                assert_eq!(response[key], expected, "{label}: {key}");
                assert_eq!(row.get::<_, String>(key), expected, "{label}: DB {key}");
            }
            if method == "POST" {
                db.execute("DELETE FROM issue_label WHERE id=$1", &[&saved])
                    .await
                    .unwrap();
            }
        }
    }
    // Go's label decoder has no MaxBytesReader and ignores bytes after value one.
    let mut large = br#"{"name":"Large","color":"abcdef","description":""#.to_vec();
    large.extend(std::iter::repeat_n(b'x', 2 * 1024 * 1024 + 1));
    large.extend_from_slice(b"\"}");
    let (status, response) = call_raw(&app, "POST", &collection, &owner, &large, false).await;
    assert_eq!(status, StatusCode::CREATED, "{response}");
    assert_eq!(
        response["description"].as_str().unwrap().len(),
        2 * 1024 * 1024 + 1
    );
    let mut trailing = br#"{"description":"tail"}"#.to_vec();
    trailing.extend(std::iter::repeat_n(b'!', 2 * 1024 * 1024 + 1));
    let (status, response) = call_raw(&app, "PUT", &stable_path, &owner, &trailing, false).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["description"], "tail");
}
