//! Property definition mutations on a disposable, migrated PostgreSQL database.
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
const SECRET: &str = "zbsi65-properties-synthetic-secret";
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    call_raw(app, method, path, token, body.to_string().as_bytes()).await
}
async fn call_raw(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: &[u8],
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_owned()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
#[tokio::test]
#[ignore = "requires migrated, owned loopback multica_zbsi65 database"]
async fn property_definitions_admin_scope_options_and_cap() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let admin: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Admin',$1) RETURNING id",
            &[&format!("admin-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let member: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Member',$1) RETURNING id",
            &[&format!("member-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Other',$1) RETURNING id",
            &[&format!("other-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Properties',$1) RETURNING id",
            &[&format!("props-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Other',$1) RETURNING id",
            &[&format!("other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role) in [(admin, "owner"), (member, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
        &[&other, &outsider],
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
    let jwt = |u: Uuid, ttl: i64| {
        encode(
            &Header::default(),
            &json!({"sub":u.to_string(),"exp":chrono::Utc::now().timestamp()+ttl}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let owner = jwt(admin, 3600);
    let limited = jwt(member, 3600);
    let stranger = jwt(outsider, 3600);
    let expired = jwt(admin, -3600);
    let url = format!("/api/properties/?workspace_id={ws}");
    let value = json!({"name":"Severity","type":"select","config":{"options":[{"name":"Critical","color":"FF00aa"}]}});
    assert_eq!(
        call(&app, "POST", &url, &limited, value.clone()).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, "POST", &url, &stranger, value.clone()).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &url, &expired, value.clone()).await.0,
        StatusCode::UNAUTHORIZED
    );
    let (created, body) = call(&app, "POST", &url, &owner, value).await;
    assert_eq!(created, StatusCode::CREATED, "{body}");
    assert_eq!(body["config"]["options"][0]["color"], "#ff00aa");
    assert!(Uuid::parse_str(body["config"]["options"][0]["id"].as_str().unwrap()).is_ok());
    let prop = body["id"].as_str().unwrap();
    let option = body["config"]["options"][0]["id"].as_str().unwrap();
    let edit = format!("/api/properties/{prop}/?workspace_id={ws}");
    let cross = format!("/api/properties/{prop}/?workspace_id={other}");
    assert_eq!(
        call(&app, "PATCH", &cross, &stranger, json!({"name":"Leaked"}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "PATCH", &edit, &limited, json!({"name":"Leaked"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &format!("/api/properties/not-a-uuid/?workspace_id={ws}"),
            &owner,
            json!({})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    for payload in [
        json!({"name":"Due Date"}),
        json!({"icon":"invalid"}),
        json!({"config":{"options":[]}}),
    ] {
        assert_eq!(
            call(&app, "PATCH", &edit, &owner, payload).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,properties) VALUES($1,'Test','member',$2,1,$3) RETURNING id", &[&ws,&admin,&json!({prop:option})]).await.unwrap().get(0);
    let (conflict, problem) = call(
        &app,
        "PATCH",
        &edit,
        &owner,
        json!({"config":{"options":[{"name":"Other","color":"#abcdef"}]}}),
    )
    .await;
    assert_eq!(conflict, StatusCode::CONFLICT, "{problem}");
    let (_, untouched) = call(&app, "GET", &edit, &owner, Value::Null).await;
    assert_eq!(untouched["config"]["options"][0]["id"], option);
    let (updated, body) = call(
        &app,
        "PATCH",
        &edit,
        &owner,
        json!({"name":"Severity v2","archived":true}),
    )
    .await;
    assert_eq!(updated, StatusCode::OK, "{body}");
    assert_eq!(body["archived"], true);
    let (restored, body) = call(&app, "PATCH", &edit, &owner, json!({"archived":false})).await;
    assert_eq!(restored, StatusCode::OK, "{body}");
    assert_eq!(body["archived"], false);
    let invalid = json!({"name":"Severity v2","type":"text"});
    assert_eq!(
        call(&app, "POST", &url, &owner, invalid).await.0,
        StatusCode::CONFLICT
    );
    // The workspace advisory lock serializes concurrent creates around the cap.
    db.execute("DELETE FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap();
    // Request decoding must preserve Go's duplicate fields, nulls, and role precedence.
    use base64::Engine;
    let property_id = Uuid::parse_str(prop).unwrap();
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("account_catalog_write_properties_oracle.json")).unwrap();
    for case in cases.iter().filter(|case| case["decode_error"] == true) {
        let method = if case["method"] == "create" {
            "POST"
        } else {
            "PATCH"
        };
        let path = if method == "POST" { &url } else { &edit };
        let snapshot: String = db
            .query_one(
                "SELECT row_to_json(p)::text FROM issue_property p WHERE id=$1",
                &[&property_id],
            )
            .await
            .unwrap()
            .get(0);
        let count: i64 = db
            .query_one(
                "SELECT count(*) FROM issue_property WHERE workspace_id=$1",
                &[&ws],
            )
            .await
            .unwrap()
            .get(0);
        let raw = base64::engine::general_purpose::STANDARD
            .decode(case["body"].as_str().unwrap())
            .unwrap();
        let (status, response) = call_raw(&app, method, path, &owner, &raw).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{}: {response}",
            case["label"]
        );
        assert_eq!(
            response,
            json!({"error":"invalid request body"}),
            "{}",
            case["label"]
        );
        assert_eq!(
            db.query_one(
                "SELECT row_to_json(p)::text FROM issue_property p WHERE id=$1",
                &[&property_id]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            snapshot,
            "{}",
            case["label"]
        );
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM issue_property WHERE workspace_id=$1",
                &[&ws]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            count,
            "{}",
            case["label"]
        );
    }
    let (status, response) = call_raw(&app, "POST", &url, &limited, b"[").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        response,
        json!({"error":"insufficient workspace permissions"})
    );
    let (status, response) = call_raw(
        &app,
        "PATCH",
        &format!("/api/properties/not-a-uuid/?workspace_id={ws}"),
        &owner,
        b"[",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response, json!({"error":"invalid property id"}));

    for (raw, error) in [
        (br#"{"name":"Wire","type":"select","config":{"options":[null]}}"#.as_slice(), "option name is required"),
        (br#"{"name":"Wire","type":"select","config":{"options":[{}]}}"#.as_slice(), "option name is required"),
        (br#"{"name":"Wire","type":"select","config":{"options":[{"name":"A"}]}}"#.as_slice(), "option \"A\": color must be a 6-digit hex value like #3b82f6"),
        (br#"{"name":"Wire","type":"select","config":{"options":[{"name":"A","color":"abcdef"}]},"config":{"options":[]}}"#.as_slice(), "select properties require at least one option"),
        (br#"{"name":"Wire","type":"select","config":{"options":[{"name":"A","color":"abcdef"}]},"config":null}"#.as_slice(), "select properties require at least one option"),
    ] {
        let (status, response) = call_raw(&app, "POST", &url, &owner, raw).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
        assert_eq!(response, json!({"error":error}));
    }
    let fixed_option = "11111111-1111-4111-8111-111111111111";
    for raw in [
        br#"{"name":"Wire","NAME":null,"type":"select","CONFIG":{"OPTIONs":[{"id":"11111111-1111-4111-8111-111111111111","NAME":"A","COLOR":"ABCDEF"}]},"config":{}}"#.as_slice(),
        br#"{"name":"Wire","type":"select","config":{"options":[{"id":"11111111-1111-4111-8111-111111111111","name":"A","color":"ABCDEF"}],"options":[{"name":"A","color":null}]}}"#.as_slice(),
        br#"{"name":"Wire","type":"select","config":{"options":[{"id":"11111111-1111-4111-8111-111111111111","name":"A","color":"ABCDEF"}]},"config":{"unknown":1}} ignored"#.as_slice(),
    ] {
        let (status, response) = call_raw(&app, "POST", &url, &owner, raw).await;
        assert_eq!(status, StatusCode::CREATED, "{response}");
        assert_eq!(response["name"], "Wire");
        let expected = json!({"options":[{"id":fixed_option,"name":"A","color":"#abcdef"}]});
        assert_eq!(response["config"], expected);
        let saved = Uuid::parse_str(response["id"].as_str().unwrap()).unwrap();
        assert_eq!(db.query_one("SELECT config FROM issue_property WHERE id=$1", &[&saved])
            .await.unwrap().get::<_,Value>(0), expected);
        db.execute("DELETE FROM issue_property WHERE id=$1", &[&saved]).await.unwrap();
    }
    for raw in [
        b"null".as_slice(),
        br#"{"name":"discard","NAME":null,"description":"discard","DESCRIPTION":null,"icon":"discard","ICON":null,"config":{"options":[]},"config":null,"archived":true,"ARCHIVED":null}"#.as_slice(),
    ] {
        let (status, response) = call_raw(&app, "PATCH", &edit, &owner, raw).await;
        assert_eq!(status, StatusCode::OK, "{response}");
        assert_eq!(response["name"], "Severity v2");
        assert_eq!(response["archived"], false);
        assert_eq!(response["config"]["options"][0]["id"], option);
    }
    // These Go handlers have no MaxBytesReader, including unknown fields and trailing data.
    let mut large = br#"{"name":"Wire","type":"text","ignored":""#.to_vec();
    large.extend(std::iter::repeat_n(b'x', 2 * 1024 * 1024 + 1));
    large.extend_from_slice(b"\"}");
    let (status, response) = call_raw(&app, "POST", &url, &owner, &large).await;
    assert_eq!(status, StatusCode::CREATED, "{response}");
    let saved = Uuid::parse_str(response["id"].as_str().unwrap()).unwrap();
    db.execute("DELETE FROM issue_property WHERE id=$1", &[&saved])
        .await
        .unwrap();
    let mut trailing = br#"{"description":"tail"}"#.to_vec();
    trailing.extend(std::iter::repeat_n(b'!', 2 * 1024 * 1024 + 1));
    let (status, response) = call_raw(&app, "PATCH", &edit, &owner, &trailing).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["description"], "tail");
    assert_eq!(
        db.query_one(
            "SELECT description FROM issue_property WHERE id=$1",
            &[&property_id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "tail"
    );

    for i in 0..18 {
        let payload = json!({"name":format!("Filler {i}"),"type":"text"});
        assert_eq!(
            call(&app, "POST", &url, &owner, payload).await.0,
            StatusCode::CREATED
        );
    }
    let a = call(
        &app,
        "POST",
        &url,
        &owner,
        json!({"name":"Race A","type":"text"}),
    );
    let b = call(
        &app,
        "POST",
        &url,
        &owner,
        json!({"name":"Race B","type":"text"}),
    );
    let (a, b) = tokio::join!(a, b);
    assert_eq!(
        [a.0, b.0]
            .iter()
            .filter(|&&s| s == StatusCode::CREATED)
            .count(),
        1
    );
    assert_eq!(
        [a.0, b.0]
            .iter()
            .filter(|&&s| s == StatusCode::BAD_REQUEST)
            .count(),
        1
    );
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_property WHERE workspace_id=$1 AND archived_at IS NULL",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 20);
}
