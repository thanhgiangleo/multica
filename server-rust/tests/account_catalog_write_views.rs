//! HTTP and DB coverage for workspace-scoped saved issue-view writes.
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

const SECRET: &str = "zbsi65-saved-views-test-secret";

async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let data = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&data).unwrap_or(Value::Null))
}

async fn raw_call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: impl Into<Body>,
) -> (StatusCode, Vec<u8>) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .body(body.into())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    (
        status,
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn saved_views_create_update_delete_isolation_and_cas() {
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
    let mut users = Vec::new();
    for role in ["owner", "member", "member"] {
        let id: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[
                    &role,
                    &format!("{role}-{}-{suffix}@invalid.example", users.len()),
                ],
            )
            .await
            .unwrap()
            .get(0);
        users.push(id);
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Views',$1) RETURNING id",
            &[&format!("views-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other_ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Foreign',$1) RETURNING id",
            &[&format!("foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (id, role) in users.iter().zip(["owner", "member", "member"]) {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&ws, id, &role],
        )
        .await
        .unwrap();
    }
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Foreign') RETURNING id",
            &[&other_ws],
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
    let jwt = |id: Uuid| {
        encode(
            &Header::default(),
            &json!({"sub":id.to_string(),"exp":chrono::Utc::now().timestamp()+3600}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let owner = jwt(users[0]);
    let member = jwt(users[1]);
    let stranger = jwt(users[2]);
    let collection = format!("/api/issue-views/?workspace_id={ws}");
    let foreign_collection = format!("/api/issue-views/?workspace_id={other_ws}");
    let payload = json!({"name":"Shared","scope_type":"workspace","visibility":"workspace","query":{"filter":"all"}});
    assert_eq!(
        call(&app, "POST", &foreign_collection, &owner, payload.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &collection,
            &owner,
            json!({"name":"Bad","scope_type":"project","scope_id":project,"query":{}})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for bad in [
        json!({"name":"","scope_type":"workspace","query":{}}),
        json!({"name":"Bad","scope_type":"workspace","query":null}),
        json!({"name":"Bad","scope_type":"workspace","query":{},"display":[]}),
        json!({"name":"Bad","scope_type":"my","query":{}}),
        json!({"name":"Bad","scope_type":"project","scope_id":"not-a-uuid","query":{}}),
    ] {
        assert_eq!(
            call(&app, "POST", &collection, &owner, bad).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let (created, view) = call(&app, "POST", &collection, &owner, payload).await;
    assert_eq!(created, StatusCode::CREATED);
    assert_eq!(view["revision"], 1);
    assert_eq!(view["display"], json!({}));
    let id = view["id"].as_str().unwrap();
    let detail = format!("/api/issue-views/{id}/?workspace_id={ws}");
    assert_eq!(
        call(&app, "GET", &detail, &member, Value::Null).await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &detail,
            &stranger,
            json!({"expected_revision":1,"name":"Not yours"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &detail,
            &owner,
            json!({"expected_revision":0})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (a, b) = tokio::join!(
        call(
            &app,
            "PATCH",
            &detail,
            &owner,
            json!({"expected_revision":1,"name":"First"})
        ),
        call(
            &app,
            "PATCH",
            &detail,
            &owner,
            json!({"expected_revision":1,"name":"Second"})
        ),
    );
    assert!([a.0, b.0].contains(&StatusCode::OK));
    assert!([a.0, b.0].contains(&StatusCode::CONFLICT));
    assert_eq!(
        call(&app, "DELETE", &detail, &member, Value::Null).await.0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "INSERT INTO pinned_item(workspace_id,user_id,item_type,item_id) VALUES($1,$2,'view',$3)",
        &[&ws, &users[1], &Uuid::parse_str(id).unwrap()],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "DELETE", &detail, &owner, Value::Null).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&app, "GET", &detail, &owner, Value::Null).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        db.query_one(
            "SELECT COUNT(*) FROM issue_view WHERE id=$1",
            &[&Uuid::parse_str(id).unwrap()]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(db.query_one(
        "SELECT COUNT(*) FROM pinned_item WHERE workspace_id=$1 AND item_type='view' AND item_id=$2",
        &[&ws, &Uuid::parse_str(id).unwrap()]
    ).await.unwrap().get::<_, i64>(0), 0);
    let shared = call(&app, "POST", &collection, &member, json!({"name":"Member shared","scope_type":"workspace","visibility":"workspace","query":{}})).await;
    assert_eq!(shared.0, StatusCode::CREATED);
    let shared_path = format!(
        "/api/issue-views/{}/?workspace_id={ws}",
        shared.1["id"].as_str().unwrap()
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &shared_path,
            &owner,
            json!({"expected_revision":1,"name":"Admin edited"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &shared_path,
            &owner,
            json!({"expected_revision":1,"name":"Stale"})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &shared_path,
            &owner,
            json!({"expected_revision":2,"visibility":"private"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, "DELETE", &shared_path, &owner, Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let private = call(&app,"POST",&collection,&member,json!({"name":"Mine","scope_type":"my","scope_variant":"assigned","visibility":"workspace","query":{}})).await;
    assert_eq!(private.0, StatusCode::CREATED);
    assert_eq!(private.1["visibility"], "private");
    let private_path = format!(
        "/api/issue-views/{}/?workspace_id={ws}",
        private.1["id"].as_str().unwrap()
    );
    assert_eq!(
        call(&app, "DELETE", &private_path, &owner, Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "GET", &private_path, &stranger, Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    // Decode errors must not touch saved views; no Content-Type is required.
    use base64::Engine;
    let oracle: Vec<Value> =
        serde_json::from_str(include_str!("account_catalog_write_views_oracle.json")).unwrap();
    let before: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_view WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    for case in oracle.iter().filter(|case| case["decode_error"] == true) {
        let body = base64::engine::general_purpose::STANDARD
            .decode(case["body"].as_str().unwrap())
            .unwrap();
        let (method, path) = if case["method"] == "create" {
            ("POST", format!("/api/issue-views/?workspace_id={ws}"))
        } else {
            ("PATCH", private_path.clone())
        };
        let (status, _) = raw_call(&app, method, &path, &member, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{}", case["label"]);
    }
    let after: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_view WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
    let (missing_status, missing_raw) = raw_call(
        &app,
        "POST",
        &format!("/api/issue-views/?workspace_id={ws}"),
        &member,
        r#"{"name":"Missing display","scope_type":"workspace","query":{}}"#,
    )
    .await;
    assert_eq!(missing_status, StatusCode::CREATED);
    assert_eq!(
        serde_json::from_slice::<Value>(&missing_raw).unwrap()["display"],
        json!({})
    );
    let (null_status, _) = raw_call(
        &app,
        "POST",
        &format!("/api/issue-views/?workspace_id={ws}"),
        &member,
        r#"{"name":"Null display","scope_type":"workspace","query":{},"display":null}"#,
    )
    .await;
    assert_eq!(null_status, StatusCode::BAD_REQUEST);
    let (folded_status, folded_raw) = raw_call(&app, "POST", &format!("/api/issue-views/?workspace_id={ws}"),
        &member, r#"{"NAME":"Folded","scope_type":"workspace","query":{},"display":null,"DISPLAY":{"sort":"new"}}"#).await;
    assert_eq!(folded_status, StatusCode::CREATED);
    let folded_view: Value = serde_json::from_slice(&folded_raw).unwrap();
    assert_eq!(folded_view["display"], json!({"sort":"new"}));
    let folded_path = format!(
        "/api/issue-views/{}/?workspace_id={ws}",
        folded_view["id"].as_str().unwrap()
    );
    let (null_patch, _) = raw_call(
        &app,
        "PATCH",
        &folded_path,
        &member,
        r#"{"expected_revision":1,"display":null}"#,
    )
    .await;
    assert_eq!(null_patch, StatusCode::BAD_REQUEST);
    let (folded_patch, patch_raw) = raw_call(
        &app,
        "PATCH",
        &folded_path,
        &member,
        r#"{"expected_revision":1,"DISPLAY":{"sort":"old"},"display":{"sort":"updated"}}"#,
    )
    .await;
    assert_eq!(folded_patch, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&patch_raw).unwrap()["display"],
        json!({"sort":"updated"})
    );
    let stored: String = db
        .query_one(
            "SELECT display::text FROM issue_view WHERE id=$1",
            &[&Uuid::parse_str(folded_view["id"].as_str().unwrap()).unwrap()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        serde_json::from_str::<Value>(&stored).unwrap(),
        json!({"sort":"updated"})
    );
    #[derive(serde::Deserialize)]
    struct OpaqueView {
        id: Uuid,
        query: Box<serde_json::value::RawValue>,
        display: Box<serde_json::value::RawValue>,
        revision: i32,
    }
    let (big_status, big_raw) = raw_call(&app, "POST", &format!("/api/issue-views/?workspace_id={ws}"),
        &member, r#"{"name":"Opaque","scope_type":"workspace","query":{"n":1e400},"display":{"n":123456789012345678901234567890}}"#).await;
    assert_eq!(big_status, StatusCode::CREATED);
    let big: OpaqueView = serde_json::from_slice(&big_raw).unwrap();
    assert_eq!(big.revision, 1);
    let expanded = format!("1{}", "0".repeat(400));
    assert!(big.query.get().contains(&expanded));
    assert!(big.display.get().contains("123456789012345678901234567890"));
    let big_path = format!("/api/issue-views/{}/?workspace_id={ws}", big.id);
    for (body, expected) in [
        (
            r#"{"expected_revision":1,"query":{"bad":"\ud800"}}"#,
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            r#"{"expected_revision":1,"display":{"bad":"\u0000"}}"#,
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            r#"{"expected_revision":1,"query":{"n":1e999999}}"#,
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            r#"{"expected_revision":1,"query":null}"#,
            StatusCode::BAD_REQUEST,
        ),
    ] {
        assert_eq!(
            raw_call(&app, "PATCH", &big_path, &member, body).await.0,
            expected
        );
    }
    let (unchanged, unchanged_raw) = raw_call(
        &app,
        "PATCH",
        &big_path,
        &member,
        r#"{"expected_revision":1}"#,
    )
    .await;
    assert_eq!(unchanged, StatusCode::OK);
    let unchanged_view: OpaqueView = serde_json::from_slice(&unchanged_raw).unwrap();
    assert_eq!(unchanged_view.query.get(), big.query.get());
    assert_eq!(unchanged_view.display.get(), big.display.get());
    assert_eq!(unchanged_view.revision, 2);
    assert_eq!(
        db.query_one("SELECT query->>'n' FROM issue_view WHERE id=$1", &[&big.id])
            .await
            .unwrap()
            .get::<_, String>(0),
        expanded
    );
    let mut trailing = br#"{"expected_revision":2,"name":"Trailing"}"#.to_vec();
    trailing.extend(vec![b'x'; 2 * 1024 * 1024 + 1]);
    assert_eq!(
        raw_call(&app, "PATCH", &big_path, &member, trailing)
            .await
            .0,
        StatusCode::OK
    );
    let oversized = format!(
        r#"{{"expected_revision":3,"display":{{"x":"{}"}}}}"#,
        "x".repeat(128 * 1024)
    );
    assert_eq!(
        raw_call(&app, "PATCH", &big_path, &member, oversized)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let local_project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'UUID forms') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let alternate_project = local_project.to_string().replace('-', "x");
    let (status, project_view) = call(
        &app,
        "POST",
        &collection,
        &owner,
        json!({"name":"PGX UUID","scope_type":"project","scope_id":alternate_project,"query":{}}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(project_view["scope_id"], json!(local_project));
    assert_eq!(
        call(
            &app,
            "POST",
            &collection,
            &owner,
            json!({"name":"Rejected UUID","scope_type":"project","scope_id":format!("{{{local_project}}}"),"query":{}}),
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let project_view_id = Uuid::parse_str(project_view["id"].as_str().unwrap()).unwrap();
    let alternate_view_path = format!(
        "/api/issue-views/{}/?workspace_id={ws}",
        project_view_id.to_string().replace('-', "x")
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &alternate_view_path,
            &owner,
            json!({"expected_revision":1,"name":"Updated UUID"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT name FROM issue_view WHERE id=$1",
            &[&project_view_id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "Updated UUID"
    );
    assert_eq!(
        call(&app, "DELETE", &alternate_view_path, &owner, Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert!(
        db.query_opt("SELECT 1 FROM issue_view WHERE id=$1", &[&project_view_id])
            .await
            .unwrap()
            .is_none()
    );
    // The quota is scoped to (workspace, owner), and simultaneous creates at
    // the boundary must not allow more than 100 saved views.
    let existing: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_view WHERE workspace_id=$1 AND owner_id=$2",
            &[&ws, &users[0]],
        )
        .await
        .unwrap()
        .get(0);
    assert!(existing < 100);
    for n in existing..99 {
        db.execute(
            "INSERT INTO issue_view(workspace_id,owner_id,name,scope_type,visibility,query,display) VALUES ($1,$2,$3,'workspace','private','{}','{}')",
            &[&ws, &users[0], &format!("Quota seed {n}")],
        )
        .await
        .unwrap();
    }
    let mut requests = tokio::task::JoinSet::new();
    for n in 0..8 {
        let app = app.clone();
        let path = collection.clone();
        let token = owner.clone();
        requests.spawn(async move {
            call(
                &app,
                "POST",
                &path,
                &token,
                json!({"name":format!("Concurrent view {n}"),"scope_type":"workspace","query":{}}),
            )
            .await
            .0
        });
    }
    let mut created = 0;
    while let Some(result) = requests.join_next().await {
        match result.unwrap() {
            StatusCode::CREATED => created += 1,
            StatusCode::BAD_REQUEST => {}
            other => panic!("unexpected quota response: {other}"),
        }
    }
    assert_eq!(created, 1);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_view WHERE workspace_id=$1 AND owner_id=$2",
            &[&ws, &users[0]],
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        100
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &collection,
            &member,
            json!({"name":"Other owner quota","scope_type":"workspace","query":{}})
        )
        .await
        .0,
        StatusCode::CREATED,
        "another owner has a separate quota"
    );
    let cookie_request = Request::builder()
        .method("POST")
        .uri(collection)
        .header("cookie", format!("multica_auth={owner}"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"name":"Denied"}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie_request).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/issue-views/?workspace_id=not-a-uuid",
            &owner,
            json!({})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/issue-views/?workspace_id={ws}"),
            "invalid",
            json!({})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
}
