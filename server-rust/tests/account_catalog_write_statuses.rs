//! Issue status catalog mutations against an owned migrated database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{
        ApiState,
        account_catalog_write_catalog_publish::{CatalogPublication, CatalogPublisher},
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;
const SECRET: &str = "zbsi65-status-synthetic-secret";
#[derive(Default)]
struct Recorder(Mutex<Vec<CatalogPublication>>);
impl CatalogPublisher for Recorder {
    fn disconnect_member<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("unexpected member disconnect") })
    }
    fn invalidate_membership<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("unexpected membership invalidation") })
    }
    fn notify_daemon_workspaces_changed<'a>(
        &'a self,
        _: Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("unexpected daemon notification") })
    }
    fn notify_runtime_profiles_changed<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("unexpected profile notification") })
    }
    fn publish<'a>(
        &'a self,
        event: CatalogPublication,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event);
        })
    }
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
async fn call_raw(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    raw: &str,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(raw.to_owned()))
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
async fn status_catalog_mutations_are_atomic_scoped_and_role_gated() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|host| matches!(host,tokio_postgres::config::Host::Tcp(h) if h=="127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(10)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let tag = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for role in ["owner", "member", "outsider"] {
        let id: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                &[&role, &format!("{role}-{tag}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        users.push(id);
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Status test',$1) RETURNING id",
            &[&format!("status-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign status test',$1) RETURNING id",
            &[&format!("status-other-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role, workspace) in [
        (users[0], "owner", ws),
        (users[1], "member", ws),
        (users[2], "owner", other),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&workspace, &user, &role],
        )
        .await
        .unwrap();
    }
    let recorder = Arc::new(Recorder::default());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec()).with_catalog_publisher(recorder.clone()),
    );
    let jwt = |id: Uuid, ttl: i64| {
        encode(
            &Header::default(),
            &json!({"sub":id,"exp":chrono::Utc::now().timestamp()+ttl}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let owner = jwt(users[0], 3600);
    let member = jwt(users[1], 3600);
    let outsider = jwt(users[2], 3600);
    let expired = jwt(users[0], -3600);
    let create = format!("/api/issue-statuses/?workspace_id={ws}");
    let payload =
        json!({"name":"Awaiting Review","category":"in_review","color":"AABBcc","icon":"check"});
    assert_eq!(
        call(&app, "POST", &create, &expired, payload.clone())
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "POST", &create, &member, payload.clone())
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, "POST", &create, &outsider, payload.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &create,
            &owner,
            json!({"name":"bad","category":"other","color":"#3b82f6"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &create,
            &owner,
            json!({"name":"bad","category":"done","color":"javascript:bad"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (status, folded) = call_raw(
        &app,
        "POST",
        &create,
        &owner,
        r##"{"NAME":"İtem","name":null,"CATEGORY":"started","COLOR":"#123abc"} trailing"##,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{folded}");
    assert_eq!(folded["key"], "item");
    let folded_id = Uuid::parse_str(folded["id"].as_str().unwrap()).unwrap();
    let stored_key: String = db
        .query_one(
            "SELECT key FROM issue_status WHERE id=$1 AND workspace_id=$2",
            &[&folded_id, &ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored_key, "item");
    db.execute("DELETE FROM issue_status WHERE id=$1", &[&folded_id])
        .await
        .unwrap();
    let (status, explicit) = call(
        &app,
        "POST",
        &create,
        &owner,
        json!({"name":"Explicit Unicode","key":"İnceleme","category":"started","color":"#123abc"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{explicit}");
    assert_eq!(explicit["key"], "inceleme");
    let explicit_id = Uuid::parse_str(explicit["id"].as_str().unwrap()).unwrap();
    let stored_key: String = db
        .query_one(
            "SELECT key FROM issue_status WHERE id=$1 AND workspace_id=$2",
            &[&explicit_id, &ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored_key, "inceleme");
    db.execute("DELETE FROM issue_status WHERE id=$1", &[&explicit_id])
        .await
        .unwrap();
    let (status, invalid) = call_raw(
        &app,
        "POST",
        &create,
        &owner,
        r#"{"name":"Invalid","category":"started","color":42}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_status WHERE workspace_id=$1 AND name='Invalid'",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let (status, created) = call(&app, "POST", &create, &owner, payload.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["key"], "awaiting_review");
    assert_eq!(created["category"], "in_progress");
    assert_eq!(created["color"], "#aabbcc");
    let id = created["id"].as_str().unwrap();
    let edit = format!("/api/issue-statuses/{id}?workspace_id={ws}");
    let (status, updated) = call_raw(
        &app, "PATCH", &edit, &owner,
        r##"{"NAME":"Folded Name","name":null,"POSITION":2.25,"position":null,"COLOR":"#ABCDEF"} trailing"##,
    ).await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["name"], "Awaiting Review");
    assert_eq!(updated["color"], "#abcdef");
    let stored: (String, f64) = {
        let row = db
            .query_one(
                "SELECT name,position FROM issue_status WHERE id=$1",
                &[&Uuid::parse_str(id).unwrap()],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    };
    assert_eq!(stored.0, "Awaiting Review");
    assert_ne!(stored.1, 2.25);

    let foreign = format!("/api/issue-statuses/{id}?workspace_id={other}");
    assert_eq!(
        call(&app, "PATCH", &foreign, &outsider, json!({"name":"Leaked"}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "PATCH", &edit, &member, json!({"name":"Leaked"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &format!("/api/issue-statuses/not-uuid?workspace_id={ws}"),
            &owner,
            json!({})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (status, changed) = call(
        &app,
        "PATCH",
        &edit,
        &owner,
        json!({"name":"Waiting Review","icon":"circle","description":"Next"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{changed}");
    assert_eq!(changed["key"], "awaiting_review");
    let (status, duplicate) = call(&app, "POST", &create, &owner, payload.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{duplicate}");
    assert_eq!(duplicate["key"], "awaiting_review_2");
    let dup_id = duplicate["id"].as_str().unwrap();
    let reorder = format!("/api/issue-statuses/reorder?workspace_id={ws}");
    let raw_reorder = format!(
        r#"{{"CATEGORY":"started","category":null,"IDS":["{dup_id}","{id}"],"include_system":false,"include_system":null}} trailing"#,
    );
    let (status, reordered) = call_raw(&app, "PATCH", &reorder, &owner, &raw_reorder).await;
    assert_eq!(status, StatusCode::OK, "{reordered}");
    let (status, invalid) = call_raw(
        &app,
        "PATCH",
        &reorder,
        &owner,
        r#"{"category":"started","ids":[null]}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
    assert_eq!(invalid["error"], "invalid issue status id");

    assert_eq!(
        call(
            &app,
            "PATCH",
            &reorder,
            &owner,
            json!({"category":"started","ids":[id]})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &reorder,
            &member,
            json!({"category":"started","ids":[id,dup_id]})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, listed) = call(
        &app,
        "PATCH",
        &reorder,
        &owner,
        json!({"category":"started","ids":[dup_id,id]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    let positions:Vec<(String,f64)>=db.query("SELECT key,position FROM issue_status WHERE workspace_id=$1 AND key IN ('awaiting_review','awaiting_review_2')",&[&ws]).await.unwrap().iter().map(|r|(r.get(0),r.get(1))).collect();
    assert_eq!(
        positions
            .iter()
            .find(|(k, _)| k == "awaiting_review")
            .unwrap()
            .1,
        1.
    );
    let issue: Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,number) VALUES($1,'Using status','member',$2,'awaiting_review',1) RETURNING id",&[&ws,&users[0]]).await.unwrap().get(0);
    let (blocked, details) = call(&app, "DELETE", &edit, &owner, Value::Null).await;
    assert_eq!(blocked, StatusCode::CONFLICT, "{details}");
    assert_eq!(details["code"], "issue_status_in_use");
    assert_eq!(details["issue_count"], 1);
    db.execute("DELETE FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap();
    let workflow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps,owner_id) VALUES($1,'Named','todo',$2,$3) RETURNING id",&[&ws,&json!([{"status_key":"awaiting_review"}]),&users[0]]).await.unwrap().get(0);
    let (blocked, details) = call(&app, "DELETE", &edit, &owner, Value::Null).await;
    assert_eq!(blocked, StatusCode::CONFLICT, "{details}");
    assert_eq!(details["code"], "issue_status_in_workflow");
    db.execute("DELETE FROM issue_workflow WHERE id=$1", &[&workflow])
        .await
        .unwrap();
    let default:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps,is_default) VALUES($1,'Default','todo',$2,true) RETURNING id",&[&ws,&json!([{"status_key":"todo"},{"status_key":"awaiting_review"}])]).await.unwrap().get(0);
    let (status, deleted) = call(&app, "DELETE", &edit, &owner, Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    assert!(!deleted["archived_at"].is_null());
    let remaining: Value = db
        .query_one("SELECT steps FROM issue_workflow WHERE id=$1", &[&default])
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, json!([{"status_key":"todo"}]));
    assert_eq!(
        call(&app, "DELETE", &edit, &owner, Value::Null).await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, "PATCH", &edit, &owner, json!({"name":"Cannot edit"}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let (status, custom) = call(
        &app,
        "POST",
        &create,
        &owner,
        json!({"name":"等待","category":"started","color":"#123abc"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{custom}");
    assert_eq!(custom["key"], "started_2");
    let custom_id = custom["id"].as_str().unwrap();
    let (status, _) = call(
        &app,
        "DELETE",
        &format!("/api/issue-statuses/{custom_id}?workspace_id={ws}"),
        &owner,
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        &app,
        "PATCH",
        &format!("/api/issue-statuses/{custom_id}?workspace_id={ws}"),
        &owner,
        json!({"name":"Oops"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let mut creates = Vec::new();
    for label in ["审批甲", "审批乙"] {
        let app = app.clone();
        let url = create.clone();
        let token = owner.clone();
        creates.push(tokio::spawn(async move {
            call(
                &app,
                "POST",
                &url,
                &token,
                json!({"name":label,"category":"started","color":"#3b82f6"}),
            )
            .await
        }));
    }
    let mut keys = Vec::new();
    for result in creates {
        let (status, body) = result.await.unwrap();
        assert_eq!(status, StatusCode::CREATED, "{body}");
        keys.push(body["key"].as_str().unwrap().to_owned());
    }
    keys.sort();
    assert_eq!(keys, ["started_3", "started_4"]);
    assert_eq!(
        call(
            &app,
            "POST",
            &create,
            &owner,
            json!({"key":"in_progress","name":"Reserved","category":"started","color":"#3b82f6"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&app, "PATCH", &edit, &owner, json!({"name":"Archived"}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    db.execute("DELETE FROM issue_workflow WHERE id=$1", &[&default])
        .await
        .unwrap();
    db.execute("DELETE FROM issue_status WHERE workspace_id=$1", &[&ws])
        .await
        .unwrap();
    for workspace in [ws, other] {
        db.execute("DELETE FROM member WHERE workspace_id=$1", &[&workspace])
            .await
            .unwrap();
        db.execute("DELETE FROM workspace WHERE id=$1", &[&workspace])
            .await
            .unwrap();
    }
    for user in users {
        db.execute("DELETE FROM \"user\" WHERE id=$1", &[&user])
            .await
            .unwrap();
    }
}
