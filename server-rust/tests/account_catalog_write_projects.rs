//! Project definition HTTP/DB tests on a disposable migrated database.
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

const SECRET: &str = "synthetic-zbsi65-project-secret";
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
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
fn jwt(id: Uuid, expires: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":id.to_string(),"exp":expires}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}
#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn project_create_update_delete_effects_and_guards() {
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
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Project owner',$1) RETURNING id",
            &[&format!("owner-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Outsider',$1) RETURNING id",
            &[&format!("outsider-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Project',$1) RETURNING id",
            &[&format!("project-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign',$1) RETURNING id",
            &[&format!("foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($3,$2,'member')",
        &[&ws, &user, &foreign],
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
    let now = chrono::Utc::now().timestamp();
    let owner = jwt(user, now + 3600);
    let outsider = jwt(other, now + 3600);
    let expired = jwt(user, now - 300);
    let path = format!("/api/projects/?workspace_id={ws}");
    let bundled = json!({"title":"Demo","start_date":"2026-11-02","resources":[{"resource_type":"github_repo","resource_ref":{"url":"https://github.com/org/repo","unknown":"ignored"}}]});
    assert_eq!(
        call(&app, "POST", &path, &outsider, bundled.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &path, &expired, bundled.clone()).await.0,
        StatusCode::UNAUTHORIZED
    );
    for bad in [
        json!({"title":""}),
        json!({"title":"No","status":"active"}),
        json!({"title":"No","start_date":"2026-02-30"}),
        json!({"title":"No","workflow_id":Uuid::new_v4()}),
        json!({"title":"No","resources":[{"resource_type":"github_repo","resource_ref":{"url":"invalid"}}]}),
        json!({"title":"No","resources":[{"resource_type":"local_directory","resource_ref":{"local_path":"/a","daemon_id":"d"}},{"resource_type":"local_directory","resource_ref":{"local_path":"/b","daemon_id":"d"}}]}),
    ] {
        assert_eq!(
            call(&app, "POST", &path, &owner, bad).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let cookie = Request::builder()
        .method("POST")
        .uri(path.as_str())
        .header("cookie", format!("multica_auth={owner}"))
        .header("content-type", "application/json")
        .body(Body::from(bundled.to_string()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let duplicate = json!({"title":"Rollback","resources":[{"resource_type":"github_repo","resource_ref":{"url":"https://github.com/org/same"}},{"resource_type":"github_repo","resource_ref":{"url":"https://github.com/org/same"}}]});
    assert_eq!(
        call(&app, "POST", &path, &owner, duplicate).await.0,
        StatusCode::CONFLICT
    );
    let (conflict_status, conflict_body) = call(&app, "POST", &path, &owner,
        json!({"title":"Indexed conflict","resources":[{"resource_type":"github_repo","resource_ref":{"url":"https://github.com/org/duplicate"}},{"resource_type":"github_repo","resource_ref":{"url":"https://github.com/org/duplicate"}}]})).await;
    assert_eq!(conflict_status, StatusCode::CONFLICT);
    assert_eq!(
        conflict_body["error"],
        "resources[1]: this resource is already attached"
    );
    let (invalid_status, invalid_body) = call(&app, "POST", &path, &owner,
        json!({"title":"Indexed bad ref","resources":[{"resource_type":"github_repo","resource_ref":{"url":"https://github.com/org/valid"}},{"resource_type":"github_repo","resource_ref":{"url":"invalid"}}]})).await;
    assert_eq!(invalid_status, StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid_body["error"],
        "resources[1]: github_repo: url must be a valid http(s) or ssh git URL"
    );
    let (same_daemon_status, same_daemon) = call(&app, "POST", &path, &owner,
        json!({"title":"Indexed daemon","resources":[{"resource_type":"local_directory","resource_ref":{"local_path":"/a","daemon_id":"d"}},{"resource_type":"local_directory","resource_ref":{"local_path":"/b","daemon_id":"d"}}]})).await;
    assert_eq!(same_daemon_status, StatusCode::BAD_REQUEST);
    assert_eq!(
        same_daemon["error"],
        "resources[1]: duplicate local_directory for daemon (already at index 0); each daemon may attach at most one local_directory per project"
    );
    let rolled_back: i64 = db
        .query_one(
            "SELECT count(*) FROM project WHERE workspace_id=$1 AND title='Rollback'",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(rolled_back, 0);
    let (midnight_status, midnight) = call(
        &app,
        "POST",
        &path,
        &owner,
        json!({"title":"UTC midnight","start_date":"2026-03-01T00:00:00Z","due_date":"2026-03-01T01:00:00+01:00"}),
    )
    .await;
    assert_eq!(midnight_status, StatusCode::CREATED, "{midnight}");
    assert_eq!(midnight["start_date"], "2026-03-01");
    assert_eq!(midnight["due_date"], "2026-03-01");
    let midnight_id = Uuid::parse_str(midnight["id"].as_str().unwrap()).unwrap();
    let stored_dates = db
        .query_one(
            "SELECT start_date::text, due_date::text FROM project WHERE id=$1",
            &[&midnight_id],
        )
        .await
        .unwrap();
    assert_eq!(stored_dates.get::<_, String>(0), "2026-03-01");
    assert_eq!(stored_dates.get::<_, String>(1), "2026-03-01");
    let midnight_path = format!("/api/projects/{midnight_id}/?workspace_id={ws}");
    let (update_status, updated_midnight) = call(
        &app,
        "PUT",
        &midnight_path,
        &owner,
        json!({"start_date":"2026-03-02T00:00:00Z","due_date":"2026-03-02T02:00:00+02:00"}),
    )
    .await;
    assert_eq!(update_status, StatusCode::OK, "{updated_midnight}");
    assert_eq!(updated_midnight["start_date"], "2026-03-02");
    assert_eq!(updated_midnight["due_date"], "2026-03-02");
    for invalid in [
        "2026-02-28T16:00:00Z",
        "2026-03-01T00:00:00+08:00",
        "2026-03-01T00:00:00.000000001Z",
        "2026-03-01T00:00:01Z",
    ] {
        let (rejected, body) = call(
            &app,
            "PUT",
            &midnight_path,
            &owner,
            json!({"start_date": invalid}),
        )
        .await;
        assert_eq!(rejected, StatusCode::BAD_REQUEST, "{invalid}: {body}");
        assert_eq!(
            body["error"],
            "invalid start_date format, expected YYYY-MM-DD"
        );
        let still_stored: chrono::NaiveDate = db
            .query_one(
                "SELECT start_date FROM project WHERE id=$1",
                &[&midnight_id],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(still_stored.to_string(), "2026-03-02");
    }
    let (status, created) = call(&app, "POST", &path, &owner, bundled).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["resource_count"], 1);
    assert_eq!(
        created["resources"][0]["resource_ref"],
        json!({"url":"https://github.com/org/repo"})
    );
    let raw_create = Request::builder().method("POST").uri(path.as_str())
        .header("authorization", format!("Bearer {owner}"))
        .body(Body::from(r#"{"TITLE":"First","title":null,"resources":[{"RESOURCE_TYPE":"github_repo","resource_ref":{"url":"https://example.org/old","url":"https://example.org/new"}}]} trailing"#)).unwrap();
    let raw_create_response = app.clone().oneshot(raw_create).await.unwrap();
    assert_eq!(raw_create_response.status(), StatusCode::CREATED);
    let raw_created: Value = serde_json::from_slice(
        &raw_create_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes(),
    )
    .unwrap();
    assert_eq!(raw_created["title"], "First");
    assert_eq!(
        raw_created["resources"][0]["resource_ref"]["url"],
        "https://example.org/new"
    );
    let project = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    let detail = format!("/api/projects/{project}/?workspace_id={ws}");
    let cross = format!("/api/projects/{project}/?workspace_id={foreign}");
    assert_eq!(
        call(&app, "PUT", &cross, &owner, json!({"title":"Escape"}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "DELETE", &cross, &owner, Value::Null).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "PUT", &detail, &owner, json!({"status":"bad"}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&app, "PUT", &detail, &owner, json!({"due_date":"oops"}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let (status, updated) = call(
        &app,
        "PUT",
        &detail,
        &owner,
        json!({"description":"kept","icon":"star","due_date":"2026-11-03","title":"Renamed"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["resource_count"], 1);
    assert_eq!(updated["description"], "kept");
    let raw_request = |uri: &str, raw: &str| {
        Request::builder()
            .method("PUT")
            .uri(uri)
            .header("authorization", format!("Bearer {owner}"))
            .body(Body::from(raw.to_owned()))
            .unwrap()
    };
    let raw = app
        .clone()
        .oneshot(raw_request(
            &detail,
            r#"{"description":"new","DESCRIPTION":null,"Due_Date":null,"TITLE":"Folded"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(raw.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&raw.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert!(body["description"].is_null());
    assert_eq!(body["due_date"], "2026-11-03");
    assert_eq!(body["title"], "Folded");
    let invalid = app
        .clone()
        .oneshot(raw_request(
            &detail,
            r#"{"description":"should not change"} trailing"#,
        ))
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let missing = app
        .clone()
        .oneshot(raw_request(&cross, r#"{"description":1}"#))
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    let no_write: Option<String> = db
        .query_one("SELECT description FROM project WHERE id=$1", &[&project])
        .await
        .unwrap()
        .get(0);
    assert!(no_write.is_none());
    let (_, cleared) = call(
        &app,
        "PUT",
        &detail,
        &owner,
        json!({"description":null,"due_date":null}),
    )
    .await;
    assert!(cleared["description"].is_null());
    assert!(cleared["due_date"].is_null());
    assert_eq!(cleared["icon"], "star");
    assert_eq!(
        call(&app, "DELETE", &detail, &expired, Value::Null).await.0,
        StatusCode::UNAUTHORIZED
    );
    let foreign_project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Foreign') RETURNING id",
            &[&foreign],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_delete = format!("/api/projects/{foreign_project}/?workspace_id={foreign}");
    assert_eq!(
        call(&app, "DELETE", &foreign_delete, &owner, Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Project test agent','cloud',$2) RETURNING id",&[&ws,&user]).await.unwrap().get(0);
    let chat:Uuid=db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,project_id) VALUES($1,$2,$3,$4) RETURNING id",&[&ws,&agent,&user,&project]).await.unwrap().get(0);
    let view:Uuid=db.query_one("INSERT INTO issue_view(workspace_id,owner_id,name,scope_type,scope_id,visibility,query) VALUES($1,$2,'Project view','project',$3,'private','{}') RETURNING id",&[&ws,&user,&project]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO pinned_item(workspace_id,user_id,item_type,item_id) VALUES($1,$2,'view',$3)",
        &[&ws, &user, &view],
    )
    .await
    .unwrap();
    let (first, second) = tokio::join!(
        call(&app, "DELETE", &detail, &owner, Value::Null),
        call(&app, "DELETE", &detail, &owner, Value::Null)
    );
    let mut statuses = vec![first.0, second.0];
    statuses.sort();
    assert_eq!(
        statuses,
        vec![StatusCode::NO_CONTENT, StatusCode::NOT_FOUND]
    );
    let remaining: i64 = db
        .query_one(
            "SELECT count(*) FROM project_resource WHERE project_id=$1",
            &[&project],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 0);
    let views: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_view WHERE scope_id=$1",
            &[&project],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(views, 0);
    let pins: i64 = db
        .query_one(
            "SELECT count(*) FROM pinned_item WHERE item_id=$1",
            &[&view],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(pins, 0);
    let chat_project: Option<Uuid> = db
        .query_one("SELECT project_id FROM chat_session WHERE id=$1", &[&chat])
        .await
        .unwrap()
        .get(0);
    assert!(chat_project.is_none());
    let no_project: Option<Uuid> = db
        .query_opt("SELECT id FROM project WHERE id=$1", &[&project])
        .await
        .unwrap()
        .map(|r| r.get(0));
    assert!(no_project.is_none());
}
