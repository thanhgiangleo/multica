//! Project resource HTTP + database checks against an owned disposable database.
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

const SECRET: &str = "zbsi65-resource-test-secret";
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
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn raw_call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Vec<u8>,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn project_resources_scope_mutations_and_race() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
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
            "INSERT INTO \"user\"(name,email) VALUES('Resource', $1) RETURNING id",
            &[&format!("res-{suffix}@invalid.example")],
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
            "INSERT INTO workspace(name,slug) VALUES('Resource', $1) RETURNING id",
            &[&format!("res-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign', $1) RETURNING id",
            &[&format!("foreign-{suffix}")],
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
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&other, &user],
    )
    .await
    .unwrap();
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Test') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Foreign') RETURNING id",
            &[&other],
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
    let owner = jwt(user);
    let outsider = jwt(stranger);
    let path = format!("/api/projects/{project}/resources?workspace_id={ws}");
    let foreign_path = format!("/api/projects/{foreign_project}/resources?workspace_id={ws}");
    let payload = json!({"resource_type":"github_repo","resource_ref":{"url":" https://github.com/org/repo ","ref":" release/1 ","unknown":"discard"}});
    assert_eq!(
        call(&app, "POST", &path, &outsider, payload.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &foreign_path, &owner, payload.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    for bad in [
        json!({"resource_type":"github_repo","resource_ref":{"url":"oops"}}),
        json!({"resource_type":"github_repo","resource_ref":{"url":"https://example.org/a", "ref":"../bad"}}),
        json!({"resource_type":"local_directory","resource_ref":{"local_path":"relative","daemon_id":"d"}}),
    ] {
        assert_eq!(
            call(&app, "POST", &path, &owner, bad).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let expired = encode(
        &Header::default(),
        &json!({"sub":user.to_string(),"exp":chrono::Utc::now().timestamp()-600}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();
    assert_eq!(
        call(&app, "POST", &path, &expired, payload.clone()).await.0,
        StatusCode::UNAUTHORIZED
    );
    let cookie = Request::builder()
        .method("POST")
        .uri(path.as_str())
        .header("cookie", format!("multica_auth={owner}"))
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let (created, resource) = call(&app, "POST", &path, &owner, payload.clone()).await;
    assert_eq!(created, StatusCode::CREATED, "{resource:?}");
    assert_eq!(
        resource["resource_ref"]["url"],
        "https://github.com/org/repo"
    );
    assert_eq!(resource["resource_ref"]["ref"], "release/1");
    assert!(resource["resource_ref"].get("unknown").is_none());
    assert_eq!(resource["created_by"], user.to_string());
    // Go treats only the first @ as the scp-style user separator.
    let scp_url = "git@github.com:owner/repo@branch.git";
    let (status, scp_resource) = call(
        &app,
        "POST",
        &path,
        &owner,
        json!({"resource_type":"github_repo","resource_ref":{"url":scp_url}}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{scp_resource:?}");
    assert_eq!(scp_resource["resource_ref"]["url"], scp_url);
    let scp_id = Uuid::parse_str(scp_resource["id"].as_str().unwrap()).unwrap();
    let saved_ref: Value = db
        .query_one(
            "SELECT resource_ref FROM project_resource WHERE id=$1",
            &[&scp_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(saved_ref["url"], scp_url);
    let (status, invalid_scp) = call(
        &app,
        "POST",
        &path,
        &owner,
        json!({"resource_type":"github_repo","resource_ref":{"url":"github.com:owner/repo@branch.git"}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid_scp:?}");
    assert_eq!(
        call(&app, "POST", &path, &owner, payload).await.0,
        StatusCode::CONFLICT
    );
    let rid = Uuid::parse_str(resource["id"].as_str().unwrap()).unwrap();
    let item = format!(
        "/api/projects/{}/resources/{}?workspace_id={ws}",
        project.simple(),
        rid.to_string().replace('-', "x")
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("/api/projects/{project}/resources/nope?workspace_id={ws}"),
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
            "PUT",
            &format!("/api/projects/{foreign_project}/resources/{rid}?workspace_id={other}"),
            &owner,
            json!({})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (first, second) = tokio::join!(
        call(&app, "PUT", &item, &owner, json!({"label":" Repo "})),
        call(&app, "PUT", &item, &owner, json!({"position":5}))
    );
    assert_eq!(first.0, StatusCode::OK, "{first:?}");
    assert_eq!(second.0, StatusCode::OK, "{second:?}");
    let row = db
        .query_one(
            "SELECT label,position FROM project_resource WHERE id=$1",
            &[&rid],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<String>>(0).as_deref(), Some("Repo"));
    assert_eq!(row.get::<_, i32>(1), 5);
    let directory = json!({"resource_type":"local_directory","resource_ref":{"local_path":"/home/test/repo","daemon_id":"daemon-test"}});
    let (a, b) = tokio::join!(
        call(&app, "POST", &path, &owner, directory.clone()),
        call(&app, "POST", &path, &owner, directory)
    );
    assert!([a.0, b.0].contains(&StatusCode::CREATED), "{a:?} {b:?}");
    assert!([a.0, b.0].contains(&StatusCode::CONFLICT), "{a:?} {b:?}");
    assert_eq!(call(&app,"POST",&path,&owner,json!({"resource_type":"local_directory","resource_ref":{"local_path":"/home/test/repo2","daemon_id":"daemon-test","execution_mode":"worktree"}})).await.0,StatusCode::CONFLICT);
    assert_eq!(call(&app,"POST",&path,&owner,json!({"resource_type":"local_directory","resource_ref":{"local_path":"/home/test/repo2","daemon_id":"unknown","execution_mode":"worktree"}})).await.0,StatusCode::UNPROCESSABLE_ENTITY);
    let directory = if a.0 == StatusCode::CREATED {
        &a.1
    } else {
        &b.1
    };
    let directory_id = Uuid::parse_str(directory["id"].as_str().unwrap()).unwrap();
    let directory_path =
        format!("/api/projects/{project}/resources/{directory_id}?workspace_id={ws}");
    let (status, directory) =
        call(&app, "PUT", &directory_path, &owner, json!({"label":"New"})).await;
    assert_eq!(status, StatusCode::OK, "{directory:?}");
    assert_eq!(directory["resource_ref"]["label"], "New");
    let (status, directory) = call(&app, "PUT", &directory_path, &owner, json!({"resource_ref":{"local_path":"/home/test/repo","daemon_id":"daemon-test","label":"Old client rename"}})).await;
    assert_eq!(status, StatusCode::OK, "{directory:?}");
    assert_eq!(directory["label"], "Old client rename");
    let (status, directory) =
        call(&app, "PUT", &directory_path, &owner, json!({"label":null})).await;
    assert_eq!(status, StatusCode::OK, "{directory:?}");
    assert_eq!(directory["resource_ref"].get("label"), None);
    db.execute("INSERT INTO agent_runtime(workspace_id,daemon_id,name,runtime_mode,provider,metadata,last_seen_at) VALUES($1,'capable-test','local','local','claude',$2,now())", &[&ws,&json!({"capabilities":["local-worktree-v1"]})]).await.unwrap();
    let (status, worktree) = call(&app, "POST", &path, &owner, json!({"resource_type":"local_directory","resource_ref":{"local_path":"/tmp/repo","daemon_id":"capable-test","execution_mode":"worktree"}})).await;
    assert_eq!(status, StatusCode::CREATED, "{worktree:?}");
    let worktree_id = Uuid::parse_str(worktree["id"].as_str().unwrap()).unwrap();
    let worktree_path =
        format!("/api/projects/{project}/resources/{worktree_id}?workspace_id={ws}");
    db.execute("UPDATE agent_runtime SET metadata='{}'::jsonb, last_seen_at=now() WHERE workspace_id=$1 AND daemon_id='capable-test'", &[&ws]).await.unwrap();
    assert_eq!(
        call(
            &app,
            "PUT",
            &worktree_path,
            &owner,
            json!({"label":"rename after downgrade"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(call(&app, "PUT", &worktree_path, &owner, json!({"resource_ref":{"local_path":"/tmp/other","daemon_id":"capable-test","execution_mode":"worktree"}})).await.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        call(&app, "DELETE", &item, &outsider, Value::Null).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "DELETE", &item, &owner, Value::Null).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&app, "DELETE", &item, &owner, Value::Null).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn resource_request_and_structured_worktree_contract() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
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
            "INSERT INTO \"user\"(name,email) VALUES('Resource', $1) RETURNING id",
            &[&format!("res-{suffix}@invalid.example")],
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
            "INSERT INTO workspace(name,slug) VALUES('Resource', $1) RETURNING id",
            &[&format!("res-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign', $1) RETURNING id",
            &[&format!("foreign-{suffix}")],
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
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&other, &user],
    )
    .await
    .unwrap();
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Test') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Foreign') RETURNING id",
            &[&other],
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
    let owner = jwt(user);
    let outsider = jwt(stranger);
    let path = format!("/api/projects/{project}/resources?workspace_id={ws}");
    let foreign_path = format!("/api/projects/{foreign_project}/resources?workspace_id={ws}");

    // Project/resource visibility wins over malformed request bodies.
    assert_eq!(
        raw_call(&app, "POST", &foreign_path, &owner, b"[".to_vec())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        raw_call(&app, "POST", &path, &outsider, b"[".to_vec())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let missing = format!(
        "/api/projects/{project}/resources/{}?workspace_id={ws}",
        Uuid::new_v4()
    );
    assert_eq!(
        raw_call(&app, "PUT", &missing, &owner, b"[".to_vec())
            .await
            .0,
        StatusCode::NOT_FOUND
    );

    use base64::Engine;
    let decoder_cases: Vec<Value> = serde_json::from_str(include_str!(
        "account_catalog_write_resource_decode_oracle.json"
    ))
    .unwrap();
    for case in decoder_cases
        .iter()
        .filter(|case| case["decode_error"] == true)
    {
        let raw = base64::engine::general_purpose::STANDARD
            .decode(case["body"].as_str().unwrap())
            .unwrap();
        let (status, body) = raw_call(&app, "POST", &path, &owner, raw).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{} {body}", case["label"]);
        assert_eq!(
            body,
            json!({"error":"invalid request body"}),
            "{}",
            case["label"]
        );
    }
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM project_resource WHERE project_id=$1",
            &[&project],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    assert_eq!(
        raw_call(&app, "POST", &path, &owner, b"null trailing".to_vec())
            .await
            .1,
        json!({"error":"resource_type is required"})
    );
    let (status,resource)=raw_call(&app,"POST",&path,&owner,br#"{"RESOURCE_TYPE":"github_repo","resource_type":null,"RESOURCE_REF":{"URL":"https://example.org/raw","url":null,"unknown":1e999999},"label":"old","LABEL":null,"position":3,"POSITION":null} garbage"#.to_vec()).await;
    assert_eq!(status, StatusCode::CREATED, "{resource}");
    assert_eq!(
        resource["resource_ref"],
        json!({"url":"https://example.org/raw"})
    );
    assert_eq!(resource["position"], 0);
    assert!(resource["label"].is_null());
    let item = format!(
        "/api/projects/{project}/resources/{}?workspace_id={ws}",
        resource["id"].as_str().unwrap()
    );
    let (_, updated) = raw_call(
        &app,
        "PUT",
        &item,
        &owner,
        b"{\"label\":\"kept\",\"position\":7} trailing".to_vec(),
    )
    .await;
    assert_eq!(updated["label"], "kept");
    assert_eq!(updated["position"], 7);
    for raw in [
        b"null".as_slice(),
        b"{\"LABEL\":null,\"POSITION\":9}",
        b"{\"position\":null}",
    ] {
        let (status, updated) = raw_call(&app, "PUT", &item, &owner, raw.to_vec()).await;
        assert_eq!(status, StatusCode::OK, "{updated}");
        assert_eq!(updated["label"], "kept");
        assert_eq!(updated["position"], 7);
    }
    for (raw, message) in [
        (
            b"{\"label\":1}".as_slice(),
            "label must be a string or null",
        ),
        (b"{\"position\":1.0}", "position must be an integer"),
    ] {
        let (status, body) = raw_call(&app, "PUT", &item, &owner, raw.to_vec()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, json!({"error":message}));
    }
    let huge = format!(
        "{{\"label\":\"large\",\"ignored\":\"{}\"}}",
        "x".repeat(2 * 1024 * 1024 + 1)
    );
    assert_eq!(
        raw_call(&app, "PUT", &item, &owner, huge.into_bytes())
            .await
            .0,
        StatusCode::OK
    );
    let huge_trailer = format!("{{\"label\":null}} {}", "x".repeat(2 * 1024 * 1024 + 1));
    assert_eq!(
        raw_call(&app, "PUT", &item, &owner, huge_trailer.into_bytes())
            .await
            .0,
        StatusCode::OK
    );

    let cases: Vec<Value> = serde_json::from_str(include_str!(
        "account_catalog_write_resource_gate_oracle.json"
    ))
    .unwrap();
    for case in cases {
        // JSONB canonicalization can reorder folded duplicate metadata tags;
        // those raw-decoder-only cases are covered by the helper oracle test.
        if ["metadata-9", "metadata-10", "metadata-11"].contains(&case["label"].as_str().unwrap()) {
            continue;
        }
        db.execute(
            "DELETE FROM agent_runtime WHERE workspace_id=$1 AND daemon_id='d'",
            &[&ws],
        )
        .await
        .unwrap();
        for (index, runtime) in case["runtimes"].as_array().unwrap().iter().enumerate() {
            let metadata: Value =
                serde_json::from_str(runtime["metadata"].as_str().unwrap()).unwrap();
            let seen = runtime["seen"].as_str().map(|v| {
                chrono::DateTime::parse_from_rfc3339(v)
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            });
            let created = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc)
                + chrono::Duration::seconds(index as i64);
            db.execute("INSERT INTO agent_runtime(workspace_id,daemon_id,name,runtime_mode,provider,metadata,last_seen_at,created_at) VALUES($1,'d','gate','local',$2,$3,$4,$5)",&[&ws,&format!("p{index}"),&metadata,&seen,&created]).await.unwrap();
        }
        // A foreign workspace's capable row must never satisfy this gate.
        db.execute("INSERT INTO agent_runtime(workspace_id,daemon_id,name,runtime_mode,provider,metadata,last_seen_at) VALUES($1,'d','foreign','local',$2,$3,now())",&[&other,&format!("foreign-{}",Uuid::new_v4()),&json!({"capabilities":["local-worktree-v1"]})]).await.unwrap();
        let reference = &case["reference"];
        let denied = !case["rejection"].is_null();
        let payload = json!({"resource_type":"local_directory","resource_ref":reference});
        let (status, body) = call(&app, "POST", &path, &owner, payload.clone()).await;
        if denied {
            assert_eq!(
                status,
                StatusCode::UNPROCESSABLE_ENTITY,
                "{} {body}",
                case["label"]
            );
            assert_eq!(body, case["rejection"], "{}", case["label"]);
        } else {
            assert_eq!(status, StatusCode::CREATED, "{} {body}", case["label"]);
            let delete = format!(
                "/api/projects/{project}/resources/{}?workspace_id={ws}",
                body["id"].as_str().unwrap()
            );
            assert_eq!(
                call(&app, "DELETE", &delete, &owner, Value::Null).await.0,
                StatusCode::NO_CONTENT
            );
        }
        let (status,in_place)=call(&app,"POST",&path,&owner,json!({"resource_type":"local_directory","resource_ref":{"local_path":"/tmp/stored","daemon_id":"d"}})).await;
        assert_eq!(status, StatusCode::CREATED, "{in_place}");
        let update = format!(
            "/api/projects/{project}/resources/{}?workspace_id={ws}",
            in_place["id"].as_str().unwrap()
        );
        let (status, body) = call(
            &app,
            "PUT",
            &update,
            &owner,
            json!({"resource_ref":reference}),
        )
        .await;
        if denied {
            assert_eq!(
                status,
                StatusCode::UNPROCESSABLE_ENTITY,
                "{} {body}",
                case["label"]
            );
            assert_eq!(body, case["rejection"]);
            let stored: Value = db
                .query_one(
                    "SELECT resource_ref FROM project_resource WHERE id=$1",
                    &[&Uuid::parse_str(in_place["id"].as_str().unwrap()).unwrap()],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(stored, in_place["resource_ref"]);
        } else {
            assert_eq!(status, StatusCode::OK, "{} {body}", case["label"]);
        }
        assert_eq!(
            call(&app, "DELETE", &update, &owner, Value::Null).await.0,
            StatusCode::NO_CONTENT
        );
        let title = format!("gate-{}", Uuid::new_v4());
        let bundle_path = format!("/api/projects/?workspace_id={ws}");
        let (status, body) = call(
            &app,
            "POST",
            &bundle_path,
            &owner,
            json!({"title":title,"resources":[payload]}),
        )
        .await;
        if denied {
            assert_eq!(
                status,
                StatusCode::UNPROCESSABLE_ENTITY,
                "{} {body}",
                case["label"]
            );
            assert_eq!(body, case["rejection"]);
            let count: i64 = db
                .query_one(
                    "SELECT count(*) FROM project WHERE workspace_id=$1 AND title=$2",
                    &[&ws, &title],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(count, 0);
        } else {
            assert_eq!(status, StatusCode::CREATED, "{} {body}", case["label"]);
        }
    }
}
