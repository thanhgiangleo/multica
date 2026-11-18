//! Synthetic packages and write-only MCP contracts on a migrated disposable DB.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::api::{self, ApiState};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires run-owned migrated multica_zbsi72 database on loopback"]
async fn mcp_mutations_recheck_role_after_workspace_fence() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi72"));
    assert!(!config.get_hosts().is_empty());
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = pool(config);
    let mut db = pool.get().await.unwrap();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('MCP race',$1) RETURNING id",
            &[&format!("mcp-race-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('MCP race',$1) RETURNING id",
            &[&format!("mcp-race-{}", Uuid::new_v4())],
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
    let server: Uuid = db.query_one("INSERT INTO workspace_mcp_server(workspace_id,name,config,created_by) VALUES($1,'original',$2,$3) RETURNING id", &[&ws,&json!({"command":"synthetic-never-executed"}),&user]).await.unwrap().get(0);
    let app =
        api::routes(ApiState::new(pool.clone(), SECRET.to_vec()).with_extensions_sprint(true));
    for method in ["POST", "PUT", "DELETE"] {
        db.execute(
            "UPDATE member SET role='owner' WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .unwrap();
        let tx = db.transaction().await.unwrap();
        tx.query_one("SELECT id FROM workspace WHERE id=$1 FOR UPDATE", &[&ws])
            .await
            .unwrap();
        tx.execute(
            "UPDATE member SET role='member' WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .unwrap();
        let path = if method == "POST" {
            format!("/api/workspaces/{ws}/mcp-servers")
        } else {
            format!("/api/workspaces/{ws}/mcp-servers/{server}")
        };
        let app = app.clone();
        let auth = session(user);
        let mut request = tokio::spawn(async move {
            call(
                &app,
                method,
                &path,
                Some(&auth),
                None,
                json!({"name":"changed","config":{"command":"synthetic"}}),
            )
            .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut request)
                .await
                .is_err(),
            "{method} bypassed workspace fence"
        );
        tx.commit().await.unwrap();
        let (status, body) = tokio::time::timeout(Duration::from_secs(5), request)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, StatusCode::FORBIDDEN, "{method}: {body}");
        let rows = db
            .query(
                "SELECT id,name FROM workspace_mcp_server WHERE workspace_id=$1",
                &[&ws],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, Uuid>(0), server);
        assert_eq!(rows[0].get::<_, String>(1), "original");
    }
}

const SECRET: &[u8] = b"synthetic-zbsi72-session-secret";
fn session(user: Uuid) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+3600}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}
fn pool(config: Config) -> Arc<Pool> {
    Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(12)
            .build()
            .unwrap(),
    )
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    auth: Option<&str>,
    installation: Option<Uuid>,
    body: Value,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = auth {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    if let Some(id) = installation {
        req = req.header("x-multica-plugin-installation", id.to_string());
    }
    let response = app
        .clone()
        .oneshot(
            req.body(if body.is_null() {
                Body::empty()
            } else {
                Body::from(body.to_string())
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let out = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if headers
        .get("content-type")
        .is_some_and(|v| v == "application/problem+json")
    {
        assert_eq!(
            out["request_id"].as_str(),
            headers.get("x-request-id").and_then(|v| v.to_str().ok())
        );
        assert_eq!(out["status"], status.as_u16());
        assert_eq!(out["detail"], out["error"]);
    }
    (status, out)
}

#[tokio::test]
async fn public_origin_and_disabled_feature_fail_before_database_access() {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(1)
        .connect_timeout(Duration::from_millis(50));
    let app = api::routes(ApiState::new(pool(config), SECRET.to_vec()));
    for token in [
        None,
        Some("mul_synthetic"),
        Some("mat_synthetic"),
        Some("mdt_synthetic"),
        Some("not-a-plugin-token"),
    ] {
        let (status, body) = call(&app, "GET", "/v1/context", token, None, Value::Null).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
        assert_eq!(body["code"], "unauthorized");
    }
    let (status, body) = call(
        &app,
        "GET",
        "/v1/context",
        Some("mpi_synthetic"),
        None,
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "plugin_api_disabled");
    let request = Request::builder()
        .uri("/v1/context")
        .header(
            "cookie",
            format!("multica_auth={}", session(Uuid::new_v4())),
        )
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{}/mcp-servers", Uuid::new_v4()),
            None,
            None,
            json!({"name":"x","config":{"command":"fake"}})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
#[ignore = "requires run-owned migrated multica_zbsi72 database on loopback"]
async fn public_issue_comment_reads_are_scoped_bounded_and_explicit() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi72"));
    assert!(!config.get_hosts().is_empty());
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = pool(config);
    let db = pool.get().await.unwrap();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Public Reader',$1) RETURNING id",
            &[&format!("reader-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES('Public Reads',$1,'PUB') RETURNING id", &[&format!("reads-{}",Uuid::new_v4())]).await.unwrap().get(0);
    let other: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES('Foreign',$1,'OTHER') RETURNING id", &[&format!("foreign-{}",Uuid::new_v4())]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($3,$2,'owner')",
        &[&ws, &user, &other],
    )
    .await
    .unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,description,metadata,properties,start_date,due_date) VALUES($1,'Visible','member',$2,42,'Public body',$3,$4,'2026-11-01','2026-11-02') RETURNING id", &[&ws,&user,&json!({"note":"visible"}),&json!({"p":"value"})]).await.unwrap().get(0);
    let foreign: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Foreign','member',$2,42) RETURNING id", &[&other,&user]).await.unwrap().get(0);
    let manifest = json!({"manifest_version":1,"key":"example.public-reads","name":"Public Reads","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["issues:read","comments:read"],"contributes":{}});
    let package: Uuid = db.query_one("INSERT INTO plugin_package(workspace_id,plugin_key,name) VALUES($1,'example.public-reads','Public Reads') RETURNING id", &[&ws]).await.unwrap().get(0);
    let version: Uuid = db.query_one("INSERT INTO plugin_package_version(package_id,workspace_id,version,manifest,digest,size_bytes) VALUES($1,$2,'1.0.0',$3,$4,0) RETURNING id", &[&package,&ws,&manifest,&"0".repeat(64)]).await.unwrap().get(0);
    let app = api::routes(
        ApiState::new(pool.clone(), SECRET.to_vec())
            .with_extensions_sprint(true)
            .with_extensions_sprint_signing_key([7; 32]),
    );
    let auth = session(user);
    let (status, installed) = call(
        &app,
        "POST",
        &format!("/api/workspaces/{ws}/plugins"),
        Some(&auth),
        None,
        json!({"version_id":version,"granted_scopes":["issues:read","comments:read"]}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{installed}");
    let installation = Uuid::parse_str(installed["id"].as_str().unwrap()).unwrap();
    let (status, token) = call(
        &app,
        "POST",
        &format!("/api/workspaces/{ws}/plugins/{installation}/token"),
        Some(&auth),
        None,
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{token}");
    let token = token["token"].as_str().unwrap();
    let public = format!("/v1/issues/{issue}");
    let bridge = format!("/api/plugin-bridge/v1/issues/{issue}");
    let mut expected = None;
    for (path, credential, header) in [
        (&public, token, None),
        (&"/v1/issues/pub-42".to_owned(), token, None),
        (&bridge, auth.as_str(), Some(installation)),
    ] {
        let (status, body) = call(&app, "GET", path, Some(credential), header, Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["id"], issue.to_string());
        assert_eq!(body["identifier"], "PUB-42");
        assert_eq!(body["status_category"], "backlog");
        assert_eq!(body["description"], "Public body");
        assert_eq!(body["metadata"], json!({"note":"visible"}));
        assert_eq!(body["properties"], json!({"p":"value"}));
        assert_eq!(body["start_date"], "2026-11-01");
        assert_eq!(body["due_date"], "2026-11-02");
        for private in [
            "acceptance_criteria",
            "context_refs",
            "origin_id",
            "triage_state",
            "duplicate_of_issue_id",
            "attachments",
            "labels",
        ] {
            assert!(body.get(private).is_none(), "{private}");
        }
        if let Some(ref expected) = expected {
            assert_eq!(&body, expected);
        } else {
            expected = Some(body);
        }
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&public)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.headers().get("etag").unwrap(),
        &format!(
            "W/\"{}\"",
            expected.as_ref().unwrap()["revision"].as_i64().unwrap()
        )
    );
    for path in [
        format!("/v1/issues/{foreign}"),
        "/v1/issues/OTHER-42".into(),
        "/v1/issues/PUB-0".into(),
        "/v1/issues/PUB-nope".into(),
        format!("/v1/issues/{foreign}/comments"),
    ] {
        assert_eq!(
            call(&app, "GET", &path, Some(token), None, Value::Null)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(&app, "GET", &public, Some(&auth), None, Value::Null)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &public,
            Some("mpc_synthetic"),
            None,
            Value::Null
        )
        .await
        .1["code"],
        "callback_tokens_disabled"
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{public}/comments"),
            Some(token),
            None,
            Value::Null
        )
        .await
        .1,
        json!({"comments":[]})
    );
    db.execute("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content,created_at) SELECT $1,$2,'member',$3,'comment-'||n, '2026-11-01T00:00:00Z'::timestamptz+n*interval '1 second' FROM generate_series(1,205) n", &[&issue,&ws,&user]).await.unwrap();
    let root: Uuid = db
        .query_one(
            "SELECT id FROM comment WHERE issue_id=$1 AND content='comment-1'",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("UPDATE comment SET parent_id=$2,deleted_at='2026-11-01T01:00:00Z',content='private tombstone content' WHERE issue_id=$1 AND content='comment-205'", &[&issue,&root]).await.unwrap();
    // Corrupt cross-workspace relation must not widen the installation's reach.
    db.execute("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content,created_at) VALUES($1,$2,'member',$3,'foreign-hidden','2026-11-02T00:00:00Z')", &[&issue,&other,&user]).await.unwrap();
    let (status, comments) = call(
        &app,
        "GET",
        &format!("{public}/comments"),
        Some(token),
        None,
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{comments}");
    let rows = comments["comments"].as_array().unwrap();
    assert_eq!(rows.len(), 200);
    assert_eq!(rows[0]["content"], "comment-6");
    assert!(rows[0].get("parent_id").is_none());
    assert_eq!(rows[199]["parent_id"], root.to_string());
    assert_eq!(rows[199]["content"], "");
    assert!(rows[199]["deleted_at"].is_string());
    assert!(!comments.to_string().contains("foreign-hidden"));
    assert!(!comments.to_string().contains("private tombstone content"));
    for private in [
        "workspace_id",
        "issue_id",
        "via_plugin_id",
        "resolved_at",
        "revision",
        "source_task_id",
    ] {
        assert!(rows[199].get(private).is_none(), "{private}");
    }
    let bridge_comments = call(
        &app,
        "GET",
        &format!("{bridge}/comments"),
        Some(&auth),
        Some(installation),
        Value::Null,
    )
    .await;
    assert_eq!(bridge_comments, (status, comments));
    for (scopes, path) in [
        (json!(["comments:read"]), public.clone()),
        (json!(["issues:read"]), format!("{public}/comments")),
        (json!([]), public.clone()),
    ] {
        db.execute(
            "UPDATE plugin_installation SET granted_scopes=$2 WHERE id=$1",
            &[&installation, &scopes],
        )
        .await
        .unwrap();
        let (status, body) = call(&app, "GET", &path, Some(token), None, Value::Null).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["code"], "missing_scope");
    }
    db.execute(
        "UPDATE plugin_installation SET granted_scopes=$2 WHERE id=$1",
        &[&installation, &json!(["issues:read", "comments:read"])],
    )
    .await
    .unwrap();
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &user],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "GET",
            &bridge,
            Some(&auth),
            Some(installation),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "GET", &public, Some(token), None, Value::Null)
            .await
            .0,
        StatusCode::OK
    );
    let mut lifecycle = pool.get().await.unwrap();
    let tx = lifecycle.transaction().await.unwrap();
    tx.execute(
        "UPDATE plugin_installation SET enabled=false WHERE id=$1",
        &[&installation],
    )
    .await
    .unwrap();
    let mut pending = Box::pin(call(&app, "GET", &public, Some(token), None, Value::Null));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), pending.as_mut())
            .await
            .is_err()
    );
    tx.commit().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .0,
        StatusCode::FORBIDDEN
    );
    for (method, path) in [
        ("PATCH", public.clone()),
        ("POST", format!("{public}/comments")),
    ] {
        assert_eq!(
            call(
                &app,
                method,
                &path,
                Some(token),
                None,
                json!({"title":"must not mutate","content":"must not mutate"})
            )
            .await
            .0,
            StatusCode::METHOD_NOT_ALLOWED
        );
    }
}

#[tokio::test]
#[ignore = "requires run-owned migrated multica_zbsi72 database on loopback"]
async fn synthetic_packages_storage_and_mcp_write_guards() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi72"));
    assert!(!config.get_hosts().is_empty());
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = pool(config);
    let db = pool.get().await.unwrap();
    let mut users = Vec::new();
    for name in ["owner", "admin", "member", "outsider"] {
        let id: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                &[&name, &format!("{name}-{}@invalid.example", Uuid::new_v4())],
            )
            .await
            .unwrap()
            .get(0);
        users.push(id);
    }
    let ws: Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES('Synthetic',$1,'EXT') RETURNING id", &[&format!("ext-{}",Uuid::new_v4())]).await.unwrap().get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Other',$1) RETURNING id",
            &[&format!("other-{}", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'admin'),($1,$4,'member'),($5,$2,'owner')", &[&ws,&users[0],&users[1],&users[2],&other]).await.unwrap();
    let manifest = json!({"manifest_version":1,"key":"example.synthetic","name":"Synthetic Package","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["storage:workspace","storage:user","net:example.invalid"],"config":{"non_secret":{"type":"string","label":"Visible"}},"contributes":{"surfaces":[],"hooks":[]}});
    let package: Uuid=db.query_one("INSERT INTO plugin_package(workspace_id,plugin_key,name) VALUES($1,'example.synthetic','Synthetic') RETURNING id", &[&ws]).await.unwrap().get(0);
    let version: Uuid=db.query_one("INSERT INTO plugin_package_version(package_id,workspace_id,version,manifest,digest,size_bytes) VALUES($1,$2,'1.0.0',$3,$4,0) RETURNING id", &[&package,&ws,&manifest,&"0".repeat(64)]).await.unwrap().get(0);
    let raw_token = "mpi_synthetic-zbsi72-install-token";
    let token_hash = hex::encode(Sha256::digest(raw_token.as_bytes()));
    let install: Uuid=db.query_one("INSERT INTO plugin_installation(workspace_id,plugin_key,version,manifest,package_version_id,granted_scopes,config,token_hash) VALUES($1,'example.synthetic','1.0.0',$2,$3,$4,$5,$6) RETURNING id", &[&ws,&manifest,&version,&json!(["storage:workspace","storage:user","net:example.invalid"]),&json!({"non_secret":"visible"}),&token_hash]).await.unwrap().get(0);
    let second: Uuid=db.query_one("INSERT INTO plugin_installation(workspace_id,plugin_key,version,manifest,package_version_id,granted_scopes) VALUES($1,'example.second','1.0.0',$2,$3,$4) RETURNING id", &[&ws,&manifest,&version,&json!(["storage:workspace","storage:user"])]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO plugin_secret(installation_id,key,ciphertext) VALUES($1,'hidden',$2)",
        &[&install, &b"synthetic-encrypted-secret".to_vec()],
    )
    .await
    .unwrap();
    let app = api::routes(
        ApiState::new(pool.clone(), SECRET.to_vec())
            .with_extensions_sprint(true)
            .with_extensions_sprint_signing_key([7; 32]),
    );
    let (owner, admin, member, outsider) = (
        session(users[0]),
        session(users[1]),
        session(users[2]),
        session(users[3]),
    );
    let mcp = format!("/api/workspaces/{ws}/mcp-servers");
    let secret = "synthetic-private-mcp-token";
    let config = json!({"type":"WebSocket","url":format!("wss://invalid.example/{secret}"),"env":{"TOKEN":secret}});
    for token in [&member, &outsider] {
        let expected = if token == &member {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::NOT_FOUND
        };
        assert_eq!(
            call(
                &app,
                "POST",
                &mcp,
                Some(token),
                None,
                json!({"name":"safe","config":config})
            )
            .await
            .0,
            expected
        );
    }
    for bad in [json!(null), json!([]), json!({}), json!(secret)] {
        let (status, body) = call(
            &app,
            "POST",
            &mcp,
            Some(&owner),
            None,
            json!({"name":"safe","config":bad}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!body.to_string().contains(secret));
    }
    let (status, created) = call(
        &app,
        "POST",
        &mcp,
        Some(&admin),
        None,
        json!({"name":"  safe  ","config":config}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["transport"], "websocket");
    assert_eq!(created["name"], "safe");
    assert!(!created.to_string().contains(secret));
    assert!(created.get("config").is_none());
    let server = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &mcp,
            Some(&owner),
            None,
            json!({"name":"safe","config":config})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let target = format!("{mcp}/{server}");
    assert_eq!(
        call(
            &app,
            "PUT",
            &target,
            Some(&owner),
            None,
            json!({"config":null})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("/api/workspaces/{other}/mcp-servers/{server}"),
            Some(&owner),
            None,
            json!({"name":"stolen"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let updated = call(
        &app,
        "PUT",
        &target,
        Some(&owner),
        None,
        json!({"name":"renamed"}),
    )
    .await;
    assert_eq!(updated.0, StatusCode::OK);
    assert_eq!(updated.1["transport"], "websocket");
    let stored: Value = db
        .query_one(
            "SELECT config FROM workspace_mcp_server WHERE id=$1",
            &[&server],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, config);
    let listing = call(&app, "GET", &mcp, Some(&member), None, Value::Null).await;
    assert_eq!(listing.0, StatusCode::OK);
    assert!(!listing.1.to_string().contains(secret));
    // A binding without an agent still has to be swept: no FK is assumed.
    let binding = Uuid::new_v4();
    db.execute(
        "INSERT INTO agent_mcp_server(agent_id,server_id) VALUES($1,$2)",
        &[&binding, &server],
    )
    .await
    .unwrap();
    db.batch_execute("CREATE FUNCTION extensions_sprint_reject_sweep() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'synthetic sweep failure'; END$$; CREATE TRIGGER extensions_sprint_reject_sweep BEFORE DELETE ON agent_mcp_server FOR EACH ROW EXECUTE FUNCTION extensions_sprint_reject_sweep();").await.unwrap();
    assert_eq!(
        call(&app, "DELETE", &target, Some(&owner), None, Value::Null)
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(
        db.query_opt("SELECT 1 FROM workspace_mcp_server WHERE id=$1", &[&server])
            .await
            .unwrap()
            .is_some()
    );
    db.batch_execute("DROP TRIGGER extensions_sprint_reject_sweep ON agent_mcp_server; DROP FUNCTION extensions_sprint_reject_sweep();").await.unwrap();
    assert_eq!(
        call(&app, "DELETE", &target, Some(&owner), None, Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert!(
        db.query_opt(
            "SELECT 1 FROM agent_mcp_server WHERE server_id=$1",
            &[&server]
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(
        call(&app, "DELETE", &target, Some(&owner), None, Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );

    let bridge = "/api/plugin-bridge/v1";
    let (status, context) = call(
        &app,
        "GET",
        "/v1/context",
        Some(raw_token),
        None,
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{context}");
    assert_eq!(context["actor"], "plugin");
    assert!(context.get("user").is_none());
    assert_eq!(context["config"], json!({"non_secret":"visible"}));
    assert!(!context.to_string().contains("hidden"));
    assert_eq!(
        call(&app, "GET", "/v1/context", Some(&owner), None, Value::Null)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/context",
            Some("mpc_synthetic-unknown"),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{bridge}/context"),
            Some(&outsider),
            Some(install),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let context = call(
        &app,
        "GET",
        &format!("{bridge}/context?workspace_id={other}"),
        Some(&member),
        Some(install),
        Value::Null,
    )
    .await;
    assert_eq!(context.0, StatusCode::OK);
    assert_eq!(context.1["workspace"]["id"], ws.to_string());
    assert_eq!(context.1["actor"], "member");
    assert!(context.1["user"].get("email").is_none());
    let storage = format!("{bridge}/storage");
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("{storage}/user/private"),
            Some(&owner),
            Some(install),
            json!({"value":"owner-only"})
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{storage}/user/private"),
            Some(&member),
            Some(install),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{storage}/user/private"),
            Some(&owner),
            Some(second),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/storage/user/private",
            Some(raw_token),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            "/v1/storage/workspace/shared",
            Some(raw_token),
            None,
            json!({"value":"éé"})
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{storage}/workspace/shared"),
            Some(&member),
            Some(install),
            Value::Null
        )
        .await
        .1,
        json!({"value":"éé"})
    );
    let keys = call(
        &app,
        "GET",
        "/v1/storage/workspace",
        Some(raw_token),
        None,
        Value::Null,
    )
    .await;
    assert_eq!(keys.0, StatusCode::OK);
    assert_eq!(keys.1["keys"][0]["size_bytes"], 4);
    assert!(keys.1["keys"][0].get("value").is_none());
    assert_eq!(
        call(
            &app,
            "PUT",
            "/v1/storage/workspace/too-big",
            Some(raw_token),
            None,
            json!({"value":"é".repeat(51201)})
        )
        .await
        .0,
        StatusCode::INSUFFICIENT_STORAGE
    );
    db.execute(
        "UPDATE plugin_installation SET granted_scopes='[]'::jsonb WHERE id=$1",
        &[&install],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/storage/workspace",
            Some(raw_token),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE plugin_installation SET granted_scopes=$2,enabled=false WHERE id=$1",
        &[&install, &json!(["storage:workspace", "storage:user"])],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/context",
            Some(raw_token),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("{storage}/user/private"),
            Some(&owner),
            Some(install),
            json!({"value":"no"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE plugin_installation SET enabled=true,token_hash=NULL WHERE id=$1",
        &[&install],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/context",
            Some(raw_token),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE plugin_installation SET token_hash=$2 WHERE id=$1",
        &[&install, &token_hash],
    )
    .await
    .unwrap();
    let token_route = format!("/api/workspaces/{ws}/plugins/{install}/token");
    assert_eq!(
        call(&app, "POST", &token_route, Some(&member), None, Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &token_route,
            Some(&outsider),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{other}/plugins/{install}/token"),
            Some(&owner),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (status, rotated) = call(&app, "POST", &token_route, Some(&admin), None, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    let new_token = rotated["token"].as_str().unwrap();
    assert!(new_token.starts_with("mpi_"));
    assert_eq!(rotated.as_object().unwrap().len(), 2);
    let signing = rotated["signing_secret"].as_str().unwrap();
    assert!(signing.starts_with("whsec_"));
    let hash: Option<String> = db
        .query_one(
            "SELECT token_hash FROM plugin_installation WHERE id=$1",
            &[&install],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        hash.unwrap(),
        hex::encode(Sha256::digest(new_token.as_bytes()))
    );
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/context",
            Some(raw_token),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/context",
            Some(new_token),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, rotated_again) =
        call(&app, "POST", &token_route, Some(&owner), None, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(rotated_again["token"], rotated["token"]);
    assert_eq!(rotated_again["signing_secret"], rotated["signing_secret"]);
    assert_eq!(
        call(
            &app,
            "DELETE",
            &token_route,
            Some(&admin),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let hash: Option<String> = db
        .query_one(
            "SELECT token_hash FROM plugin_installation WHERE id=$1",
            &[&install],
        )
        .await
        .unwrap()
        .get(0);
    assert!(hash.is_none());
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/context",
            rotated_again["token"].as_str(),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE plugin_installation SET token_hash=$2 WHERE id=$1",
        &[&install, &token_hash],
    )
    .await
    .unwrap();
    // Concurrency: only one of two new keys can occupy the final quota slot.
    db.execute(
        "DELETE FROM plugin_storage WHERE installation_id=$1 AND scope_type='workspace'",
        &[&install],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO plugin_storage(installation_id,scope_type,scope_id,key,value) SELECT $1,'workspace',$2,'seed-'||n::text,'' FROM generate_series(1,999) n", &[&install,&ws]).await.unwrap();
    let (one, two) = tokio::join!(
        call(
            &app,
            "PUT",
            "/v1/storage/workspace/concurrent-one",
            Some(raw_token),
            None,
            json!({"value":"1"})
        ),
        call(
            &app,
            "PUT",
            "/v1/storage/workspace/concurrent-two",
            Some(raw_token),
            None,
            json!({"value":"2"})
        ),
    );
    let statuses = [one.0, two.0];
    assert_eq!(
        statuses
            .iter()
            .filter(|s| **s == StatusCode::NO_CONTENT)
            .count(),
        1,
        "{one:?} {two:?}"
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|s| **s == StatusCode::INSUFFICIENT_STORAGE)
            .count(),
        1
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            "/v1/storage/workspace/seed-1",
            Some(raw_token),
            None,
            json!({"value":"replacement"})
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            "/v1/storage/workspace/seed-1",
            Some(raw_token),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            "/v1/storage/workspace/seed-1",
            Some(raw_token),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &users[0]],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{bridge}/context"),
            Some(&owner),
            Some(install),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let count: i64=db.query_one("SELECT count(*) FROM plugin_storage WHERE installation_id=$1 AND scope_type='workspace'", &[&install]).await.unwrap().get(0);
    assert_eq!(count, 999);
}

#[tokio::test]
#[ignore = "requires run-owned migrated multica_zbsi72 database on loopback"]
async fn published_storage_only_installation_guards() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi72"));
    assert!(!config.get_hosts().is_empty());
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = pool(config);
    let db = pool.get().await.unwrap();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Installer',$1) RETURNING id",
            &[&format!("install-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Install guards',$1) RETURNING id",
            &[&format!("install-{}", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Unrelated',$1) RETURNING id",
            &[&format!("unrelated-{}", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'admin'),($3,$2,'owner')",
        &[&ws, &user, &other],
    )
    .await
    .unwrap();
    let package: Uuid=db.query_one("INSERT INTO plugin_package(workspace_id,plugin_key,name) VALUES($1,'example.storage','Storage only') RETURNING id", &[&ws]).await.unwrap().get(0);
    let manifest = json!({"manifest_version":1,"key":"example.storage","name":"Storage only","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["storage:workspace","storage:user"],"contributes":{}});
    let version: Uuid=db.query_one("INSERT INTO plugin_package_version(package_id,workspace_id,version,manifest,digest,size_bytes) VALUES($1,$2,'1.0.0',$3,$4,0) RETURNING id", &[&package,&ws,&manifest,&"0".repeat(64)]).await.unwrap().get(0);
    let app =
        api::routes(ApiState::new(pool.clone(), SECRET.to_vec()).with_extensions_sprint(true));
    let auth = session(user);
    let base = format!("/api/workspaces/{ws}/plugins");
    let req = json!({"version_id":version,"granted_scopes":["storage:user","storage:workspace"]});
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{base}/preview"),
            Some(&auth),
            None,
            json!({"version_id":version})
        )
        .await
        .0,
        StatusCode::OK
    );
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM plugin_installation WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0, "preview must not install");
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{other}/plugins"),
            Some(&auth),
            None,
            req.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for scopes in [
        json!([]),
        json!(["storage:user"]),
        json!(["storage:user", "storage:user"]),
        json!(["storage:user", "storage:workspace", "issues:write"]),
    ] {
        assert_eq!(
            call(
                &app,
                "POST",
                &base,
                Some(&auth),
                None,
                json!({"version_id":version,"granted_scopes":scopes})
            )
            .await
            .0,
            StatusCode::CONFLICT
        );
    }
    db.execute(
        "UPDATE member SET role='member' WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &user],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "POST", &base, Some(&auth), None, req.clone())
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE member SET role='admin' WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let mut unsafe_manifest = manifest.clone();
    unsafe_manifest["contributes"] = json!({"hooks":[{"key":"must-not-run","transport":{"type":"http","url":"https://example.invalid/hook"}}]});
    let unsupported: Uuid=db.query_one("INSERT INTO plugin_package_version(package_id,workspace_id,version,manifest,digest,size_bytes) VALUES($1,$2,'2.0.0',$3,$4,0) RETURNING id", &[&package,&ws,&unsafe_manifest,&"1".repeat(64)]).await.unwrap().get(0);
    assert_eq!(
        call(
            &app,
            "POST",
            &base,
            Some(&auth),
            None,
            json!({"version_id":unsupported,"granted_scopes":["storage:workspace","storage:user"]})
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let (status, installed) = call(&app, "POST", &base, Some(&auth), None, req.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{installed}");
    assert_eq!(installed["package_version_id"], version.to_string());
    assert_eq!(installed["config"], json!({}));
    assert_eq!(installed["configured_secrets"], json!([]));
    assert!(installed.get("token").is_none());
    let id = Uuid::parse_str(installed["id"].as_str().unwrap()).unwrap();
    let row=db.query_one("SELECT manifest,granted_scopes,installed_by,token_hash FROM plugin_installation WHERE id=$1", &[&id]).await.unwrap();
    assert_eq!(row.get::<_, Value>("manifest"), manifest);
    assert_eq!(row.get::<_, Value>("granted_scopes"), manifest["scopes"]);
    assert_eq!(row.get::<_, Option<Uuid>>("installed_by"), Some(user));
    assert!(row.get::<_, Option<String>>("token_hash").is_none());
    assert_eq!(
        call(&app, "POST", &base, Some(&auth), None, req).await.0,
        StatusCode::CONFLICT
    );
    let (status, preview) = call(
        &app,
        "POST",
        &format!("{base}/preview"),
        Some(&auth),
        None,
        json!({"version_id":version}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(preview["installed"], true);
    assert_eq!(preview["installed_version"], "1.0.0");

    let member: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Member',$1) RETURNING id",
            &[&format!("member-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Outsider',$1) RETURNING id",
            &[&format!("outsider-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&ws, &member],
    )
    .await
    .unwrap();
    let target = format!("{base}/{id}");
    let member_auth = session(member);
    let outsider_auth = session(outsider);
    let (status, listed) = call(&app, "GET", &base, Some(&member_auth), None, Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    assert!(
        listed["plugins"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["id"] == id.to_string())
    );
    assert_eq!(
        call(&app, "GET", &base, Some(&outsider_auth), None, Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    for suffix in ["disable", "enable"] {
        assert_eq!(
            call(
                &app,
                "POST",
                &format!("{target}/{suffix}"),
                Some(&member_auth),
                None,
                Value::Null
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            call(
                &app,
                "POST",
                &format!("{target}/{suffix}"),
                Some(&outsider_auth),
                None,
                Value::Null
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            call(
                &app,
                "POST",
                &format!("/api/workspaces/{other}/plugins/{id}/{suffix}"),
                Some(&auth),
                None,
                Value::Null
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(
            &app,
            "DELETE",
            &target,
            Some(&member_auth),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/workspaces/{other}/plugins/{id}"),
            Some(&auth),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let raw_token = "mpi_zbsi72-only-synthetic";
    let token_hash = hex::encode(Sha256::digest(raw_token.as_bytes()));
    db.execute(
        "UPDATE plugin_installation SET token_hash=$2 WHERE id=$1",
        &[&id, &token_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "PUT",
            "/v1/storage/workspace/retain",
            Some(raw_token),
            None,
            json!({"value":"persist"})
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let disabled = call(
        &app,
        "POST",
        &format!("{target}/disable"),
        Some(&auth),
        None,
        Value::Null,
    )
    .await;
    assert_eq!(disabled.0, StatusCode::OK, "{disabled:?}");
    assert_eq!(disabled.1["enabled"], false);
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/context",
            Some(raw_token),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let enabled = call(
        &app,
        "POST",
        &format!("{target}/enable"),
        Some(&auth),
        None,
        Value::Null,
    )
    .await;
    assert_eq!(enabled.0, StatusCode::OK, "{enabled:?}");
    assert_eq!(enabled.1["enabled"], true);
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/storage/workspace/retain",
            Some(raw_token),
            None,
            Value::Null
        )
        .await
        .1["value"],
        "persist"
    );

    // Synthetic advanced installation: disable still stops schedules; enabling
    // refuses to assert hook readiness before a real scheduler/transport exists.
    let mut hooked = manifest.clone();
    hooked["contributes"] = json!({"hooks":[{"key":"run","name":"Run","triggers":["schedule"],"transport":{"type":"http","url":"https://example.invalid/secret"}}]});
    hooked["config"] = json!({"safe":{"type":"string","label":"Visible"},"secret":{"type":"secret","label":"Secret"}});
    db.execute("UPDATE plugin_installation SET manifest=$2,config=$3 WHERE id=$1", &[&id,&hooked,&json!({"safe":"visible","secret":"synthetic-sensitive-value","injected":"synthetic-sensitive-value"})]).await.unwrap();
    db.execute(
        "INSERT INTO plugin_secret(installation_id,key,ciphertext) VALUES($1,'secret',$2)",
        &[&id, &b"encrypted-synthetic".to_vec()],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO plugin_hook_schedule(installation_id,workspace_id,hook_key,cron_expression,timezone) VALUES($1,$2,'run','0 * * * *','UTC')", &[&id,&ws]).await.unwrap();
    db.execute("INSERT INTO plugin_invocation(installation_id,workspace_id,hook_key,trigger,status) VALUES($1,$2,'run','manual','ok')", &[&id,&ws]).await.unwrap();
    let (status, listed) = call(&app, "GET", &base, Some(&member_auth), None, Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    let listed = listed["plugins"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["id"] == id.to_string())
        .unwrap();
    assert_eq!(listed["config"], json!({"safe":"visible"}));
    assert_eq!(listed["configured_secrets"], json!(["secret"]));
    assert_eq!(listed["hooks"][0]["schedule"]["cron"], "0 * * * *");
    assert!(!listed.to_string().contains("synthetic-sensitive-value"));
    let disabled = call(
        &app,
        "POST",
        &format!("{target}/disable"),
        Some(&auth),
        None,
        Value::Null,
    )
    .await;
    assert_eq!(disabled.0, StatusCode::OK, "{disabled:?}");
    let active: bool = db
        .query_one(
            "SELECT enabled FROM plugin_hook_schedule WHERE installation_id=$1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!active);
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{target}/enable"),
            Some(&auth),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert!(
        !db.query_one(
            "SELECT enabled FROM plugin_installation WHERE id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );

    // A late failure must roll back every delete, including storage and secrets.
    db.batch_execute("CREATE FUNCTION extensions_sprint_reject_uninstall() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'synthetic cleanup failure'; END$$; CREATE TRIGGER extensions_sprint_reject_uninstall BEFORE DELETE ON plugin_installation FOR EACH ROW EXECUTE FUNCTION extensions_sprint_reject_uninstall();").await.unwrap();
    assert_eq!(
        call(&app, "DELETE", &target, Some(&auth), None, Value::Null)
            .await
            .0,
        StatusCode::BAD_GATEWAY
    );
    for table in [
        "plugin_installation",
        "plugin_storage",
        "plugin_secret",
        "plugin_hook_schedule",
        "plugin_invocation",
    ] {
        let query = format!(
            "SELECT count(*) FROM {table} WHERE {}=$1",
            if table == "plugin_installation" {
                "id"
            } else {
                "installation_id"
            }
        );
        let count: i64 = db.query_one(&query, &[&id]).await.unwrap().get(0);
        assert!(count > 0, "{table} was deleted despite rollback");
    }
    db.batch_execute("DROP TRIGGER extensions_sprint_reject_uninstall ON plugin_installation; DROP FUNCTION extensions_sprint_reject_uninstall();").await.unwrap();
    assert_eq!(
        call(&app, "DELETE", &target, Some(&auth), None, Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    for table in [
        "plugin_installation",
        "plugin_storage",
        "plugin_secret",
        "plugin_hook_schedule",
        "plugin_invocation",
    ] {
        let query = format!(
            "SELECT count(*) FROM {table} WHERE {}=$1",
            if table == "plugin_installation" {
                "id"
            } else {
                "installation_id"
            }
        );
        let count: i64 = db.query_one(&query, &[&id]).await.unwrap().get(0);
        assert_eq!(count, 0, "{table} orphaned after uninstall");
    }
    assert_eq!(
        call(&app, "DELETE", &target, Some(&auth), None, Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "GET",
            "/v1/context",
            Some(raw_token),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
}
