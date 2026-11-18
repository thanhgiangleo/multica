//! Workspace-private plugin package catalog and delete on disposable migrated DB.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::api::{self, ApiState};
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"synthetic-zbsi72-package-session-secret";

fn session(user: Uuid) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+3600}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    auth: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = auth {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(
            request
                .body(if body.is_null() {
                    Body::empty()
                } else {
                    Body::from(body.to_string())
                })
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn bundle(manifest: &Value) -> Vec<u8> {
    use std::io::Write;
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file(
            "multica.plugin.json",
            zip::write::SimpleFileOptions::default(),
        )
        .unwrap();
    writer.write_all(manifest.to_string().as_bytes()).unwrap();
    // Unreferenced bytes must not become stored or serveable code.
    writer
        .start_file("unused.js", zip::write::SimpleFileOptions::default())
        .unwrap();
    writer
        .write_all(b"throw new Error('never execute synthetic package');")
        .unwrap();
    writer.finish().unwrap().into_inner()
}

async fn upload(app: &axum::Router, path: &str, token: &str, zip: &[u8]) -> (StatusCode, Value) {
    let mut bytes=b"--SyntheticPackageBoundary\r\nContent-Disposition: form-data; name=\"bundle\"; filename=\"synthetic.zip\"\r\nContent-Type: application/zip\r\n\r\n".to_vec();
    bytes.extend_from_slice(zip);
    bytes.extend_from_slice(b"\r\n--SyntheticPackageBoundary--\r\n");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .header(
                    "content-type",
                    "multipart/form-data; boundary=SyntheticPackageBoundary",
                )
                .body(Body::from(bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn disposable_db() -> Config {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi72"));
    assert!(
        config.get_hosts().iter().all(
            |host| matches!(host,tokio_postgres::config::Host::Tcp(name) if name=="127.0.0.1")
        )
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    config
}

#[tokio::test]
async fn package_routes_deny_missing_auth() {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(1)
        .connect_timeout(std::time::Duration::from_millis(50));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let app = api::routes(ApiState::new(pool, SECRET.to_vec()));
    let ws = Uuid::new_v4();
    let path = format!("/api/workspaces/{ws}/plugins/packages");
    assert_eq!(
        call(&app, "GET", &path, None, Value::Null).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
#[ignore = "requires run-owned migrated multica_zbsi72 database on loopback"]
async fn package_catalog_and_deletion_are_scoped_atomic_and_install_safe() {
    let pool = Arc::new(
        Pool::builder(Manager::new(disposable_db(), NoTls))
            .max_size(12)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Package Author',$1) RETURNING id",
            &[&format!("author-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let reader: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Package Reader',$1) RETURNING id",
            &[&format!("reader-{}@invalid.example", Uuid::new_v4())],
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
    let ws: Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES('Packages',$1,'PKG') RETURNING id", &[&format!("packages-{}",Uuid::new_v4())]).await.unwrap().get(0);
    let other: Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES('Other Packages',$1,'PKO') RETURNING id", &[&format!("packages-{}",Uuid::new_v4())]).await.unwrap().get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&user,&reader,&other]).await.unwrap();
    let package: Uuid=db.query_one("INSERT INTO plugin_package(workspace_id,plugin_key,name,created_by) VALUES($1,'example.catalog','Catalog',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let foreign: Uuid=db.query_one("INSERT INTO plugin_package(workspace_id,plugin_key,name,created_by) VALUES($1,'example.catalog','Foreign',$2) RETURNING id", &[&other,&user]).await.unwrap().get(0);
    let manifest = json!({"manifest_version":1,"key":"example.catalog","name":"Catalog","version":"1.0.0","author":{"name":"Test"},"scopes":["issues:read"],"contributes":{}});
    let version: Uuid=db.query_one("INSERT INTO plugin_package_version(package_id,workspace_id,version,manifest,digest,size_bytes) VALUES($1,$2,'1.0.0',$3,$4,4) RETURNING id", &[&package,&ws,&manifest,&"a".repeat(64)]).await.unwrap().get(0);
    let latest: Uuid=db.query_one("INSERT INTO plugin_package_version(package_id,workspace_id,version,manifest,digest,size_bytes,created_at) VALUES($1,$2,'1.0.1',$3,$4,0,now()+interval '1 second') RETURNING id", &[&package,&ws,&manifest,&"b".repeat(64)]).await.unwrap().get(0);
    db.execute("INSERT INTO plugin_package_file(version_id,path,content,size_bytes,sha256) VALUES($1,'synthetic.txt',$2,4,$3)", &[&version,&b"test".to_vec(),&"c".repeat(64)]).await.unwrap();
    let app =
        api::routes(ApiState::new(pool.clone(), SECRET.to_vec()).with_extensions_sprint(true));
    let path = format!("/api/workspaces/{ws}/plugins/packages");
    let url = format!("{path}/{package}");
    let owner = session(user);
    let member = session(reader);
    let stranger = session(outsider);
    let (status, body) = call(&app, "GET", &path, Some(&member), Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["packages"].as_array().unwrap().len(), 1);
    assert_eq!(body["packages"][0]["id"], package.to_string());
    assert_eq!(body["packages"][0]["versions"][0]["id"], latest.to_string());
    assert_eq!(
        body["packages"][0]["versions"][1]["id"],
        version.to_string()
    );
    assert_eq!(body["packages"][0]["versions"][1]["installed"], false);
    assert!(!body.to_string().contains("synthetic.txt"));
    assert!(!body.to_string().contains("content"));
    for (path, token, status) in [
        (path.as_str(), stranger.as_str(), StatusCode::NOT_FOUND),
        (url.as_str(), member.as_str(), StatusCode::FORBIDDEN),
    ] {
        let method = if path == url { "DELETE" } else { "GET" };
        assert_eq!(
            call(&app, method, path, Some(token), Value::Null).await.0,
            status
        );
    }
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("{path}/{foreign}"),
            Some(&owner),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (status, installed) = call(
        &app,
        "POST",
        &format!("/api/workspaces/{ws}/plugins"),
        Some(&owner),
        json!({"version_id":version,"granted_scopes":["issues:read"]}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{installed}");
    assert_eq!(
        call(&app, "DELETE", &url, Some(&owner), Value::Null)
            .await
            .0,
        StatusCode::CONFLICT
    );
    let (status, body) = call(&app, "GET", &path, Some(&member), Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["packages"][0]["versions"][1]["installed"], true);
    let install = installed["id"].as_str().unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/workspaces/{ws}/plugins/{install}"),
            Some(&owner),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );

    // Failure after deleting files must roll back all package rows.
    db.batch_execute(&format!("CREATE FUNCTION package_delete_failure_{}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic failure'; END $$; CREATE TRIGGER package_delete_fail_{} BEFORE DELETE ON plugin_package_version FOR EACH ROW WHEN (OLD.package_id='{}'::uuid) EXECUTE FUNCTION package_delete_failure_{}()", package.simple(),package.simple(),package,package.simple())).await.unwrap();
    assert_eq!(
        call(&app, "DELETE", &url, Some(&owner), Value::Null)
            .await
            .0,
        StatusCode::BAD_GATEWAY
    );
    let file_count: i64 = db
        .query_one(
            "SELECT count(*) FROM plugin_package_file WHERE version_id=$1",
            &[&version],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(file_count, 1);
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM plugin_package_version WHERE package_id=$1",
            &[&package],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);
    db.batch_execute(&format!("DROP TRIGGER package_delete_fail_{} ON plugin_package_version; DROP FUNCTION package_delete_failure_{}()",package.simple(),package.simple())).await.unwrap();
    assert_eq!(
        call(&app, "DELETE", &url, Some(&owner), Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&app, "DELETE", &url, Some(&owner), Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    for (table, column, id) in [
        ("plugin_package", "id", package),
        ("plugin_package_version", "package_id", package),
        ("plugin_package_file", "version_id", version),
    ] {
        let count: i64 = db
            .query_one(
                &format!("SELECT count(*) FROM {table} WHERE {column}=$1"),
                &[&id],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 0, "{table}");
    }
    let foreign_count: i64 = db
        .query_one(
            "SELECT count(*) FROM plugin_package WHERE id=$1 AND workspace_id=$2",
            &[&foreign, &other],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(foreign_count, 1);
}

#[tokio::test]
#[ignore = "requires run-owned migrated multica_zbsi72 database on loopback"]
async fn config_package_publishes_previews_installs_and_updates_without_secret_leak() {
    let pool = Arc::new(
        Pool::builder(Manager::new(disposable_db(), NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Config Publisher',$1) RETURNING id",
            &[&format!(
                "config-publisher-{}@invalid.example",
                Uuid::new_v4()
            )],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Config Package',$1) RETURNING id",
            &[&format!("config-package-{}", Uuid::new_v4())],
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
    let app = api::routes(
        ApiState::new(pool.clone(), SECRET.to_vec())
            .with_extensions_sprint(true)
            .with_extensions_sprint_config_key([42; 32]),
    );
    let auth = session(user);
    let manifest = json!({"manifest_version":1,"key":"example.config-package","name":"Config Package","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["storage:workspace"],"config":{"endpoint":{"type":"string","label":"Endpoint"},"token":{"type":"secret","label":"Token","required":true}},"contributes":{}});
    let (status, published) = upload(
        &app,
        &format!("/api/workspaces/{ws}/plugins/packages"),
        &auth,
        &bundle(&manifest),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{published}");
    let version = Uuid::parse_str(published["versions"][0]["id"].as_str().unwrap()).unwrap();
    let (status, preview) = call(
        &app,
        "POST",
        &format!("/api/workspaces/{ws}/plugins/preview"),
        Some(&auth),
        json!({"version_id":version}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{preview}");
    assert_eq!(preview["config_schema"].as_array().unwrap().len(), 2);
    assert_eq!(preview["manifest"]["config"], manifest["config"]);
    let (status, installed) = call(
        &app,
        "POST",
        &format!("/api/workspaces/{ws}/plugins"),
        Some(&auth),
        json!({"version_id":version,"granted_scopes":["storage:workspace"]}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{installed}");
    assert_eq!(installed["config_schema"].as_array().unwrap().len(), 2);
    let id = installed["id"].as_str().unwrap();
    let (status, configured) = call(
        &app,
        "PUT",
        &format!("/api/workspaces/{ws}/plugins/{id}/config"),
        Some(&auth),
        json!({"values":{"endpoint":"https://example.invalid","token":"synthetic-secret-72"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{configured}");
    assert_eq!(configured["config"]["endpoint"], "https://example.invalid");
    assert_eq!(configured["configured_secrets"], json!(["token"]));
    assert!(!configured.to_string().contains("synthetic-secret-72"));
    let row = db
        .query_one(
            "SELECT config FROM plugin_installation WHERE id=$1",
            &[&Uuid::parse_str(id).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, Value>(0),
        json!({"endpoint":"https://example.invalid"})
    );
    assert!(
        db.query_opt(
            "SELECT ciphertext FROM plugin_secret WHERE installation_id=$1 AND key='token'",
            &[&Uuid::parse_str(id).unwrap()]
        )
        .await
        .unwrap()
        .is_some()
    );
}

#[tokio::test]
#[ignore = "requires run-owned migrated multica_zbsi72 database on loopback"]
async fn inert_package_publish_is_immutable_and_serializes_with_install_delete() {
    let pool = Arc::new(
        Pool::builder(Manager::new(disposable_db(), NoTls))
            .max_size(12)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Publisher',$1) RETURNING id",
            &[&format!("publisher-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let reader: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Reader',$1) RETURNING id",
            &[&format!("reader-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Publishing',$1) RETURNING id",
            &[&format!("publishing-{}", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign Publish',$1) RETURNING id",
            &[&format!("publishing-{}", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'member')",
        &[&ws, &user, &reader],
    )
    .await
    .unwrap();
    let app =
        api::routes(ApiState::new(pool.clone(), SECRET.to_vec()).with_extensions_sprint(true));
    let owner = session(user);
    let member = session(reader);
    let path = format!("/api/workspaces/{ws}/plugins/packages");
    let manifest = json!({"manifest_version":1,"key":"example.published","name":"Published","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["issues:read"],"contributes":{}});
    let zip = bundle(&manifest);
    assert_eq!(
        upload(&app, &path, &member, &zip).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        upload(
            &app,
            &format!("/api/workspaces/{other}/plugins/packages"),
            &owner,
            &zip
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let disabled = api::routes(ApiState::new(pool.clone(), SECRET.to_vec()));
    assert_eq!(
        upload(&disabled, &path, &owner, &zip).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&disabled, "GET", &path, Some(&member), Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        upload(&app, &path, &owner, b"not a ZIP").await.0,
        StatusCode::BAD_REQUEST
    );
    let mut unsafe_manifest = manifest.clone();
    unsafe_manifest["scopes"] = json!(["issues:write"]);
    assert_eq!(
        upload(&app, &path, &owner, &bundle(&unsafe_manifest))
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let (status, first) = upload(&app, &path, &owner, &zip).await;
    assert_eq!(status, StatusCode::CREATED, "{first}");
    let package = Uuid::parse_str(first["id"].as_str().unwrap()).unwrap();
    let version = Uuid::parse_str(first["versions"][0]["id"].as_str().unwrap()).unwrap();
    assert_eq!(first["versions"][0]["size_bytes"], 0);
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM plugin_package_file WHERE version_id=$1",
            &[&version],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    assert_eq!(
        upload(&app, &path, &owner, &zip).await.0,
        StatusCode::CONFLICT
    );
    let mut changed = manifest.clone();
    changed["name"] = json!("Must Roll Back");
    assert_eq!(
        upload(&app, &path, &owner, &bundle(&changed)).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        db.query_one("SELECT name FROM plugin_package WHERE id=$1", &[&package])
            .await
            .unwrap()
            .get::<_, String>(0),
        "Published"
    );
    let row = db
        .query_one(
            "SELECT manifest,digest FROM plugin_package_version WHERE id=$1",
            &[&version],
        )
        .await
        .unwrap();
    let canonical = row.get::<_, Value>("manifest");
    assert_eq!(canonical["config"], json!({}));
    assert_eq!(canonical["contributes"], json!({}));
    assert_eq!(
        first["versions"][0]["digest"],
        row.get::<_, String>("digest")
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{ws}/plugins/preview"),
            Some(&owner),
            json!({"version_id":version})
        )
        .await
        .0,
        StatusCode::OK
    );

    // Publishing newer versions never changes an installation's consented version.
    let install_path = format!("/api/workspaces/{ws}/plugins");
    let (status, installation) = call(
        &app,
        "POST",
        &install_path,
        Some(&owner),
        json!({"version_id":version,"granted_scopes":["issues:read"]}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{installation}");

    // Two publications of one version commit exactly once; a losing rename rolls back.
    let mut a = manifest.clone();
    a["version"] = json!("1.0.1");
    a["name"] = json!("Winner A");
    let mut b = a.clone();
    b["name"] = json!("Winner B");
    let zip_a = bundle(&a);
    let zip_b = bundle(&b);
    let (a, b) = tokio::join!(
        upload(&app, &path, &owner, &zip_a),
        upload(&app, &path, &owner, &zip_b)
    );
    assert!(
        matches!(
            (a.0, b.0),
            (StatusCode::CREATED, StatusCode::CONFLICT)
                | (StatusCode::CONFLICT, StatusCode::CREATED)
        ),
        "{a:?} {b:?}"
    );
    let winner = if a.0 == StatusCode::CREATED {
        "Winner A"
    } else {
        "Winner B"
    };
    assert_eq!(
        db.query_one("SELECT name FROM plugin_package WHERE id=$1", &[&package])
            .await
            .unwrap()
            .get::<_, String>(0),
        winner
    );
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM plugin_package_version WHERE package_id=$1",
            &[&package],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);

    let installed_version: Uuid = db.query_one(
        "SELECT package_version_id FROM plugin_installation WHERE workspace_id=$1 AND plugin_key='example.published'", &[&ws],
    ).await.unwrap().get(0);
    assert_eq!(installed_version, version);
    let id = installation["id"].as_str().unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("{install_path}/{id}"),
            Some(&owner),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );

    // Publish failure after package upsert cannot leave an empty package identity.
    let key = format!("example.rollback-{}", Uuid::new_v4().simple());
    let mut failure = manifest.clone();
    failure["key"] = json!(key);
    db.batch_execute(&format!("CREATE FUNCTION package_publish_failure_{}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic failure'; END $$; CREATE TRIGGER package_publish_fail_{} BEFORE INSERT ON plugin_package_version FOR EACH ROW WHEN (NEW.manifest->>'key'='{}') EXECUTE FUNCTION package_publish_failure_{}()", ws.simple(),ws.simple(),key,ws.simple())).await.unwrap();
    assert_eq!(
        upload(&app, &path, &owner, &bundle(&failure)).await.0,
        StatusCode::BAD_GATEWAY
    );
    db.batch_execute(&format!("DROP TRIGGER package_publish_fail_{} ON plugin_package_version; DROP FUNCTION package_publish_failure_{}()",ws.simple(),ws.simple())).await.unwrap();
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM plugin_package WHERE workspace_id=$1 AND plugin_key=$2",
            &[&ws, &key],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);

    // Install and delete must never both commit for the same published version.
    let install_path = format!("/api/workspaces/{ws}/plugins");
    let delete_path = format!("{path}/{package}");
    let (installed, deleted) = tokio::join!(
        call(
            &app,
            "POST",
            &install_path,
            Some(&owner),
            json!({"version_id":version,"granted_scopes":["issues:read"]})
        ),
        call(&app, "DELETE", &delete_path, Some(&owner), Value::Null),
    );
    assert!(
        matches!(
            (installed.0, deleted.0),
            (StatusCode::CREATED, StatusCode::CONFLICT)
                | (StatusCode::NOT_FOUND, StatusCode::NO_CONTENT)
        ),
        "{installed:?} {deleted:?}"
    );
    if installed.0 == StatusCode::CREATED {
        let id = installed.1["id"].as_str().unwrap();
        assert_eq!(
            call(
                &app,
                "DELETE",
                &format!("{install_path}/{id}"),
                Some(&owner),
                Value::Null
            )
            .await
            .0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            call(&app, "DELETE", &delete_path, Some(&owner), Value::Null)
                .await
                .0,
            StatusCode::NO_CONTENT
        );
    }
    assert!(db.query_opt("SELECT 1 FROM plugin_installation WHERE workspace_id=$1 AND plugin_key='example.published'", &[&ws]).await.unwrap().is_none());
    assert!(
        db.query_opt(
            "SELECT 1 FROM plugin_package_version WHERE package_id=$1",
            &[&package]
        )
        .await
        .unwrap()
        .is_none()
    );
}
