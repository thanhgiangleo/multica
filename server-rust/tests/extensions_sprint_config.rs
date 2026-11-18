//! Configuration lifecycle against synthetic installations on a disposable database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::api::{self, ApiState};
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SESSION_KEY: &[u8] = b"synthetic-zbsi72-config-session";
const CONFIG_KEY: [u8; 32] = [19; 32];
const PRIVATE: &str = "synthetic-write-only-config-secret";
fn session(user: Uuid) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+3600}),
        &EncodingKey::from_secret(SESSION_KEY),
    )
    .unwrap()
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    auth: Option<&str>,
    installation: Option<Uuid>,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(auth) = auth {
        request = request.header("authorization", format!("Bearer {auth}"));
    }
    if let Some(id) = installation {
        request = request.header("x-multica-plugin-installation", id.to_string());
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
struct Fixture {
    pool: Arc<Pool>,
    app: axum::Router,
    no_key: axum::Router,
    disabled: axum::Router,
    ws: Uuid,
    other: Uuid,
    install: Uuid,
    owner: String,
    admin: String,
    member: String,
    outsider: String,
    token: String,
}
impl Fixture {
    async fn new() -> Self {
        let config =
            Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(config.get_dbname(), Some("multica_zbsi72"));
        assert!(!config.get_hosts().is_empty());
        assert!(
            config
                .get_hosts()
                .iter()
                .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1"))
        );
        assert!(config.get_ports().iter().all(|p| *p != 5432));
        let pool = Arc::new(
            Pool::builder(Manager::new(config, NoTls))
                .max_size(12)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let mut users = Vec::new();
        for name in ["Owner", "Admin", "Member", "Outsider"] {
            users.push(
                db.query_one(
                    "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                    &[&name, &format!("config-{}@invalid.example", Uuid::new_v4())],
                )
                .await
                .unwrap()
                .get::<_, Uuid>(0),
            );
        }
        let ws=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES('Config',$1,'CFG') RETURNING id", &[&format!("cfg-{}",Uuid::new_v4())]).await.unwrap().get::<_,Uuid>(0);
        let other = db
            .query_one(
                "INSERT INTO workspace(name,slug) VALUES('Other',$1) RETURNING id",
                &[&format!("cfg-other-{}", Uuid::new_v4())],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0);
        db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'admin'),($1,$4,'member'),($5,$2,'owner')", &[&ws,&users[0],&users[1],&users[2],&other]).await.unwrap();
        let manifest = json!({"manifest_version":1,"key":"example.config","name":"Configuration Fixture","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["storage:workspace"],"config":{
            "text":{"type":"string","label":"Text"},"count":{"type":"number","label":"Count"},"flag":{"type":"bool","label":"Flag"},"mode":{"type":"enum","label":"Mode","options":["a","b"]},"secret":{"type":"secret","label":"Secret"},"secret2":{"type":"secret","label":"Second secret"}},"contributes":{}});
        let package=db.query_one("INSERT INTO plugin_package(workspace_id,plugin_key,name) VALUES($1,'example.config','Configuration Fixture') RETURNING id", &[&ws]).await.unwrap().get::<_,Uuid>(0);
        let version=db.query_one("INSERT INTO plugin_package_version(package_id,workspace_id,version,manifest,digest,size_bytes) VALUES($1,$2,'1.0.0',$3,$4,0) RETURNING id", &[&package,&ws,&manifest,&"0".repeat(64)]).await.unwrap().get::<_,Uuid>(0);
        let token = format!("mpi_synthetic_config_{}", Uuid::new_v4());
        let hash = hex::encode(Sha256::digest(token.as_bytes()));
        let install=db.query_one("INSERT INTO plugin_installation(workspace_id,plugin_key,version,manifest,package_version_id,granted_scopes,config,token_hash) VALUES($1,'example.config','1.0.0',$2,$3,$4,$5,$6) RETURNING id", &[&ws,&manifest,&version,&json!(["storage:workspace"]),&json!({"text":"initial","count":0}),&hash]).await.unwrap().get::<_,Uuid>(0);
        drop(db);
        let app = api::routes(
            ApiState::new(pool.clone(), SESSION_KEY.to_vec())
                .with_extensions_sprint(true)
                .with_extensions_sprint_config_key(CONFIG_KEY),
        );
        let no_key = api::routes(
            ApiState::new(pool.clone(), SESSION_KEY.to_vec()).with_extensions_sprint(true),
        );
        let disabled = api::routes(
            ApiState::new(pool.clone(), SESSION_KEY.to_vec())
                .with_extensions_sprint_config_key(CONFIG_KEY),
        );
        Self {
            pool,
            app,
            no_key,
            disabled,
            ws,
            other,
            install,
            owner: session(users[0]),
            admin: session(users[1]),
            member: session(users[2]),
            outsider: session(users[3]),
            token,
        }
    }
    fn path(&self) -> String {
        format!(
            "/api/workspaces/{}/plugins/{}/config",
            self.ws, self.install
        )
    }
    async fn update(&self, values: Value) -> (StatusCode, Value) {
        call(
            &self.app,
            "PUT",
            &self.path(),
            Some(&self.owner),
            None,
            json!({"values":values}),
        )
        .await
    }
    async fn config(&self) -> Value {
        self.pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT config FROM plugin_installation WHERE id=$1",
                &[&self.install],
            )
            .await
            .unwrap()
            .get(0)
    }
    async fn ciphertext(&self, key: &str) -> Vec<u8> {
        self.pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT ciphertext FROM plugin_secret WHERE installation_id=$1 AND key=$2",
                &[&self.install, &key],
            )
            .await
            .unwrap()
            .get(0)
    }
}
fn decrypt(sealed: &[u8]) -> Vec<u8> {
    let key = LessSafeKey::new(UnboundKey::new(&aead::AES_256_GCM, &CONFIG_KEY).unwrap());
    let mut bytes = sealed[12..].to_vec();
    key.open_in_place(
        Nonce::try_assume_unique_for_key(&sealed[..12]).unwrap(),
        Aad::empty(),
        &mut bytes,
    )
    .unwrap()
    .to_vec()
}

#[tokio::test]
async fn config_route_requires_auth_without_database_access() {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(1)
        .connect_timeout(Duration::from_millis(50));
    let pool = Arc::new(Pool::builder(Manager::new(config, NoTls)).build().unwrap());
    let app = api::routes(ApiState::new(pool, SESSION_KEY.to_vec()).with_extensions_sprint(true));
    let path = format!(
        "/api/workspaces/{}/plugins/{}/config",
        Uuid::new_v4(),
        Uuid::new_v4()
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &path,
            None,
            None,
            json!({"values":{"secret":PRIVATE}})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
#[ignore = "requires run-owned migrated multica_zbsi72 database on loopback"]
async fn config_encryption_projection_validation_and_authorization() {
    let f = Fixture::new().await;
    let values = json!({"text":"visible","count":1.25,"flag":false,"mode":"b","secret":PRIVATE});
    for (app, auth, path, expected) in [
        (&f.app, &f.member, f.path(), StatusCode::FORBIDDEN),
        (&f.app, &f.outsider, f.path(), StatusCode::NOT_FOUND),
        (&f.disabled, &f.owner, f.path(), StatusCode::FORBIDDEN),
        (
            &f.app,
            &f.owner,
            format!("/api/workspaces/{}/plugins/{}/config", f.other, f.install),
            StatusCode::NOT_FOUND,
        ),
    ] {
        let (status, body) = call(
            app,
            "PUT",
            &path,
            Some(auth),
            None,
            json!({"values":values}),
        )
        .await;
        assert_eq!(status, expected, "{body}");
        assert!(!body.to_string().contains(PRIVATE));
    }
    assert_eq!(f.config().await, json!({"text":"initial","count":0}));
    let (status, body) = call(
        &f.app,
        "PUT",
        &f.path(),
        Some(&f.admin),
        None,
        json!({"values":values}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["config"],
        json!({"text":"visible","count":1.25,"flag":false,"mode":"b"})
    );
    assert_eq!(body["configured_secrets"], json!(["secret"]));
    assert!(!body.to_string().contains(PRIVATE));
    let first = f.ciphertext("secret").await;
    assert_eq!(decrypt(&first), PRIVATE.as_bytes());
    assert!(
        !first
            .windows(PRIVATE.len())
            .any(|w| w == PRIVATE.as_bytes())
    );
    assert_eq!(f.update(json!({"secret":PRIVATE})).await.0, StatusCode::OK);
    assert_ne!(first, f.ciphertext("secret").await);
    assert_eq!(f.config().await, body["config"]);
    // Both public contexts, list and storage must omit plaintext and ciphertext.
    for (path, auth, id) in [
        ("/v1/context".to_owned(), f.token.as_str(), None),
        (
            "/api/plugin-bridge/v1/context".to_owned(),
            f.owner.as_str(),
            Some(f.install),
        ),
        (
            format!("/api/workspaces/{}/plugins", f.ws),
            f.owner.as_str(),
            None,
        ),
        ("/v1/storage/workspace".to_owned(), f.token.as_str(), None),
    ] {
        let (status, body) = call(&f.app, "GET", &path, Some(auth), id, Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(!body.to_string().contains(PRIVATE));
        assert!(!body.to_string().contains("ciphertext"));
    }
    let before = f.config().await;
    let before_secret = f.ciphertext("secret").await;
    for values in [
        json!({"unknown":PRIVATE}),
        json!({"text":3}),
        json!({"count":"1"}),
        json!({"flag":1}),
        json!({"mode":"invalid"}),
        json!({"text":"é".repeat(2049)}),
        json!({"secret":"é".repeat(4097)}),
        json!({"secret":false}),
        json!({"count":null}),
    ] {
        let (status, body) = f.update(values).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(!body.to_string().contains(PRIVATE));
        assert_eq!(f.config().await, before);
        assert_eq!(f.ciphertext("secret").await, before_secret);
    }
    // Secret key absent: mixed update and even explicit clear must fail atomically.
    for values in [
        json!({"text":"no-key-change","secret":PRIVATE}),
        json!({"text":"no-key-change","secret":""}),
    ] {
        let (status, body) = call(
            &f.no_key,
            "PUT",
            &f.path(),
            Some(&f.owner),
            None,
            json!({"values":values}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
        assert!(!body.to_string().contains(PRIVATE));
        assert_eq!(f.config().await, before);
        assert_eq!(f.ciphertext("secret").await, before_secret);
    }
    assert_eq!(
        call(
            &f.no_key,
            "PUT",
            &f.path(),
            Some(&f.owner),
            None,
            json!({"values":{"count":9}})
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, body) = f.update(json!({"secret":""})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["configured_secrets"], json!([]));
    assert_eq!(body["config"]["text"], "visible");
    assert_eq!(body["config"]["count"], 9);
    assert_eq!(
        f.pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT count(*) FROM plugin_secret WHERE installation_id=$1",
                &[&f.install]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}

#[tokio::test]
#[ignore = "requires run-owned migrated multica_zbsi72 database on loopback"]
async fn config_concurrency_rollback_and_uninstall_leave_no_orphans() {
    let f = Fixture::new().await;
    assert_eq!(
        f.update(json!({"secret":"old-synthetic","secret2":"keep-synthetic"}))
            .await
            .0,
        StatusCode::OK
    );
    for index in 0..6 {
        let text = format!("concurrent-{index}");
        let (first, second) = tokio::join!(
            f.update(json!({"text":text})),
            f.update(json!({"count":index}))
        );
        assert_eq!(first.0, StatusCode::OK, "{}", first.1);
        assert_eq!(second.0, StatusCode::OK, "{}", second.1);
        let current = f.config().await;
        assert_eq!(current["text"], text);
        assert_eq!(current["count"], index);
    }
    let before = f.config().await;
    let old = f.ciphertext("secret").await;
    let keep = f.ciphertext("secret2").await;
    let db = f.pool.get().await.unwrap();
    // A late failure after both secret mutations must roll all tables back.
    let suffix = f.install.simple().to_string();
    db.batch_execute(&format!("CREATE FUNCTION config_fail_{suffix}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='{}'::uuid THEN RAISE EXCEPTION 'synthetic late config failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER config_fail_{suffix} BEFORE UPDATE OF config ON plugin_installation FOR EACH ROW EXECUTE FUNCTION config_fail_{suffix}();",f.install)).await.unwrap();
    let (status, body) = f
        .update(json!({"text":"rollback","secret":PRIVATE,"secret2":""}))
        .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(!body.to_string().contains(PRIVATE));
    assert_eq!(f.config().await, before);
    assert_eq!(f.ciphertext("secret").await, old);
    assert_eq!(f.ciphertext("secret2").await, keep);
    db.batch_execute(&format!("DROP TRIGGER config_fail_{suffix} ON plugin_installation; DROP FUNCTION config_fail_{suffix}();")).await.unwrap();
    // Config forms are editable while disabled; no runtime is implicitly invoked.
    db.execute(
        "UPDATE plugin_installation SET enabled=false WHERE id=$1",
        &[&f.install],
    )
    .await
    .unwrap();
    assert_eq!(
        f.update(json!({"text":"disabled edit"})).await.0,
        StatusCode::OK
    );
    let delete_path = format!("/api/workspaces/{}/plugins/{}", f.ws, f.install);
    let (configure, delete) = tokio::join!(
        f.update(json!({"secret":PRIVATE})),
        call(
            &f.app,
            "DELETE",
            &delete_path,
            Some(&f.owner),
            None,
            Value::Null
        )
    );
    assert!(
        matches!(configure.0, StatusCode::OK | StatusCode::NOT_FOUND),
        "{:?}",
        configure
    );
    assert_eq!(delete.0, StatusCode::NO_CONTENT, "{}", delete.1);
    assert_eq!(db.query_one("SELECT (SELECT count(*) FROM plugin_installation WHERE id=$1)+(SELECT count(*) FROM plugin_secret WHERE installation_id=$1)", &[&f.install]).await.unwrap().get::<_,i64>(0),0);
    assert_eq!(
        f.update(json!({"secret":PRIVATE})).await.0,
        StatusCode::NOT_FOUND
    );
}
