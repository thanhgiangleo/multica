//! Write-side avatar policy against an isolated migrated PostgreSQL database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use deadpool_postgres::{Manager, Pool};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{ApiState, account_catalog_write_avatar::AvatarStorageUrls},
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &str = "synthetic-zbsi65-avatar-secret";
const BASE: &str = "https://objects.invalid/";

struct StorageUrls;
impl AvatarStorageUrls for StorageUrls {
    fn key_from_url(&self, url: &str) -> String {
        url.strip_prefix(BASE)
            .unwrap_or_else(|| url.rsplit('/').next().unwrap_or(""))
            .to_owned()
    }
    fn object_url(&self, key: &str) -> String {
        format!("{BASE}{key}")
    }
}

fn jwt(user: Uuid, exp: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":exp}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

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

fn signed_url(key: &str) -> String {
    let signing_key = Sha256::digest([b"avatar-url:".as_slice(), SECRET.as_bytes()].concat());
    let mut mac = Hmac::<Sha256>::new_from_slice(&signing_key).unwrap();
    mac.update(key.as_bytes());
    format!(
        "/api/avatars/{}/{key}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn avatar_admission_checks_database_and_normalizes_signed_replies() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let owner: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Owner',$1) RETURNING id",
            &[&format!("owner-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Outsider',$1) RETURNING id",
            &[&format!("outsider-{suffix}@invalid.example")],
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
        ApiState::new(pool, SECRET.as_bytes().to_vec())
            .with_avatar_storage_urls(Arc::new(StorageUrls)),
    );
    let now = chrono::Utc::now().timestamp();
    let owner_jwt = jwt(owner, now + 3600);
    let outsider_jwt = jwt(outsider, now + 3600);
    let (status, workspace) = call(
        &app,
        "POST",
        "/api/workspaces",
        &owner_jwt,
        json!({"name":"Avatar Test", "slug":format!("avatar-{suffix}")}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{workspace}");
    let ws = Uuid::parse_str(workspace["id"].as_str().unwrap()).unwrap();
    let path = format!("/api/workspaces/{ws}");

    let good = Uuid::new_v4();
    let bound = Uuid::new_v4();
    let non_image = Uuid::new_v4();
    for (id, content_type, task_id) in [
        (good, "image/png", None),
        (bound, "image/png", Some(Uuid::new_v4())),
        (non_image, "text/plain", None),
    ] {
        db.execute("INSERT INTO attachment(id,workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes,task_id) VALUES($1,$2,'member',$3,'avatar.png',$4,$5,1,$6)", &[&id,&ws,&owner,&format!("{BASE}workspaces/{ws}/{id}.png"),&content_type,&task_id]).await.unwrap();
    }
    let key = |id: Uuid| format!("workspaces/{ws}/{id}.png");
    let url = |id: Uuid| format!("{BASE}{}", key(id));
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES($1,'Avatar binding','member',$2) RETURNING id", &[&ws,&owner]).await.unwrap().get(0);
    let comment: Uuid = db.query_one("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content) VALUES($1,$2,'member',$3,'Synthetic binding') RETURNING id", &[&ws,&issue,&owner]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Synthetic avatar fixture','cloud',$2) RETURNING id", &[&ws,&owner]).await.unwrap().get(0);
    let chat: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id) VALUES($1,$2,$3) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let message: Uuid = db.query_one("INSERT INTO chat_message(chat_session_id,role,content) VALUES($1,'user','Synthetic binding') RETURNING id", &[&chat]).await.unwrap().get(0);
    let mut denied = vec![
        url(bound),
        url(non_image),
        url(Uuid::new_v4()),
        format!("{BASE}workspaces/{ws}/invalid.png"),
        format!("{BASE}workspaces/{ws}/{good}.svg"),
    ];
    for (column, binding) in [
        ("issue_id", issue),
        ("comment_id", comment),
        ("chat_session_id", chat),
        ("chat_message_id", message),
    ] {
        let id = Uuid::new_v4();
        db.execute(&format!("INSERT INTO attachment(id,workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes,{column}) VALUES($1,$2,'member',$3,'avatar.png',$4,'image/png',1,$5)"), &[&id,&ws,&owner,&url(id),&binding]).await.unwrap();
        denied.push(url(id));
    }
    for avatar_url in denied {
        let (status, response) = call(
            &app,
            "PATCH",
            "/api/me",
            &owner_jwt,
            json!({"avatar_url":avatar_url}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{response}");
        assert_eq!(
            call(
                &app,
                "PATCH",
                &path,
                &owner_jwt,
                json!({"avatar_url":avatar_url})
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
    }
    // A lossy KeyFromURL must not classify somebody else's CDN as ours.
    let third_party = format!("https://other.invalid/workspaces/{ws}/{bound}.png");
    assert_eq!(
        call(
            &app,
            "PATCH",
            "/api/me",
            &owner_jwt,
            json!({"avatar_url":third_party})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &path,
            &outsider_jwt,
            json!({"avatar_url":url(good)})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            "/api/me",
            &jwt(owner, now - 3600),
            json!({"avatar_url":url(good)})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let cookie = Request::builder()
        .method("PATCH")
        .uri("/api/me")
        .header("cookie", format!("multica_auth={owner_jwt}"))
        .header("content-type", "application/json")
        .body(Body::from(json!({"avatar_url":url(good)}).to_string()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let (status, profile) = call(
        &app,
        "PATCH",
        "/api/me",
        &owner_jwt,
        json!({"avatar_url":url(good)}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{profile}");
    assert_eq!(profile["avatar_url"], url(good));
    let served = signed_url(&key(good));
    let (status, workspace) = call(
        &app,
        "PATCH",
        &path,
        &owner_jwt,
        json!({"avatar_url":format!("https://api.invalid{served}")}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{workspace}");
    assert_eq!(workspace["avatar_url"], url(good));
    let row = db
        .query_one("SELECT avatar_url FROM workspace WHERE id=$1", &[&ws])
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, Option<String>>(0).as_deref(),
        Some(url(good).as_str())
    );
    let (status, profile) = call(
        &app,
        "PATCH",
        "/api/me",
        &owner_jwt,
        json!({"avatar_url":served}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{profile}");
    assert_eq!(profile["avatar_url"], url(good));
    // Binding revokes future cross-entity use, but an unchanged re-send is valid.
    db.execute(
        "UPDATE attachment SET task_id=$2 WHERE id=$1",
        &[&good, &Uuid::new_v4()],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "PATCH",
            "/api/me",
            &owner_jwt,
            json!({"avatar_url":url(good)})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &path,
            &owner_jwt,
            json!({"avatar_url":url(good)})
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, _) = call(
        &app,
        "PATCH",
        "/api/me",
        &outsider_jwt,
        json!({"avatar_url":url(good)}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let stored = db
        .query_one("SELECT avatar_url FROM \"user\" WHERE id=$1", &[&owner])
        .await
        .unwrap();
    assert_eq!(
        stored.get::<_, Option<String>>(0).as_deref(),
        Some(url(good).as_str())
    );
}
