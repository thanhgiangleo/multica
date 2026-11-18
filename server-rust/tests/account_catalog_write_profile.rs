//! HTTP and DB coverage for account profile and cloud-waitlist writes.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::Engine;
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
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
    raw_call(
        app,
        method,
        path,
        token,
        &body.to_string(),
        Some("application/json"),
    )
    .await
}

async fn raw_call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: &str,
    content_type: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"));
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    let request = request.body(Body::from(body.to_owned())).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let data = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&data).unwrap_or(Value::Null))
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn profile_and_waitlist_are_user_scoped_and_validated() {
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
    let mut ids = Vec::new();
    for n in 0..2 {
        let id: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES ('Profile', $1) RETURNING id",
                &[&format!("{n}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        ids.push(id);
    }
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
    let caller = jwt(ids[0]);
    let other = jwt(ids[1]);
    for invalid in [
        json!({"name":"  "}),
        json!({"language":"xx"}),
        json!({"timezone":"../../etc/passwd"}),
        json!({"profile_description":"x".repeat(2001)}),
    ] {
        assert_eq!(
            call(&app, "PATCH", "/api/me", &caller, invalid).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        call(
            &app,
            "PATCH",
            "/api/me",
            &caller,
            json!({"avatar_url":"workspaces/foreign/attachment.png"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            "/api/me",
            "invalid-token",
            json!({"name":"Bad"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    for later_invalid in [
        json!({"language":"xx"}),
        json!({"timezone":"Unknown/City"}),
        json!({"profile_description":"x".repeat(2001)}),
    ] {
        let mut payload = later_invalid;
        payload["avatar_url"] = json!("workspaces/foreign/attachment.png");
        let (status, body) = call(&app, "PATCH", "/api/me", &caller, payload).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(
            body["error"],
            "avatar_url must reference a standalone image upload, not a file attached to an issue, comment, or chat"
        );
    }
    let (status, body) = call(
        &app,
        "PATCH",
        "/api/me",
        &caller,
        json!({"name":" ","avatar_url":"workspaces/foreign/attachment.png","language":"xx"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "name is required");
    let cookie = Request::builder()
        .method("PATCH")
        .uri("/api/me")
        .header("cookie", format!("multica_auth={caller}"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"name":"No CSRF"}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let (status, profile) = call(&app,"PATCH","/api/me",&caller,json!({"name":"  New ","language":"fr","timezone":"Asia/Ho_Chi_Minh","profile_description":"  mô tả  "})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(profile["id"], ids[0].to_string());
    assert_eq!(profile["name"], "New");
    assert_eq!(profile["profile_description"], "mô tả");
    let (status, custom_zone) = call(
        &app,
        "PATCH",
        "/api/me",
        &caller,
        json!({"timezone":"Multica/Custom_Zone"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(custom_zone["timezone"], "Multica/Custom_Zone");
    assert_eq!(
        db.query_one("SELECT timezone FROM \"user\" WHERE id=$1", &[&ids[0]])
            .await
            .unwrap()
            .get::<_, String>(0),
        "Multica/Custom_Zone"
    );
    assert_eq!(
        call(&app, "PATCH", "/api/me", &caller, json!({"timezone":""}))
            .await
            .1["timezone"],
        Value::Null
    );
    assert_eq!(
        call(&app, "GET", "/api/me", &other, Value::Null).await.1["name"],
        "Profile"
    );
    for payload in [
        Value::Null,
        json!({}),
        json!({"name":null,"language":null,"avatar_url":null,"profile_description":null,"timezone":null}),
    ] {
        let (status, unchanged) = call(&app, "PATCH", "/api/me", &caller, payload).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(unchanged["name"], "New");
        assert_eq!(unchanged["language"], "fr");
        assert_eq!(unchanged["profile_description"], "mô tả");
        assert_eq!(unchanged["timezone"], Value::Null);
    }
    for raw in [
        r#"[null,null,null,null,null]"#,
        r#"{"name":{}}"#,
        "",
        "false",
    ] {
        let (status, body) = raw_call(&app, "PATCH", "/api/me", &caller, raw, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{raw}");
        assert_eq!(body["error"], "invalid request body");
    }
    let (status, profile) = raw_call(&app, "PATCH", "/api/me", &caller,
        r#"{"NAME":"Ignored by later null","name":null,"unknown":[false,{}]} {"name":"not decoded"}"#,
        Some("text/plain")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(profile["name"], "New");

    let waitlist = "/api/me/onboarding/cloud-waitlist";
    for raw in [
        "null",
        "{}",
        r#"{"email":null}"#,
        r#"{"reason":"only reason"}"#,
    ] {
        let (status, body) = raw_call(&app, "POST", waitlist, &caller, raw, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{raw}");
        assert_eq!(body["error"], "email is required");
    }
    for raw in [
        "[]",
        "false",
        "1",
        r#"{"email":7}"#,
        r#"{"email":"a@b","reason":[]}"#,
        r#"{"email":{},"email":"a@b"}"#,
    ] {
        let (status, body) = raw_call(&app, "POST", waitlist, &caller, raw, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{raw}");
        assert_eq!(body["error"], "invalid request body");
    }
    for bad in [
        json!({"email":""}),
        json!({"email":"not-an-email","reason":"ok"}),
        json!({"email":"a@example.com","reason":"x".repeat(501)}),
    ] {
        assert_eq!(
            call(
                &app,
                "POST",
                "/api/me/onboarding/cloud-waitlist",
                &caller,
                bad
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    let (status, body) = call(
        &app,
        "POST",
        "/api/me/onboarding/cloud-waitlist",
        &caller,
        json!({"email":"  Me@Example.com ","reason":"   "}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.get("cloud_waitlist_email").is_none());
    let row = db.query_one("SELECT cloud_waitlist_email,cloud_waitlist_reason,onboarded_at FROM \"user\" WHERE id=$1", &[&ids[0]]).await.unwrap();
    assert_eq!(
        row.get::<_, Option<String>>(0).as_deref(),
        Some("me@example.com")
    );
    assert_eq!(row.get::<_, Option<String>>(1), None);
    assert_eq!(row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(2), None);
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/me/onboarding/cloud-waitlist",
            &caller,
            json!({"email":"next@example.com","reason":"  hi  "})
        )
        .await
        .0,
        StatusCode::OK
    );
    let row = db
        .query_one(
            "SELECT cloud_waitlist_email,cloud_waitlist_reason FROM \"user\" WHERE id=$1",
            &[&ids[0]],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, Option<String>>(0).as_deref(),
        Some("next@example.com")
    );
    assert_eq!(row.get::<_, Option<String>>(1).as_deref(), Some("hi"));
    let inputs: Vec<String> =
        serde_json::from_str(include_str!("account_catalog_write_mail_inputs.json")).unwrap();
    let oracle: Vec<(String, bool)> =
        serde_json::from_str(include_str!("account_catalog_write_mail_oracle.json")).unwrap();
    assert_eq!(inputs.len(), oracle.len());
    let mut stored_email = "next@example.com".to_owned();
    let mut stored_reason = Some("hi");
    for (index, (raw, (normalized, valid))) in inputs.into_iter().zip(oracle).enumerate() {
        let payload = if index % 2 == 0 {
            json!({"email":raw})
        } else {
            json!({"email":raw,"reason":null})
        };
        let (status, body) = call(&app, "POST", waitlist, &caller, payload).await;
        if valid {
            assert_eq!(status, StatusCode::OK, "{raw:?}: {body}");
            assert!(body.get("cloud_waitlist_email").is_none());
            assert!(body.get("cloud_waitlist_reason").is_none());
            stored_email = normalized;
            stored_reason = None;
        } else {
            assert_eq!(status, StatusCode::BAD_REQUEST, "{raw:?}: {body}");
            assert_eq!(
                body["error"],
                if normalized.is_empty() {
                    "email is required"
                } else {
                    "email is invalid"
                }
            );
        }
        let row = db.query_one("SELECT cloud_waitlist_email,cloud_waitlist_reason,onboarded_at FROM \"user\" WHERE id=$1", &[&ids[0]]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), stored_email, "{raw:?}");
        assert_eq!(
            row.get::<_, Option<String>>(1).as_deref(),
            stored_reason,
            "{raw:?}"
        );
        assert_eq!(row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(2), None);
    }
    let (status, body) = raw_call(&app, "POST", waitlist, &caller,
        r#"{"EMAIL":" Kept@LOCALHOST ","email":null,"reason":"kept","REASON":null} {"email":"ignored@b"}"#,
        Some("text/plain")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let row = db
        .query_one(
            "SELECT cloud_waitlist_email,cloud_waitlist_reason FROM \"user\" WHERE id=$1",
            &[&ids[0]],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "kept@localhost");
    assert_eq!(row.get::<_, String>(1), "kept");
    for (email, reason, expected) in [
        (format!("{}@b", "é".repeat(126)), "é".repeat(250), None),
        (
            format!("{}@b", "é".repeat(127)),
            "ok".into(),
            Some("email is too long"),
        ),
        ("a@b".into(), "é".repeat(251), Some("reason is too long")),
    ] {
        let (status, body) = call(
            &app,
            "POST",
            waitlist,
            &caller,
            json!({"email":email,"reason":reason}),
        )
        .await;
        if let Some(message) = expected {
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["error"], message);
        } else {
            assert_eq!(status, StatusCode::OK, "{body}");
        }
    }
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/me/onboarding/cloud-waitlist",
            &other,
            json!({"email":"other@example.com","reason":"ok"})
        )
        .await
        .0,
        StatusCode::OK
    );
    let row = db
        .query_one(
            "SELECT cloud_waitlist_email,cloud_waitlist_reason FROM \"user\" WHERE id=$1",
            &[&ids[0]],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), format!("{}@b", "é".repeat(126)));
    assert_eq!(row.get::<_, String>(1), "é".repeat(250));
    let expired = encode(
        &Header::default(),
        &json!({"sub":ids[0].to_string(),"exp":chrono::Utc::now().timestamp()-3600}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();
    for token in ["bad-token", &expired] {
        assert_eq!(
            call(&app, "POST", waitlist, token, Value::Null).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    let cookie = Request::builder()
        .method("POST")
        .uri(waitlist)
        .header("cookie", format!("multica_auth={caller}"))
        .body(Body::from(r#"{"email":"a@b"}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Account test',$1) RETURNING id",
            &[&format!("account-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Account test agent','cloud',$2) RETURNING id", &[&ws,&ids[0]]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Account test runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let task_token = format!("mat_account_{suffix}");
    let task_hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&task_hash,&task,&agent,&ws,&ids[0]]).await.unwrap();
    for (method, path) in [("POST", waitlist), ("PATCH", "/api/me")] {
        assert_eq!(
            call(&app, method, path, &task_token, Value::Null).await.0,
            StatusCode::FORBIDDEN
        );
    }
    db.execute("DELETE FROM task_token WHERE token_hash=$1", &[&task_hash])
        .await
        .unwrap();
    assert_eq!(
        call(&app, "POST", waitlist, &task_token, Value::Null)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("DELETE FROM agent_task_queue WHERE id=$1", &[&task])
        .await
        .unwrap();
    db.execute("DELETE FROM agent WHERE id=$1", &[&agent])
        .await
        .unwrap();
    db.execute("DELETE FROM agent_runtime WHERE id=$1", &[&runtime])
        .await
        .unwrap();
    db.execute("DELETE FROM workspace WHERE id=$1", &[&ws])
        .await
        .unwrap();
    let cases: Vec<Value> = serde_json::from_str(include_str!(
        "account_catalog_write_profile_edges_oracle.json"
    ))
    .unwrap();
    let other_before = db
        .query_one("SELECT to_jsonb(u) FROM \"user\" u WHERE id=$1", &[&ids[1]])
        .await
        .unwrap()
        .get::<_, Value>(0);
    for case in cases {
        let label = case["label"].as_str().unwrap();
        let is_profile = case["kind"] == "profile";
        db.execute("UPDATE \"user\" SET name='Edge baseline',language='en',profile_description='Baseline',timezone='UTC',cloud_waitlist_email='baseline@example.com',cloud_waitlist_reason='Baseline' WHERE id=$1", &[&ids[0]]).await.unwrap();
        let payload = base64::engine::general_purpose::STANDARD
            .decode(case["body"].as_str().unwrap())
            .unwrap();
        let request = Request::builder()
            .method(if is_profile { "PATCH" } else { "POST" })
            .uri(if is_profile { "/api/me" } else { waitlist })
            .header("authorization", format!("Bearer {caller}"))
            .body(Body::from(payload))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        if case["error"] == "" {
            assert_eq!(status, StatusCode::OK, "{label}: {body}");
            if is_profile {
                for field in ["name", "language", "profile_description", "timezone"] {
                    assert_eq!(body[field], case["stored"][field], "{label}/{field}");
                }
            }
            assert!(body.get("cloud_waitlist_email").is_none(), "{label}");
            assert!(body.get("cloud_waitlist_reason").is_none(), "{label}");
        } else {
            assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {body}");
            assert_eq!(body["error"], case["error"], "{label}");
        }
        let stored: Value = db.query_one("SELECT jsonb_build_object('name',name,'language',language,'profile_description',profile_description,'timezone',timezone,'cloud_waitlist_email',cloud_waitlist_email,'cloud_waitlist_reason',cloud_waitlist_reason) FROM \"user\" WHERE id=$1", &[&ids[0]]).await.unwrap().get(0);
        assert_eq!(stored, case["stored"], "{label}");
    }
    let other_after = db
        .query_one("SELECT to_jsonb(u) FROM \"user\" u WHERE id=$1", &[&ids[1]])
        .await
        .unwrap()
        .get::<_, Value>(0);
    assert_eq!(other_before, other_after);
    for id in ids {
        db.execute("DELETE FROM \"user\" WHERE id=$1", &[&id])
            .await
            .unwrap();
    }
}
