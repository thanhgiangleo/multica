//! Daemon bundle HTTP/DB checks; all identities/data are synthetic and disposable.
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
use sha2::{Digest, Sha256};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls, config::Host};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"zbsi71-bundle-synthetic-secret";

fn jwt(user: Uuid) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+3600}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

async fn post(
    app: &axum::Router,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires child-owned migrated multica_zbsi71 PostgreSQL on random loopback port"]
async fn bundle_resolve_scoping_machine_auth_hashes_and_snapshot_consistency() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi71"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,Host::Tcp(s) if s=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let [
        owner,
        peer,
        ws,
        foreign,
        runtime,
        other_runtime,
        agent,
        issue,
        task,
    ]: [Uuid; 9] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, peer] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES($1,'Bundle',$2)",
            &[&user, &format!("bundle-{user}@invalid.example")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, foreign] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES($1,'Bundle',$2)",
            &[&workspace, &format!("bundle-{workspace}")],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'admin')",
        &[&ws, &owner, &peer],
    )
    .await
    .unwrap();
    for (id, daemon) in [
        (runtime, "bundle-machine"),
        (other_runtime, "other-machine"),
    ] {
        db.execute("INSERT INTO agent_runtime(id,workspace_id,daemon_id,name,runtime_mode,provider,status,owner_id) VALUES($1,$2,$3,'Bundle','local','synthetic','online',$4)", &[&id,&ws,&daemon,&owner]).await.unwrap();
    }
    db.execute("INSERT INTO agent(id,workspace_id,name,owner_id,runtime_id,runtime_mode,status) VALUES($1,$2,'Bundle',$3,$4,'local','working')", &[&agent,&ws,&owner,&runtime]).await.unwrap();
    db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id) VALUES($1,$2,'Bundle','member',$3)", &[&issue,&ws,&owner]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,dispatched_at) VALUES($1,$2,$3,$4,'dispatched',now())", &[&task,&agent,&runtime,&issue]).await.unwrap();
    let mut skills = Vec::<Uuid>::new();
    for (workspace, name, enabled) in [
        (ws, "allowed", true),
        (ws, "disabled", false),
        (foreign, "foreign", true),
        (ws, "unrequested", true),
    ] {
        let id:Uuid = db.query_one("INSERT INTO skill(workspace_id,name,description,content,created_by) VALUES($1,$2,'UTF-8 dữ liệu','v0',$3) RETURNING id", &[&workspace,&name,&owner]).await.unwrap().get(0);
        db.execute(
            "INSERT INTO agent_skill(agent_id,skill_id,enabled) VALUES($1,$2,$3)",
            &[&agent, &id, &enabled],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO skill_file(skill_id,path,content) VALUES($1,'references/a.md','v0')",
            &[&id],
        )
        .await
        .unwrap();
        skills.push(id);
    }
    let token = format!("mdt_bundle_{ws}");
    db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES($1,$2,'bundle-machine',$3,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(token.as_bytes())),&ws,&owner]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let path = format!("/api/daemon/runtimes/{runtime}/tasks/{task}/skill-bundles/resolve");
    let wanted =
        json!({"skills":[{"id":skills[0],"source":"workspace","hash":"stale"}]}).to_string();
    for bad in [None, Some("mat_not-daemon"), Some("mdt_invalid")] {
        assert_eq!(
            post(&app, &path, bad, &wanted).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&path)
                .header("cookie", format!("multica_auth={}", jwt(owner)))
                .body(Body::from(wanted.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "daemon API ignores browser cookies"
    );
    assert_eq!(
        post(&app, &path, Some(&jwt(peer)), &wanted).await.0,
        StatusCode::NOT_FOUND,
        "admin is not machine owner"
    );
    assert_eq!(
        post(&app, &path, Some(&token), "bad JSON").await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post(&app, &path, Some(&token), r#"{"skills":[null]}"#)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    for raw in ["null", "{}", r#"{"skills":null}"#] {
        assert_eq!(
            post(&app, &path, Some(&token), raw).await,
            (StatusCode::OK, json!({"bundles":[]}))
        );
    }
    let (status, response) = post(&app, &path, Some(&token), &wanted).await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    assert_eq!(response["bundles"].as_array().unwrap().len(), 1);
    assert_eq!(response["bundles"][0]["id"], skills[0].to_string());
    assert_eq!(response["bundles"][0]["content"], "v0");
    assert_eq!(response["bundles"][0]["files"][0]["content"], "v0");
    assert!(
        response["bundles"][0]["hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_ne!(response["bundles"][0]["hash"], "stale");
    assert!(response["bundles"][0].get("config").is_none());
    assert!(response["bundles"][0].get("created_by").is_none());
    assert_eq!(
        post(&app, &path, Some(&jwt(owner)), &wanted).await.0,
        StatusCode::OK
    );
    for (id, source) in [
        (skills[1].to_string(), "workspace"),
        (skills[2].to_string(), "workspace"),
        (Uuid::new_v4().to_string(), "workspace"),
        ("malformed".into(), "workspace"),
        (skills[0].to_string(), "plugin"),
        (skills[0].to_string(), "other"),
    ] {
        let req = json!({"skills":[{"id":id,"source":source,"hash":"old"}]}).to_string();
        let (status, response) = post(&app, &path, Some(&token), &req).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{id} {response:?}");
        assert_eq!(response["error"], "skill bundle not found");
    }
    let req=json!({"skills":[{"id":skills[0],"source":"workspace","hash":"old"},{"id":skills[1],"source":"workspace","hash":"old"}]}).to_string();
    assert_eq!(
        post(&app, &path, Some(&token), &req).await.0,
        StatusCode::NOT_FOUND,
        "never return partial bundles"
    );
    let repeated=json!({"Skills":[{"ID":skills[0],"SOURCE":"workspace","Hash":"old"},{"id":skills[0],"source":"workspace","hash":"old"}]}).to_string();
    let (status, repeated) = post(
        &app,
        &path,
        Some(&token),
        &format!("{repeated} {{\"ignored\":true}}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{repeated:?}");
    assert_eq!(repeated["bundles"][0], repeated["bundles"][1]);
    let vectors: Value =
        serde_json::from_str(include_str!("runtime_skills_sprint_vectors.json")).unwrap();
    for vector in vectors
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["skill"]["Source"] == "builtin")
    {
        let req =
            json!({"skills":[{"id":vector["skill"]["ID"],"source":"builtin","hash":"stale"}]})
                .to_string();
        let (status, body) = post(&app, &path, Some(&token), &req).await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        assert_eq!(body["bundles"][0]["hash"], vector["manifest"]["Hash"]);
    }
    let wrong_path =
        format!("/api/daemon/runtimes/{other_runtime}/tasks/{task}/skill-bundles/resolve");
    assert_eq!(
        post(&app, &wrong_path, Some(&token), &wanted).await.0,
        StatusCode::NOT_FOUND
    );
    for state in ["running", "completed", "cancelled"] {
        db.execute(
            "UPDATE agent_task_queue SET status=$2 WHERE id=$1",
            &[&task, &state],
        )
        .await
        .unwrap();
        assert_eq!(
            post(&app, &path, Some(&token), &wanted).await,
            (
                StatusCode::CONFLICT,
                json!({"error":"task is not preparing"})
            )
        );
    }
    db.execute(
        "UPDATE agent_task_queue SET status='waiting_local_directory' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&app, &path, Some(&token), &wanted).await.0,
        StatusCode::OK
    );
    // Transient file-query failure must not yield a cacheable, incomplete bundle.
    db.batch_execute("ALTER TABLE skill_file RENAME TO runtime_skills_sprint_unavailable_file")
        .await
        .unwrap();
    let failure = post(&app, &path, Some(&token), &wanted).await;
    db.batch_execute("ALTER TABLE runtime_skills_sprint_unavailable_file RENAME TO skill_file")
        .await
        .unwrap();
    assert_eq!(
        failure,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to load skill bundles"})
        )
    );
    // A writer commits body and file together; every resolve must see one version.
    let writer = async {
        let mut writer = pool.get().await.unwrap();
        for i in 1..=20 {
            let tx = writer.transaction().await.unwrap();
            let value = format!("v{i}");
            tx.execute(
                "UPDATE skill SET content=$2 WHERE id=$1",
                &[&skills[0], &value],
            )
            .await
            .unwrap();
            tx.execute(
                "UPDATE skill_file SET content=$2 WHERE skill_id=$1",
                &[&skills[0], &value],
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            tokio::task::yield_now().await;
        }
    };
    let reader = async {
        for _ in 0..30 {
            let (status, body) = post(&app, &path, Some(&token), &wanted).await;
            assert_eq!(status, StatusCode::OK, "{body:?}");
            assert_eq!(
                body["bundles"][0]["content"],
                body["bundles"][0]["files"][0]["content"]
            );
        }
    };
    tokio::join!(writer, reader);
    db.execute(
        "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1",
        &[&runtime, &peer],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&app, &path, Some(&token), &wanted).await.0,
        StatusCode::NOT_FOUND,
        "old bound daemon cannot follow owner changes"
    );
    db.execute(
        "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1",
        &[&runtime, &owner],
    )
    .await
    .unwrap();
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&app, &path, Some(&jwt(owner)), &wanted).await.0,
        StatusCode::NOT_FOUND,
        "user-credential membership removal revokes access"
    );
}
