//! Workspace creation and membership role HTTP/DB checks on a disposable database.
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
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &str = "synthetic-zbsi65-workspace-secret";
fn jwt(user: Uuid, expires: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":expires}),
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
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}
#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn workspace_create_and_role_guards() {
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
    let mut users = Vec::new();
    for name in ["Owner", "Second", "Admin", "Outsider"] {
        let id: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                &[
                    &name,
                    &format!("{}-{suffix}@invalid.example", name.to_ascii_lowercase()),
                ],
            )
            .await
            .unwrap()
            .get(0);
        users.push(id);
    }
    let [owner_id, second_id, admin_id, outsider_id] = <[Uuid; 4]>::try_from(users).unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec()),
    );
    let now = chrono::Utc::now().timestamp();
    let owner = jwt(owner_id, now + 3600);
    let second = jwt(second_id, now + 3600);
    let admin = jwt(admin_id, now + 3600);
    let outsider = jwt(outsider_id, now + 3600);
    let expired = jwt(owner_id, now - 300);
    let slug = format!("member-{suffix}");
    let create = json!({"name":"  My Workspace  ", "slug":slug,"issue_prefix":"  ab9  "});
    assert_eq!(
        call(&app, "POST", "/api/workspaces", &expired, create.clone())
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let cookie = Request::builder()
        .method("POST")
        .uri("/api/workspaces")
        .header("cookie", format!("multica_auth={owner}"))
        .header("content-type", "application/json")
        .body(Body::from(create.to_string()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    for invalid in [
        json!({"name":"","slug":"hello"}),
        json!({"name":"yes","slug":"-bad"}),
        json!({"name":"yes","slug":"login"}),
        json!({"name":"yes","slug":"work","issue_prefix":"A-1"}),
    ] {
        assert_eq!(
            call(&app, "POST", "/api/workspaces", &owner, invalid)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let (status, result) = call(&app, "POST", "/api/workspaces", &owner, create.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{result}");
    assert_eq!(result["name"], "My Workspace");
    assert_eq!(result["issue_prefix"], "AB9");
    let unicode_slug = format!("İdea-{suffix}");
    let (status, unicode_created) = call(
        &app,
        "POST",
        "/api/workspaces",
        &owner,
        json!({"name":"Unicode workspace","slug":unicode_slug,"issue_prefix":" ıſ9 "}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{unicode_created}");
    assert_eq!(unicode_created["slug"], format!("idea-{suffix}"));
    assert_eq!(unicode_created["issue_prefix"], "IS9");
    let unicode_id = Uuid::parse_str(unicode_created["id"].as_str().unwrap()).unwrap();
    let unicode_path = format!("/api/workspaces/{unicode_id}");
    let (status, unicode_updated) = call(
        &app,
        "PUT",
        &unicode_path,
        &owner,
        json!({"issue_prefix":" ıſ7 "}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{unicode_updated}");
    assert_eq!(unicode_updated["issue_prefix"], "IS7");
    assert_eq!(
        call(
            &app,
            "PUT",
            &unicode_path,
            &owner,
            json!({"issue_prefix":"ß"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let persisted = db
        .query_one(
            "SELECT slug,issue_prefix FROM workspace WHERE id=$1",
            &[&unicode_id],
        )
        .await
        .unwrap();
    assert_eq!(persisted.get::<_, String>("slug"), format!("idea-{suffix}"));
    assert_eq!(persisted.get::<_, String>("issue_prefix"), "IS7");
    let ws = Uuid::parse_str(result["id"].as_str().unwrap()).unwrap();
    let member_count: i64 = db
        .query_one(
            "SELECT count(*) FROM member WHERE workspace_id=$1 AND user_id=$2 AND role='owner'",
            &[&ws, &owner_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(member_count, 1);
    let statuses: Vec<String> = db
        .query(
            "SELECT key FROM issue_status WHERE workspace_id=$1 AND is_system",
            &[&ws],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(statuses.len(), 7);
    assert_eq!(
        call(&app, "POST", "/api/workspaces/", &owner, create)
            .await
            .0,
        StatusCode::CONFLICT
    );
    let ws_count: i64 = db
        .query_one("SELECT count(*) FROM workspace WHERE slug=$1", &[&slug])
        .await
        .unwrap()
        .get(0);
    assert_eq!(ws_count, 1);
    let raced = json!({"name":"Concurrent", "slug":format!("race-{suffix}")});
    let (a, b) = tokio::join!(
        call(&app, "POST", "/api/workspaces", &owner, raced.clone()),
        call(&app, "POST", "/api/workspaces", &owner, raced)
    );
    let mut create_statuses = [a.0, b.0];
    create_statuses.sort();
    assert_eq!(create_statuses, [StatusCode::CREATED, StatusCode::CONFLICT]);
    db.batch_execute("CREATE FUNCTION pg_temp.reject_workspace_seed() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic seed fault'; END $$; CREATE TRIGGER account_catalog_seed_fault BEFORE INSERT ON issue_status FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_workspace_seed()").await.unwrap();
    let rollback_slug = format!("rollback-{suffix}");
    let (status, failure) = call(
        &app,
        "POST",
        "/api/workspaces",
        &owner,
        json!({"name":"Rollback","slug":rollback_slug}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{failure}");
    db.batch_execute("DROP TRIGGER account_catalog_seed_fault ON issue_status")
        .await
        .unwrap();
    let rolled_back: i64 = db
        .query_one(
            "SELECT count(*) FROM workspace WHERE slug=$1",
            &[&rollback_slug],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(rolled_back, 0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign',$1) RETURNING id",
            &[&format!("foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member'),($1,$3,'admin'),($4,$2,'owner')",
        &[&ws,&second_id,&admin_id,&foreign]).await.unwrap();
    let members: Vec<Uuid> = db
        .query(
            "SELECT id FROM member WHERE workspace_id=$1 AND user_id IN ($2,$3) ORDER BY user_id",
            &[&ws, &second_id, &admin_id],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    let second_member: Uuid = db
        .query_one(
            "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &second_id],
        )
        .await
        .unwrap()
        .get(0);
    let owner_member: Uuid = db
        .query_one(
            "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &owner_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(members.len(), 2);
    let path = format!("/api/workspaces/{ws}/members/{second_member}");
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Workspace test agent','cloud',$2) RETURNING id", &[&ws,&owner_id]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,$2,'cloud','synthetic','online') RETURNING id", &[&ws,&format!("Workspace runtime {suffix}")]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let task_token = format!("mat_workspace_{suffix}");
    let task_hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&task_hash,&task,&agent,&ws,&owner_id]).await.unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/workspaces/",
            &task_token,
            json!({"name":"No","slug":format!("no-{suffix}")})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, "PATCH", &path, &task_token, json!({"role":"admin"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &format!("/api/workspaces/{foreign}/members/{second_member}"),
            &task_token,
            json!({"role":"admin"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    for selected in [ws, foreign] {
        assert_eq!(
            call(
                &app,
                "PATCH",
                &format!("/api/workspaces/{selected}/"),
                &task_token,
                json!({"name":"Task cannot rename"})
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
    }
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&task_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "PATCH", &path, &task_token, json!({"role":"admin"}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "PATCH", &path, &outsider, json!({"role":"admin"}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "PATCH", &path, &second, json!({"role":"admin"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, "PATCH", &path, &expired, json!({"role":"admin"}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &format!("/api/workspaces/{foreign}/members/{second_member}"),
            &second,
            json!({"role":"admin"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &format!("/api/workspaces/{ws}/members/not-a-uuid"),
            &owner,
            json!({"role":"admin"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    for value in [json!({}), json!({"role":"viewer"}), json!({"role":" "})] {
        assert_eq!(
            call(&app, "PATCH", &path, &owner, value).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let malformed_member = format!("/api/workspaces/{ws}/members/not-a-uuid");
    assert_eq!(
        call(
            &app,
            "PATCH",
            &malformed_member,
            &outsider,
            json!({"role":"admin"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &malformed_member,
            &second,
            json!({"role":"admin"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let workspace_path = format!("/api/workspaces/{ws}/");
    for (token, expected) in [
        (&expired, StatusCode::UNAUTHORIZED),
        (&outsider, StatusCode::NOT_FOUND),
        (&second, StatusCode::FORBIDDEN),
        (&task_token, StatusCode::UNAUTHORIZED),
    ] {
        assert_eq!(
            call(
                &app,
                "PATCH",
                &workspace_path,
                token,
                json!({"name":"denied"})
            )
            .await
            .0,
            expected
        );
    }
    assert_eq!(
        call(
            &app,
            "PATCH",
            "/api/workspaces/invalid/",
            &owner,
            json!({"name":"bad"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let bad_cookie = Request::builder()
        .method("PUT")
        .uri(&workspace_path)
        .header("cookie", format!("multica_auth={owner}"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"name":"CSRF denied"}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(bad_cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    for bad in [
        json!({"name":"   "}),
        json!({"issue_prefix":"A-B"}),
        json!({"repos": {"url":"https://example.com/repo.git"}}),
        json!({"repos":[{"url":"not a repository"}]}),
    ] {
        let (status, response) = call(&app, "PUT", &workspace_path, &owner, bad).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    }
    assert_eq!(
        call(
            &app,
            "PUT",
            &workspace_path,
            &owner,
            json!({"avatar_url":"workspaces/foreign/upload.png"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, updated) = call(
        &app,
        "PUT",
        &workspace_path,
        &admin,
        json!({
            "name":"  Renamed Workspace  ", "description":"updated", "context":"context",
            "issue_prefix":"  new7  ",
            "repos":[{"url":" https://github.com/example/repo.git ","description":" primary "},
                     {"url":"https://github.com/example/repo.git"},
                     {"url":"git@example.com:other/repo.git"}],
            "settings":{"pr_auto_complete_enabled":false,"foo":"bar"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["name"], "Renamed Workspace");
    assert_eq!(updated["issue_prefix"], "NEW7");
    assert_eq!(updated["settings"]["pr_merge_status"], "none");
    assert_eq!(updated["settings"]["pr_auto_complete_enabled"], false);
    assert_eq!(updated["repos"].as_array().unwrap().len(), 2);
    let (status, patched) = call(
        &app,
        "PATCH",
        &format!("/api/workspaces/{ws}"),
        &owner,
        json!({"settings":{"pr_auto_complete_enabled":true}, "issue_prefix":""}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["settings"]["pr_merge_status"], "done");
    assert!(
        patched["settings"]
            .get("pr_auto_complete_enabled")
            .is_none()
    );
    assert_eq!(patched["issue_prefix"], "NEW7");
    let stored = db
        .query_one(
            "SELECT name,issue_prefix,settings,repos FROM workspace WHERE id=$1",
            &[&ws],
        )
        .await
        .unwrap();
    assert_eq!(stored.get::<_, String>("name"), "Renamed Workspace");
    assert_eq!(stored.get::<_, String>("issue_prefix"), "NEW7");
    assert_eq!(
        stored.get::<_, Value>("settings")["pr_merge_status"],
        "done"
    );
    assert_eq!(stored.get::<_, Value>("repos").as_array().unwrap().len(), 2);
    let owner_path = format!("/api/workspaces/{ws}/members/{owner_member}");
    assert_eq!(
        call(&app, "PATCH", &owner_path, &owner, json!({"role":"member"}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&app, "PATCH", &path, &admin, json!({"role":"owner"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let (status, updated) = call(&app, "PATCH", &path, &admin, json!({"role":"admin"})).await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["name"], "Second");
    assert_eq!(updated["role"], "admin");
    assert_eq!(
        call(&app, "PATCH", &path, &owner, json!({"role":"owner"}))
            .await
            .0,
        StatusCode::OK
    );
    let (a, b) = tokio::join!(
        call(&app, "PATCH", &owner_path, &owner, json!({"role":"member"})),
        call(&app, "PATCH", &path, &second, json!({"role":"member"}))
    );
    let mut outcomes = [a.0, b.0];
    outcomes.sort();
    assert_eq!(outcomes, [StatusCode::OK, StatusCode::BAD_REQUEST]);
    let remaining: i64 = db
        .query_one(
            "SELECT count(*) FROM member WHERE workspace_id=$1 AND role='owner'",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 1);
}
