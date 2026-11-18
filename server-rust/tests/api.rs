//! Real PostgreSQL only. Run against a disposable multica_zbsi63 after migrate-up.
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
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

const SECRET: &str = "isolated-zbsi63-secret";

fn token(user: Uuid, exp: usize) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user.to_string(),"exp":exp,"sid":"session-1"}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

fn request(
    path: &str,
    method: Method,
    credential: Option<(&str, bool)>,
    csrf: Option<&str>,
) -> Request<Body> {
    let mut r = Request::builder().uri(path).method(method);
    if let Some((token, cookie)) = credential {
        r = if cookie {
            r.header("cookie", format!("multica_auth={token}"))
        } else {
            r.header("authorization", format!("Bearer {token}"))
        };
    }
    if let Some(csrf) = csrf {
        r = r.header("x-csrf-token", csrf);
    }
    r.header("x-user-id", Uuid::new_v4().to_string())
        .header("x-agent-id", Uuid::new_v4().to_string())
        .header("x-actor-source", "member")
        .body(Body::empty())
        .unwrap()
}

async fn call(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi63 disposable database migrated to both ledgers"]
async fn authenticated_workspace_reads_are_isolated() {
    let url = std::env::var("RUST_API_TEST_DATABASE_URL").expect("set RUST_API_TEST_DATABASE_URL");
    let config = Config::from_str(&url).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let user_a: Uuid = db.query_one("INSERT INTO \"user\"(name,email) VALUES ('Alice','alice-zbsi63@invalid.example') RETURNING id", &[]).await.unwrap().get(0);
    let user_b: Uuid = db.query_one("INSERT INTO \"user\"(name,email) VALUES ('Bob','bob-zbsi63@invalid.example') RETURNING id", &[]).await.unwrap().get(0);
    let outsider: Uuid = db.query_one("INSERT INTO \"user\"(name,email) VALUES ('Outsider','outsider-zbsi63@invalid.example') RETURNING id", &[]).await.unwrap().get(0);
    let shadow: Uuid = db.query_one("INSERT INTO \"user\"(name,email,zbs_is_shadow) VALUES ('Shadow','shadow-zbsi63@invalid.example',true) RETURNING id", &[]).await.unwrap().get(0);
    let ws_a: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Alpha','zbsi63-alpha','ALPH') RETURNING id", &[]).await.unwrap().get(0);
    let ws_b: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Beta','zbsi63-beta','BETA') RETURNING id", &[]).await.unwrap().get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$3,'admin')", &[&ws_a,&user_a,&user_b,&ws_b]).await.unwrap();
    let pat = "mul_zbsi63testtoken";
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    let pat_id: Uuid = db.query_one("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'test',$2,'mul_zbsi') RETURNING id", &[&user_a,&hash]).await.unwrap().get(0);
    let expired = "mul_expired_zbsi63";
    let expired_hash = hex::encode(Sha256::digest(expired.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,expires_at) VALUES ($1,'expired',$2,'mul_exp',now()-interval '1 second')", &[&user_a,&expired_hash]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.as_bytes().to_vec()),
    );
    let expires = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600) as usize;
    let a = token(user_a, expires);
    let b = token(user_b, expires);
    let outsider_token = token(outsider, expires);
    let shadow_token = token(shadow, expires);
    let read = |p: &str, t: &str, c: bool| request(p, Method::GET, Some((t, c)), None);
    // The real middleware must append renewal cookies only after a validated
    // cookie JWT and a successful user lookup; bearer auth cannot rotate them.
    let cookie_response = app
        .clone()
        .oneshot(request("/api/me", Method::GET, Some((&a, true)), None))
        .await
        .unwrap();
    assert_eq!(cookie_response.status(), StatusCode::OK);
    let renewed: Vec<_> = cookie_response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_owned())
        .collect();
    assert_eq!(renewed.len(), 3);
    assert!(renewed.iter().any(|v| v.starts_with("multica_auth=")));
    assert!(renewed.iter().any(|v| v.starts_with("multica_csrf=")));
    assert!(
        renewed
            .iter()
            .any(|v| v.starts_with("multica_csrf_session="))
    );
    let bearer_response = app
        .clone()
        .oneshot(request("/api/me", Method::GET, Some((&a, false)), None))
        .await
        .unwrap();
    assert_eq!(bearer_response.status(), StatusCode::OK);
    assert!(
        bearer_response
            .headers()
            .get_all("set-cookie")
            .iter()
            .next()
            .is_none()
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize;
    let dead = token(user_a, now - 1);
    for from_cookie in [true, false] {
        let expired_response = app
            .clone()
            .oneshot(request(
                "/api/me",
                Method::GET,
                Some((&dead, from_cookie)),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(expired_response.status(), StatusCode::UNAUTHORIZED);
        assert!(
            expired_response
                .headers()
                .get_all("set-cookie")
                .iter()
                .next()
                .is_none()
        );
    }
    let (status, mine) = call(&app, read("/api/workspaces/", &a, true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(mine.as_array().unwrap().len(), 1);
    assert_eq!(mine[0]["id"], ws_a.to_string());
    for (path, expected) in [
        (format!("/api/workspaces/{ws_b}/"), StatusCode::NOT_FOUND),
        (
            format!("/api/workspaces/{ws_b}/members"),
            StatusCode::NOT_FOUND,
        ),
    ] {
        let (s, data) = call(&app, read(&path, &a, false)).await;
        assert_eq!(s, expected);
        assert_eq!(data, json!({"error":"workspace not found"}));
    }
    for role in [&a, &b] {
        let (s, members) = call(
            &app,
            read(&format!("/api/workspaces/{ws_a}/members"), role, false),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(members.as_array().unwrap().len(), 2);
        assert!(
            members
                .as_array()
                .unwrap()
                .iter()
                .all(|m| m["workspace_id"] == ws_a.to_string())
        );
    }
    let (s, theirs) = call(&app, read("/api/workspaces/", &b, false)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(theirs.as_array().unwrap().len(), 2);
    let (s, empty) = call(&app, read("/api/workspaces/", &outsider_token, false)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(empty, json!([]));
    let (s, _) = call(
        &app,
        read(
            &format!("/api/workspaces/{ws_a}/members"),
            &outsider_token,
            false,
        ),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, me) = call(&app, read("/api/me", &a, false)).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(me["email"], "alice-zbsi63@invalid.example");
    assert_eq!(me["onboarding_questionnaire"], json!({}));
    let (s, _) = call(&app, read(&format!("/api/workspaces/{ws_a}/"), pat, false)).await;
    assert_eq!(s, StatusCode::OK);
    let last_used: Option<chrono::DateTime<chrono::Utc>> = db
        .query_one(
            "SELECT last_used_at FROM personal_access_token WHERE id=$1",
            &[&pat_id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(last_used.is_some());
    for t in [
        expired,
        "mul_invalid",
        &shadow_token,
        "mat_bad",
        "mdt_bad",
        "mcn_bad",
        "not.a.jwt",
        &token(user_a, 1),
    ] {
        let (s, _) = call(&app, read("/api/me", t, false)).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED, "token {t}");
    }
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE id=$1",
        &[&pat_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, read("/api/me", pat, false)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, request("/api/me", Method::GET, None, None))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, read("/api/workspaces/not-a-uuid/", &a, false))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            read(
                "/api/workspaces/00000000-0000-0000-0000-000000000001/",
                &a,
                false
            )
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            request("/api/daemon/workspaces", Method::GET, None, None)
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            request("/api/me", Method::POST, Some((&a, true)), None)
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    // A valid Go session-bound CSRF header clears the auth middleware, but the
    // method remains unimplemented, never turned into a successful write.
    let nonce = [4u8; 16];
    let mut mac = hmac::Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
    use hmac::Mac;
    mac.update(b"session-1\0");
    mac.update(&nonce);
    let csrf = format!(
        "{}.{}",
        hex::encode(nonce),
        hex::encode(mac.finalize().into_bytes())
    );
    assert_eq!(
        call(
            &app,
            request("/api/me", Method::POST, Some((&a, true)), Some(&csrf))
        )
        .await
        .0,
        StatusCode::METHOD_NOT_ALLOWED
    );
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM workspace WHERE slug LIKE 'zbsi63-%'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);
    let changed: i64 = db
        .query_one(
            "SELECT count(*) FROM member WHERE workspace_id=$1",
            &[&ws_a],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(changed, 2);
    let missing = token(Uuid::new_v4(), expires);
    assert_eq!(
        call(&app, read("/api/me", &missing, false)).await.0,
        StatusCode::UNAUTHORIZED
    );
    // Auth DB failure must never authenticate by a forged identity header.
    let mut unavailable = Config::from_str(&url).unwrap();
    unavailable.options("-c search_path=zbsi63_missing");
    let unavailable_pool = Arc::new(
        Pool::builder(Manager::new(unavailable, NoTls))
            .max_size(1)
            .build()
            .unwrap(),
    );
    let unavailable_app = router_with_api(
        Arc::new(Health::new(
            Some(unavailable_pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(unavailable_pool, SECRET.as_bytes().to_vec()),
    );
    let (s, err) = call(&unavailable_app, read("/api/me", &a, false)).await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        err,
        json!({"error":"authentication temporarily unavailable"})
    );
    // Auth succeeds through a test schema view, but the missing workspace table
    // fails the handler safely with an error object, not an empty/leaked row.
    db.batch_execute("CREATE SCHEMA zbsi63_partial; CREATE VIEW zbsi63_partial.\"user\" AS SELECT * FROM public.\"user\"").await.unwrap();
    let mut partial = Config::from_str(&url).unwrap();
    partial.options("-c search_path=zbsi63_partial");
    let partial_pool = Arc::new(
        Pool::builder(Manager::new(partial, NoTls))
            .max_size(1)
            .build()
            .unwrap(),
    );
    let partial_app = router_with_api(
        Arc::new(Health::new(
            Some(partial_pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(partial_pool, SECRET.as_bytes().to_vec()),
    );
    let (s, err) = call(&partial_app, read("/api/workspaces/", &a, false)).await;
    assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(err, json!({"error":"failed to list workspaces"}));
}

#[tokio::test]
#[ignore = "requires run-owned migrated multica_zbsi63 disposable database"]
async fn catalog_reads_preserve_filters_counts_seeding_and_tenant_guard() {
    let url = std::env::var("RUST_API_TEST_DATABASE_URL").expect("set RUST_API_TEST_DATABASE_URL");
    let config = Config::from_str(&url).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Catalog', $1) RETURNING id",
            &[&format!("catalog-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Catalog', $1, 'CAT') RETURNING id", &[&format!("catalog-{suffix}")]).await.unwrap().get(0);
    let other:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Other', $1, 'OTH') RETURNING id", &[&format!("other-{suffix}")]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let project:Uuid=db.query_one("INSERT INTO project(workspace_id,title,priority) VALUES ($1,'Catalog project','high') RETURNING id", &[&ws]).await.unwrap().get(0);
    let foreign_project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'Other project') RETURNING id",
            &[&other],
        )
        .await
        .unwrap()
        .get(0);
    let label:Uuid=db.query_one("INSERT INTO issue_label(workspace_id,name,color) VALUES ($1,'Beta','#abcdef') RETURNING id", &[&ws]).await.unwrap().get(0);
    let foreign_label:Uuid=db.query_one("INSERT INTO issue_label(workspace_id,name,color) VALUES ($1,'Foreign','#abcdef') RETURNING id", &[&other]).await.unwrap().get(0);
    let property:Uuid=db.query_one("INSERT INTO issue_property(workspace_id,name,type,config) VALUES ($1,'Category','select',$2) RETURNING id", &[&ws,&json!({"options":[{"id":"x","name":"X","color":"#abcdef"}],"ignored":"value"})]).await.unwrap().get(0);
    let foreign_property:Uuid=db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,'Foreign','text') RETURNING id", &[&other]).await.unwrap().get(0);
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,project_id,title,creator_type,creator_id,status,properties) VALUES ($1,$2,'Done','member',$3,'done',$4) RETURNING id",
        &[&ws,&project,&user,&json!({property.to_string():"x"})]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO issue_to_label(issue_id,label_id) VALUES ($1,$2)",
        &[&issue, &label],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO project_resource(project_id,workspace_id,resource_type,resource_ref) VALUES ($1,$2,'url',$3)", &[&project,&ws,&json!({"url":"https://example.invalid"})]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.as_bytes().to_vec()),
    );
    let expires = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600) as usize;
    let auth = token(user, expires);
    let read = |path: &str, selector: Option<(&str, String)>| {
        let mut req = request(path, Method::GET, Some((&auth, false)), None);
        if let Some((key, value)) = selector {
            req.headers_mut().insert(
                axum::http::header::HeaderName::from_bytes(key.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        req
    };
    let scope = |path: &str| read(path, Some(("x-workspace-id", ws.to_string())));
    // Middleware must return its connection before a handler checks one out.
    let single_pool = Arc::new(
        Pool::builder(Manager::new(Config::from_str(&url).unwrap(), NoTls))
            .max_size(1)
            .build()
            .unwrap(),
    );
    let single_app = router_with_api(
        Arc::new(Health::new(
            Some(single_pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(single_pool, SECRET.as_bytes().to_vec()),
    );
    let single_response = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        call(&single_app, scope("/api/projects/")),
    )
    .await
    .expect("catalog request deadlocked with one database connection");
    assert_eq!(single_response.0, StatusCode::OK);
    for path in [
        "/api/projects/",
        "/api/labels/",
        "/api/properties/",
        "/api/issue-statuses/",
    ] {
        assert_eq!(
            call(&app, read(path, None)).await,
            (
                StatusCode::BAD_REQUEST,
                json!({"error":"workspace_id or workspace_slug is required"})
            )
        );
        assert_eq!(
            call(
                &app,
                read(path, Some(("x-workspace-id", other.to_string())))
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }
    let (code, projects) = call(&app, scope("/api/projects/?status=planned&priority=high")).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(projects["total"], 1);
    assert_eq!(projects["projects"][0]["id"], project.to_string());
    assert_eq!(projects["projects"][0]["issue_count"], 1);
    assert_eq!(projects["projects"][0]["done_count"], 1);
    assert_eq!(projects["projects"][0]["resource_count"], 1);
    assert!(projects["projects"][0]["workflow_id"].is_null());
    assert_eq!(
        call(&app, scope("/api/projects/?priority=low")).await.1["total"],
        0
    );
    let (code, detail) = call(&app, scope(&format!("/api/projects/{project}/"))).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(detail["done_count"], 1);
    assert_eq!(detail["resource_count"], 1);
    assert_eq!(
        call(&app, scope("/api/projects/not-a-uuid/")).await.1,
        json!({"error":"invalid project id"})
    );
    assert_eq!(
        call(&app, scope(&format!("/api/projects/{foreign_project}/")))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (code, labels) = call(&app, scope("/api/labels/")).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(labels["labels"][0]["usage_count"], 1);
    assert_eq!(
        call(&app, scope("/api/labels/?resource_type=other"))
            .await
            .1,
        json!({"error":"resource_type must be issue, agent, or skill"})
    );
    assert_eq!(
        call(&app, scope("/api/labels/?resource_type=skill"))
            .await
            .1["total"],
        0
    );
    assert_eq!(
        call(&app, scope(&format!("/api/labels/{label}/"))).await.1["usage_count"],
        0
    );
    assert_eq!(
        call(&app, scope(&format!("/api/labels/{foreign_label}/")))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (code, props) = call(&app, scope("/api/properties/")).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(props["properties"][0]["usage_count"], 1);
    assert_eq!(
        props["properties"][0]["config"],
        json!({"options":[{"id":"x","name":"X","color":"#abcdef"}]})
    );
    assert_eq!(
        call(&app, scope(&format!("/api/properties/{property}/")))
            .await
            .1["usage_count"],
        0
    );
    assert_eq!(
        call(&app, scope(&format!("/api/properties/{foreign_property}/")))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE issue_property SET archived_at=now() WHERE id=$1",
        &[&property],
    )
    .await
    .unwrap();
    assert_eq!(call(&app, scope("/api/properties/")).await.1["total"], 0);
    assert_eq!(
        call(&app, scope("/api/properties/?include_archived=true"))
            .await
            .1["properties"][0]["archived"],
        true
    );
    assert_eq!(
        call(&app, scope(&format!("/api/properties/{property}/")))
            .await
            .1["archived"],
        true
    );
    let (code, statuses) = call(&app, scope("/api/issue-statuses/")).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(statuses["total"], 7);
    assert_eq!(
        statuses["categories"],
        json!([
            "backlog",
            "todo",
            "in_progress",
            "in_review",
            "blocked",
            "done",
            "cancelled"
        ])
    );
    assert_eq!(statuses["statuses"][0]["key"], "backlog");
    assert_eq!(statuses["statuses"][0]["category"], "backlog");
    assert_eq!(
        call(&app, scope("/api/issue-statuses/")).await.1["total"],
        7
    );
    let slug = format!("catalog-{suffix}");
    assert_eq!(
        call(
            &app,
            read("/api/projects/", Some(("x-workspace-slug", slug)))
        )
        .await
        .1["total"],
        1
    );
    let pat = format!("mul_zbsi63_catalog_{suffix}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    let pat_id: Uuid = db.query_one("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'catalog-test',$2,'mul_zbsi') RETURNING id", &[&user,&hash]).await.unwrap().get(0);
    let read_only_schema = format!("zbsi63_readonly_{suffix}");
    db.batch_execute(&format!(
        "CREATE SCHEMA {read_only_schema}; \
         CREATE VIEW {read_only_schema}.\"user\" AS SELECT * FROM public.\"user\"; \
         CREATE VIEW {read_only_schema}.personal_access_token AS \
         SELECT p.* FROM public.personal_access_token p \
         JOIN public.\"user\" u ON u.id = p.user_id"
    ))
    .await
    .unwrap();
    let mut read_only = Config::from_str(&url).unwrap();
    read_only.options(format!("-c search_path={read_only_schema}"));
    let read_only_pool = Arc::new(
        Pool::builder(Manager::new(read_only, NoTls))
            .max_size(1)
            .build()
            .unwrap(),
    );
    let read_only_app = router_with_api(
        Arc::new(Health::new(
            Some(read_only_pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(read_only_pool, SECRET.as_bytes().to_vec()),
    );
    // The joined PAT view accepts reads but rejects UPDATE. Failed usage
    // bookkeeping cannot turn an authenticated request into a 503.
    assert_eq!(
        call(
            &read_only_app,
            request("/api/me", Method::GET, Some((&pat, false)), None)
        )
        .await
        .0,
        StatusCode::OK
    );
    let last_used: Option<chrono::DateTime<chrono::Utc>> = db
        .query_one(
            "SELECT last_used_at FROM personal_access_token WHERE id=$1",
            &[&pat_id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(last_used.is_none());
    // Query selector takes precedence over the header selector and cannot cross tenants.
    assert_eq!(
        call(
            &app,
            read(
                &format!("/api/projects/?workspace_id={other}"),
                Some(("x-workspace-id", ws.to_string()))
            )
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn catalog_routes_require_auth_without_reaching_postgres() {
    let config = Config::from_str("host=127.0.0.1 port=1 dbname=unused").unwrap();
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(1)
            .build()
            .unwrap(),
    );
    let app = router_with_api(
        Arc::new(Health::new(
            None,
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec()),
    );
    for path in [
        "/api/projects/",
        "/api/projects/00000000-0000-0000-0000-000000000001/",
        "/api/labels/",
        "/api/labels/00000000-0000-0000-0000-000000000001/",
        "/api/properties/",
        "/api/properties/00000000-0000-0000-0000-000000000001/",
        "/api/issue-statuses/",
    ] {
        assert_eq!(
            call(&app, request(path, Method::GET, None, None)).await,
            (
                StatusCode::UNAUTHORIZED,
                json!({"error":"missing authorization"})
            ),
            "{path}"
        );
    }
}
