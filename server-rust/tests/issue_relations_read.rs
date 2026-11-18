//! Synthetic issue relation reads on a run-owned migrated PostgreSQL database.
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

async fn get(app: &axum::Router, path: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(path);
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 on a random loopback port"]
async fn issue_relations_preserve_auth_tenant_shape_and_read_only_behavior() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for role in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&role, &format!("relation-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES ('A',$1,'ABC') RETURNING id",
            &[&format!("relation-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES ('B',$1,'XYZ') RETURNING id",
            &[&format!("relation-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')",
        &[&ws,&users[0],&users[1],&foreign_ws]).await.unwrap();
    let make_issue = |workspace: Uuid,
                      number: i32,
                      title: &str,
                      parent: Option<Uuid>,
                      status: &str| {
        let db = &db;
        let owner = users[0];
        let title = title.to_string();
        let status = status.to_string();
        async move {
            db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,parent_issue_id,status) VALUES ($1,$2,'member',$3,$4,$5,$6) RETURNING id",
                &[&workspace,&title,&owner,&number,&parent,&status]).await.unwrap().get::<_,Uuid>(0)
        }
    };
    let parent = make_issue(ws, 31, "Visible", None, "todo").await;
    let other = make_issue(ws, 32, "Second", None, "todo").await;
    let child1 = make_issue(ws, 34, "first", Some(parent), "todo").await;
    let child2 = make_issue(ws, 35, "custom", Some(parent), "done_custom").await;
    let child3 = make_issue(ws, 36, "other parent", Some(other), "in_progress").await;
    let foreign_parent = make_issue(foreign_ws, 31, "Foreign parent", None, "todo").await;
    let foreign_child = make_issue(
        foreign_ws,
        34,
        "Foreign child",
        Some(foreign_parent),
        "todo",
    )
    .await;
    // Model a damaged cross-tenant pointer: never reveal foreign child through
    // the selected workspace even though frozen single-parent SQL misses scope.
    db.execute(
        "UPDATE issue SET parent_issue_id=$1 WHERE id=$2",
        &[&parent, &foreign_child],
    )
    .await
    .unwrap();
    let original = make_issue(ws, 37, "Original", None, "todo").await;
    let duplicate = make_issue(ws, 38, "Duplicate", None, "cancelled").await;
    let second_dup = make_issue(ws, 39, "Second duplicate", None, "cancelled").await;
    let stale = make_issue(ws, 40, "Stale", None, "todo").await;
    let cross = make_issue(ws, 41, "Foreign original", None, "cancelled").await;
    db.execute(
        "UPDATE issue SET duplicate_of_issue_id=$1 WHERE id=ANY($2::uuid[])",
        &[&original, &vec![duplicate, second_dup, stale]],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issue SET duplicate_of_issue_id=$1 WHERE id=$2",
        &[&foreign_parent, &cross],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,is_system,position) VALUES ($1,'done_custom','Finished custom','done','#123456',false,99)", &[&ws]).await.unwrap();
    let label: Uuid=db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'alpha','#123456','issue') RETURNING id",
        &[&ws]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO issue_to_label(issue_id,label_id) VALUES ($1,$2)",
        &[&child2, &label],
    )
    .await
    .unwrap();
    let token = format!("mul_relations_{suffix}");
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'synthetic',$2,'mul_relations')", &[&users[0],&hash]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-relations-secret".to_vec()),
    );
    let auth = |id: Uuid| {
        encode(&Header::default(),&json!({"sub":id,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),
        &EncodingKey::from_secret(b"synthetic-relations-secret")).unwrap()
    };
    let owner = auth(users[0]);
    let member = auth(users[1]);
    let outsider = auth(users[2]);
    let single = format!("/api/issues/{parent}/children?workspace_id={ws}");
    assert_eq!(get(&app, &single, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        get(&app, &single, Some(&outsider)).await.0,
        StatusCode::NOT_FOUND
    );
    let before: i64 = db
        .query_one(
            "SELECT sum(revision)::bigint FROM issue WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let result = get(&app, &single, Some(&member)).await;
    assert_eq!(result.0, StatusCode::OK, "{result:?}");
    let issues = result.1["issues"].as_array().unwrap();
    assert_eq!(issues.len(), 2, "{issues:?}");
    assert_eq!(issues[0]["id"], child1.to_string());
    assert_eq!(issues[1]["id"], child2.to_string());
    assert_eq!(issues[0]["labels"], json!([]));
    assert_eq!(issues[1]["labels"][0]["id"], label.to_string());
    assert_eq!(issues[1]["status_category"], "done");
    assert_eq!(issues[1]["status_name"], "");
    assert!(issues[1]["duplicate_of"].is_null());
    assert_eq!(issues[1]["identifier"], "ABC-35");
    assert!(issues[1]["metadata"].is_object());
    assert!(issues[1]["properties"].is_object());
    assert!(issues[1].get("source_context").is_none());
    assert_eq!(get(&app, &single, Some(&token)).await, result);
    // Safe reads accept browser cookies without a CSRF header.
    let cookie_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&single)
                .header("cookie", format!("multica_auth={owner}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cookie_response.status(), StatusCode::OK);
    let cookie_body = cookie_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&cookie_body).unwrap(),
        result.1
    );
    assert_eq!(
        get(
            &app,
            &format!("/api/issues/ABC-31/children?workspace_id={ws}"),
            Some(&owner)
        )
        .await,
        result
    );
    let batch = format!(
        "/api/issues/children?workspace_id={ws}&parent_ids={other},{parent},{foreign_parent}"
    );
    let group = get(&app, &batch, Some(&owner)).await;
    assert_eq!(group.0, StatusCode::OK, "{group:?}");
    assert_eq!(
        group.1["issues"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        if parent < other {
            vec![child1.to_string(), child2.to_string(), child3.to_string()]
        } else {
            vec![child3.to_string(), child1.to_string(), child2.to_string()]
        }
    );
    assert_eq!(
        get(
            &app,
            &format!("/api/issues/children?workspace_id={ws}"),
            Some(&owner)
        )
        .await
        .1,
        json!({"issues":[]})
    );
    assert_eq!(
        get(
            &app,
            &format!("/api/issues/children?workspace_id={ws}&parent_ids={parent}&parent_ids=bad"),
            Some(&owner)
        )
        .await
        .1["issues"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    for (input, expected) in [
        ("bad", "invalid parent_ids"),
        (&",".repeat(200), "too many parent_ids"),
    ] {
        let got = get(
            &app,
            &format!("/api/issues/children?workspace_id={ws}&parent_ids={input}"),
            Some(&owner),
        )
        .await;
        assert_eq!(got, (StatusCode::BAD_REQUEST, json!({"error":expected})));
    }
    let foreign_lookup = format!("/api/issues/{foreign_parent}/children?workspace_id={ws}");
    assert_eq!(
        get(&app, &foreign_lookup, Some(&owner)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(
            &app,
            &format!("/api/issues/{parent}/children?workspace_id={foreign_ws}"),
            Some(&owner)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let rel = |id: Uuid| format!("/api/issues/{id}/duplicates?workspace_id={ws}");
    let original_result = get(&app, &rel(original), Some(&owner)).await;
    assert_eq!(original_result.0, StatusCode::OK, "{original_result:?}");
    assert_eq!(original_result.1["duplicates"].as_array().unwrap().len(), 2);
    assert_eq!(
        original_result.1["duplicates"][0]["id"],
        duplicate.to_string()
    );
    assert_eq!(
        original_result.1["duplicates"][1]["id"],
        second_dup.to_string()
    );
    assert!(original_result.1["duplicate_of"].is_null());
    let marked = get(&app, &rel(duplicate), Some(&member)).await;
    assert_eq!(marked.1["duplicate_of"]["id"], original.to_string());
    assert_eq!(marked.1["duplicate_of"]["status_category"], "todo");
    assert_eq!(marked.1["duplicate_of"]["status_name"], "");
    assert!(marked.1["duplicate_of"].get("labels").is_none());
    assert!(get(&app, &rel(stale), Some(&member)).await.1["duplicate_of"].is_null());
    assert!(get(&app, &rel(cross), Some(&member)).await.1["duplicate_of"].is_null());
    assert_eq!(
        get(&app, &rel(foreign_parent), Some(&member)).await.0,
        StatusCode::NOT_FOUND
    );
    let after: i64 = db
        .query_one(
            "SELECT sum(revision)::bigint FROM issue WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after, "GET must not update issue revisions");
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &single, Some(&token)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&app, &rel(original), Some("invalid-token")).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "ALTER TABLE issue_to_label RENAME TO issue_to_label_unavailable",
        &[],
    )
    .await
    .unwrap();
    let unlabeled = get(&app, &single, Some(&owner)).await;
    assert_eq!(unlabeled.0, StatusCode::OK);
    assert_eq!(unlabeled.1["issues"].as_array().unwrap().len(), 2);
    assert_eq!(unlabeled.1["issues"][1]["labels"], json!([]));
    db.execute(
        "ALTER TABLE issue_to_label_unavailable RENAME TO issue_to_label",
        &[],
    )
    .await
    .unwrap();
    db.execute("ALTER TABLE issue RENAME TO issue_unavailable", &[])
        .await
        .unwrap();
    assert_eq!(
        get(&app, &batch, Some(&owner)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list child issues"})
        )
    );
    db.execute("ALTER TABLE issue_unavailable RENAME TO issue", &[])
        .await
        .unwrap();
}
