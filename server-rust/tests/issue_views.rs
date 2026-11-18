//! Saved-view reads on a run-owned migrated PostgreSQL database.
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

async fn read(app: &axum::Router, path: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(path);
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let code = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 on a random loopback port"]
async fn saved_view_visibility_filters_and_error_contracts() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
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
        let id: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&role, &format!("view-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        users.push(id);
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Views A',$1) RETURNING id",
            &[&format!("views-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Views B',$1) RETURNING id",
            &[&format!("views-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&other]).await.unwrap();
    let insert = "INSERT INTO issue_view(workspace_id,owner_id,name,scope_type,scope_id,scope_variant,visibility,query,display) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id";
    let mut ids = Vec::new();
    let project = Uuid::new_v4();
    for (workspace, owner, name, scope, scope_id, variant, visibility) in [
        (
            ws,
            users[0],
            "Own private",
            "workspace",
            None,
            None,
            "private",
        ),
        (ws, users[0], "Shared", "workspace", None, None, "workspace"),
        (
            ws,
            users[1],
            "Member private",
            "workspace",
            None,
            None,
            "private",
        ),
        (
            ws,
            users[1],
            "Project shared",
            "project",
            Some(project),
            Some("members"),
            "workspace",
        ),
        (
            ws,
            users[0],
            "My personal",
            "my",
            None,
            Some("assigned"),
            "private",
        ),
        (
            other,
            users[0],
            "Foreign",
            "workspace",
            None,
            None,
            "workspace",
        ),
    ] {
        let id: Uuid = db
            .query_one(
                insert,
                &[
                    &workspace,
                    &owner,
                    &name,
                    &scope,
                    &scope_id,
                    &variant,
                    &visibility,
                    &json!({"q":"synthetic"}),
                    &json!({"columns":["title"]}),
                ],
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
        ApiState::new(pool.clone(), b"synthetic-view-secret".to_vec()),
    );
    let jwt = |id: Uuid| {
        encode(&Header::default(), &json!({"sub":id,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"synthetic-view-secret")).unwrap()
    };
    let owner = jwt(users[0]);
    let member = jwt(users[1]);
    let outsider = jwt(users[2]);
    let path = format!("/api/issue-views/?workspace_id={ws}&scope_type=workspace");
    assert_eq!(read(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &path, Some(&outsider)).await.0,
        StatusCode::NOT_FOUND
    );
    let own = read(&app, &path, Some(&owner)).await;
    assert_eq!(own.0, StatusCode::OK);
    let views = own.1.as_array().unwrap();
    assert_eq!(views.len(), 2);
    assert_eq!(views[0]["id"], ids[0].to_string());
    assert_eq!(views[1]["id"], ids[1].to_string());
    assert_eq!(views[0]["query"], json!({"q":"synthetic"}));
    assert_eq!(views[0]["display"], json!({"columns":["title"]}));
    assert_eq!(views[0]["revision"], 1);
    let member_views = read(&app, &path, Some(&member)).await.1;
    assert_eq!(member_views.as_array().unwrap().len(), 2);
    assert_eq!(member_views[0]["id"], ids[1].to_string());
    assert_eq!(member_views[1]["id"], ids[2].to_string());
    let detail =
        |id: Uuid, selected: Uuid| format!("/api/issue-views/{id}/?workspace_id={selected}");
    assert_eq!(
        read(&app, &detail(ids[0], ws), Some(&member)).await,
        (StatusCode::NOT_FOUND, json!({"error":"view not found"}))
    );
    assert_eq!(
        read(&app, &detail(ids[1], ws), Some(&member)).await.0,
        StatusCode::OK
    );
    let alternate_id = ids[1].to_string().replace('-', "x");
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-views/{alternate_id}/?workspace_id={ws}"),
            Some(&member)
        )
        .await
        .1["id"],
        ids[1].to_string()
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-views/{alternate_id}/?workspace_id={other}"),
            Some(&owner)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &detail(ids[5], ws), Some(&owner)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &detail(ids[0], other), Some(&member)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, "/api/issue-views/nope/?workspace_id=", Some(&owner))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-views/nope/?workspace_id={ws}"),
            Some(&owner)
        )
        .await,
        (StatusCode::BAD_REQUEST, json!({"error":"invalid view id"}))
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-views/?workspace_id={ws}"),
            Some(&owner)
        )
        .await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid scope_type"})
        )
    );
    assert_eq!(
        read(&app, &format!("{path}&scope_id=broken"), Some(&owner)).await,
        (StatusCode::BAD_REQUEST, json!({"error":"invalid scope_id"}))
    );
    assert_eq!(
        read(&app, &format!("{path}&scope_id={project}"), Some(&owner))
            .await
            .1,
        json!([])
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-views/?workspace_id={ws}&scope_type=project&scope_id={project}"),
            Some(&owner)
        )
        .await
        .1[0]["id"],
        ids[3].to_string()
    );
    assert_eq!(
        read(
            &app,
            &format!(
                "/api/issue-views/?workspace_id={ws}&scope_type=project&scope_id={}",
                project.to_string().replace('-', "x")
            ),
            Some(&owner)
        )
        .await
        .1[0]["id"],
        ids[3].to_string()
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-views/?workspace_id={ws}&scope_type=my"),
            Some(&owner)
        )
        .await
        .1[0]["id"],
        ids[4].to_string()
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-views/?workspace_id={ws}&scope_type=project"),
            Some(&owner)
        )
        .await
        .1,
        json!([])
    );
    // The frozen query limits the returned view payload even if historical
    // data bypasses the normal 100-per-owner create quota.
    db.execute(
        "INSERT INTO issue_view(workspace_id,owner_id,name,scope_type,visibility,query) \
         SELECT $1,$2,'Overflow ' || n,'workspace','private','{}'::jsonb FROM generate_series(1,201) n",
        &[&ws, &users[0]],
    ).await.unwrap();
    assert_eq!(
        read(&app, &path, Some(&owner))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        200
    );
    assert_eq!(
        read(&app, &path, Some(&member))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        2
    );
    db.execute(
        "ALTER TABLE issue_view RENAME TO issue_view_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&owner)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list views"})
        )
    );
    assert_eq!(
        read(&app, &detail(ids[1], ws), Some(&owner)).await,
        (StatusCode::NOT_FOUND, json!({"error":"view not found"}))
    );
    db.execute(
        "ALTER TABLE issue_view_unavailable RENAME TO issue_view",
        &[],
    )
    .await
    .unwrap();
    let unchanged: i64 = db
        .query_one("SELECT COUNT(*) FROM issue_view", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(unchanged, 207);
}
