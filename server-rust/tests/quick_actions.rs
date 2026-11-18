//! Real PostgreSQL quick-action catalog: private prompts never leave creator scope.
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
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 on a random loopback port"]
async fn quick_actions_hide_private_prompts_and_resolve_targets() {
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
            .max_size(3)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = vec![];
    for name in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("quick-{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Quick A',$1) RETURNING id",
            &[&format!("quick-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Quick B',$1) RETURNING id",
            &[&format!("quick-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&other]).await.unwrap();
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let public: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,runtime_id,owner_id,permission_mode) VALUES ($1,'Public Agent','cloud',$2,$3,'public_to') RETURNING id", &[&ws,&runtime,&users[0]]).await.unwrap().get(0);
    let private: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,runtime_id,owner_id,permission_mode) VALUES ($1,'Restricted Agent','cloud',$2,$3,'private') RETURNING id", &[&ws,&runtime,&users[0]]).await.unwrap().get(0);
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'workspace',$2)", &[&public,&ws]).await.unwrap();
    let squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES ($1,'Quick Squad',$2,$3) RETURNING id", &[&ws,&public,&users[0]]).await.unwrap().get(0);
    let gone = Uuid::new_v4();
    for (name, creator, visibility, status, target_type, target, count) in [
        (
            "Public",
            users[0],
            "public",
            "active",
            "agent",
            Some(public),
            5i64,
        ),
        (
            "Squad",
            users[0],
            "public",
            "active",
            "squad",
            Some(squad),
            4,
        ),
        (
            "Private owner",
            users[0],
            "private",
            "active",
            "agent",
            Some(private),
            3,
        ),
        (
            "Private member",
            users[1],
            "private",
            "active",
            "agent",
            Some(private),
            2,
        ),
        ("Gone", users[0], "public", "active", "agent", Some(gone), 1),
        ("Archived", users[0], "public", "archived", "", None, 0),
    ] {
        db.execute("INSERT INTO quick_action(workspace_id,name,created_by_type,created_by_id,visibility,status,assignee_type,assignee_id,prompt,use_count) VALUES ($1,$2,'member',$3,$4,$5,$6,$7,$8,$9)", &[&ws,&name,&creator,&visibility,&status,&target_type,&target,&format!("private prompt {name}"),&count]).await.unwrap();
    }
    let snapshot: Value = db.query_one("SELECT jsonb_agg(to_jsonb(q) ORDER BY q.name) FROM quick_action q WHERE workspace_id=$1", &[&ws]).await.unwrap().get(0);
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'Personal project') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'Foreign project') RETURNING id",
            &[&other],
        )
        .await
        .unwrap()
        .get(0);
    let pin: Uuid = db.query_one("INSERT INTO pinned_item(workspace_id,user_id,item_type,item_id,position) VALUES ($1,$2,'project',$3,2.5) RETURNING id", &[&ws,&users[0],&project]).await.unwrap().get(0);
    let view = Uuid::new_v4();
    db.execute("INSERT INTO pinned_item(workspace_id,user_id,item_type,item_id,position) VALUES ($1,$2,'view',$3,3.5),($1,$4,'project',$5,1)", &[&ws,&users[0],&view,&users[1],&project]).await.unwrap();
    db.execute(
        "INSERT INTO notification_preference(workspace_id,user_id,preferences) VALUES ($1,$2,$3)",
        &[&ws, &users[0], &json!({"mentions":"muted","custom":"all"})],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO issue_view_preference(workspace_id,user_id,scope_type,scope_id,prefs) VALUES ($1,$2,'project',$3,$4)", &[&ws,&users[0],&project,&json!({"hidden":["builtin:all"]})]).await.unwrap();
    let pat = format!("mul_quick_{suffix}");
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'quick-test',$2,'mul_quick')", &[&users[0],&hex::encode(Sha256::digest(pat.as_bytes()))]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"quick-synthetic-secret".to_vec()),
    );
    drop(db);
    let auth = |id: Uuid| {
        encode(&Header::default(),&json!({"sub":id.to_string(),"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(b"quick-synthetic-secret")).unwrap()
    };
    let owner = auth(users[0]);
    let member = auth(users[1]);
    let outsider = auth(users[2]);
    let path = format!("/api/quick-actions/?workspace_id={ws}");
    assert_eq!(read(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &path, Some(&outsider)).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error":"workspace not found"})
        )
    );
    let owner_response = read(&app, &path, Some(&owner)).await;
    assert_eq!(owner_response.0, StatusCode::OK);
    let actions = owner_response.1["quick_actions"].as_array().unwrap();
    assert_eq!(
        actions
            .iter()
            .map(|a| a["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["Public", "Squad", "Private owner", "Gone"]
    );
    assert_eq!(actions[0]["target_name"], "Public Agent");
    assert_eq!(actions[0]["target_public"], true);
    assert_eq!(actions[1]["target_name"], "Quick Squad");
    assert_eq!(actions[1]["target_public"], true);
    assert_eq!(actions[2]["target_public"], false);
    assert_eq!(actions[3]["target_missing"], true);
    assert!(actions[3].get("target_name").is_none());
    assert_eq!(read(&app, &path, Some(&pat)).await.1, owner_response.1);
    let member_response = read(&app, &path, Some(&member)).await.1;
    assert_eq!(
        member_response["quick_actions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["Public", "Squad", "Private member", "Gone"]
    );
    assert!(!member_response.to_string().contains("Private owner"));
    assert!(!owner_response.1.to_string().contains("Private member"));
    let archived = read(
        &app,
        &format!("{path}&include_archived=true"),
        Some(&member),
    )
    .await;
    assert_eq!(archived.1["quick_actions"].as_array().unwrap().len(), 5);
    let archived_action = archived.1["quick_actions"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(archived_action["assignee_id"], "");
    assert_eq!(archived_action["target_public"], true);
    assert_eq!(archived_action["target_missing"], false);
    assert_eq!(
        read(
            &app,
            &format!("/api/quick-actions/?workspace_id={other}"),
            Some(&owner)
        )
        .await
        .1,
        json!({"quick_actions":[]})
    );
    let pin_path = format!("/api/pins/?workspace_id={ws}");
    let pins = read(&app, &pin_path, Some(&owner)).await;
    assert_eq!(pins.0, StatusCode::OK);
    assert_eq!(pins.1.as_array().unwrap().len(), 1);
    assert_eq!(pins.1[0]["id"], pin.to_string());
    assert_eq!(pins.1[0]["position"], 2.5);
    assert_eq!(
        read(&app, &format!("{pin_path}&include=view"), Some(&owner))
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_ne!(
        read(&app, &pin_path, Some(&member)).await.1[0]["id"],
        pin.to_string()
    );
    let notif_path = format!("/api/notification-preferences/?workspace_id={ws}");
    assert_eq!(
        read(&app, &notif_path, Some(&owner)).await.1,
        json!({"workspace_id":ws,"preferences":{"mentions":"muted","custom":"all"}})
    );
    assert_eq!(
        read(&app, &notif_path, Some(&member)).await.1,
        json!({"workspace_id":ws,"preferences":{}})
    );
    let view_path = format!(
        "/api/issue-view-preferences?workspace_id={ws}&scope_type=project&scope_id={project}"
    );
    assert_eq!(
        read(&app, &view_path, Some(&owner)).await.1["prefs"],
        json!({"hidden":["builtin:all"]})
    );
    let alternate_view_path = format!(
        "/api/issue-view-preferences?workspace_id={ws}&scope_type=project&scope_id={}",
        project.to_string().replace('-', "x")
    );
    assert_eq!(
        read(&app, &alternate_view_path, Some(&owner)).await.1["prefs"],
        json!({"hidden":["builtin:all"]})
    );
    assert_eq!(
        read(&app, &view_path, Some(&member)).await.1,
        json!({"scope_type":"project","scope_id":project,"prefs":{},"updated_at":""})
    );
    let my = read(
        &app,
        &format!(
            "/api/issue-view-preferences?workspace_id={ws}&scope_type=my&scope_id={users_id}",
            users_id = users[1]
        ),
        Some(&owner),
    )
    .await;
    assert_eq!(my.1["scope_id"], users[0].to_string());
    assert_eq!(read(&app,&format!("/api/issue-view-preferences?workspace_id={ws}&scope_type=project&scope_id={foreign_project}"),Some(&owner)).await,(StatusCode::NOT_FOUND,json!({"error":"project not found"})));
    let db = pool.get().await.unwrap();
    let after:Value=db.query_one("SELECT jsonb_agg(to_jsonb(q) ORDER BY q.name) FROM quick_action q WHERE workspace_id=$1", &[&ws]).await.unwrap().get(0);
    assert_eq!(snapshot, after, "GET mutated source quick actions");
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hex::encode(Sha256::digest(pat.as_bytes()))],
    )
    .await
    .unwrap();
    drop(db);
    assert_eq!(
        read(&app, &path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
}
