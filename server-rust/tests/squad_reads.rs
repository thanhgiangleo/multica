//! Squad catalog, detail and members against isolated migrated PostgreSQL.
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
    let mut request = Request::builder().uri(path);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 on a random loopback port"]
async fn squad_reads_scope_workspace_and_preview_members() {
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
            .max_size(3)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for name in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("squad-{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Squads A',$1) RETURNING id",
            &[&format!("squad-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Squads B',$1) RETURNING id",
            &[&format!("squad-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&foreign]).await.unwrap();
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,runtime_id,owner_id) VALUES ($1,'Leader','cloud',$2,$3) RETURNING id", &[&ws,&runtime,&users[0]]).await.unwrap().get(0);
    let squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,description,instructions,avatar_url,leader_id,creator_id) VALUES ($1,'One','Description','Instructions','https://example.invalid/avatar.png',$2,$3) RETURNING id", &[&ws,&agent,&users[0]]).await.unwrap().get(0);
    let empty: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES ($1,'Empty',$2,$3) RETURNING id", &[&ws,&agent,&users[1]]).await.unwrap().get(0);
    let archived: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id,archived_at,archived_by) VALUES ($1,'Archived',$2,$3,now(),$3) RETURNING id", &[&ws,&agent,&users[0]]).await.unwrap().get(0);
    let other: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES ($1,'Private foreign',$2,$3) RETURNING id", &[&foreign,&agent,&users[0]]).await.unwrap().get(0);
    let mut member_ids = Vec::new();
    for (member_type, member_id, role) in [
        ("member", users[1], "worker"),
        ("agent", agent, "leader"),
        ("member", users[0], "owner"),
        ("agent", Uuid::new_v4(), "other"),
    ] {
        member_ids.push(db.query_one("INSERT INTO squad_member(squad_id,member_type,member_id,role) VALUES ($1,$2,$3,$4) RETURNING id", &[&squad,&member_type,&member_id,&role]).await.unwrap().get::<_,Uuid>(0));
    }
    let pat = format!("mul_squad_{suffix}");
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'synthetic',$2,'mul_squad')", &[&users[0],&hex::encode(Sha256::digest(pat.as_bytes()))]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-squad-secret".to_vec()),
    );
    drop(db);
    let auth = |id: Uuid| {
        encode(&Header::default(),&json!({"sub":id.to_string(),"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(b"synthetic-squad-secret")).unwrap()
    };
    let owner = auth(users[0]);
    let member = auth(users[1]);
    let outsider = auth(users[2]);
    let path = format!("/api/squads/?workspace_id={ws}");
    assert_eq!(read(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &path, Some(&outsider)).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error":"workspace not found"})
        )
    );
    let listing = read(&app, &path, Some(&member)).await;
    assert_eq!(listing.0, StatusCode::OK);
    let squads = listing.1.as_array().unwrap();
    assert_eq!(squads.len(), 2);
    assert!(
        squads
            .iter()
            .all(|s| s["id"] != other.to_string() && s["id"] != archived.to_string())
    );
    let populated = squads
        .iter()
        .find(|s| s["id"] == squad.to_string())
        .unwrap();
    assert_eq!(populated["member_count"], 4);
    assert_eq!(populated["member_preview"].as_array().unwrap().len(), 3);
    assert_eq!(
        populated["member_preview"][0]["member_id"],
        agent.to_string()
    );
    assert_eq!(
        populated["avatar_url"],
        "https://example.invalid/avatar.png"
    );
    assert_eq!(
        squads
            .iter()
            .find(|s| s["id"] == empty.to_string())
            .unwrap()["member_preview"],
        json!([])
    );
    assert_eq!(read(&app, &path, Some(&pat)).await.1, listing.1);
    let detail = format!("/api/squads/{squad}/?workspace_id={ws}");
    assert_eq!(read(&app, &detail, Some(&owner)).await.1, *populated);
    assert_eq!(
        read(
            &app,
            &format!("/api/squads/{empty}/?workspace_id={ws}"),
            Some(&owner)
        )
        .await
        .1["member_preview"],
        Value::Null
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/squads/{archived}/?workspace_id={ws}"),
            Some(&member)
        )
        .await
        .0,
        StatusCode::OK
    );
    let members = read(
        &app,
        &format!("/api/squads/{squad}/members?workspace_id={ws}"),
        Some(&member),
    )
    .await;
    assert_eq!(members.0, StatusCode::OK);
    let listed_ids: Vec<String> = members
        .1
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        listed_ids,
        member_ids.iter().map(Uuid::to_string).collect::<Vec<_>>()
    );
    let status_path = format!("/api/squads/{squad}/members/status?workspace_id={ws}");
    let no_tasks = read(&app, &status_path, Some(&member)).await;
    assert_eq!(no_tasks.0, StatusCode::OK);
    assert_eq!(no_tasks.1["members"].as_array().unwrap().len(), 4);
    assert_eq!(no_tasks.1["members"][0]["status"], Value::Null);
    assert_eq!(no_tasks.1["members"][0]["active_issues"], json!([]));
    assert_eq!(no_tasks.1["members"][1]["status"], "idle");
    assert_eq!(
        read(&app, &status_path, Some(&outsider)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &status_path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(read(&app, &status_path, Some(&pat)).await.1, no_tasks.1);
    let db = pool.get().await.unwrap();
    db.execute(
        "UPDATE workspace SET issue_prefix='SYN' WHERE id=$1",
        &[&ws],
    )
    .await
    .unwrap();
    let issue: Uuid = db.query_one(
        "INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Working issue','member',$2,73) RETURNING id",
        &[&ws,&users[0]],
    ).await.unwrap().get(0);
    let task: Uuid = db.query_one(
        "INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,dispatched_at) VALUES ($1,$2,$3,'waiting_local_directory',now()-interval '2 minutes') RETURNING id",
        &[&agent,&runtime,&issue],
    ).await.unwrap().get(0);
    drop(db);
    let waiting = read(&app, &status_path, Some(&owner)).await.1;
    assert_eq!(waiting["members"][1]["status"], "idle");
    assert_eq!(
        waiting["members"][1]["active_issues"][0]["identifier"],
        "SYN-73"
    );
    assert_eq!(
        waiting["members"][1]["active_issues"][0]["issue_id"],
        issue.to_string()
    );
    let db = pool.get().await.unwrap();
    db.execute(
        "UPDATE agent_task_queue SET status='dispatched' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    db.execute("UPDATE agent_runtime SET status='offline',last_seen_at=now()-interval '20 minutes' WHERE id=$1", &[&runtime]).await.unwrap();
    drop(db);
    assert_eq!(
        read(&app, &status_path, Some(&member)).await.1["members"][1]["status"],
        "working"
    );
    let db = pool.get().await.unwrap();
    db.execute("UPDATE agent SET archived_at=now() WHERE id=$1", &[&agent])
        .await
        .unwrap();
    drop(db);
    assert_eq!(
        read(&app, &status_path, Some(&member)).await.1["members"][1]["status"],
        "archived"
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/squads/{archived}/members/status?workspace_id={ws}"),
            Some(&member)
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/squads/{other}/members/status?workspace_id={ws}"),
            Some(&owner)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/squads/bad/members/status?workspace_id={ws}"),
            Some(&owner)
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let db = pool.get().await.unwrap();
    db.execute(
        "ALTER TABLE squad_member RENAME TO squad_member_hidden_test",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &status_path, Some(&owner)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list squad member status"})
        )
    );
    db.execute(
        "ALTER TABLE squad_member_hidden_test RENAME TO squad_member",
        &[],
    )
    .await
    .unwrap();
    drop(db);
    for id in [other, Uuid::new_v4()] {
        for path in [
            format!("/api/squads/{id}/?workspace_id={ws}"),
            format!("/api/squads/{id}/members?workspace_id={ws}"),
        ] {
            assert_eq!(
                read(&app, &path, Some(&owner)).await,
                (StatusCode::NOT_FOUND, json!({"error":"squad not found"}))
            );
        }
    }
    assert_eq!(
        read(
            &app,
            &format!("/api/squads/bad/?workspace_id={ws}"),
            Some(&owner)
        )
        .await,
        (StatusCode::BAD_REQUEST, json!({"error":"invalid squad id"}))
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/squads/{squad}/?workspace_id={foreign}"),
            Some(&member)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let db = pool.get().await.unwrap();
    let before: i64 = db
        .query_one("SELECT count(*) FROM squad_member", &[])
        .await
        .unwrap()
        .get(0);
    drop(db);
    let _ = read(&app, &path, Some(&owner)).await;
    let db = pool.get().await.unwrap();
    let after: i64 = db
        .query_one("SELECT count(*) FROM squad_member", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
    db.execute(
        "ALTER TABLE squad_member RENAME TO squad_member_hidden_test",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&owner)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list squad member preview"})
        )
    );
    assert_eq!(
        read(&app, &detail, Some(&owner)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to load squad member preview"})
        )
    );
    db.execute(
        "ALTER TABLE squad_member_hidden_test RENAME TO squad_member",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hex::encode(Sha256::digest(pat.as_bytes()))],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
}
