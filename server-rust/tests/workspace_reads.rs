//! Workspace metadata parity and secret projection on a disposable migrated DB.
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

const SECRET: &str = "synthetic-workspace-read-secret";

async fn read(app: &axum::Router, path: &str, auth: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(path);
    if let Some(token) = auth {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let res = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "needs run-owned migrated PostgreSQL on loopback, never port 5432"]
async fn workspace_reads_enforce_roles_binding_secrets_and_errors() {
    let url = std::env::var("RUST_API_TEST_DATABASE_URL").unwrap();
    let config = Config::from_str(&url).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(!config.get_hosts().is_empty());
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(2)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let users = ["owner", "admin", "member", "outsider"];
    let mut ids = Vec::new();
    for name in users {
        let id: Uuid = db
            .query_one(
                "INSERT INTO \"user\" (name,email) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        ids.push(id);
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace (name,slug) VALUES ('Test space',$1) RETURNING id",
            &[&format!("test-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace (name,slug) VALUES ('Other space',$1) RETURNING id",
            &[&format!("other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let empty: Uuid = db
        .query_one(
            "INSERT INTO workspace (name,slug) VALUES ('Empty space',$1) RETURNING id",
            &[&format!("empty-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member (workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'admin'),($1,$4,'member'),($5,$2,'owner'),($6,$2,'owner')", &[&ws,&ids[0],&ids[1],&ids[2],&other,&empty]).await.unwrap();
    let invite: Uuid = db.query_one("INSERT INTO workspace_invitation (workspace_id,inviter_id,invitee_email,role) VALUES ($1,$2,'pending@invalid.example','member') RETURNING id", &[&ws,&ids[0]]).await.unwrap().get(0);
    db.execute("INSERT INTO workspace_invitation (workspace_id,inviter_id,invitee_email,role,status) VALUES ($1,$2,'old@invalid.example','member','declined')", &[&ws,&ids[0]]).await.unwrap();
    db.execute("INSERT INTO workspace_invitation (workspace_id,inviter_id,invitee_email,role,expires_at) VALUES ($1,$2,'expired@invalid.example','member',now()-interval '1 hour')", &[&ws,&ids[0]]).await.unwrap();
    db.execute("INSERT INTO workspace_invitation (workspace_id,inviter_id,invitee_email,role) VALUES ($1,$2,'other@invalid.example','member')", &[&other,&ids[0]]).await.unwrap();
    let profile: Uuid = db.query_one("INSERT INTO runtime_profile(workspace_id,display_name,protocol_family,command_name,fixed_args,created_by) VALUES ($1,'Custom','codex','fake-codex',$2,$3) RETURNING id", &[&ws,&json!(["--quiet"]),&ids[0]]).await.unwrap().get(0);
    let foreign_profile: Uuid = db.query_one("INSERT INTO runtime_profile(workspace_id,display_name,protocol_family,command_name) VALUES ($1,'Foreign','claude','fake-claude') RETURNING id", &[&other]).await.unwrap().get(0);
    db.execute("INSERT INTO runtime_profile(workspace_id,display_name,protocol_family,runtime_type,command_name,enabled) VALUES ($1,'Disabled','claude','claude-code','fake-claude',false)", &[&ws]).await.unwrap();
    let secret = "synthetic-private-token-do-not-return";
    let mcp: Uuid = db.query_one("INSERT INTO workspace_mcp_server(workspace_id,name,config) VALUES ($1,'Private server',$2) RETURNING id", &[&ws,&json!({"type":"websocket", "url":format!("wss://example.invalid/{secret}"), "headers":{"Authorization":secret}})]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO workspace_mcp_server(workspace_id,name,config) VALUES ($1,'Other',$2)",
        &[&other, &json!({"type":"http","url":secret})],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO workspace_mcp_server(workspace_id,name,config) VALUES ($1,'Legacy',$2)",
        &[&ws, &json!({"command":"fake", "env":{"KEY":secret}})],
    )
    .await
    .unwrap();
    for (name, config) in [
        (
            "Upper type",
            json!({"Type":"WebSocket","URL":format!("wss://example.invalid/{secret}")}),
        ),
        (
            "Upper command",
            json!({"COMMAND":null,"env":{"KEY":secret}}),
        ),
        (
            "Invalid upper type",
            json!({"TYPE":7,"URL":format!("https://example.invalid/{secret}")}),
        ),
    ] {
        db.execute(
            "INSERT INTO workspace_mcp_server(workspace_id,name,config) VALUES ($1,$2,$3)",
            &[&ws, &name, &config],
        )
        .await
        .unwrap();
    }
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Fake','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'Fake agent','cloud',$2,$3) RETURNING id", &[&ws,&ids[0],&runtime]).await.unwrap().get(0);
    let archived: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id,archived_at) VALUES ($1,'Archived','cloud',$2,$3,now()) RETURNING id", &[&ws,&ids[0],&runtime]).await.unwrap().get(0);
    db.execute("INSERT INTO agent_mcp_server(agent_id,server_id,enabled) VALUES ($1,$3,false),($2,$3,true)", &[&agent,&archived,&mcp]).await.unwrap();
    db.execute("INSERT INTO workspace_share_link(workspace_id,code,created_by,role) VALUES ($1,$2,$3,'member')", &[&ws,&format!("active-{suffix}"),&ids[0]]).await.unwrap();
    db.execute("INSERT INTO workspace_share_link(workspace_id,code,created_by,role,is_active) VALUES ($1,$2,$3,'member',false)", &[&ws,&format!("revoked-{suffix}"),&ids[0]]).await.unwrap();
    db.execute("INSERT INTO workspace_share_link(workspace_id,code,created_by,role) VALUES ($1,$2,$3,'member')", &[&other,&format!("foreign-{suffix}"),&ids[0]]).await.unwrap();
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let task_token = format!("mat_workspace_{suffix}");
    let hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&ids[0]]).await.unwrap();
    let before: i64 = db
        .query_one(
            "SELECT count(*) FROM workspace_share_link WHERE workspace_id=$1 AND use_count=0",
            &[&ws],
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
        ApiState::new(pool.clone(), SECRET.as_bytes().to_vec()),
    );
    drop(db);
    let exp = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600) as usize;
    let auth = |id: Uuid| {
        encode(
            &Header::default(),
            &json!({"sub":id.to_string(),"exp":exp}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let (owner, admin, member, outsider) = (auth(ids[0]), auth(ids[1]), auth(ids[2]), auth(ids[3]));
    let path = |tail: &str| format!("/api/workspaces/{ws}/{tail}");
    for tail in [
        "invitations",
        "runtime-profiles",
        "mcp-servers",
        "share-links",
    ] {
        assert_eq!(
            read(&app, &path(tail), None).await,
            (
                StatusCode::UNAUTHORIZED,
                json!({"error":"missing authorization"})
            )
        );
        assert_eq!(
            read(&app, &path(tail), Some(&outsider)).await,
            (
                StatusCode::NOT_FOUND,
                json!({"error":"workspace not found"})
            )
        );
        assert_eq!(
            read(
                &app,
                &format!("/api/workspaces/{other}/{tail}"),
                Some(&task_token)
            )
            .await,
            (
                StatusCode::FORBIDDEN,
                json!({"error":"task token is bound to a different workspace"})
            )
        );
        assert_eq!(
            read(
                &app,
                &format!("/api/workspaces/not-uuid/{tail}"),
                Some(&owner)
            )
            .await,
            (
                StatusCode::BAD_REQUEST,
                json!({"error":"invalid workspace_id"})
            )
        );
    }
    let invitations = read(&app, &path("invitations"), Some(&member)).await;
    assert_eq!(invitations.0, StatusCode::OK);
    assert_eq!(invitations.1.as_array().unwrap().len(), 1);
    assert_eq!(invitations.1[0]["id"], invite.to_string());
    assert!(invitations.1[0].get("workspace_name").is_none());
    assert_eq!(invitations.1[0]["inviter_name"], "owner");
    let profiles = read(&app, &path("runtime-profiles"), Some(&task_token)).await;
    assert_eq!(profiles.0, StatusCode::OK);
    assert_eq!(profiles.1["runtime_profiles"].as_array().unwrap().len(), 2);
    assert_eq!(profiles.1["runtime_profiles"][0]["runtime_type"], "codex");
    assert_eq!(
        profiles.1["runtime_profiles"][0]["fixed_args"],
        json!(["--quiet"])
    );
    assert_eq!(
        read(
            &app,
            &path(&format!("runtime-profiles/{profile}")),
            Some(&member)
        )
        .await
        .1["id"],
        profile.to_string()
    );
    assert_eq!(
        read(
            &app,
            &path(&format!("runtime-profiles/{foreign_profile}")),
            Some(&owner)
        )
        .await,
        (
            StatusCode::NOT_FOUND,
            json!({"error":"runtime profile not found"})
        )
    );
    assert_eq!(
        read(&app, &path("runtime-profiles/bad-id"), Some(&owner)).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid profile id"})
        )
    );
    let mcp_list = read(&app, &path("mcp-servers"), Some(&member)).await;
    assert_eq!(mcp_list.0, StatusCode::OK);
    assert_eq!(mcp_list.1.as_array().unwrap().len(), 5);
    for (name, expected) in [
        ("Private server", "websocket"),
        ("Upper type", "websocket"),
        ("Upper command", "stdio"),
        ("Invalid upper type", "unknown"),
    ] {
        let server = mcp_list
            .1
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["name"] == name)
            .unwrap();
        assert_eq!(server["transport"], expected, "{name}");
    }
    assert_eq!(
        mcp_list
            .1
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["name"] == "Private server")
            .unwrap()["agent_count"],
        1
    );
    assert!(!mcp_list.1.to_string().contains(secret));
    assert!(
        mcp_list
            .1
            .as_array()
            .unwrap()
            .iter()
            .all(|server| server.get("config").is_none() && server.get("enabled").is_none())
    );
    assert_eq!(
        read(&app, &path("share-links"), Some(&member)).await,
        (
            StatusCode::FORBIDDEN,
            json!({"error":"insufficient permissions"})
        )
    );
    assert_eq!(
        read(&app, &path("share-links"), Some(&task_token)).await.0,
        StatusCode::OK
    ); // Go role guard uses bound owner's membership.
    assert_eq!(
        read(&app, &path("share-links"), Some(&admin)).await.0,
        StatusCode::OK
    );
    let listed = read(&app, &path("share-links"), Some(&owner)).await.1;
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["code"], format!("active-{suffix}"));
    assert!(listed[0].get("expires_at").is_none());
    assert!(listed[0].get("max_uses").is_none());
    let empty_list = read(
        &app,
        &format!("/api/workspaces/{empty}/share-links"),
        Some(&owner),
    )
    .await;
    assert_eq!(empty_list, (StatusCode::OK, json!([])));
    let preview = read(&app, &format!("/api/share-links/active-{suffix}"), None).await;
    assert_eq!(preview.0, StatusCode::OK);
    assert_eq!(
        preview.1,
        json!({"workspace_name":"Test space","workspace_slug":format!("test-{suffix}"),"creator_name":"owner","role":"member"})
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/share-links/active-{suffix}"),
            Some("invalid")
        )
        .await,
        preview
    );
    assert_eq!(
        read(&app, &format!("/api/share-links/revoked-{suffix}"), None).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error":"share link not found or expired"})
        )
    );
    assert_eq!(
        read(&app, &format!("/api/share-links/foreign-{suffix}"), None)
            .await
            .0,
        StatusCode::OK
    ); // Public route intentionally exposes bounded preview from any workspace.
    let db = pool.get().await.unwrap();
    let after: i64 = db
        .query_one(
            "SELECT count(*) FROM workspace_share_link WHERE workspace_id=$1 AND use_count=0",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
    db.execute(
        "UPDATE workspace_share_link SET use_count=1, max_uses=1 WHERE code=$1",
        &[&format!("active-{suffix}")],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &format!("/api/share-links/active-{suffix}"), None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute("UPDATE workspace_share_link SET use_count=0, max_uses=NULL, expires_at=now()-interval '1 hour' WHERE code=$1", &[&format!("active-{suffix}")]).await.unwrap();
    assert_eq!(
        read(&app, &format!("/api/share-links/active-{suffix}"), None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}
