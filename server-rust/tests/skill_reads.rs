//! Skill read contracts on a disposable migrated PostgreSQL instance.
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
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 on a random loopback port"]
async fn skill_reads_scope_workspace_content_labels_and_metadata() {
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
                &[&name, &format!("skill-{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Skills A',$1) RETURNING id",
            &[&format!("skill-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Skills B',$1) RETURNING id",
            &[&format!("skill-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&other]).await.unwrap();
    let skill: Uuid = db.query_one("INSERT INTO skill(workspace_id,name,description,content,config,created_by) VALUES ($1,'A','Description','hello \\d+',NULLIF($2::jsonb,'null'::jsonb),$3) RETURNING id", &[&ws,&json!({"nested":42}),&users[0]]).await.unwrap().get(0);
    let foreign_skill: Uuid = db.query_one("INSERT INTO skill(workspace_id,name,content) VALUES ($1,'Foreign','private body') RETURNING id", &[&other]).await.unwrap().get(0);
    let label: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'Blue','#123456','skill') RETURNING id", &[&ws]).await.unwrap().get(0);
    let foreign_label: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'Private','#123456','skill') RETURNING id", &[&other]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO skill_to_label(skill_id,label_id) VALUES ($1,$2),($1,$3)",
        &[&skill, &label, &foreign_label],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO skill_file(skill_id,path,content) VALUES ($1,'z.md','backslash \\d+'),($1,'a.md','🍕')", &[&skill]).await.unwrap();
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES ($1,'Skill agent','cloud',$2) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let foreign_agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES ($1,'Foreign agent','cloud',$2) RETURNING id", &[&other,&users[0]]).await.unwrap().get(0);
    let system_agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,kind) VALUES ($1,'System agent','cloud',$2,'system') RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO agent_skill(agent_id,skill_id,enabled) VALUES ($1,$2,false),($3,$4,true)",
        &[&agent, &skill, &foreign_agent, &foreign_skill],
    )
    .await
    .unwrap();
    let agent_label: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'Agent Blue','#abcdef','agent') RETURNING id", &[&ws]).await.unwrap().get(0);
    let foreign_agent_label: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'Foreign agent','#abcdef','agent') RETURNING id", &[&other]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO agent_to_label(agent_id,label_id) VALUES ($1,$2),($1,$3)",
        &[&agent, &agent_label, &foreign_agent_label],
    )
    .await
    .unwrap();
    let snapshot: Value = db.query_one("SELECT jsonb_build_object('skills',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) FROM skill s WHERE s.workspace_id=$1),'files',(SELECT jsonb_agg(to_jsonb(f) ORDER BY f.id) FROM skill_file f WHERE f.skill_id=$2),'labels',(SELECT jsonb_agg(to_jsonb(l) ORDER BY l.id) FROM issue_label l WHERE l.workspace_id=$1))", &[&ws,&skill]).await.unwrap().get(0);
    let pat = format!("mul_skill_{suffix}");
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'synthetic',$2,'mul_skill')", &[&users[0],&hex::encode(Sha256::digest(pat.as_bytes()))]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-skill-secret".to_vec()),
    );
    drop(db);
    let auth = |id: Uuid| {
        encode(&Header::default(),&json!({"sub":id,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(b"synthetic-skill-secret")).unwrap()
    };
    let owner = auth(users[0]);
    let member = auth(users[1]);
    let outsider = auth(users[2]);
    let listing = format!("/api/skills/?workspace_id={ws}");
    assert_eq!(read(&app, &listing, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &listing, Some(&outsider)).await.0,
        StatusCode::NOT_FOUND
    );
    let listed = read(&app, &listing, Some(&member)).await;
    assert_eq!(listed.0, StatusCode::OK);
    assert_eq!(listed.1.as_array().unwrap().len(), 1);
    assert_eq!(listed.1[0]["labels"][0]["id"], label.to_string());
    assert!(listed.1[0].get("content").is_none());
    assert_eq!(read(&app, &listing, Some(&pat)).await.1, listed.1);
    let agent_path = format!("/api/agents/{agent}/skills?workspace_id={ws}");
    let assigned = read(&app, &agent_path, Some(&member)).await;
    assert_eq!(assigned.0, StatusCode::OK);
    assert_eq!(assigned.1.as_array().unwrap().len(), 1);
    assert_eq!(assigned.1[0]["enabled"], false);
    assert!(assigned.1[0].get("content").is_none());
    assert!(assigned.1[0].get("labels").is_none());
    assert_eq!(read(&app, &agent_path, Some(&pat)).await, assigned);
    assert_eq!(
        read(&app, &agent_path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, &agent_path, Some(&outsider)).await.0,
        StatusCode::NOT_FOUND
    );
    for id in [foreign_agent, system_agent] {
        assert_eq!(
            read(
                &app,
                &format!("/api/agents/{id}/skills?workspace_id={ws}"),
                Some(&owner)
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        read(
            &app,
            &format!("/api/agents/invalid/skills?workspace_id={ws}"),
            Some(&owner)
        )
        .await
        .1,
        json!({"error":"invalid agent id"})
    );
    let db = pool.get().await.unwrap();
    // The junction table has no workspace FK: never serve a foreign skill even
    // when an invalid association is inserted by a privileged DB writer.
    db.execute(
        "INSERT INTO agent_skill(agent_id,skill_id) VALUES ($1,$2)",
        &[&agent, &foreign_skill],
    )
    .await
    .unwrap();
    assert_eq!(read(&app, &agent_path, Some(&owner)).await.1, assigned.1);
    db.execute(
        "DELETE FROM agent_skill WHERE agent_id=$1 AND skill_id=$2",
        &[&agent, &foreign_skill],
    )
    .await
    .unwrap();

    let server: Uuid = db.query_one(
        "INSERT INTO workspace_mcp_server(workspace_id,name,config) VALUES ($1,'Alpha MCP',$2) RETURNING id",
        &[&ws, &json!({"type":"sse", "headers":{"Authorization":"synthetic secret"}})],
    ).await.unwrap().get(0);
    let foreign_server: Uuid = db.query_one(
        "INSERT INTO workspace_mcp_server(workspace_id,name,config) VALUES ($1,'Foreign MCP',$2) RETURNING id",
        &[&other, &json!({"type":"http", "headers":{"Authorization":"foreign secret"}})],
    ).await.unwrap().get(0);
    db.execute("INSERT INTO agent_mcp_server(agent_id,server_id,enabled) VALUES ($1,$2,false),($1,$3,true)", &[&agent,&server,&foreign_server]).await.unwrap();
    let mcp_path = format!("/api/agents/{agent}/mcp-servers?workspace_id={ws}");
    assert_eq!(
        read(&app, &mcp_path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, &mcp_path, Some(&outsider)).await.0,
        StatusCode::NOT_FOUND
    );
    let mcp = read(&app, &mcp_path, Some(&member)).await;
    assert_eq!(mcp.0, StatusCode::OK);
    assert_eq!(mcp.1.as_array().unwrap().len(), 1);
    assert_eq!(mcp.1[0]["id"], server.to_string());
    assert_eq!(mcp.1[0]["transport"], "sse");
    assert_eq!(mcp.1[0]["enabled"], false);
    assert!(!mcp.1.to_string().contains("secret"));
    assert_eq!(read(&app, &mcp_path, Some(&pat)).await, mcp);
    for invalid in [foreign_agent, system_agent] {
        assert_eq!(
            read(
                &app,
                &format!("/api/agents/{invalid}/mcp-servers?workspace_id={ws}"),
                Some(&owner)
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        read(
            &app,
            &format!("/api/agents/invalid/mcp-servers?workspace_id={ws}"),
            Some(&owner)
        )
        .await
        .1,
        json!({"error":"invalid agent id"})
    );
    db.execute(
        "ALTER TABLE agent_mcp_server RENAME TO agent_mcp_gate_hidden",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &mcp_path, Some(&owner)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list the agent's MCP servers"})
        )
    );
    db.execute(
        "ALTER TABLE agent_mcp_gate_hidden RENAME TO agent_mcp_server",
        &[],
    )
    .await
    .unwrap();
    let agent_labels = format!("/api/agents/{agent}/labels?workspace_id={ws}");
    let skill_labels = format!("/api/skills/{skill}/labels?workspace_id={ws}");
    for (path, expected) in [(&agent_labels, agent_label), (&skill_labels, label)] {
        assert_eq!(read(&app, path, None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            read(&app, path, Some(&outsider)).await.0,
            StatusCode::NOT_FOUND
        );
        let response = read(&app, path, Some(&member)).await;
        assert_eq!(response.0, StatusCode::OK);
        assert_eq!(response.1["labels"].as_array().unwrap().len(), 1);
        assert_eq!(response.1["labels"][0]["id"], expected.to_string());
        assert_eq!(response.1["labels"][0]["usage_count"], 0);
        assert_eq!(read(&app, path, Some(&pat)).await, response);
    }
    assert_eq!(
        read(
            &app,
            &format!("/api/skills/{foreign_skill}/labels?workspace_id={ws}"),
            Some(&owner)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for id in [foreign_agent, system_agent] {
        assert_eq!(
            read(
                &app,
                &format!("/api/agents/{id}/labels?workspace_id={ws}"),
                Some(&owner)
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        read(
            &app,
            &format!("/api/agents/bad/labels?workspace_id={ws}"),
            Some(&owner)
        )
        .await
        .1,
        json!({"error":"invalid agent id"})
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/skills/bad/labels?workspace_id={ws}"),
            Some(&owner)
        )
        .await
        .1,
        json!({"error":"invalid skill id"})
    );
    let detail = format!("/api/skills/{skill}/?workspace_id={ws}");
    let full = read(&app, &detail, Some(&owner)).await;
    assert_eq!(full.1["content"], r"hello \d+");
    assert_eq!(full.1["files"][0]["path"], "a.md");
    assert_eq!(full.1["files"][0]["content"], "🍕");
    assert!(full.1.get("labels").is_none());
    let meta = read(&app, &format!("{detail}&include=metadata"), Some(&member)).await;
    assert_eq!(meta.0, StatusCode::OK);
    assert!(meta.1.get("content").is_none());
    assert_eq!(meta.1["content_size"], r"hello \d+".len());
    assert_eq!(
        meta.1["content_hash"],
        hex::encode(Sha256::digest(br"hello \d+"))
    );
    assert_eq!(meta.1["files"][0]["size"], "🍕".len());
    assert_eq!(
        meta.1["files"][1]["content_hash"],
        hex::encode(Sha256::digest(br"backslash \d+"))
    );
    assert!(meta.1["files"][0].get("content").is_none());
    assert_eq!(
        read(
            &app,
            &format!("/api/skills/{skill}/files?workspace_id={ws}&include=metadata"),
            Some(&member)
        )
        .await
        .1,
        meta.1["files"]
    );
    for path in [
        format!("/api/skills/{foreign_skill}/?workspace_id={ws}"),
        format!("/api/skills/{foreign_skill}/files?workspace_id={ws}"),
    ] {
        assert_eq!(
            read(&app, &path, Some(&owner)).await.0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        read(&app, &format!("{detail}&include=bad"), Some(&owner)).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid include: expected \"content\" or \"metadata\""})
        )
    );
    assert_eq!(
        read(&app, "/api/skills/bad/?workspace_id=bad", Some(&owner))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/skills/{skill}/?workspace_id={other}"),
            Some(&member)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let db = pool.get().await.unwrap();
    let after:Value=db.query_one("SELECT jsonb_build_object('skills',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) FROM skill s WHERE s.workspace_id=$1),'files',(SELECT jsonb_agg(to_jsonb(f) ORDER BY f.id) FROM skill_file f WHERE f.skill_id=$2),'labels',(SELECT jsonb_agg(to_jsonb(l) ORDER BY l.id) FROM issue_label l WHERE l.workspace_id=$1))", &[&ws,&skill]).await.unwrap().get(0);
    assert_eq!(snapshot, after);
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_skill WHERE agent_id=$1",
            &[&agent],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
    db.execute(
        "ALTER TABLE agent_to_label RENAME TO agent_label_gate_hidden",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &agent_labels, Some(&owner)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list agent labels"})
        )
    );
    db.execute(
        "ALTER TABLE agent_label_gate_hidden RENAME TO agent_to_label",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "ALTER TABLE skill_to_label RENAME TO skill_label_gate_hidden",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &skill_labels, Some(&owner)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list skill labels"})
        )
    );
    db.execute(
        "ALTER TABLE skill_label_gate_hidden RENAME TO skill_to_label",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "ALTER TABLE agent_skill RENAME TO agent_skill_gate_hidden",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &agent_path, Some(&owner)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list agent skills"})
        )
    );
    db.execute(
        "ALTER TABLE agent_skill_gate_hidden RENAME TO agent_skill",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "ALTER TABLE skill_file RENAME TO skill_file_gate_hidden",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &detail, Some(&owner)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list skill files"})
        )
    );
    db.execute(
        "ALTER TABLE skill_file_gate_hidden RENAME TO skill_file",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE member SET role='admin' WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &users[1]],
    )
    .await
    .unwrap();
    assert_eq!(read(&app, &listing, Some(&member)).await.0, StatusCode::OK);
    assert_eq!(
        read(&app, &agent_path, Some(&member)).await.0,
        StatusCode::OK
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &users[1]],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &listing, Some(&member)).await.0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE user_id=$1",
        &[&users[0]],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &listing, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, &agent_path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
}
