//! Workspace wakeup board: frozen SQL, filter/counts and actor scope on disposable PostgreSQL.
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

const SECRET: &[u8] = b"synthetic-workspace-wakeups";

async fn read(
    app: &axum::Router,
    ws: Uuid,
    query: &str,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(format!("/api/issue-wakeups?workspace_id={ws}&{query}"));
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    (status, body)
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi63 disposable database migrated to both ledgers"]
async fn workspace_wakeup_board_filters_redacts_and_keeps_counts_scoped() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(3)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let [
        owner,
        member,
        outsider,
        ws,
        other_ws,
        agent,
        private,
        issue,
        foreign_issue,
        normal,
        paused,
        system,
        foreign,
    ]: [Uuid; 13] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, member, outsider] {
        db.execute(
            r#"INSERT INTO "user"(id,name,email) VALUES ($1,'synthetic',$2)"#,
            &[&user, &format!("{user}@workspace-wakeup.invalid")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, other_ws] {
        db.execute(
            "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,'test',$2,'WW')",
            &[&workspace, &format!("ww-{workspace}")],
        )
        .await
        .unwrap();
    }
    for (user, role) in [(owner, "owner"), (member, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    for agent_id in [agent, private] {
        db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id) VALUES ($1,$2,$3,'cloud',$4)", &[&agent_id, &ws, &format!("agent-{agent_id}"), &owner]).await.unwrap();
    }
    for (id, workspace, number) in [(issue, ws, 1), (foreign_issue, other_ws, 2)] {
        db.execute("INSERT INTO issue(id,workspace_id,title,status,creator_type,creator_id,number) VALUES ($1,$2,'needle title','in_progress','member',$3,$4)", &[&id, &workspace, &owner, &number]).await.unwrap();
    }
    for (id, workspace, issue_id, agent_id, mode, reason, system_rule) in [
        (normal, ws, issue, agent, "once", None, None),
        (paused, ws, issue, agent, "continuous", Some("rate"), None),
        (system, ws, issue, agent, "once", None, Some("child_done")),
        (foreign, other_ws, foreign_issue, agent, "once", None, None),
    ] {
        db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,enabled,disabled_at,paused_reason,system_rule,filter_actor_type,filter_actor_id,filter_agent_id) VALUES ($1,$2,$3,$4,$5,'secret instruction','event',$6,$7,CASE WHEN $7 THEN NULL ELSE now() END,$8,$9,'agent',$10,$10)",
            &[&id, &workspace, &issue_id, &agent_id, &owner, &mode, &reason.is_none(), &reason, &system_rule, &private]).await.unwrap();
    }
    let child = Uuid::new_v4();
    db.execute("INSERT INTO issue(id,workspace_id,title,status,creator_type,creator_id,number,parent_issue_id,stage) VALUES ($1,$2,'child','todo','member',$3,3,$4,1)", &[&child, &ws, &owner, &issue]).await.unwrap();
    let run = Uuid::new_v4();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,issue_id,status,context,completed_at) VALUES ($1,$2,$3,'completed',$4,now())", &[&run, &agent, &issue, &json!({"wakeup_id":paused})]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    );
    let jwt = |user| {
        encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
    };
    assert_eq!(read(&app, ws, "", None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, ws, "", Some("broken")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, ws, "", Some(&jwt(outsider))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, other_ws, "", Some(&jwt(owner))).await.0,
        StatusCode::NOT_FOUND
    );
    for (query, message) in [
        ("scope=unknown", "invalid wakeup scope"),
        ("kind=unknown", "invalid wakeup kind"),
        ("source=unknown", "invalid wakeup source"),
        ("offset=-1", "invalid pagination"),
        ("limit=101", "invalid pagination"),
        ("agent_id=broken", "invalid agent id"),
    ] {
        assert_eq!(
            read(&app, ws, query, Some(&jwt(owner))).await,
            (StatusCode::BAD_REQUEST, json!({"error":message}))
        );
    }
    let (status, body) = read(&app, ws, "scope=all&limit=100", Some(&jwt(owner))).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["total"], 3);
    assert_eq!(
        body["counts"],
        json!({"all":3,"active":2,"paused":1,"disabled":0,"ended":0})
    );
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 3);
    assert!(!items.iter().any(|item| item["id"] == json!(foreign)));
    let normal_row = items.iter().find(|row| row["id"] == json!(normal)).unwrap();
    assert_eq!(normal_row["source"], "member");
    assert_eq!(normal_row["can_manage"], true);
    assert_eq!(normal_row["filter_actor_id"], json!(private));
    assert_eq!(normal_row["filter_agent_id"], json!(private));
    let paused_row = items.iter().find(|row| row["id"] == json!(paused)).unwrap();
    assert_eq!(paused_row["runs_7d"], 1);
    assert_eq!(paused_row["paused_reason"], "rate");
    let sys_row = items.iter().find(|row| row["id"] == json!(system)).unwrap();
    assert_eq!(sys_row["source"], "system");
    assert_eq!(sys_row["rule"], "child_done");
    assert_eq!(sys_row["system_remaining"], 1);
    assert_eq!(sys_row["can_manage"], true);
    let (_, member_body) = read(&app, ws, "scope=all&limit=100", Some(&jwt(member))).await;
    let member_row = member_body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == json!(normal))
        .unwrap();
    assert_eq!(member_row["can_manage"], false);
    assert!(member_row["filter_actor_id"].is_null());
    assert!(member_row["filter_agent_id"].is_null());
    assert_eq!(member_body["agents"].as_array().unwrap().len(), 1);
    let (_, page) = read(&app, ws, "scope=all&offset=1&limit=1", Some(&jwt(owner))).await;
    assert_eq!(page["total"], 3);
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    let (_, filtered) = read(
        &app,
        ws,
        "scope=paused&kind=event&source=member&search=needle",
        Some(&jwt(owner)),
    )
    .await;
    assert_eq!(filtered["total"], 1);
    assert_eq!(filtered["items"][0]["id"], json!(paused));
    let (_, empty) = read(&app, ws, "scope=all&search=no-match", Some(&jwt(owner))).await;
    assert_eq!(empty["total"], 0);
    assert_eq!(empty["items"], json!([]));
    assert_eq!(empty["counts"]["all"], 3);
    assert_eq!(
        read(
            &app,
            ws,
            &format!("scope=all&agent_id={private}"),
            Some(&jwt(owner))
        )
        .await
        .1["total"],
        0
    );
    let pat = b"mul_workspace_wakeup_synthetic";
    let hash = hex::encode(Sha256::digest(pat));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'synthetic',$2,'mul_work')", &[&owner, &hash]).await.unwrap();
    assert_eq!(
        read(
            &app,
            ws,
            "scope=all",
            Some(std::str::from_utf8(pat).unwrap())
        )
        .await
        .0,
        StatusCode::OK
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, "", Some(std::str::from_utf8(pat).unwrap()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    db.execute(
        "UPDATE agent SET runtime_id=$2 WHERE id=$1",
        &[&agent, &runtime],
    )
    .await
    .unwrap();
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,originator_user_id,accountable_user_id) VALUES ($1,$2,$3,'running',$4,$4) RETURNING id", &[&agent,&runtime,&issue,&member]).await.unwrap().get(0);
    let token = format!("mat_workspace_wakeup_{task}");
    let task_hash = hex::encode(Sha256::digest(token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&task_hash,&task,&agent,&ws,&owner]).await.unwrap();
    let (_, task_board) = read(&app, ws, "scope=all", Some(&token)).await;
    let task_item = task_board["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == json!(normal))
        .unwrap();
    assert_eq!(task_item["can_manage"], false); // task token's owner does not lend management rights
    db.execute(
        "UPDATE agent_task_queue SET originator_user_id=$2,accountable_user_id=$2 WHERE id=$1",
        &[&task, &owner],
    )
    .await
    .unwrap();
    let (_, task_board) = read(&app, ws, "scope=all", Some(&token)).await;
    let task_item = task_board["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == json!(normal))
        .unwrap();
    assert_eq!(task_item["can_manage"], true);
    db.execute(
        "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    let (_, task_board) = read(&app, ws, "scope=all", Some(&token)).await;
    let task_item = task_board["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == json!(normal))
        .unwrap();
    assert_eq!(task_item["can_manage"], false); // terminal tasks cannot keep borrowing originator rights
    let before: i64 = db
        .query_one(
            "SELECT fire_count::bigint FROM issue_wakeup WHERE id=$1",
            &[&normal],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, 0);
    db.execute(
        "ALTER TABLE issue_wakeup RENAME TO issue_wakeup_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, "scope=all", Some(&jwt(owner))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"could not save wakeup"})
        )
    );
    db.execute(
        "ALTER TABLE issue_wakeup_unavailable RENAME TO issue_wakeup",
        &[],
    )
    .await
    .unwrap();
    let after: i64 = db
        .query_one(
            "SELECT fire_count::bigint FROM issue_wakeup WHERE id=$1",
            &[&normal],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
}
