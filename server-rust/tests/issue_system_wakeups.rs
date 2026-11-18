//! Frozen Go issue child-done system rule on run-owned migrated PostgreSQL.
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

const SECRET: &[u8] = b"synthetic-issue-system-wakeup";

async fn read(
    app: &axum::Router,
    ws: Uuid,
    issue: &str,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(format!(
        "/api/issues/{issue}/system-wakeups?workspace_id={ws}"
    ));
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
async fn child_done_rule_observes_stage_defaults_actor_and_workspace() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let [
        owner,
        member,
        outsider,
        ws,
        foreign,
        parent,
        other,
        rule,
        agent,
    ]: [Uuid; 9] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, member, outsider] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES ($1,'Synthetic Owner',$2)",
            &[&user, &format!("{user}@system.invalid")],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO workspace(id,name,slug,issue_prefix,settings) VALUES ($1,'Synthetic',$2,'SYN',$3),($4,'Foreign',$5,'FOR','{}')", &[&ws,&format!("system-{ws}"),&json!({"system_wakeup_child_done":false,"system_wakeup_child_done_instruction":"  Do the next stage.  "}),&foreign,&format!("system-{foreign}")]).await.unwrap();
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&owner,&member,&foreign]).await.unwrap();
    db.execute("INSERT INTO issue(id,workspace_id,title,status,creator_type,creator_id,number) VALUES ($1,$2,'Parent','todo','member',$3,1),($4,$5,'Foreign','todo','member',$3,1)", &[&parent,&ws,&owner,&other,&foreign]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    );
    let jwt = |user| {
        encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(SECRET)).unwrap()
    };
    assert_eq!(
        read(&app, ws, &parent.to_string(), Some(&jwt(owner))).await,
        (StatusCode::OK, json!([]))
    );
    assert_eq!(
        read(&app, ws, &parent.to_string(), None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, ws, &parent.to_string(), Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, ws, &parent.to_string(), Some(&jwt(outsider)))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, ws, &other.to_string(), Some(&jwt(owner)))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, foreign, &parent.to_string(), Some(&jwt(owner)))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,position,archived_at) VALUES ($1,'shipped','Shipped','done','#00aa00',1,now())", &[&ws]).await.unwrap();
    for (number, status, stage) in [
        (2, "done", Some(1)),
        (3, "todo", Some(1)),
        (4, "shipped", Some(2)),
        (5, "backlog", Some(2)),
        (6, "todo", None),
    ] {
        db.execute("INSERT INTO issue(workspace_id,parent_issue_id,title,status,creator_type,creator_id,number,stage) VALUES ($1,$2,'Child',$3,'member',$4,$5,$6)", &[&ws,&parent,&status,&owner,&number,&stage]).await.unwrap();
    }
    let check = |value: &Value| {
        assert_eq!(value.as_array().unwrap().len(), 1);
        assert_eq!(value[0]["stage"], 1);
        assert_eq!(value[0]["staged"], true);
        assert_eq!(value[0]["total"], 2);
        assert_eq!(value[0]["remaining"], 1);
        assert_eq!(value[0]["waiting"], json!(["SYN-3"]));
        assert_eq!(value[0]["blocked"], "no_assignee");
        assert_eq!(value[0]["workspace_default"], false);
        assert_eq!(value[0]["enabled"], false);
        assert_eq!(value[0]["default_instruction"], "Do the next stage.");
        assert_eq!(value[0]["id"], "");
        assert_eq!(value[0]["revision"], 0);
        assert_eq!(value[0]["target"], Value::Null);
    };
    let value = read(&app, ws, "SYN-1", Some(&jwt(member))).await;
    assert_eq!(value.0, StatusCode::OK);
    check(&value.1);
    let pat = format!("mul_issue_system_{parent}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'test',$2,'mul_issue_system')", &[&member,&hash]).await.unwrap();
    check(&read(&app, ws, &parent.to_string(), Some(&pat)).await.1);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, &parent.to_string(), Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "UPDATE issue SET assignee_type='member',assignee_id=$2,status='backlog' WHERE id=$1",
        &[&parent, &member],
    )
    .await
    .unwrap();
    let value = read(&app, ws, &parent.to_string(), Some(&jwt(owner)))
        .await
        .1;
    assert_eq!(value[0]["blocked"], "member_assignee");
    assert_eq!(
        value[0]["target"],
        json!({"type":"member","id":member,"name":"Synthetic Owner"})
    );
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,kind) VALUES ($1,$2,'Foreign agent','local','user')", &[&agent,&foreign]).await.unwrap();
    db.execute(
        "UPDATE issue SET assignee_type='agent',assignee_id=$2 WHERE id=$1",
        &[&parent, &agent],
    )
    .await
    .unwrap();
    let value = read(&app, ws, &parent.to_string(), Some(&jwt(owner)))
        .await
        .1;
    assert_eq!(value[0]["blocked"], "no_assignee");
    assert_eq!(value[0]["target"], Value::Null);
    db.execute(
        "UPDATE agent SET workspace_id=$2 WHERE id=$1",
        &[&agent, &ws],
    )
    .await
    .unwrap();
    let value = read(&app, ws, &parent.to_string(), Some(&jwt(owner)))
        .await
        .1;
    assert_eq!(value[0]["blocked"], "backlog");
    assert_eq!(
        value[0]["target"],
        json!({"type":"agent","id":agent,"name":"Foreign agent"})
    );
    db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,system_rule,instruction,kind,mode,enabled,customized_at,paused_reason) VALUES ($1,$2,$3,'child_done','Override','event','continuous',true,now(),'loop')", &[&rule,&ws,&parent]).await.unwrap();
    let value = read(&app, ws, &parent.to_string(), Some(&jwt(owner)))
        .await
        .1;
    assert_eq!(value[0]["id"], rule.to_string());
    assert_eq!(value[0]["enabled"], true);
    assert_eq!(value[0]["instruction"], "Override");
    assert_eq!(value[0]["customized"], true);
    assert_eq!(value[0]["paused_reason"], "loop");
    db.execute("UPDATE issue SET status='done' WHERE id=$1", &[&parent])
        .await
        .unwrap();
    assert_eq!(
        read(&app, ws, &parent.to_string(), Some(&jwt(owner))).await,
        (StatusCode::OK, json!([]))
    );
    db.execute("UPDATE issue SET status='todo' WHERE id=$1", &[&parent])
        .await
        .unwrap();
    db.execute(
        "UPDATE issue SET status='done' WHERE parent_issue_id=$1 AND number=3",
        &[&parent],
    )
    .await
    .unwrap();
    let value = read(&app, ws, &parent.to_string(), Some(&jwt(owner)))
        .await
        .1;
    assert_eq!(value[0]["stage"], 2);
    assert_eq!(value[0]["waiting"], json!(["SYN-5"]));
    db.execute(
        "UPDATE issue SET status='done' WHERE parent_issue_id=$1 AND number=5",
        &[&parent],
    )
    .await
    .unwrap();
    let value = read(&app, ws, &parent.to_string(), Some(&jwt(owner)))
        .await
        .1;
    assert_eq!(value[0]["staged"], false);
    assert_eq!(value[0]["waiting"], json!(["SYN-6"]));
    db.execute(
        "UPDATE issue SET status='done' WHERE parent_issue_id=$1 AND number=6",
        &[&parent],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, &parent.to_string(), Some(&jwt(owner))).await,
        (StatusCode::OK, json!([]))
    );
    db.execute(
        "UPDATE issue SET status='unknown_custom' WHERE id=$1",
        &[&parent],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, &parent.to_string(), Some(&jwt(owner))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"could not load system wakeups"})
        )
    );
    db.execute("UPDATE issue SET status='todo' WHERE id=$1", &[&parent])
        .await
        .unwrap();
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &member],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, &parent.to_string(), Some(&jwt(member)))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_wakeup WHERE issue_id=$1",
            &[&parent]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
}
