//! Go system-wakeup PUT contract on owned, disposable migrated PostgreSQL.
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

const SECRET: &[u8] = b"synthetic-system-wakeup-put";

async fn request(app: &axum::Router, uri: &str, token: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(uri)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    (status, body)
}
fn jwt(user: Uuid) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi100_system_put PostgreSQL migrated to both ledgers"]
async fn issue_customization_and_workspace_default_follow_go_contract() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_system_put"));
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
        admin,
        member,
        ws,
        p1,
        p2,
        c1,
        c2,
        c3,
        agent,
        runtime,
        task,
        queued,
        started,
    ]: [Uuid; 14] = std::array::from_fn(|_| Uuid::new_v4());
    for u in [owner, admin, member] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES($1,'Synthetic',$2)",
            &[&u, &format!("{u}@system.invalid")],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES($1,'Synthetic',$2,'SYS')",
        &[&ws, &format!("system-put-{ws}")],
    )
    .await
    .unwrap();
    for (u, role) in [(owner, "owner"), (admin, "admin"), (member, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&ws, &u, &role],
        )
        .await
        .unwrap();
    }
    for (id, number, status, parent, stage) in [
        (p1, 1, "todo", None, None),
        (p2, 2, "todo", None, None),
        (c1, 3, "done", Some(p1), Some(1)),
        (c2, 4, "todo", Some(p1), Some(2)),
        (c3, 5, "done", Some(p2), Some(1)),
    ] {
        db.execute("INSERT INTO issue(id,workspace_id,title,status,creator_type,creator_id,number,parent_issue_id,stage) VALUES($1,$2,'Synthetic',$3,'member',$4,$5,$6,$7)",
            &[&id,&ws,&status,&owner,&number,&parent,&stage]).await.unwrap();
    }
    // Finish pending trigger observations: prior facts are already observed before toggling.
    db.execute(
        "UPDATE issue_child_event SET processed_at=now() WHERE parent_id IN ($1,$2)",
        &[&p1, &p2],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_runtime(id,workspace_id,name,runtime_mode,provider,status) VALUES($1,$2,'Synthetic','cloud','synthetic','online')", &[&runtime,&ws]).await.unwrap();
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id,runtime_id,kind) VALUES($1,$2,'Synthetic','cloud',$3,$4,'user')", &[&agent,&ws,&owner,&runtime]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status) VALUES($1,$2,$3,$4,'queued')", &[&task,&agent,&runtime,&p1]).await.unwrap();
    let mat = format!("mat_system_put_{task}");
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')",
        &[&hex::encode(Sha256::digest(mat.as_bytes())),&task,&agent,&ws,&owner]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    );
    let issue_path = format!("/api/issues/{p1}/system-wakeups/child_done?workspace_id={ws}");
    let workspace_path = format!("/api/system-wakeups/child_done?workspace_id={ws}");
    for (uri, token, body, expected) in [
        (
            issue_path.replace("child_done", "invalid"),
            jwt(owner),
            json!({}),
            StatusCode::NOT_FOUND,
        ),
        (
            workspace_path.replace("child_done", "invalid"),
            jwt(owner),
            json!({}),
            StatusCode::NOT_FOUND,
        ),
        (
            issue_path.clone(),
            jwt(owner),
            json!({"unknown":true}),
            StatusCode::BAD_REQUEST,
        ),
        (
            issue_path.clone(),
            jwt(owner),
            json!({"instruction":"x".repeat(4001)}),
            StatusCode::BAD_REQUEST,
        ),
        (
            workspace_path.clone(),
            jwt(owner),
            json!({"instruction":"x".repeat(4001)}),
            StatusCode::BAD_REQUEST,
        ),
        (
            issue_path.clone(),
            mat.clone(),
            json!({"enabled":false}),
            StatusCode::FORBIDDEN,
        ),
        (
            workspace_path.clone(),
            mat,
            json!({"enabled":false}),
            StatusCode::FORBIDDEN,
        ),
        (
            workspace_path.clone(),
            jwt(member),
            json!({"enabled":false}),
            StatusCode::FORBIDDEN,
        ),
    ] {
        let (status, response) = request(&app, &uri, &token, body).await;
        assert_eq!(status, expected, "{uri}: {response:?}");
    }
    let (status, response) = request(
        &app,
        &workspace_path,
        &jwt(admin),
        json!({"enabled":false,"instruction":"  Wrap up  "}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    assert_eq!(response[0]["enabled"], false);
    assert_eq!(response[0]["instruction"], "Wrap up");
    let (status, response) = request(
        &app,
        &issue_path,
        &jwt(member),
        json!({"enabled":true,"instruction":"  Per issue  "}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    assert_eq!(response[0]["customized"], true);
    assert_eq!(response[0]["instruction"], "Per issue");
    let rule: Uuid = response[0]["id"].as_str().unwrap().parse().unwrap();
    let row = db
        .query_one(
            "SELECT condition_state,enabled,condition,event_types FROM issue_wakeup WHERE id=$1",
            &[&rule],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "stage:1");
    assert_eq!(row.get::<_, bool>(1), true);
    assert_eq!(
        row.get::<_, Value>(2),
        json!({"type":"children_done","each_stage":true})
    );
    assert_eq!(row.get::<_, Vec<String>>(3), vec!["issue.status_changed"]);
    db.execute("INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload) VALUES(gen_random_uuid(),$1,1,'old','children.changed','{}'::jsonb)", &[&rule]).await.unwrap();
    for (id, status, started_at) in [
        (queued, "queued", None),
        (started, "running", Some(chrono::Utc::now())),
    ] {
        db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,context,started_at) VALUES($1,$2,$3,$4,$5,$6,$7)",
            &[&id,&agent,&runtime,&p1,&status,&json!({"wakeup_id":rule}),&started_at]).await.unwrap();
    }
    let (status, response) =
        request(&app, &issue_path, &jwt(member), json!({"enabled":false})).await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    assert_eq!(response[0]["enabled"], false);
    assert_eq!(response[0]["instruction"], "Per issue");
    assert_eq!(db.query_one("SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NOT NULL", &[&rule]).await.unwrap().get::<_,i64>(0),1);
    for (id, expected) in [(queued, "cancelled"), (started, "running")] {
        assert_eq!(
            db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&id])
                .await
                .unwrap()
                .get::<_, String>(0),
            expected
        );
    }
    db.execute("UPDATE issue SET status='done' WHERE id=$1", &[&c2])
        .await
        .unwrap();
    let (status, response) = request(&app, &issue_path, &jwt(owner), json!({"enabled":true})).await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    assert_eq!(response, json!([])); // all children are closed: Go returns no public rule
    assert_eq!(
        db.query_one(
            "SELECT condition_state FROM issue_wakeup WHERE id=$1",
            &[&rule]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "all"
    );
    // A second, uncustomized materialized rule follows workspace defaults.
    let uncustomized = Uuid::new_v4();
    db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,system_rule,instruction,kind,mode,enabled) VALUES($1,$2,$3,'child_done','','event','continuous',false)", &[&uncustomized,&ws,&p2]).await.unwrap();
    let (status, response) =
        request(&app, &workspace_path, &jwt(owner), json!({"enabled":true})).await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    assert_eq!(response[0]["customized"], 1); // p1 remains customized
    assert_eq!(
        db.query_one(
            "SELECT enabled,condition_state FROM issue_wakeup WHERE id=$1",
            &[&uncustomized]
        )
        .await
        .unwrap()
        .get::<_, bool>(0),
        true
    );
    assert_eq!(
        db.query_one(
            "SELECT condition_state FROM issue_wakeup WHERE id=$1",
            &[&uncustomized]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "all"
    );
    assert_eq!(
        db.query_one("SELECT enabled FROM issue_wakeup WHERE id=$1", &[&rule])
            .await
            .unwrap()
            .get::<_, bool>(0),
        true
    );
    db.execute("INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload) VALUES(gen_random_uuid(),$1,1,'default','children.changed','{}'::jsonb)", &[&uncustomized]).await.unwrap();
    let (status, response) =
        request(&app, &workspace_path, &jwt(owner), json!({"enabled":false})).await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    assert_eq!(response[0]["enabled"], false);
    assert_eq!(
        db.query_one(
            "SELECT enabled FROM issue_wakeup WHERE id=$1",
            &[&uncustomized]
        )
        .await
        .unwrap()
        .get::<_, bool>(0),
        false
    );
    assert_eq!(db.query_one("SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NOT NULL", &[&uncustomized]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        db.query_one("SELECT enabled FROM issue_wakeup WHERE id=$1", &[&rule])
            .await
            .unwrap()
            .get::<_, bool>(0),
        true
    );
}
