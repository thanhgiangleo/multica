//! Disposable-DB contract for Go Save(existingID), never a real workspace.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{ApiState, issue_write::*},
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"wakeup-put-test-secret";
#[derive(Default)]
struct Sink(Mutex<Vec<IssueTaskCancelledEvent>>);
impl IssueEventSink for Sink {
    fn publish_issue_deleted(&self, _: IssueDeletedEvent) {}
    fn publish_comment_created(&self, _: IssueCommentCreatedEvent) {}
    fn publish_comment_changed(&self, _: IssueCommentChangedEvent) {}
    fn publish_metadata_changed(&self, _: IssueMetadataEvent) {}
    fn publish_reaction_changed(&self, _: IssueReactionEvent) {}
    fn publish_assignment_changed(&self, _: IssueAssignmentEvent) {}
    fn publish_subscriber_changed(&self, _: IssueSubscriberEvent) {}
    fn publish_issue_updated(&self, _: IssueUpdatedEvent) {}
    fn publish_issue_created(&self, _: IssueCreatedEvent) {}
    fn publish_comment_resolution(&self, _: IssueCommentResolutionEvent) {}
    fn publish_wakeup_activity(&self, _: IssueWakeupActivityEvent) {}
    fn publish_task_cancelled(&self, event: IssueTaskCancelledEvent) {
        self.0.lock().unwrap().push(event);
    }
    fn request_wakeup_dispatch(&self, _: IssueWakeupDispatchRequest) {}
}
async fn put(
    app: &axum::Router,
    ws: Uuid,
    issue: Uuid,
    rule: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/api/issues/{issue}/wakeups/{rule}?workspace_id={ws}"
                ))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires owned disposable multica_zbsi100_wakeup_put PostgreSQL"]
async fn put_replaces_atomically_fences_old_work_and_baselines_condition() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_wakeup_put"));
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
        outsider,
        ws,
        other_ws,
        issue,
        other_issue,
        rule,
        system,
        agent,
        runtime,
        source,
        queued,
        deferred,
        running,
    ]: [Uuid; 14] = std::array::from_fn(|_| Uuid::new_v4());
    for u in [owner, outsider] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES($1,'test',$2)",
            &[&u, &format!("{u}@example.invalid")],
        )
        .await
        .unwrap();
    }
    for (w, name) in [(ws, "WAKE"), (other_ws, "OTHER")] {
        db.execute(
            "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES($1,'test',$2,$3)",
            &[&w, &format!("wakeup-put-{w}"), &name],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_runtime(id,workspace_id,name,runtime_mode,provider,status) VALUES($1,$2,'synthetic','cloud','synthetic','online')", &[&runtime,&ws]).await.unwrap();
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,$2,'agent','cloud',$3,$4)", &[&agent,&ws,&owner,&runtime]).await.unwrap();
    for (id, w) in [(issue, ws), (other_issue, other_ws)] {
        db.execute("INSERT INTO issue(id,workspace_id,title,status,creator_type,creator_id,number) VALUES($1,$2,'test','todo','member',$3,1)", &[&id,&w,&owner]).await.unwrap();
    }
    db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,event_types,fire_count,condition_state,last_error,last_task_id) VALUES($1,$2,$3,$4,$5,'old','event','once',ARRAY['comment.created'],2,'old state','old failure',$6)", &[&rule,&ws,&issue,&agent,&owner,&running]).await.unwrap();
    db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,system_rule) VALUES($1,$2,$3,$4,$5,'system','event','once','child_done')", &[&system,&ws,&issue,&agent,&owner]).await.unwrap();
    db.execute("INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload) VALUES(gen_random_uuid(),$1,1,'old','comment.created','{}'::jsonb)", &[&rule]).await.unwrap();
    for (id, status, started) in [
        (queued, "queued", false),
        (deferred, "deferred", false),
        (running, "running", true),
    ] {
        db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,context,started_at) VALUES($1,$2,$3,$4,$5,$6,$7)",
            &[&id,&agent,&runtime,&issue,&status,&json!({"wakeup_id":rule}),&started.then(chrono::Utc::now)]).await.unwrap();
    }
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,completed_at) VALUES($1,$2,$3,$4,'completed',now())", &[&source,&agent,&runtime,&issue]).await.unwrap();
    let sink = Arc::new(Sink::default());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()).with_issue_event_sink(sink.clone()),
    );
    let token = |user: Uuid| {
        encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
    };
    let body = json!({"agent_id":agent,"instruction":" replacement ","kind":"event","event_types":["task.completed"],"filter_task_id":source,"mode":"once"});
    for (w, i, r, b, expected) in [
        (ws, issue, "bad-id", body.clone(), StatusCode::BAD_REQUEST),
        (
            ws,
            other_issue,
            &rule.to_string(),
            body.clone(),
            StatusCode::NOT_FOUND,
        ),
        (
            ws,
            issue,
            &system.to_string(),
            body.clone(),
            StatusCode::FORBIDDEN,
        ),
        (
            ws,
            issue,
            &rule.to_string(),
            json!({"agent_id":agent,"kind":"event","instruction":"x","event_types":["task.completed"],"filter_task_id":Uuid::new_v4()}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let response = put(&app, w, i, r, &token(owner), b).await;
        assert_eq!(response.0, expected, "{response:?}");
    }
    let unauthorized = put(
        &app,
        ws,
        issue,
        &rule.to_string(),
        &token(outsider),
        body.clone(),
    )
    .await;
    assert_eq!(unauthorized.0, StatusCode::NOT_FOUND, "{unauthorized:?}");
    assert_eq!(
        db.query_one("SELECT revision FROM issue_wakeup WHERE id=$1", &[&rule])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(db.query_one("SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NOT NULL", &[&rule]).await.unwrap().get::<_,i64>(0),0);
    let response = put(&app, ws, issue, &rule.to_string(), &token(owner), body).await;
    assert_eq!(response.0, StatusCode::OK, "{response:?}");
    let rule_data = response.1;
    assert_eq!(rule_data["id"], json!(rule));
    assert_eq!(rule_data["revision"], 2);
    assert_eq!(rule_data["instruction"], "replacement");
    assert_eq!(rule_data["fire_count"], 0);
    assert_eq!(rule_data["condition_state"], "");
    assert_eq!(rule_data["last_error"], Value::Null);
    assert_eq!(rule_data["last_task_id"], Value::Null);
    assert_eq!(db.query_one("SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND revision=1 AND processed_at IS NOT NULL", &[&rule]).await.unwrap().get::<_,i64>(0),1);
    let receipt = db.query_one("SELECT revision,event_type,processed_at FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND revision=2", &[&rule]).await.unwrap();
    assert_eq!(receipt.get::<_, i64>(0), 2);
    assert_eq!(receipt.get::<_, String>(1), "task.completed");
    assert!(
        receipt
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>(2)
            .is_none()
    );
    assert_eq!(rule_data["enabled"], false); // consumed once by terminal registration snapshot
    for (id, expected) in [
        (queued, "cancelled"),
        (deferred, "cancelled"),
        (running, "running"),
    ] {
        assert_eq!(
            db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&id])
                .await
                .unwrap()
                .get::<_, String>(0),
            expected
        );
    }
    assert_eq!(sink.0.lock().unwrap().len(), 2);
    // A checks_finished condition must baseline the current snapshot on the
    // replacement revision, rather than instantly firing a stale green check.
    let condition = json!({"agent_id":agent,"instruction":"wait for CI","kind":"event","condition":{"type":"pull_request","event":"checks_finished"}});
    let updated = put(&app, ws, issue, &rule.to_string(), &token(owner), condition).await;
    assert_eq!(updated.0, StatusCode::OK, "{updated:?}");
    assert_eq!(updated.1["revision"], 3);
    assert_eq!(
        updated.1["condition"],
        json!({"type":"pull_request","event":"checks_finished"})
    );
    assert!(updated.1["next_fire_at"].is_string());
    assert!(
        updated.1["last_error"]
            .as_str()
            .unwrap()
            .starts_with("Condition pending: ")
    );
    let status_condition = json!({"agent_id":agent,"instruction":"wait for review","kind":"event","condition":{"type":"issue_field","field":"status","value":"in_review"}});
    let updated = put(
        &app,
        ws,
        issue,
        &rule.to_string(),
        &token(owner),
        status_condition,
    )
    .await;
    assert_eq!(updated.0, StatusCode::OK, "{updated:?}");
    assert_eq!(updated.1["revision"], 4);
    assert_eq!(updated.1["event_types"], json!(["issue.status_changed"]));
    assert_eq!(updated.1["condition_state"], "");
    assert_eq!(updated.1["last_error"], Value::Null);
}
