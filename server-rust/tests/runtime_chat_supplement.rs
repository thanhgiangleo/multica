//! Human supplement lifecycle on disposable PostgreSQL; no provider is executed.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Config, NoTls};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tower::ServiceExt;
use uuid::Uuid;
const SECRET: &[u8] = b"zbsi67-supplement-synthetic-only";
fn jwt(user: Uuid) -> String {
    encode(&Header::default(),&json!({"sub":user,"exp":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(SECRET)).unwrap()
}
struct Fixture {
    pool: Arc<Pool>,
    app: axum::Router,
    workspace: Uuid,
    other_workspace: Uuid,
    author: Uuid,
    other: Uuid,
    agent: Uuid,
    issue: Uuid,
    task: Uuid,
}
impl Fixture {
    async fn new() -> Self {
        let config =
            Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(config.get_dbname(), Some("multica_zbsi67"));
        assert!(
            config
                .get_hosts()
                .iter()
                .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
        );
        assert!(config.get_ports().iter().all(|p| *p != 5432));
        let pool = Arc::new(
            Pool::builder(Manager::new(config, NoTls))
                .max_size(16)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let [
            workspace,
            other_workspace,
            author,
            other,
            agent,
            issue,
            task,
            runtime,
            trigger,
        ]: [Uuid; 9] = std::array::from_fn(|_| Uuid::new_v4());
        for user in [author, other] {
            db.execute(
                "INSERT INTO \"user\"(id,name,email) VALUES ($1,'Supplement',$2)",
                &[&user, &format!("supplement-{user}@invalid.example")],
            )
            .await
            .unwrap();
        }
        for ws in [workspace, other_workspace] {
            db.execute("INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,'Supplement',$2,'SUP')", &[&ws,&format!("supplement-{ws}")]).await.unwrap();
        }
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member')",
            &[&workspace, &author, &other],
        )
        .await
        .unwrap();
        db.execute("INSERT INTO agent_runtime(id,workspace_id,daemon_id,name,runtime_mode,provider,status,owner_id) VALUES ($1,$2,'supplement-daemon','Synthetic','cloud','codex','online',$3)", &[&runtime,&workspace,&author]).await.unwrap();
        db.execute("INSERT INTO agent(id,workspace_id,name,owner_id,runtime_id,runtime_mode,permission_mode,status) VALUES ($1,$2,'Synthetic',$3,$4,'cloud','private','working')", &[&agent,&workspace,&author,&runtime]).await.unwrap();
        db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id) VALUES ($1,$2,'Supplement','member',$3)", &[&issue,&workspace,&author]).await.unwrap();
        db.execute("INSERT INTO comment(id,issue_id,workspace_id,author_type,author_id,content) VALUES ($1,$2,$3,'member',$4,'original')", &[&trigger,&issue,&workspace,&author]).await.unwrap();
        db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,trigger_comment_id,status,started_at) VALUES ($1,$2,$3,$4,$5,'running',now())", &[&task,&agent,&runtime,&issue,&trigger]).await.unwrap();
        db.execute("INSERT INTO task_supplement_capability(task_id,workspace_id,issue_id,capability) VALUES ($1,$2,$3,'task-supplement-v1')", &[&task,&workspace,&issue]).await.unwrap();
        let app = router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            ApiState::new(pool.clone(), SECRET.to_vec()),
        );
        Self {
            pool,
            app,
            workspace,
            other_workspace,
            author,
            other,
            agent,
            issue,
            task,
        }
    }
    fn path(&self) -> String {
        format!("/api/issues/{}/tasks/{}/supplements", self.issue, self.task)
    }
    async fn post(
        &self,
        user: Uuid,
        path: &str,
        body: &str,
        workspace: Uuid,
    ) -> (StatusCode, Value) {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("authorization", format!("Bearer {}", jwt(user)))
                    .header("x-workspace-id", workspace.to_string())
                    .body(Body::from(body.to_owned()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            if bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&bytes).unwrap()
            },
        )
    }
    async fn post_with_token(
        &self,
        token: &str,
        cookie: bool,
        csrf: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder()
            .method("POST")
            .uri(self.path())
            .header("x-workspace-id", self.workspace.to_string());
        builder = if cookie {
            builder.header("cookie", format!("multica_auth={token}"))
        } else {
            builder.header("authorization", format!("Bearer {token}"))
        };
        if let Some(csrf) = csrf {
            builder = builder.header("x-csrf-token", csrf);
        }
        let response = self
            .app
            .clone()
            .oneshot(
                builder
                    .body(Body::from(
                        json!({"content":"check","client_request_id":Uuid::new_v4()}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }
    async fn submit(&self, author: Uuid, request: Uuid) -> (StatusCode, Value) {
        self.post(
            author,
            &self.path(),
            &json!({"content":"change scope", "client_request_id":request}).to_string(),
            self.workspace,
        )
        .await
    }
    async fn receipt_count(&self) -> i64 {
        self.pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT count(*) FROM task_supplement WHERE task_id=$1",
                &[&self.task],
            )
            .await
            .unwrap()
            .get(0)
    }
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn create_idempotency_concurrent_delivery_and_retry() {
    let f = Fixture::new().await;
    let request = Uuid::new_v4();
    let before: i64 = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&f.issue])
        .await
        .unwrap()
        .get(0);
    let (first, second) = tokio::join!(f.submit(f.author, request), f.submit(f.author, request));
    let (created, replayed) = if first.0 == StatusCode::CREATED {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(created.0, StatusCode::CREATED, "{}", created.1);
    assert_eq!(replayed.0, StatusCode::OK, "{}", replayed.1);
    let comment = created.1["id"].as_str().unwrap();
    assert_eq!(created.1["id"], replayed.1["id"]);
    assert_eq!(created.1["content"], "change scope");
    assert_eq!(
        created.1["parent_id"],
        f.pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT trigger_comment_id FROM agent_task_queue WHERE id=$1",
                &[&f.task]
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0)
            .to_string()
    );
    assert_eq!(created.1["supplement_task_id"], f.task.to_string());
    assert_eq!(created.1["supplement_status"], "pending");
    assert_eq!(created.1["supplements"][0]["agent_id"], f.agent.to_string());
    assert_eq!(created.1["issue_revision"], before + 1);
    assert_eq!(f.receipt_count().await, 1);
    let db = f.pool.get().await.unwrap();
    let revision: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&f.issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(revision, before + 1);
    let comment_id = Uuid::parse_str(comment).unwrap();
    let stored=db.query_one("SELECT s.client_request_id,s.status,c.parent_id,c.author_type FROM task_supplement s JOIN comment c ON c.id=s.comment_id WHERE s.comment_id=$1", &[&comment_id]).await.unwrap();
    assert_eq!(stored.get::<_, Uuid>("client_request_id"), request);
    assert_eq!(stored.get::<_, String>("status"), "pending");
    assert_eq!(stored.get::<_, String>("author_type"), "member");
    let retry = format!("{}/{comment}/retry", f.path());
    assert_eq!(
        f.post(f.author, &retry, "", f.workspace).await.1["supplement_status"],
        "pending"
    );
    db.execute(
        "UPDATE task_supplement SET status='failed',failure_reason='timeout' WHERE comment_id=$1",
        &[&comment_id],
    )
    .await
    .unwrap();
    let retried = f.post(f.author, &retry, "", f.workspace).await;
    assert_eq!(retried.0, StatusCode::OK, "{}", retried.1);
    assert_eq!(retried.1["supplement_status"], "pending");
    assert!(retried.1.get("supplement_failure_reason").is_none());
    assert_eq!(
        f.post(f.author, &retry, "", f.workspace).await.0,
        StatusCode::OK
    );
    db.execute(
        "UPDATE task_supplement SET status='delivered',delivered_at=now() WHERE comment_id=$1",
        &[&comment_id],
    )
    .await
    .unwrap();
    assert_eq!(
        f.post(f.author, &retry, "", f.workspace).await.1["code"],
        "task_supplement_not_retryable"
    );
    db.execute(
        "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1",
        &[&f.task],
    )
    .await
    .unwrap();
    assert_eq!(f.submit(f.author, request).await.0, StatusCode::OK);
    assert_eq!(
        f.submit(f.author, Uuid::new_v4()).await.1["code"],
        "task_supplement_turn_ended"
    );
    assert_eq!(f.receipt_count().await, 1);
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn human_workspace_invocation_capability_validation_and_terminal_fences() {
    let f = Fixture::new().await;
    let request = Uuid::new_v4();
    assert_eq!(
        f.post(f.other, &f.path(), "{}", f.workspace).await.1["code"],
        "invocation_not_allowed"
    );
    assert_eq!(
        f.post(f.author, &f.path(), "{}", f.other_workspace).await.0,
        StatusCode::NOT_FOUND
    );
    let db = f.pool.get().await.unwrap();
    for body in ["[]", "{bad", r#"{"content":false}"#] {
        assert_eq!(
            f.post(f.author, &f.path(), body, f.workspace).await.1["error"],
            "invalid request body"
        );
    }
    assert_eq!(
        f.post(f.author, &f.path(), r#"{"content":"  "}"#, f.workspace)
            .await
            .1["error"],
        "content is required"
    );
    assert_eq!(
        f.post(
            f.author,
            &f.path(),
            r#"{"content":"ok","client_request_id":"bad"}"#,
            f.workspace
        )
        .await
        .1["error"],
        "invalid client_request_id"
    );
    db.execute(
        "DELETE FROM task_supplement_capability WHERE task_id=$1",
        &[&f.task],
    )
    .await
    .unwrap();
    assert_eq!(
        f.submit(f.author, request).await.1["code"],
        "task_supplement_unsupported"
    );
    db.execute("INSERT INTO task_supplement_capability(task_id,workspace_id,issue_id,capability) VALUES ($1,$2,$3,'task-supplement-v1')", &[&f.task,&f.workspace,&f.issue]).await.unwrap();
    db.execute(
        "UPDATE agent_task_queue SET status='failed',completed_at=now() WHERE id=$1",
        &[&f.task],
    )
    .await
    .unwrap();
    assert_eq!(
        f.submit(f.author, request).await.1["code"],
        "task_supplement_turn_ended"
    );
    assert_eq!(f.receipt_count().await, 0);
    db.execute(
        "UPDATE agent_task_queue SET status='running',completed_at=NULL WHERE id=$1",
        &[&f.task],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&f.agent, &f.other],
    )
    .await
    .unwrap();
    assert_eq!(
        f.submit(f.author, request).await.1["code"],
        "invocation_not_allowed"
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&f.workspace, &f.author],
    )
    .await
    .unwrap();
    assert_eq!(f.submit(f.author, request).await.0, StatusCode::NOT_FOUND);
}

include!("runtime_chat_support/human_supplement_cases.inc.rs");
