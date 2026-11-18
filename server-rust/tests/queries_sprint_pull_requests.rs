//! PR read tests use only owned disposable PostgreSQL and a queue recorder.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{
        ApiState,
        queries_sprint_pull_requests::{PullRequestRefresh, PullRequestView, RefreshAdmission},
    },
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

const SECRET: &[u8] = b"synthetic-queries-pr-read";
fn token(user: Uuid) -> String {
    encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(SECRET)).unwrap()
}
struct Recorder {
    enabled: bool,
    admission: RefreshAdmission,
    views: Mutex<Vec<PullRequestView>>,
}
impl PullRequestRefresh for Recorder {
    fn enabled(&self) -> bool {
        self.enabled
    }
    fn maybe_enqueue_on_view(&self, view: PullRequestView) -> RefreshAdmission {
        self.views.lock().unwrap().push(view);
        self.admission
    }
}
fn app(pool: Arc<Pool>, refresh: Option<Arc<Recorder>>) -> axum::Router {
    let mut state = ApiState::new(pool.clone(), SECRET.to_vec());
    if let Some(refresh) = refresh {
        state = state.with_pull_request_refresh(refresh);
    }
    router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "queries-pr".into(),
        )),
        state,
    )
}
struct Fixture {
    pool: Arc<Pool>,
    app: axum::Router,
    ws: Uuid,
    foreign: Uuid,
    user: Uuid,
    issue: Uuid,
    other: Uuid,
    connection: Uuid,
    foreign_connection: Uuid,
    recorder: Arc<Recorder>,
}
impl Fixture {
    async fn new(enabled: bool) -> Self {
        let config =
            Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
        assert!(
            config
                .get_hosts()
                .iter()
                .all(|host| matches!(host,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
        );
        assert!(config.get_ports().iter().all(|p| *p != 5432 && *p != 0));
        let pool = Arc::new(
            Pool::builder(Manager::new(config, NoTls))
                .max_size(8)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let nonce = Uuid::new_v4();
        let user: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES('PR reader',$1) RETURNING id",
                &[&format!("pr-{nonce}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        let mut spaces = Vec::new();
        let mut issues = Vec::new();
        let mut connections = Vec::new();
        for kind in ["local", "foreign"] {
            let ws: Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES('Pull requests',$1,'PRS') RETURNING id", &[&format!("pr-{kind}-{nonce}")]).await.unwrap().get(0);
            db.execute(
                "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
                &[&ws, &user],
            )
            .await
            .unwrap();
            let issue: Uuid=db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES($1,1,'PR fixture','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
            let connection: Uuid=db.query_one("INSERT INTO vcs_connection(workspace_id,instance_url,account_login,access_token_encrypted,webhook_secret_encrypted) VALUES($1,'https://synthetic.invalid','test','synthetic-not-a-secret','synthetic-not-a-secret') RETURNING id", &[&ws]).await.unwrap().get(0);
            spaces.push(ws);
            issues.push(issue);
            connections.push(connection);
        }
        let recorder = Arc::new(Recorder {
            enabled,
            admission: RefreshAdmission::Queued,
            views: Mutex::new(Vec::new()),
        });
        let app = app(pool.clone(), Some(recorder.clone()));
        Self {
            pool,
            app,
            ws: spaces[0],
            foreign: spaces[1],
            user,
            issue: issues[0],
            other: issues[1],
            connection: connections[0],
            foreign_connection: connections[1],
            recorder,
        }
    }
    async fn get_app(
        &self,
        app: axum::Router,
        raw: &str,
        ws: Uuid,
        jwt: Option<String>,
    ) -> (StatusCode, Value) {
        let mut req =
            Request::builder().uri(format!("/api/issues/{raw}/pull-requests?workspace_id={ws}"));
        if let Some(jwt) = jwt {
            req = req.header("authorization", format!("Bearer {jwt}"));
        }
        let response = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    }
    async fn get(&self) -> (StatusCode, Value) {
        self.get_app(
            self.app.clone(),
            &self.issue.to_string(),
            self.ws,
            Some(token(self.user)),
        )
        .await
    }
    async fn github(&self, number: i32, ws: Uuid, issue: Uuid, state: &str, created: &str) -> Uuid {
        let db = self.pool.get().await.unwrap();
        let id: Uuid=db.query_one("INSERT INTO github_pull_request(workspace_id,installation_id,repo_owner,repo_name,pr_number,title,state,html_url,branch,head_sha,pr_created_at,pr_updated_at) VALUES($1,123,'synthetic-owner','repo',$2,'fix PRS-1',$3,'https://github.invalid/pull/1','feat/PRS-1','current',$4::text::timestamptz,$4::text::timestamptz) RETURNING id", &[&ws,&number,&state,&created]).await.unwrap().get(0);
        db.execute(
            "INSERT INTO issue_pull_request(issue_id,pull_request_id) VALUES($1,$2)",
            &[&issue, &id],
        )
        .await
        .unwrap();
        id
    }
    async fn vcs(
        &self,
        number: i32,
        ws: Uuid,
        issue: Uuid,
        connection: Uuid,
        state: &str,
        created: &str,
    ) -> Uuid {
        let db = self.pool.get().await.unwrap();
        let id: Uuid=db.query_one("INSERT INTO vcs_pull_request(workspace_id,connection_id,repo_owner,repo_name,pr_number,title,state,html_url,head_sha,pr_created_at,pr_updated_at) VALUES($1,$2,'synthetic-owner','repo',$3,'PR card',$4,'https://forgejo.invalid/pull/1','current',$5::text::timestamptz,$5::text::timestamptz) RETURNING id", &[&ws,&connection,&number,&state,&created]).await.unwrap().get(0);
        db.execute(
            "INSERT INTO issue_vcs_pull_request(issue_id,pull_request_id) VALUES($1,$2)",
            &[&issue, &id],
        )
        .await
        .unwrap();
        id
    }
    async fn setting(&self, value: Value) {
        self.pool
            .get()
            .await
            .unwrap()
            .execute(
                "UPDATE workspace SET settings=$2 WHERE id=$1",
                &[&self.ws, &value],
            )
            .await
            .unwrap();
    }
    async fn status(&self, status: &str, triage: Option<&str>) {
        self.pool
            .get()
            .await
            .unwrap()
            .execute(
                "UPDATE issue SET status=$2,triage_state=$3 WHERE id=$1",
                &[&self.issue, &status, &triage],
            )
            .await
            .unwrap();
    }
    async fn decision(&self, expected: &str) -> Value {
        let (s, v) = self.get().await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert_eq!(v["auto_complete"]["state"], expected, "{v}");
        v["auto_complete"].clone()
    }
}
#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_frozen_go_sql_aggregation_difference() {
    let f = Fixture::new(true).await;
    let db = f.pool.get().await.unwrap();
    for number in 1..=12 {
        let g = f
            .github(
                number,
                f.ws,
                f.issue,
                "open",
                &format!("2026-11-01T00:00:{number:02}Z"),
            )
            .await;
        f.vcs(
            number,
            f.ws,
            f.issue,
            f.connection,
            "open",
            &format!("2026-11-01T00:00:{number:02}Z"),
        )
        .await;
        db.execute("UPDATE github_pull_request SET snapshot_head_sha='current',snapshot_fetched_at=now() WHERE id=$1", &[&g]).await.unwrap();
        for (ordinal, conclusion) in [
            (1, Some("failure")),
            (2, Some("cancelled")),
            (3, Some("success")),
            (4, Some("neutral")),
            (5, Some("skipped")),
            (6, None),
            (7, Some("unknown")),
        ] {
            db.execute("INSERT INTO github_pull_request_check_run(pr_id,head_sha,ordinal,name,status,conclusion) VALUES($1,'current',$2,$3,'completed',$4)", &[&g,&ordinal,&format!("check-{ordinal}"),&conclusion]).await.unwrap();
        }
        db.execute("INSERT INTO github_pull_request_check_run(pr_id,head_sha,ordinal,name,status,conclusion) VALUES($1,'old',8,'exclude old','completed','failure')", &[&g]).await.unwrap();
    }
    for (context, sha, state) in [
        ("pass", "current", "passed"),
        ("fail", "current", "failed"),
        ("wait", "current", "pending"),
        ("old", "old", "failed"),
    ] {
        db.execute(
            "INSERT INTO vcs_commit_status(connection_id,sha,context,state) VALUES($1,$2,$3,$4)",
            &[&f.connection, &sha, &context, &state],
        )
        .await
        .unwrap();
    }
    for (oracle, owned) in [
        (
            include_str!("queries_sprint_pull_requests_frozen_github.sql"),
            include_str!("../src/api/queries_sprint_pull_requests.sql"),
        ),
        (
            include_str!("queries_sprint_pull_requests_frozen_vcs.sql"),
            include_str!("../src/api/queries_sprint_pull_requests_vcs.sql"),
        ),
    ] {
        let oracle = db
            .query(
                &format!("SELECT to_jsonb(t) FROM ({oracle}) t"),
                &[&f.issue],
            )
            .await
            .unwrap();
        let owned = db
            .query(
                &format!("SELECT to_jsonb(t) FROM ({owned}) t"),
                &[
                    &f.issue,
                    &f.ws,
                    &false,
                    &None::<chrono::DateTime<chrono::Utc>>,
                    &None::<Uuid>,
                ],
            )
            .await
            .unwrap();
        assert_eq!(oracle.len(), 12);
        assert_eq!(oracle.len(), owned.len());
        for (oracle, owned) in oracle.iter().zip(owned.iter()) {
            let oracle: Value = oracle.get(0);
            let owned: Value = owned.get(0);
            for (key, value) in oracle.as_object().unwrap() {
                let target = if key == "checks_pending" {
                    "checks_running"
                } else {
                    key
                };
                if key == "failed_check_names" {
                    let mut a: Vec<_> = value
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|s| s.as_str().unwrap())
                        .collect();
                    let mut b: Vec<_> = owned[target]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|s| s.as_str().unwrap())
                        .collect();
                    a.sort();
                    b.sort();
                    assert_eq!(a, b);
                } else {
                    assert_eq!(value, &owned[target], "{key}");
                }
            }
        }
    }
    let (s, b) = f.get().await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["pull_requests"].as_array().unwrap().len(), 24);
}

fn pr(body: &Value, id: Uuid) -> &Value {
    body["pull_requests"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["id"] == id.to_string())
        .unwrap()
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_auth_workspace_identifiers_and_empty_shape() {
    let f = Fixture::new(true).await;
    assert_eq!(
        f.get_app(f.app.clone(), &f.issue.to_string(), f.ws, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    for raw in [
        f.other.to_string(),
        "bad".into(),
        "PRS-0".into(),
        "PRS-2147483648".into(),
        "X-1".into(),
        Uuid::new_v4().to_string(),
    ] {
        assert_eq!(
            f.get_app(f.app.clone(), &raw, f.ws, Some(token(f.user)))
                .await
                .0,
            StatusCode::NOT_FOUND,
            "{raw}"
        );
    }
    let outsider: Uuid = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Outsider',$1) RETURNING id",
            &[&format!("pr-outsider-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        f.get_app(
            f.app.clone(),
            &f.issue.to_string(),
            f.ws,
            Some(token(outsider))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for raw in [f.issue.to_string(), "prs-1".into()] {
        let (s, v) = f
            .get_app(f.app.clone(), &raw, f.ws, Some(token(f.user)))
            .await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert_eq!(
            v,
            json!({"pull_requests":[],"auto_complete":{"state":"none","pull_request_ids":[],"issue_disabled":false,"workspace_enabled":true,"target_status":"done"}})
        );
    }
    assert!(f.recorder.views.lock().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_provider_merge_order_scope_and_link_sources() {
    let f = Fixture::new(true).await;
    let g = f
        .github(1, f.ws, f.issue, "open", "2026-11-01T00:00:00.100Z")
        .await;
    let v = f
        .vcs(
            2,
            f.ws,
            f.issue,
            f.connection,
            "draft",
            "2026-11-01T00:00:00.900Z",
        )
        .await;
    let newest = f
        .vcs(
            3,
            f.ws,
            f.issue,
            f.connection,
            "merged",
            "2026-11-01T00:00:01Z",
        )
        .await;
    let foreign = f
        .github(4, f.foreign, f.issue, "merged", "2026-11-02T00:00:00Z")
        .await;
    f.vcs(
        5,
        f.foreign,
        f.issue,
        f.foreign_connection,
        "merged",
        "2026-11-02T00:00:00Z",
    )
    .await;
    f.vcs(
        6,
        f.ws,
        f.issue,
        f.foreign_connection,
        "merged",
        "2026-11-02T00:00:00Z",
    )
    .await;
    f.github(7, f.ws, f.other, "merged", "2026-11-02T00:00:00Z")
        .await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "UPDATE issue_vcs_pull_request SET linked_by_type='member' WHERE pull_request_id=$1",
        &[&newest],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE vcs_pull_request SET branch='feat/prs-1' WHERE id=$1",
        &[&v],
    )
    .await
    .unwrap();
    let (s, b) = f.get().await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(
        b["pull_requests"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["id"].clone())
            .collect::<Vec<_>>(),
        vec![json!(newest), json!(g), json!(v)]
    );
    assert_eq!(pr(&b, g)["link_source"], "title");
    assert_eq!(pr(&b, v)["link_source"], "branch");
    assert_eq!(pr(&b, newest)["link_source"], "manual");
    assert!(pr(&b, v).get("snapshot_available").is_none());
    assert_eq!(pr(&b, g)["snapshot_available"], false);
    assert_eq!(b["auto_complete"]["pull_request_ids"], json!([g, v]));
    {
        let views = f.recorder.views.lock().unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].pull_request_id, g);
        assert_ne!(views[0].pull_request_id, foreign);
        assert_eq!(views[0].workspace_id, f.ws);
        assert_eq!(views[0].issue_id, f.issue);
        assert_eq!(views[0].installation_id, 123);
        assert!(!views[0].current_snapshot);
    }
    db.execute(
        "UPDATE github_pull_request SET title='PRS-10',branch='_PRS-1' WHERE id=$1",
        &[&g],
    )
    .await
    .unwrap();
    assert_eq!(pr(&f.get().await.1, g)["link_source"], "auto");
    db.execute("UPDATE workspace SET issue_prefix='' WHERE id=$1", &[&f.ws])
        .await
        .unwrap();
    db.execute(
        "UPDATE github_pull_request SET title='PUL-1' WHERE id=$1",
        &[&g],
    )
    .await
    .unwrap();
    assert_eq!(
        f.get_app(f.app.clone(), "PUL-1", f.ws, Some(token(f.user)))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(pr(&f.get().await.1, g)["link_source"], "title");
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_github_snapshot_checks_head_and_configuration() {
    let f = Fixture::new(true).await;
    let g = f
        .github(1, f.ws, f.issue, "open", "2026-11-01T00:00:00Z")
        .await;
    let db = f.pool.get().await.unwrap();
    db.execute("UPDATE github_pull_request SET snapshot_head_sha='current',snapshot_fetched_at=now()-interval '31 minutes',api_mergeable='MERGEABLE',api_merge_state_status='CLEAN',checks_rollup_state='UNKNOWN',mergeable_state='clean',additions=8,deletions=3,changed_files=2 WHERE id=$1", &[&g]).await.unwrap();
    for (ordinal, head, status, conclusion) in [
        (1, "current", "completed", Some("failure")),
        (2, "current", "completed", Some("success")),
        (3, "current", "in_progress", None),
        (4, "old", "completed", Some("failure")),
        (5, "current", "completed", Some("neutral")),
        (6, "current", "completed", Some("skipped")),
    ] {
        db.execute("INSERT INTO github_pull_request_check_run(pr_id,head_sha,ordinal,name,status,conclusion) VALUES($1,$2,$3,$4,$5,$6)", &[&g,&head,&ordinal,&format!("check-{ordinal}"),&status,&conclusion]).await.unwrap();
    }
    let (s, b) = f.get().await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let p = pr(&b, g);
    for (key, want) in [
        ("snapshot_available", json!(true)),
        ("snapshot_stale", json!(true)),
        ("checks_total", json!(5)),
        ("checks_failed", json!(1)),
        ("checks_passed", json!(3)),
        ("checks_running", json!(1)),
        ("checks_pending", json!(1)),
        ("checks_conclusion", json!("failed")),
        ("failed_check_names", json!(["check-1"])),
        ("mergeable", json!("mergeable")),
        ("merge_state_status", json!("clean")),
        ("checks_rollup", json!("unknown")),
        ("additions", json!(8)),
    ] {
        assert_eq!(p[key], want, "{key}: {p}");
    }
    let view = f.recorder.views.lock().unwrap()[0].clone();
    assert!(view.current_snapshot);
    assert!(view.snapshot_fetched_at.is_some());
    db.execute(
        "UPDATE github_pull_request SET state='merged' WHERE id=$1",
        &[&g],
    )
    .await
    .unwrap();
    assert_eq!(pr(&f.get().await.1, g)["snapshot_stale"], false);
    for query in [
        "snapshot_head_sha='old'",
        "snapshot_fetched_at=NULL",
        "snapshot_head_sha=''",
        "head_sha=''",
        "snapshot_head_sha='current',head_sha='current',snapshot_fetched_at=now()+interval '1 hour'",
    ] {
        db.execute(
            &format!("UPDATE github_pull_request SET {query} WHERE id=$1"),
            &[&g],
        )
        .await
        .unwrap();
        let b = f.get().await.1;
        let p = pr(&b, g);
        if query.contains("interval") {
            assert_eq!(p["snapshot_available"], true);
            assert_eq!(p["snapshot_stale"], false);
        } else {
            assert_eq!(p["snapshot_available"], false);
            assert_eq!(p["checks_total"], 0);
            assert_eq!(p["failed_check_names"], json!([]));
            assert_eq!(p["snapshot_fetched_at"], Value::Null);
        }
    }
    let disabled = Arc::new(Recorder {
        enabled: false,
        admission: RefreshAdmission::Unavailable,
        views: Mutex::new(Vec::new()),
    });
    let (s, b) = f
        .get_app(
            app(f.pool.clone(), Some(disabled.clone())),
            &f.issue.to_string(),
            f.ws,
            Some(token(f.user)),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(pr(&b, g)["snapshot_available"], false);
    assert_eq!(pr(&b, g)["mergeable_state"], "clean");
    assert!(disabled.views.lock().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_vcs_current_head_counts_and_nulls() {
    let f = Fixture::new(false).await;
    let v = f
        .vcs(
            1,
            f.ws,
            f.issue,
            f.connection,
            "open",
            "2026-11-01T00:00:00Z",
        )
        .await;
    let db = f.pool.get().await.unwrap();
    for (context, sha, state) in [
        ("pass", "current", "passed"),
        ("wait", "current", "pending"),
        ("fail", "current", "failed"),
        ("old", "old", "failed"),
    ] {
        db.execute(
            "INSERT INTO vcs_commit_status(connection_id,sha,context,state) VALUES($1,$2,$3,$4)",
            &[&f.connection, &sha, &context, &state],
        )
        .await
        .unwrap();
    }
    for (delete, want, total) in [
        ("missing", Some("failed"), 3),
        ("fail", Some("pending"), 2),
        ("wait", Some("passed"), 1),
        ("pass", None, 0),
    ] {
        db.execute(
            "DELETE FROM vcs_commit_status WHERE connection_id=$1 AND context=$2",
            &[&f.connection, &delete],
        )
        .await
        .unwrap();
        let (s, b) = f.get().await;
        assert_eq!(s, StatusCode::OK, "{b}");
        let p = pr(&b, v);
        assert_eq!(p["checks_total"], total);
        assert_eq!(p["checks_conclusion"], json!(want));
        assert_eq!(p["failed_check_names"], json!([]));
        for key in [
            "mergeable_state",
            "mergeable",
            "merge_state_status",
            "checks_rollup",
            "snapshot_fetched_at",
            "author_login",
            "author_avatar_url",
            "merged_at",
            "closed_at",
            "branch",
        ] {
            assert!(p[key].is_null(), "{key}: {p}");
        }
    }
    assert!(f.recorder.views.lock().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_auto_complete_guard_precedence_and_settings_matrix() {
    let f = Fixture::new(false).await;
    let g = f
        .github(1, f.ws, f.issue, "merged", "2026-11-01T00:00:00Z")
        .await;
    let v = f
        .vcs(
            2,
            f.ws,
            f.issue,
            f.connection,
            "merged",
            "2026-11-01T00:00:00Z",
        )
        .await;
    assert_eq!(
        f.decision("all_merged").await["pull_request_ids"],
        json!([g, v])
    );
    let db = f.pool.get().await.unwrap();
    db.execute(
        "UPDATE github_pull_request SET state='closed' WHERE id=$1",
        &[&g],
    )
    .await
    .unwrap();
    assert_eq!(
        f.decision("not_merged").await["pull_request_ids"],
        json!([g])
    );
    db.execute(
        "UPDATE vcs_pull_request SET state='draft' WHERE id=$1",
        &[&v],
    )
    .await
    .unwrap();
    assert_eq!(f.decision("waiting").await["pull_request_ids"], json!([v]));
    db.execute("INSERT INTO issue_pr_automation(issue_id,workspace_id,auto_complete_disabled) VALUES($1,$2,true)", &[&f.issue,&f.ws]).await.unwrap();
    f.decision("issue_disabled").await;
    for setting in [
        json!({"pr_merge_status":"none"}),
        json!({"pr_merge_status":"blocked"}),
        json!({"pr_merge_status":"todo"}),
        json!({"pr_merge_status":"cancelled"}),
        json!({"pr_merge_status":"unknown"}),
        json!({"pr_merge_status":true}),
        json!({"pr_auto_complete_enabled":false}),
    ] {
        f.setting(setting).await;
        let d = f.decision("workspace_disabled").await;
        assert_eq!(d["issue_disabled"], true);
        assert_eq!(d["target_status"], "none");
    }
    f.status("todo", Some("pending")).await;
    f.decision("triage").await;
    f.status("done", Some("pending")).await;
    f.decision("terminal").await;
    f.status("cancelled", None).await;
    f.decision("terminal").await;
    f.status("in_review", None).await;
    f.setting(json!({"pr_merge_status":" In_Review "})).await;
    db.execute(
        "UPDATE issue_pr_automation SET auto_complete_disabled=false WHERE issue_id=$1",
        &[&f.issue],
    )
    .await
    .unwrap();
    assert_eq!(f.decision("at_target").await["target_status"], "in_review");
    f.status("todo", None).await;
    f.setting(json!({"pr_merge_status":null})).await;
    f.decision("waiting").await;
    db.execute("UPDATE issue_pr_automation SET workspace_id=$2,auto_complete_disabled=true WHERE issue_id=$1", &[&f.issue,&f.foreign]).await.unwrap();
    assert_eq!(f.decision("waiting").await["issue_disabled"], false);
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_custom_status_archival_and_project_workflow() {
    let f = Fixture::new(false).await;
    f.github(1, f.ws, f.issue, "merged", "2026-11-01T00:00:00Z")
        .await;
    let db = f.pool.get().await.unwrap();
    for (key, category) in [
        ("custom_finish", "done"),
        ("custom_work", "started"),
        ("custom_closed", "closed"),
    ] {
        db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,position) VALUES($1,$2,$2,$3,'#123456',1)", &[&f.ws,&key,&category]).await.unwrap();
    }
    f.status("custom_finish", None).await;
    f.decision("terminal").await;
    f.status("custom_closed", None).await;
    f.decision("terminal").await;
    f.status("todo", None).await;
    f.setting(json!({"pr_merge_status":"custom_work"})).await;
    f.decision("all_merged").await;
    db.execute(
        "UPDATE issue_status SET archived_at=now() WHERE workspace_id=$1 AND key='custom_work'",
        &[&f.ws],
    )
    .await
    .unwrap();
    f.decision("workspace_disabled").await;
    f.setting(json!({"pr_merge_status":"custom_finish"})).await;
    let workflow: Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'PR flow','todo','[{\"status_key\":\"todo\"}]') RETURNING id", &[&f.ws]).await.unwrap().get(0);
    let project: Uuid=db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'PR project',$2) RETURNING id", &[&f.ws,&workflow]).await.unwrap().get(0);
    db.execute(
        "UPDATE issue SET project_id=$2 WHERE id=$1",
        &[&f.issue, &project],
    )
    .await
    .unwrap();
    f.decision("not_in_workflow").await;
    db.execute("UPDATE issue_workflow SET steps='[{\"status_key\":\"todo\"},{\"status_key\":\"custom_finish\"}]' WHERE id=$1", &[&workflow]).await.unwrap();
    f.decision("all_merged").await;
    db.execute(
        "UPDATE project SET workflow_id=$2 WHERE id=$1",
        &[&project, &Uuid::new_v4()],
    )
    .await
    .unwrap();
    f.decision("all_merged").await;
    db.execute(
        "UPDATE project SET workflow_id=$2 WHERE id=$1",
        &[&project, &workflow],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issue_workflow SET steps='[\"not-a-step\"]' WHERE id=$1",
        &[&workflow],
    )
    .await
    .unwrap();
    assert_eq!(f.get().await.0, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_workflow_decoder_frozen_go_field_types() {
    let f = Fixture::new(false).await;
    f.github(1, f.ws, f.issue, "merged", "2026-11-01T00:00:00Z")
        .await;
    let db = f.pool.get().await.unwrap();
    let workflow: Uuid = db.query_one(
        "INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'PR JSON','todo','[]') RETURNING id",
        &[&f.ws],
    ).await.unwrap().get(0);
    let project: Uuid = db.query_one(
        "INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'PR JSON',$2) RETURNING id",
        &[&f.ws, &workflow],
    ).await.unwrap().get(0);
    db.execute(
        "UPDATE issue SET project_id=$2 WHERE id=$1",
        &[&f.issue, &project],
    )
    .await
    .unwrap();
    for (steps, expected) in [
        (
            json!([null, {"status_key":null,"handler":null,"instructions":null}]),
            StatusCode::OK,
        ),
        (
            json!([{"status_key":"done","handler":{"type":null,"id":null},"instructions":"", "unknown":17}]),
            StatusCode::OK,
        ),
        (
            json!([{"status_key":"done","handler":{"type":"agent","id":"not-a-uuid"},"next_status_key":"todo"}]),
            StatusCode::OK,
        ),
        (
            json!([{"status_key":"done","handler":5}]),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            json!([{"status_key":"done","handler":{"id":true}}]),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            json!([{"status_key":"done","instructions":[]}]),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            json!([{"status_key":"done","back_status_key":12}]),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            json!([{"status_key":42}]),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (json!([false]), StatusCode::INTERNAL_SERVER_ERROR),
    ] {
        db.execute(
            "UPDATE issue_workflow SET steps=$2 WHERE id=$1",
            &[&workflow, &steps],
        )
        .await
        .unwrap();
        let (status, body) = f.get().await;
        assert_eq!(status, expected, "steps={steps} body={body}");
        if status == StatusCode::OK {
            let expected_state = if steps
                .as_array()
                .is_some_and(|entries| entries.iter().any(|entry| entry["status_key"] == "done"))
            {
                "all_merged"
            } else {
                "not_in_workflow"
            };
            assert_eq!(
                body["auto_complete"]["state"], expected_state,
                "steps={steps}"
            );
        }
    }
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_refresh_fail_closed_and_list_boundary() {
    let f = Fixture::new(true).await;
    f.github(1, f.ws, f.issue, "open", "2026-11-01T00:00:00Z")
        .await;
    assert_eq!(
        f.get_app(
            app(f.pool.clone(), None),
            &f.issue.to_string(),
            f.ws,
            Some(token(f.user))
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    for admission in [
        RefreshAdmission::Unavailable,
        RefreshAdmission::Saturated,
        RefreshAdmission::NotNeeded,
    ] {
        let r = Arc::new(Recorder {
            enabled: true,
            admission,
            views: Mutex::new(Vec::new()),
        });
        let (s, _) = f
            .get_app(
                app(f.pool.clone(), Some(r.clone())),
                &f.issue.to_string(),
                f.ws,
                Some(token(f.user)),
            )
            .await;
        assert_eq!(
            s,
            if admission == RefreshAdmission::Unavailable {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::OK
            }
        );
        assert_eq!(r.views.lock().unwrap().len(), 1);
    }
    let db = f.pool.get().await.unwrap();
    db.execute("WITH inserted AS (INSERT INTO github_pull_request(workspace_id,installation_id,repo_owner,repo_name,pr_number,title,state,html_url,pr_created_at,pr_updated_at) SELECT $1,123,'bulk','repo',n,'bulk','merged','https://synthetic.invalid',now(),now() FROM generate_series(2,2000) n RETURNING id) INSERT INTO issue_pull_request(issue_id,pull_request_id) SELECT $2,id FROM inserted", &[&f.ws,&f.issue]).await.unwrap();
    let (s, b) = f.get().await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["pull_requests"].as_array().unwrap().len(), 2000);
    f.recorder.views.lock().unwrap().clear();
    f.github(2001, f.ws, f.issue, "open", "2026-11-01T00:00:00Z")
        .await;
    let (s, b) = f.get().await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let rows = b["pull_requests"].as_array().unwrap();
    assert_eq!(rows.len(), 2001);
    let unique: std::collections::HashSet<_> =
        rows.iter().map(|row| row["id"].as_str().unwrap()).collect();
    assert_eq!(
        unique.len(),
        rows.len(),
        "timestamp ties must not skip/repeat PRs"
    );
    assert_eq!(f.recorder.views.lock().unwrap().len(), 2001);
    assert_eq!(
        f.get_app(
            f.app.clone(),
            &f.issue.to_string(),
            f.foreign,
            Some(token(f.user))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_vcs_keyset_timestamp_ties_cross_pages() {
    let f = Fixture::new(true).await;
    let github = f
        .github(1, f.ws, f.issue, "merged", "2026-11-01T00:00:00Z")
        .await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "WITH inserted AS (INSERT INTO vcs_pull_request(workspace_id,connection_id,repo_owner,repo_name,pr_number,title,state,html_url,pr_created_at,pr_updated_at) SELECT $1,$2,'bulk','repo',n,'bulk','merged','https://synthetic.invalid','2026-11-01T00:00:00Z'::timestamptz,'2026-11-01T00:00:00Z'::timestamptz FROM generate_series(2,1030) n RETURNING id) INSERT INTO issue_vcs_pull_request(issue_id,pull_request_id) SELECT $3,id FROM inserted",
        &[&f.ws, &f.connection, &f.issue],
    )
    .await
    .unwrap();
    let (status, body) = f.get().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["pull_requests"].as_array().unwrap();
    assert_eq!(rows.len(), 1030);
    let ids: std::collections::HashSet<_> =
        rows.iter().map(|row| row["id"].as_str().unwrap()).collect();
    assert_eq!(
        ids.len(),
        rows.len(),
        "VCS keyset must not skip or repeat timestamp ties"
    );
    assert!(ids.contains(github.to_string().as_str()));
    assert_eq!(
        f.recorder.views.lock().unwrap().len(),
        1,
        "VCS rows never trigger GitHub refresh"
    );
    assert_eq!(body["auto_complete"]["state"], "all_merged");
    assert_eq!(
        body["auto_complete"]["pull_request_ids"]
            .as_array()
            .unwrap()
            .len(),
        rows.len()
    );
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_keep_historical_vcs_link_after_connection_removed() {
    let f = Fixture::new(true).await;
    let github = f
        .github(1, f.ws, f.issue, "merged", "2026-11-01T00:00:00Z")
        .await;
    let vcs = f
        .vcs(
            2,
            f.ws,
            f.issue,
            f.connection,
            "merged",
            "2026-11-01T00:00:01Z",
        )
        .await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "UPDATE vcs_pull_request SET head_sha='historical' WHERE id=$1",
        &[&vcs],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO vcs_commit_status(connection_id,sha,context,state) VALUES($1,'historical','CI','passed')",
        &[&f.connection],
    )
    .await
    .unwrap();
    // A historically orphaned PR row remains workspace-scoped even when its
    // connection is missing; do not silently omit it from the merge decision.
    // Normal provider disconnect removes linked PR rows transactionally.
    db.execute("DELETE FROM vcs_connection WHERE id=$1", &[&f.connection])
        .await
        .unwrap();
    let (status, body) = f.get().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["pull_requests"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{body}");
    assert_eq!(rows[0]["id"], vcs.to_string());
    assert_eq!(rows[0]["provider"], "forgejo");
    assert_eq!(rows[0]["checks_total"], 1);
    assert_eq!(rows[0]["checks_conclusion"], "passed");
    assert_eq!(rows[1]["id"], github.to_string());
    assert_eq!(body["auto_complete"]["state"], "all_merged");
    assert_eq!(
        body["auto_complete"]["pull_request_ids"],
        json!([github, vcs])
    );
    {
        let views = f.recorder.views.lock().unwrap();
        assert_eq!(
            views.len(),
            1,
            "orphan VCS must never enqueue GitHub refresh"
        );
        assert_eq!(views[0].pull_request_id, github);
    }
    let (foreign_status, _) = f
        .get_app(
            f.app.clone(),
            &f.issue.to_string(),
            f.foreign,
            Some(token(f.user)),
        )
        .await;
    assert_eq!(foreign_status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_reject_linked_vcs_rows_with_foreign_or_mismatched_connections() {
    let f = Fixture::new(true).await;
    let foreign = f
        .vcs(
            1,
            f.ws,
            f.issue,
            f.foreign_connection,
            "merged",
            "2026-11-01T00:00:00Z",
        )
        .await;
    let mismatch = f
        .vcs(
            2,
            f.ws,
            f.issue,
            f.connection,
            "merged",
            "2026-11-01T00:00:01Z",
        )
        .await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "UPDATE vcs_pull_request SET provider='gitea' WHERE id=$1",
        &[&mismatch],
    )
    .await
    .unwrap();
    let (status, body) = f.get().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["pull_requests"], json!([]));
    assert_eq!(body["auto_complete"]["state"], "all_merged");
    assert_eq!(
        body["auto_complete"]["pull_request_ids"],
        json!([foreign, mismatch])
    );
    assert!(f.recorder.views.lock().unwrap().is_empty());

    db.execute("DELETE FROM vcs_connection WHERE id=$1", &[&f.connection])
        .await
        .unwrap();
    let (status, body) = f.get().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["pull_requests"].as_array().unwrap().len(), 1);
    assert_eq!(body["pull_requests"][0]["id"], mismatch.to_string());
    assert_ne!(body["pull_requests"][0]["id"], foreign.to_string());
    assert_eq!(body["auto_complete"]["state"], "all_merged");
    assert_eq!(
        body["auto_complete"]["pull_request_ids"],
        json!([foreign, mismatch])
    );
    db.execute(
        "UPDATE vcs_pull_request SET state='open' WHERE id=$1",
        &[&foreign],
    )
    .await
    .unwrap();
    let (status, body) = f.get().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["pull_requests"][0]["id"], mismatch.to_string());
    assert_eq!(body["auto_complete"]["state"], "waiting");
    assert_eq!(body["auto_complete"]["pull_request_ids"], json!([foreign]));
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_hidden_decision_pages_timestamp_ties_without_leaking_cards() {
    let f = Fixture::new(true).await;
    let db = f.pool.get().await.unwrap();
    // The connection exists, but it belongs to another workspace. All links
    // affect the merge decision, none may surface as a card or trigger refresh.
    let hidden = db.query(
        "WITH inserted AS (INSERT INTO vcs_pull_request(workspace_id,connection_id,repo_owner,repo_name,pr_number,title,state,html_url,pr_created_at,pr_updated_at) SELECT $1,$2,'hidden','repo',n,'hidden','merged','https://synthetic.invalid','2026-11-01T00:00:00Z'::timestamptz,'2026-11-01T00:00:00Z'::timestamptz FROM generate_series(1,1030) n RETURNING id,pr_number) INSERT INTO issue_vcs_pull_request(issue_id,pull_request_id) SELECT $3,id FROM inserted RETURNING pull_request_id",
        &[&f.ws, &f.foreign_connection, &f.issue],
    ).await.unwrap();
    let foreign = f
        .vcs(
            1031,
            f.foreign,
            f.issue,
            f.foreign_connection,
            "open",
            "2026-11-01T00:00:00Z",
        )
        .await;
    let (status, body) = f.get().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["pull_requests"], json!([]));
    assert_eq!(body["auto_complete"]["state"], "all_merged");
    let ids = body["auto_complete"]["pull_request_ids"]
        .as_array()
        .unwrap();
    assert_eq!(ids.len(), hidden.len());
    let unique: std::collections::HashSet<_> = ids.iter().map(|id| id.as_str().unwrap()).collect();
    assert_eq!(
        unique.len(),
        hidden.len(),
        "hidden pages must not skip tied timestamps"
    );
    assert!(
        hidden
            .iter()
            .all(|row| unique.contains(row.get::<_, Uuid>(0).to_string().as_str()))
    );
    assert!(!unique.contains(foreign.to_string().as_str()));
    assert!(f.recorder.views.lock().unwrap().is_empty());
    let opened: Uuid = hidden.last().unwrap().get(0);
    db.execute(
        "UPDATE vcs_pull_request SET state='open' WHERE id=$1",
        &[&opened],
    )
    .await
    .unwrap();
    let (status, body) = f.get().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["pull_requests"], json!([]));
    assert_eq!(body["auto_complete"]["state"], "waiting");
    assert_eq!(body["auto_complete"]["pull_request_ids"], json!([opened]));
    assert!(f.recorder.views.lock().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_failed_vcs_page_does_not_enqueue_github_refresh() {
    let f = Fixture::new(true).await;
    let github = f
        .github(1, f.ws, f.issue, "open", "2026-11-01T00:00:00Z")
        .await;
    f.vcs(
        2,
        f.ws,
        f.issue,
        f.connection,
        "open",
        "2026-11-01T00:00:01Z",
    )
    .await;
    let mut locker = f.pool.get().await.unwrap();
    let tx = locker.transaction().await.unwrap();
    tx.batch_execute("LOCK TABLE vcs_pull_request IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let (status, _) = f.get().await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert!(
        f.recorder.views.lock().unwrap().is_empty(),
        "no refresh before both providers finish reading"
    );
    tx.rollback().await.unwrap();
    let (status, body) = f.get().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["pull_requests"].as_array().unwrap().len(), 2);
    let views = f.recorder.views.lock().unwrap();
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].pull_request_id, github);
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn pull_requests_concurrent_read_only_timeout_recovery_and_plans() {
    let f = Fixture::new(false).await;
    let g = f
        .github(1, f.ws, f.issue, "merged", "2026-11-01T00:00:00Z")
        .await;
    let db = f.pool.get().await.unwrap();
    let before: Value = db
        .query_one("SELECT to_jsonb(i) FROM issue i WHERE id=$1", &[&f.issue])
        .await
        .unwrap()
        .get(0);
    let (a, b) = tokio::join!(f.get(), f.get());
    assert_eq!(a.0, StatusCode::OK);
    assert_eq!(a, b);
    let after: Value = db
        .query_one("SELECT to_jsonb(i) FROM issue i WHERE id=$1", &[&f.issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
    let mut locker = f.pool.get().await.unwrap();
    let tx = locker.transaction().await.unwrap();
    tx.batch_execute("LOCK TABLE issue_pr_automation IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let start = std::time::Instant::now();
    let (s, b) = f.get().await;
    assert_eq!(s, StatusCode::GATEWAY_TIMEOUT, "{b}");
    assert!(start.elapsed().as_secs() >= 7);
    tx.rollback().await.unwrap();
    assert_eq!(f.get().await.0, StatusCode::OK);
    assert!(f.recorder.views.lock().unwrap().is_empty());
    let sql = include_str!("../src/api/queries_sprint_pull_requests.sql");
    let args: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
        &f.issue,
        &f.ws,
        &false,
        &None::<chrono::DateTime<chrono::Utc>>,
        &None::<Uuid>,
    ];
    let rows = db.query(sql, args).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, Uuid>("id"), g);
    let vcs_sql = include_str!("../src/api/queries_sprint_pull_requests_vcs.sql");
    let mut plans = Vec::new();
    for sql in [sql, vcs_sql] {
        let plan: Value = db
            .query_one(
                &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"),
                args,
            )
            .await
            .unwrap()
            .get(0);
        plans.push(plan);
    }
    if let Ok(path) = std::env::var("QUERIES_SPRINT_PULL_REQUESTS_OUTPUT") {
        std::fs::write(path,serde_json::to_vec_pretty(&json!({"fixture":"small authorized PR + cumulative fixtures; no forced planner settings","github":plans[0],"vcs":plans[1],"full_scale_proof":false})).unwrap()).unwrap();
    }
}
