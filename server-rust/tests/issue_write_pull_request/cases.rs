//! Owned PR-link and merge-automation HTTP/PostgreSQL cases for ZBSI-66.
//!
//! The real provider read model, merge decision and workspace bus are integration
//! dependencies, so this suite drives them through the typed runtime and asserts
//! exactly what the frozen Go handlers commit, in which order.
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::Pool;
use http_body_util::BodyExt;
use multica_server::{
    api::{ApiState, issue_write::*},
    health::Health,
    router_with_api,
};
use serde_json::{Value, json};
use sha2::Digest;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio_postgres::Client;
use tower::ServiceExt;
use uuid::Uuid;

#[path = "failures.rs"]
mod failures;

#[derive(Default)]
struct Sink;
impl IssueEventSink for Sink {
    fn publish_issue_deleted(&self, _: IssueDeletedEvent) {
        panic!("unexpected delete")
    }
    fn publish_issue_created(&self, _: IssueCreatedEvent) {
        panic!("unexpected create")
    }
    fn publish_issue_updated(&self, _: IssueUpdatedEvent) {
        panic!("unexpected update")
    }
    fn publish_subscriber_changed(&self, _: IssueSubscriberEvent) {
        panic!("unexpected subscriber")
    }
    fn publish_assignment_changed(&self, _: IssueAssignmentEvent) {
        panic!("unexpected assignment")
    }
    fn publish_comment_created(&self, _: IssueCommentCreatedEvent) {
        panic!("unexpected comment")
    }
    fn publish_comment_changed(&self, _: IssueCommentChangedEvent) {
        panic!("unexpected comment")
    }
    fn publish_metadata_changed(&self, _: IssueMetadataEvent) {
        panic!("unexpected metadata")
    }
    fn publish_reaction_changed(&self, _: IssueReactionEvent) {
        panic!("unexpected reaction")
    }
    fn publish_comment_resolution(&self, _: IssueCommentResolutionEvent) {
        panic!("unexpected resolution")
    }
    fn publish_wakeup_activity(&self, _: IssueWakeupActivityEvent) {
        panic!("unexpected wakeup")
    }
    fn publish_task_cancelled(&self, _: IssueTaskCancelledEvent) {
        panic!("unexpected cancellation")
    }
    fn request_wakeup_dispatch(&self, _: IssueWakeupDispatchRequest) {
        panic!("unexpected dispatch")
    }
}

#[derive(Default)]
struct Runtime {
    merges: Mutex<Vec<(Uuid, Uuid)>>,
    changed: Mutex<Vec<(Uuid, Uuid)>>,
    activity: Mutex<Vec<IssuePullRequestActivity>>,
    broken_list: AtomicBool,
    order: Mutex<Vec<&'static str>>,
    observed_links: Mutex<Vec<i64>>,
    pool: Option<Arc<Pool>>,
}

#[async_trait]
impl IssuePullRequestRuntime for Runtime {
    async fn list(&self, _: Uuid, _: Uuid) -> Result<Value, ()> {
        self.order.lock().unwrap().push("list");
        if self.broken_list.load(Ordering::SeqCst) {
            return Ok(json!({"pull_requests": "not-an-array"}));
        }
        Ok(json!({"pull_requests": [], "auto_complete": {}}))
    }
    async fn maybe_auto_complete(&self, ws: Uuid, issue: Uuid) {
        if let Some(pool) = &self.pool {
            // Separate connection sees only committed effects, never the handler transaction.
            let db = pool.get().await.unwrap();
            let count = db.query_one("SELECT count(*) FROM (SELECT issue_id FROM issue_pull_request UNION ALL SELECT issue_id FROM issue_vcs_pull_request) links JOIN issue i ON i.id=links.issue_id WHERE i.id=$1 AND i.workspace_id=$2", &[&issue,&ws]).await.unwrap().get(0);
            self.observed_links.lock().unwrap().push(count);
        }
        self.order.lock().unwrap().push("merge");
        self.merges.lock().unwrap().push((ws, issue));
    }
    fn publish_changed(&self, ws: Uuid, issue: Uuid) {
        self.order.lock().unwrap().push("changed");
        self.changed.lock().unwrap().push((ws, issue));
    }
    fn publish_activity(&self, event: IssuePullRequestActivity) {
        self.order.lock().unwrap().push("activity");
        self.activity.lock().unwrap().push(event);
    }
}

async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let code = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        code,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        },
    )
}

fn app_for(health: &Arc<Health>, pool: &Arc<Pool>, runtime: Option<Arc<Runtime>>) -> axum::Router {
    let state = ApiState::new(pool.clone(), b"pr-test-secret".to_vec())
        .with_issue_event_sink(Arc::new(Sink));
    let state = match runtime {
        Some(runtime) => state.with_issue_pull_request_runtime(runtime),
        None => state,
    };
    router_with_api(health.clone(), state)
}

async fn make_issue(db: &Client, ws: Uuid, user: Uuid, number: i32, title: &str) -> Uuid {
    db.query_one(
        "INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status) VALUES($1,$2,'member',$3,$4,'todo') RETURNING id",
        &[&ws, &title, &user, &number],
    )
    .await
    .unwrap()
    .get(0)
}

#[allow(clippy::too_many_arguments)]
async fn make_github_pr(db: &Client, ws: Uuid, number: i32, html_url: &str, state: &str) -> Uuid {
    db.query_one(
        "INSERT INTO github_pull_request(workspace_id,installation_id,repo_owner,repo_name,pr_number,title,state,html_url,pr_created_at,pr_updated_at) VALUES($1,7,'acme','widgets',$2,'PR',$4,$3,now(),now()) RETURNING id",
        &[&ws, &number, &html_url, &state],
    )
    .await
    .unwrap()
    .get(0)
}

async fn make_vcs_pr(db: &Client, ws: Uuid, number: i32, html_url: &str) -> Uuid {
    db.query_one(
        "INSERT INTO vcs_pull_request(workspace_id,connection_id,provider,repo_owner,repo_name,pr_number,title,state,html_url,pr_created_at,pr_updated_at) VALUES($1,gen_random_uuid(),'gitlab','grp','proj',$2,'MR','open',$3,now(),now()) RETURNING id",
        &[&ws, &number, &html_url],
    )
    .await
    .unwrap()
    .get(0)
}

async fn link_row(db: &Client, issue: Uuid, pr: Uuid) -> Option<(Option<String>, Option<Uuid>)> {
    db.query_opt(
        "SELECT linked_by_type, linked_by_id FROM issue_pull_request WHERE issue_id=$1 AND pull_request_id=$2",
        &[&issue, &pr],
    )
    .await
    .unwrap()
    .map(|row| (row.get(0), row.get(1)))
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    db: &Client,
    pool: Arc<Pool>,
    health: Arc<Health>,
    ws: Uuid,
    foreign: Uuid,
    user: Uuid,
    outsider: Uuid,
    _token: &str,
) {
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &json!({"sub": user, "exp": 4_102_444_800_u64}),
        &jsonwebtoken::EncodingKey::from_secret(b"pr-test-secret"),
    )
    .unwrap();
    let token = token.as_str();
    let runtime = Arc::new(Runtime {
        pool: Some(pool.clone()),
        ..Runtime::default()
    });
    let app = app_for(&health, &pool, Some(runtime.clone()));
    let suffix = Uuid::new_v4();
    let issue = make_issue(db, ws, user, 41, "PR target").await;
    let other = make_issue(db, ws, user, 42, "Second").await;
    let foreign_issue = make_issue(db, foreign, user, 41, "Foreign").await;
    let gh = make_github_pr(
        db,
        ws,
        42,
        "https://github.com/acme/widgets/pull/42",
        "open",
    )
    .await;
    let gh2 = make_github_pr(
        db,
        ws,
        43,
        "https://github.com/acme/widgets/pull/43",
        "merged",
    )
    .await;
    let vcs = make_vcs_pr(db, ws, 7, "https://gitlab.com/grp/proj/-/merge_requests/7").await;
    let foreign_pr = make_github_pr(
        db,
        foreign,
        42,
        "https://github.com/acme/widgets/pull/99",
        "open",
    )
    .await;
    let link_path = format!("/api/issues/{issue}/pull-requests?workspace_id={ws}");

    // Missing runtime must fail closed rather than fabricate a list.
    let disabled = app_for(&health, &pool, None);
    for (method, path, body) in [
        ("POST", link_path.clone(), json!({"url":"x"})),
        (
            "DELETE",
            format!("/api/issues/{issue}/pull-requests/{gh}?workspace_id={ws}"),
            json!({}),
        ),
        (
            "PUT",
            format!("/api/issues/{issue}/pr-auto-complete?workspace_id={ws}"),
            json!({"disabled":true}),
        ),
    ] {
        assert_eq!(
            call(&disabled, method, &path, token, body).await.0,
            StatusCode::SERVICE_UNAVAILABLE,
            "{method} {path}"
        );
    }

    // Request validation before any lookup.
    for body in [
        json!({}),
        json!({"url":"   "}),
        json!({"url":null,"pull_request_id":""}),
        json!(null),
    ] {
        assert_eq!(
            call(&app, "POST", &link_path, token, body.clone()).await,
            (StatusCode::BAD_REQUEST, json!({"error":"url is required"})),
            "{body}"
        );
    }
    assert_eq!(
        call(&app, "POST", &link_path, token, json!("not json"))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );

    // Unresolvable URL and unknown id both stay 404 with the Go message.
    let not_found = json!({"error": concat!(
        "pull request not found; it appears here once its repository ",
        "is connected and the PR has been synced"
    )});
    for body in [
        json!({"url":"https://github.com/acme/widgets/pull/9999"}),
        json!({"url":"https://github.com/acme/widgets/issues/42"}),
        json!({"pull_request_id": Uuid::new_v4().to_string()}),
        json!({"pull_request_id":"not-a-uuid"}),
    ] {
        assert_eq!(
            call(&app, "POST", &link_path, token, body.clone()).await,
            (StatusCode::NOT_FOUND, not_found.clone()),
            "{body}"
        );
    }

    // A non-empty pull_request_id wins even when the URL would resolve.
    assert_eq!(
        call(
            &app,
            "POST",
            &link_path,
            token,
            json!({
                "url":"https://github.com/acme/widgets/pull/42",
                "pull_request_id":"not-a-uuid"
            })
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );

    // Workspace isolation: the mirrored PR of another workspace is invisible.
    assert_eq!(
        call(
            &app,
            "POST",
            &link_path,
            token,
            json!({"pull_request_id": foreign_pr.to_string()})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );

    // Manual link: member attribution, exclusion cleared, merge hook once.
    db.execute(
        "INSERT INTO issue_pull_request_exclusion(issue_id,pull_request_id,workspace_id,excluded_by_type,excluded_by_id) VALUES($1,$2,$3,'member',$4)",
        &[&issue, &gh, &ws, &user],
    )
    .await
    .unwrap();
    let merges_before = runtime.merges.lock().unwrap().len();
    let out = call(
        &app,
        "POST",
        &link_path,
        token,
        json!({"url":"HTTPS://GitHub.com/Acme/Widgets/pull/42/files#diff"}),
    )
    .await;
    assert_eq!(
        out,
        (
            StatusCode::OK,
            json!({"pull_requests":[],"auto_complete":{}})
        )
    );
    assert_eq!(
        link_row(db, issue, gh).await,
        Some((Some("member".into()), Some(user)))
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_pull_request_exclusion WHERE issue_id=$1 AND pull_request_id=$2",
            &[&issue, &gh],
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(runtime.merges.lock().unwrap().len(), merges_before + 1);
    assert_eq!(runtime.changed.lock().unwrap().last(), Some(&(ws, issue)));
    // Merge hook, publication and list all run after the link is committed.
    {
        let order = runtime.order.lock().unwrap();
        assert!(order.ends_with(&["merge", "changed", "list"]), "{order:?}");
    }
    assert_eq!(runtime.observed_links.lock().unwrap().last(), Some(&1));

    // Re-linking an existing manual link changes no row: no merge, still publishes.
    let merges_before = runtime.merges.lock().unwrap().len();
    let changed_before = runtime.changed.lock().unwrap().len();
    assert_eq!(
        call(
            &app,
            "POST",
            &link_path,
            token,
            json!({"pull_request_id": gh.to_string()})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(runtime.merges.lock().unwrap().len(), merges_before);
    assert_eq!(runtime.changed.lock().unwrap().len(), changed_before + 1);

    // An automatic link is converted to manual and counts as a change.
    db.execute(
        "INSERT INTO issue_pull_request(issue_id,pull_request_id,linked_by_type,linked_by_id) VALUES($1,$2,'webhook',$3)",
        &[&other, &gh2, &user],
    )
    .await
    .unwrap();
    let merges_before = runtime.merges.lock().unwrap().len();
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/issues/{other}/pull-requests?workspace_id={ws}"),
            token,
            json!({"url":"https://github.com/acme/widgets/pull/43"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        link_row(db, other, gh2).await,
        Some((Some("member".into()), Some(user)))
    );
    assert_eq!(runtime.merges.lock().unwrap().len(), merges_before + 1);

    // VCS provider resolves through the same handler.
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/issues/{other}/pull-requests?workspace_id={ws}"),
            token,
            json!({"url":"https://gitlab.com/grp/proj/-/merge_requests/7/"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT linked_by_type FROM issue_vcs_pull_request WHERE issue_id=$1 AND pull_request_id=$2",
            &[&other, &vcs],
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "member"
    );

    // Unlink: the link and the exclusion move inside one transaction.
    let merges_before = runtime.merges.lock().unwrap().len();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/issues/{issue}/pull-requests/{gh}?workspace_id={ws}"),
            token,
            json!({})
        )
        .await,
        (
            StatusCode::OK,
            json!({"pull_requests":[],"auto_complete":{}})
        )
    );
    assert_eq!(link_row(db, issue, gh).await, None);
    assert_eq!(
        db.query_one(
            "SELECT excluded_by_type, excluded_by_id FROM issue_pull_request_exclusion WHERE issue_id=$1 AND pull_request_id=$2",
            &[&issue, &gh],
        )
        .await
        .map(|row| (row.get::<_, String>(0), row.get::<_, Uuid>(1)))
        .unwrap(),
        ("member".to_string(), user)
    );
    assert_eq!(runtime.merges.lock().unwrap().len(), merges_before + 1);

    // Unlinking a PR that was never linked removes nothing and merges nothing.
    let merges_before = runtime.merges.lock().unwrap().len();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/issues/{other}/pull-requests/{gh}?workspace_id={ws}"),
            token,
            json!({})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(runtime.merges.lock().unwrap().len(), merges_before);

    for path in [
        format!("/api/issues/{issue}/pull-requests/not-a-uuid?workspace_id={ws}"),
        format!("/api/issues/{issue}/pull-requests/{foreign_pr}?workspace_id={ws}"),
    ] {
        let expected = if path.contains("not-a-uuid") {
            (
                StatusCode::BAD_REQUEST,
                json!({"error":"invalid pull request id"}),
            )
        } else {
            (
                StatusCode::NOT_FOUND,
                json!({"error":"pull request not found"}),
            )
        };
        assert_eq!(
            call(&app, "DELETE", &path, token, json!({})).await,
            expected
        );
    }
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/issues/{foreign_issue}/pull-requests/{gh}?workspace_id={ws}"),
            token,
            json!({})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );

    // Automation toggle.
    let toggle = format!("/api/issues/{issue}/pr-auto-complete?workspace_id={ws}");
    for body in [
        json!({}),
        json!({"disabled":null}),
        json!({"disabled":"true"}),
        json!({"disabled":1}),
        json!(null),
    ] {
        assert_eq!(
            call(&app, "PUT", &toggle, token, body.clone()).await,
            (
                StatusCode::BAD_REQUEST,
                json!({"error":"disabled is required"})
            ),
            "{body}"
        );
    }
    let merges_before = runtime.merges.lock().unwrap().len();
    assert_eq!(
        call(&app, "PUT", &toggle, token, json!({"disabled":true}))
            .await
            .0,
        StatusCode::OK
    );
    let activity = runtime.activity.lock().unwrap().last().unwrap().clone();
    assert_eq!(
        (
            activity.workspace_id,
            activity.issue_id,
            activity.actor_type,
            activity.actor_id,
            activity.disabled
        ),
        (ws, issue, "member", user, true)
    );
    assert_eq!(
        db.query_one(
            "SELECT auto_complete_disabled, updated_by_type, updated_by_id FROM issue_pr_automation WHERE issue_id=$1",
            &[&issue],
        )
        .await
        .map(|row| (
            row.get::<_, bool>(0),
            row.get::<_, String>(1),
            row.get::<_, Uuid>(2),
        ))
        .unwrap(),
        (true, "member".to_string(), user)
    );
    let recorded = runtime.activity.lock().unwrap().len();
    assert_eq!(
        call(&app, "PUT", &toggle, token, json!({"disabled":true}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(runtime.activity.lock().unwrap().len(), recorded);
    assert_eq!(
        call(&app, "PUT", &toggle, token, json!({"disabled":false}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(runtime.activity.lock().unwrap().len(), recorded + 1);
    // Turning the automation back on never completes the issue by itself.
    assert_eq!(runtime.merges.lock().unwrap().len(), merges_before);

    // A list response that violates the read-model shape is a server error.
    runtime.broken_list.store(true, Ordering::SeqCst);
    assert_eq!(
        call(&app, "PUT", &toggle, token, json!({"disabled":false}))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    runtime.broken_list.store(false, Ordering::SeqCst);

    failures::run(db, &app, &runtime, ws, user, gh2, token).await;

    // Agent task tokens stay "member" links with the agent as actor, as in Go.
    let runtime_id:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'PR runtime','cloud','synthetic','online') RETURNING id",&[&ws]).await.unwrap().get(0);
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,'PR agent','cloud',$2,$3) RETURNING id",&[&ws,&user,&runtime_id]).await.unwrap().get(0);
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES($1,$2,$3,'running') RETURNING id",&[&agent,&runtime_id,&issue]).await.unwrap().get(0);
    let task_token = format!("mat_pr_{suffix}");
    let hash = hex::encode(sha2::Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')",&[&hash,&task,&agent,&ws,&user]).await.unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &link_path,
            &task_token,
            json!({"pull_request_id": gh.to_string()})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        link_row(db, issue, gh).await,
        Some((Some("member".into()), Some(agent)))
    );
    assert_eq!(
        runtime.activity.lock().unwrap().last().unwrap().actor_id,
        user
    );

    // Re-link with member credentials while link is already manual.
    assert_eq!(
        call(
            &app,
            "POST",
            &link_path,
            token,
            json!({"pull_request_id": gh.to_string()})
        )
        .await
        .0,
        StatusCode::OK
    );
    // Existing manual links retain their original actor even when another member relinks.
    assert_eq!(
        link_row(db, issue, gh).await,
        Some((Some("member".into()), Some(agent)))
    );
    let agent_unlink = format!("/api/issues/{issue}/pull-requests/{gh}?workspace_id={ws}");
    assert_eq!(
        call(&app, "DELETE", &agent_unlink, &task_token, json!({}))
            .await
            .0,
        StatusCode::OK
    );
    let exclusion = db.query_one("SELECT excluded_by_type, excluded_by_id FROM issue_pull_request_exclusion WHERE issue_id=$1 AND pull_request_id=$2", &[&issue,&gh]).await.unwrap();
    assert_eq!(exclusion.get::<_, String>(0), "agent");
    assert_eq!(exclusion.get::<_, Uuid>(1), agent);
    assert_eq!(
        call(&app, "PUT", &toggle, &task_token, json!({"disabled":true}))
            .await
            .0,
        StatusCode::OK
    );
    let activity = runtime.activity.lock().unwrap().last().unwrap().clone();
    assert_eq!((activity.actor_type, activity.actor_id), ("agent", agent));

    // VCS unlink and UUID re-link use the VCS join table, not the GitHub table.
    let vcs_unlink = format!("/api/issues/{other}/pull-requests/{vcs}?workspace_id={ws}");
    assert_eq!(
        call(&app, "DELETE", &vcs_unlink, token, json!({})).await.0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_vcs_pull_request WHERE issue_id=$1 AND pull_request_id=$2",
            &[&other, &vcs]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/issues/{other}/pull-requests?workspace_id={ws}"),
            token,
            json!({"pull_request_id":vcs})
        )
        .await
        .0,
        StatusCode::OK
    );

    // Expired JWT and a revoked task token cannot mutate or emit callbacks.
    let expired = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &json!({"sub":user,"exp":1}),
        &jsonwebtoken::EncodingKey::from_secret(b"pr-test-secret"),
    )
    .unwrap();
    db.execute("DELETE FROM task_token WHERE token_hash=$1", &[&hash])
        .await
        .unwrap();
    let callbacks = runtime.changed.lock().unwrap().len();
    for invalid in [&expired, &task_token] {
        for (method, path, body) in [
            ("POST", link_path.clone(), json!({"pull_request_id":gh})),
            ("DELETE", agent_unlink.clone(), json!({})),
            ("PUT", toggle.clone(), json!({"disabled":false})),
        ] {
            assert_eq!(
                call(&app, method, &path, invalid, body).await.0,
                StatusCode::UNAUTHORIZED
            );
        }
    }
    assert_eq!(runtime.changed.lock().unwrap().len(), callbacks);

    let outsider_token = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &json!({"sub": outsider, "exp": 4_102_444_800_u64}),
        &jsonwebtoken::EncodingKey::from_secret(b"pr-test-secret"),
    )
    .unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &link_path,
            &outsider_token,
            json!({"pull_request_id": gh.to_string()})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );

    // Cookie-only write without the CSRF header stays rejected.
    let cookie_only = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&toggle)
                .header("cookie", format!("multica_auth={token}"))
                .body(Body::from(json!({"disabled":true}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cookie_only.status(), StatusCode::FORBIDDEN);
}
