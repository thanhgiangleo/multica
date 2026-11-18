//! Timeline HTTP/DB checks against an owned disposable PostgreSQL, never providers.
use axum::{
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{ApiState, queries_sprint_timeline::TimelineAvatarUrlPolicy},
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"queries-timeline-synthetic";
fn jwt(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
}
struct Fixture {
    pool: Arc<Pool>,
    app: axum::Router,
    user: Uuid,
    ws: Uuid,
    foreign: Uuid,
    issue: Uuid,
    other: Uuid,
    token: String,
}
impl Fixture {
    async fn new() -> Self {
        let config =
            Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
        assert!(
            config
                .get_hosts()
                .iter()
                .all(|host| matches!(host,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
        );
        assert!(
            config
                .get_ports()
                .iter()
                .all(|port| *port != 5432 && *port != 0)
        );
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
                "INSERT INTO \"user\"(name,email) VALUES ('Timeline member',$1) RETURNING id",
                &[&format!("timeline-{nonce}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        let mut workspaces = Vec::new();
        for name in ["local", "foreign"] {
            let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ($1,$2,'TIME') RETURNING id", &[&name,&format!("timeline-{name}-{nonce}")]).await.unwrap().get(0);
            db.execute(
                "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
                &[&ws, &user],
            )
            .await
            .unwrap();
            workspaces.push(ws);
        }
        let (ws, foreign) = (workspaces[0], workspaces[1]);
        let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES ($1,1,'Timeline fixture','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
        let other: Uuid = db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES ($1,1,'Private fixture','member',$2) RETURNING id", &[&foreign,&user]).await.unwrap().get(0);
        let app = app(pool.clone(), None);
        Self {
            pool,
            app,
            user,
            ws,
            foreign,
            issue,
            other,
            token: jwt(user),
        }
    }
    fn id(&self, n: u32) -> Uuid {
        Uuid::from_u128((self.user.as_u128() & !u128::from(u32::MAX)) | u128::from(n))
    }
    async fn comment(&self, id: Uuid, parent: Option<Uuid>, seconds: i32) {
        self.pool.get().await.unwrap().execute("INSERT INTO comment(id,workspace_id,issue_id,author_type,author_id,content,parent_id,created_at,updated_at) VALUES ($1,$2,$3,'member',$4,'full content',$5,'2026-11-01T00:00:00.900Z'::timestamptz+$6::int4*interval '1 second','2026-11-01T00:00:00.900Z')", &[&id,&self.ws,&self.issue,&self.user,&parent,&seconds]).await.unwrap();
    }
    async fn activity(&self, id: Uuid, seconds: i32) {
        self.pool.get().await.unwrap().execute("INSERT INTO activity_log(id,workspace_id,issue_id,actor_type,actor_id,action,details,created_at) VALUES ($1,$2,$3,'member',$4,'status_changed','{\"from\":\"todo\",\"to\":\"done\"}','2026-11-01T00:00:00.100Z'::timestamptz+$5::int4*interval '1 second')", &[&id,&self.ws,&self.issue,&self.user,&seconds]).await.unwrap();
    }
    async fn bulk(&self, table: &str, count: i32, first: i32) {
        let sql = if table == "comment" {
            "INSERT INTO comment(id,workspace_id,issue_id,author_type,author_id,content,created_at) SELECT md5($1::uuid::text||'comment'||n)::uuid,$1,$2,'member',$3,'bulk','2026-11-01T00:00:00Z'::timestamptz+n*interval '1 second' FROM generate_series($4::int4,$5::int4) n"
        } else {
            assert_eq!(table, "activity_log");
            "INSERT INTO activity_log(id,workspace_id,issue_id,actor_type,actor_id,action,created_at) SELECT md5($1::uuid::text||'activity'||n)::uuid,$1,$2,'member',$3,'bulk','2026-11-01T00:00:00Z'::timestamptz+n*interval '1 second' FROM generate_series($4::int4,$5::int4) n"
        };
        self.pool
            .get()
            .await
            .unwrap()
            .execute(
                sql,
                &[
                    &self.ws,
                    &self.issue,
                    &self.user,
                    &first,
                    &(first + count - 1),
                ],
            )
            .await
            .unwrap();
    }
    async fn get(
        &self,
        issue: &str,
        query: &str,
        token: Option<&str>,
        ws: Uuid,
        comments: bool,
    ) -> (StatusCode, HeaderMap, Value) {
        let route = if comments { "comments" } else { "timeline" };
        let mut request = Request::builder().uri(format!(
            "/api/issues/{issue}/{route}?workspace_id={ws}&{query}"
        ));
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let response = self
            .app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            headers,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }
    async fn read(&self, query: &str) -> (StatusCode, HeaderMap, Value) {
        self.get(
            &self.issue.to_string(),
            query,
            Some(&self.token),
            self.ws,
            false,
        )
        .await
    }
}
fn app(pool: Arc<Pool>, policy: Option<Arc<dyn TimelineAvatarUrlPolicy>>) -> axum::Router {
    let mut state = ApiState::new(pool.clone(), SECRET.to_vec());
    if let Some(policy) = policy {
        state = state.with_timeline_avatar_url_policy(policy);
    }
    router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "queries-timeline".into(),
        )),
        state,
    )
}
fn ids(body: &Value) -> Vec<Uuid> {
    body.as_array()
        .unwrap()
        .iter()
        .map(|value| Uuid::parse_str(value["id"].as_str().unwrap()).unwrap())
        .collect()
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn timeline_auth_scope_identifiers_and_response_envelopes() {
    let f = Fixture::new().await;
    assert_eq!(
        f.get(&f.issue.to_string(), "", None, f.ws, false).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        f.get(&f.issue.to_string(), "", Some("bad"), f.ws, false)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        f.get(&f.other.to_string(), "", Some(&f.token), f.ws, false)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let outsider: Uuid = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Outsider',$1) RETURNING id",
            &[&format!(
                "timeline-outsider-{}@invalid.example",
                Uuid::new_v4()
            )],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        f.get(&f.issue.to_string(), "", Some(&jwt(outsider)), f.ws, false)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    for id in ["TIME-+1", "TIME-0", "TIME-2147483648", "WRONG-1", "invalid"] {
        assert_eq!(
            f.get(id, "", Some(&f.token), f.ws, false).await.0,
            StatusCode::NOT_FOUND,
            "{id}"
        );
    }
    assert_eq!(
        f.get("time-1", "", Some(&f.token), f.ws, false).await.2,
        json!([])
    );
    for query in [
        "",
        "limit=",
        "before=",
        "after=",
        "around=",
        "limit=&limit=1",
        "unknown=true",
        "since=invalid&fold=true",
    ] {
        let (status, headers, body) = f.read(query).await;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        assert_eq!(body, json!([]), "{query}");
        assert!(!headers.contains_key("X-Timeline-Truncated"));
    }
    for query in [
        "limit=invalid",
        "before=opaque",
        "after=opaque",
        "around=missing",
    ] {
        let (status, _, body) = f.read(query).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({"entries":[],"next_cursor":null,"prev_cursor":null,"has_more_before":false,"has_more_after":false})
        );
    }
    f.comment(f.id(20), None, 0).await;
    f.activity(f.id(10), 0).await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "INSERT INTO activity_log(workspace_id,issue_id,action) VALUES ($1,$2,'PRIVATE')",
        &[&f.foreign, &f.issue],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'PRIVATE')", &[&f.foreign,&f.issue,&f.user]).await.unwrap();
    let (status, _, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // Subseconds disagree with IDs; frozen Go sorts serialized seconds, not raw timestamps.
    assert_eq!(ids(&body), vec![f.id(10), f.id(20)]);
    assert!(!body.to_string().contains("PRIVATE"));
    let (_, _, body) = f.read(&format!("around={}", f.id(10))).await;
    assert_eq!(ids(&body["entries"]), vec![f.id(20), f.id(10)]);
    assert_eq!(body["target_index"], 1);
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn timeline_comment_fields_reactions_and_multi_receipts_match_comments() {
    let f = Fixture::new().await;
    let root = f.id(1);
    let reply = f.id(2);
    f.comment(root, None, 0).await;
    f.comment(reply, Some(root), 1).await;
    let db = f.pool.get().await.unwrap();
    let quick = Uuid::new_v4();
    let source = Uuid::new_v4();
    db.execute("UPDATE comment SET revision=7,type='system',source_task_id=$2,quick_action_id=$3,resolved_at='2026-11-01T01:00:00Z',resolved_by_type='member',resolved_by_id=$4,deleted_at='2026-11-01T01:01:00Z' WHERE id=$1", &[&reply,&source,&quick,&f.user]).await.unwrap();
    let reaction: Uuid = db.query_one("INSERT INTO comment_reaction(workspace_id,comment_id,actor_type,actor_id,emoji) VALUES ($1,$2,'member',$3,'ok') RETURNING id", &[&f.ws,&reply,&f.user]).await.unwrap().get(0);
    db.execute("INSERT INTO comment_reaction(workspace_id,comment_id,actor_type,actor_id,emoji) VALUES ($1,$2,'member',$3,'PRIVATE')", &[&f.foreign,&reply,&f.user]).await.unwrap();
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES ($1,'Synthetic timeline','cloud',$2) RETURNING id", &[&f.ws,&f.user]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Synthetic timeline','cloud','synthetic','online') RETURNING id", &[&f.ws]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[&agent,&runtime,&f.issue]).await.unwrap().get(0);
    let missing_task = f.id(200);
    for (task, ws, issue, status, seconds) in [
        (task, f.ws, f.issue, "delivered", 0),
        (missing_task, f.ws, f.issue, "failed", 1),
        (f.id(201), f.foreign, f.other, "pending", 2),
        (f.id(202), f.ws, f.other, "pending", 3),
    ] {
        db.execute("INSERT INTO task_supplement(task_id,workspace_id,issue_id,comment_id,author_id,client_request_id,status,failure_reason,delivered_at,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,CASE WHEN $7='failed' THEN 'turn_ended' END,CASE WHEN $7='delivered' THEN '2026-11-01T01:02:00Z'::timestamptz END,'2026-11-01T00:00:00Z'::timestamptz+$8::int4*interval '1 second')", &[&task,&ws,&issue,&reply,&f.user,&Uuid::new_v4(),&status,&seconds]).await.unwrap();
    }
    f.activity(f.id(3), 2).await;
    db.execute(
        "UPDATE activity_log SET actor_type=NULL,actor_id=NULL,details='null' WHERE id=$1",
        &[&f.id(3)],
    )
    .await
    .unwrap();
    let (status, _, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let c = &body[1];
    assert_eq!(c["id"], json!(reply));
    assert_eq!(c["type"], "comment");
    assert_eq!(c["actor_name"], "Timeline member");
    assert_eq!(c["parent_id"], json!(root));
    assert_eq!(c["revision"], 7);
    assert_eq!(c["comment_type"], "system");
    assert_eq!(c["source_task_id"], json!(source));
    assert_eq!(c["quick_action_id"], json!(quick));
    assert!(c["resolved_at"].is_string() && c["deleted_at"].is_string());
    assert_eq!(c["reactions"][0]["id"], json!(reaction));
    assert_eq!(c["reactions"].as_array().unwrap().len(), 1);
    assert!(c.get("attachments").is_none());
    assert!(body[0].get("reactions").is_none());
    assert_eq!(c["supplements"].as_array().unwrap().len(), 2);
    assert_eq!(c["supplements"][0]["agent_id"], json!(agent));
    assert!(c["supplements"][1].get("agent_id").is_none());
    assert_eq!(c["supplements"][1]["failure_reason"], "turn_ended");
    assert_eq!(c["supplement_task_id"], json!(task));
    assert_eq!(c["supplement_status"], "delivered");
    assert_eq!(
        c["supplement_delivered_at"],
        c["supplements"][0]["delivered_at"]
    );
    assert!(c.get("supplement_failure_reason").is_none());
    assert_eq!(body[2]["actor_id"], "");
    assert_eq!(body[2]["actor_type"], "");
    assert!(body[2]["details"].is_null());
    assert!(body[2].get("content").is_none());
    let (status, _, comments) = f
        .get(&f.issue.to_string(), "", Some(&f.token), f.ws, true)
        .await;
    assert_eq!(status, StatusCode::OK);
    for key in [
        "supplements",
        "supplement_task_id",
        "supplement_status",
        "supplement_delivered_at",
        "reactions",
    ] {
        assert_eq!(comments[1][key], c[key], "{key}");
    }
    assert!(!body.to_string().contains("PRIVATE"));
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn timeline_independent_caps_keep_old_comments_and_exact_boundary_truthful() {
    let f = Fixture::new().await;
    f.comment(f.id(1), None, -100).await;
    f.bulk("activity_log", 2000, 0).await;
    let (status, headers, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.as_array().unwrap().len(), 2001);
    assert_eq!(body[0]["id"], json!(f.id(1)));
    assert!(!headers.contains_key("X-Timeline-Truncated"));
    f.activity(f.id(2), 3000).await;
    let (_, headers, body) = f.read("limit=1").await;
    assert_eq!(headers["X-Timeline-Truncated"], "activity");
    assert_eq!(body["entries"].as_array().unwrap().len(), 2001);
    assert_eq!(body["entries"][2000]["id"], json!(f.id(1)));
    assert_eq!(body["has_more_before"], true);
    assert_eq!(body["has_more_after"], false);
    assert!(body["next_cursor"].is_null() && body["prev_cursor"].is_null());
    f.bulk("comment", 2000, 10).await;
    let (_, headers, body) = f.read("").await;
    assert_eq!(headers["X-Timeline-Truncated"], "activity,comment");
    assert_eq!(body.as_array().unwrap().len(), 4000);
    assert!(!ids(&body).contains(&f.id(1)));
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn timeline_completes_capped_threads_without_folding_or_orphans() {
    let f = Fixture::new().await;
    let root = f.id(1);
    let old = f.id(2);
    let fresh = f.id(3);
    f.comment(root, None, -20).await;
    f.comment(old, Some(root), -10).await;
    f.comment(fresh, Some(old), 3000).await;
    f.bulk("comment", 1999, 0).await;
    let db = f.pool.get().await.unwrap();
    db.execute("UPDATE comment SET resolved_at=now(),resolved_by_type='member',resolved_by_id=$2 WHERE id=$1", &[&root,&f.user]).await.unwrap();
    let (status, headers, body) = f.read("fold=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["X-Timeline-Truncated"], "comment");
    assert_eq!(body.as_array().unwrap().len(), 2002);
    let present: HashSet<Uuid> = ids(&body).into_iter().collect();
    assert!(present.contains(&root) && present.contains(&old) && present.contains(&fresh));
    for row in body.as_array().unwrap() {
        assert!(row.get("folded_count").is_none() && row.get("thread_resolved").is_none());
        if let Some(parent) = row["parent_id"].as_str() {
            assert!(present.contains(&Uuid::parse_str(parent).unwrap()));
        }
    }
    // Too much old context: omit entire affected thread, keep unrelated roots.
    db.execute("INSERT INTO comment(id,workspace_id,issue_id,author_type,author_id,content,parent_id,created_at) SELECT md5($1::uuid::text||'old'||n)::uuid,$1,$2,'member',$3,'old sibling',$4,'2026-10-30T00:00:00Z'::timestamptz FROM generate_series(1,2000) n", &[&f.ws,&f.issue,&f.user,&root]).await.unwrap();
    let (status, headers, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["X-Timeline-Truncated"], "comment");
    assert_eq!(body.as_array().unwrap().len(), 1999);
    let present: HashSet<Uuid> = ids(&body).into_iter().collect();
    assert!(!present.contains(&root) && !present.contains(&old) && !present.contains(&fresh));
}

struct AvatarPolicy {
    calls: Arc<Mutex<Vec<String>>>,
    available: bool,
}
impl TimelineAvatarUrlPolicy for AvatarPolicy {
    fn resolve(&self, stored: &str) -> Option<String> {
        self.calls.lock().unwrap().push(stored.into());
        self.available
            .then(|| "https://avatar.invalid.example/signed-synthetic".into())
    }
}
#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn timeline_departed_member_identity_and_avatar_policy_fail_closed() {
    let mut f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let departed: Uuid = db.query_one("INSERT INTO \"user\"(name,email,avatar_url) VALUES ('Departed',$1,'private://owned-avatar') RETURNING id", &[&format!("departed-{}@invalid.example", f.ws)]).await.unwrap().get(0);
    let unrelated: Uuid = db.query_one("INSERT INTO \"user\"(name,email,avatar_url) VALUES ('PRIVATE UNRELATED',$1,'private://other-avatar') RETURNING id", &[&format!("unrelated-{}@invalid.example", f.ws)]).await.unwrap().get(0);
    f.comment(f.id(1), None, 0).await;
    db.execute(
        "UPDATE comment SET author_id=$2 WHERE id=$1",
        &[&f.id(1), &departed],
    )
    .await
    .unwrap();
    f.activity(f.id(2), 1).await;
    db.execute(
        "UPDATE activity_log SET actor_type='agent',actor_id=$2 WHERE id=$1",
        &[&f.id(2), &unrelated],
    )
    .await
    .unwrap();
    let (status, _, body) = f.read("").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!body.to_string().contains("private://"));
    let calls = Arc::new(Mutex::new(Vec::new()));
    f.app = app(
        f.pool.clone(),
        Some(Arc::new(AvatarPolicy {
            calls: calls.clone(),
            available: true,
        })),
    );
    let (status, _, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body[0]["actor_name"], "Departed");
    assert_eq!(
        body[0]["actor_avatar_url"],
        "https://avatar.invalid.example/signed-synthetic"
    );
    assert!(body[1].get("actor_name").is_none() && body[1].get("actor_avatar_url").is_none());
    assert_eq!(*calls.lock().unwrap(), vec!["private://owned-avatar"]);
    f.app = app(
        f.pool.clone(),
        Some(Arc::new(AvatarPolicy {
            calls,
            available: false,
        })),
    );
    assert_eq!(f.read("").await.0, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn timeline_selected_attachment_fails_closed_and_concurrent_reads_do_not_mutate() {
    let f = Fixture::new().await;
    f.comment(f.id(1), None, 0).await;
    f.activity(f.id(2), 1).await;
    let db = f.pool.get().await.unwrap();
    let before: Value = db
        .query_one("SELECT to_jsonb(i) FROM issue i WHERE id=$1", &[&f.issue])
        .await
        .unwrap()
        .get(0);
    let (a, b, c) = tokio::join!(f.read(""), f.read(""), f.read(""));
    assert_eq!(a.0, StatusCode::OK);
    assert_eq!(b.2, a.2);
    assert_eq!(c.2, a.2);
    let after: Value = db
        .query_one("SELECT to_jsonb(i) FROM issue i WHERE id=$1", &[&f.issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
    let counts: (i64, i64) = {
        let r=db.query_one("SELECT (SELECT count(*) FROM comment WHERE issue_id=$1),(SELECT count(*) FROM activity_log WHERE issue_id=$1)", &[&f.issue]).await.unwrap();
        (r.get(0), r.get(1))
    };
    assert_eq!(counts, (1, 1));
    db.execute("INSERT INTO attachment(workspace_id,issue_id,comment_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,$2,$3,'member',$4,'private.txt','private://secret','text/plain',1)", &[&f.ws,&f.issue,&f.id(1),&f.user]).await.unwrap();
    let (status, _, body) = f.read("").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!body.to_string().contains("private://"));
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn timeline_frozen_go_sql_windows_and_observed_plans() {
    use tokio_postgres::types::Type;
    let f = Fixture::new().await;
    f.bulk("activity_log", 6000, 0).await;
    f.bulk("comment", 6000, 0).await;
    let db = f.pool.get().await.unwrap();
    db.batch_execute("ANALYZE activity_log; ANALYZE comment")
        .await
        .unwrap();
    let frozen_activity = db
        .prepare_typed(
            include_str!("queries_sprint_timeline_frozen_activity.sql"),
            &[Type::UUID, Type::INT4],
        )
        .await
        .unwrap();
    let frozen_comments = db
        .prepare_typed(
            include_str!("queries_sprint_timeline_frozen_comments.sql"),
            &[Type::UUID, Type::UUID, Type::INT4],
        )
        .await
        .unwrap();
    let activities = db
        .query(&frozen_activity, &[&f.issue, &2001i32])
        .await
        .unwrap();
    let comments = db
        .query(&frozen_comments, &[&f.issue, &f.ws, &2001i32])
        .await
        .unwrap();
    assert_eq!(activities.len(), 2001);
    assert_eq!(comments.len(), 2001);
    let mut expected: Vec<(String, String)> = activities[1..]
        .iter()
        .chain(comments[1..].iter())
        .map(|row| {
            let at: chrono::DateTime<chrono::Utc> = row.get("created_at");
            (
                at.with_timezone(&chrono::Local)
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                row.get::<_, Uuid>("id").to_string(),
            )
        })
        .collect();
    expected.sort();
    let (status, headers, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["X-Timeline-Truncated"], "activity,comment");
    assert_eq!(body.as_array().unwrap().len(), 4000);
    let actual: Vec<(String, String)> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["created_at"].as_str().unwrap().to_owned(),
                row["id"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(actual.len(), expected.len());
    for (index, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
        assert_eq!(actual, expected, "frozen SQL window differs at row {index}");
    }
    // Plans are observations, not a forced-index performance gate. With both
    // tenant predicates PostgreSQL may choose a workspace scan + top-N sort;
    // record that gap rather than disabling sequential scans or hiding it.
    let activity_plan: Value = db.query_one("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM (SELECT * FROM activity_log WHERE issue_id=$1 AND workspace_id=$2 ORDER BY created_at DESC,id DESC LIMIT 2001) recent ORDER BY created_at,id", &[&f.issue,&f.ws]).await.unwrap().get(0);
    let comment_plan: Value = db.query_one("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM (SELECT * FROM comment WHERE issue_id=$1 AND workspace_id=$2 ORDER BY created_at DESC,id DESC LIMIT 2001) recent ORDER BY created_at,id", &[&f.issue,&f.ws]).await.unwrap().get(0);
    let activity_keyset = activity_plan
        .to_string()
        .contains("idx_activity_log_issue_keyset");
    let comment_keyset = comment_plan
        .to_string()
        .contains("idx_comment_issue_keyset");
    for plan in [&activity_plan, &comment_plan] {
        assert!(
            plan.to_string().contains("Limit"),
            "missing SQL window: {plan}"
        );
        assert_eq!(plan[0]["Plan"]["Actual Rows"], 2001);
    }
    println!(
        "Observed keyset selection: activity={activity_keyset}, comment={comment_keyset}; not a scale acceptance gate"
    );
    if let Ok(path) = std::env::var("QUERIES_SPRINT_TIMELINE_OUTPUT") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&json!({
                "activity":activity_plan,"comment":comment_plan,
                "fixture_rows_per_kind":6000,"probe_rows":2001,
                "activity_keyset_selected":activity_keyset,"comment_keyset_selected":comment_keyset,
                "scale_acceptance_proven":false
            }))
            .unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn timeline_activity_lock_timeout_and_rollback_recovery() {
    let f = Fixture::new().await;
    f.activity(f.id(1), 0).await;
    let mut blocker = f.pool.get().await.unwrap();
    let lock = blocker.transaction().await.unwrap();
    lock.batch_execute("LOCK TABLE activity_log IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let (status, _, body) = f.read("").await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{body}");
    lock.rollback().await.unwrap();
    assert_eq!(f.read("").await.0, StatusCode::OK);
}
