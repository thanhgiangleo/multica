//! Read-model HTTP/DB checks; run only against an owned migrated PostgreSQL.
use axum::{
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use chrono::{DateTime, Utc};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{collections::HashSet, str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"queries-comments-synthetic";
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
                .max_size(6)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let nonce = Uuid::new_v4();
        let user: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES ('Comments member',$1) RETURNING id",
                &[&format!("comments-{nonce}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        let mut workspaces = Vec::new();
        for name in ["local", "foreign"] {
            let ws:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ($1,$2,'READ') RETURNING id", &[&name,&format!("comments-{name}-{nonce}")]).await.unwrap().get(0);
            db.execute(
                "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
                &[&ws, &user],
            )
            .await
            .unwrap();
            workspaces.push(ws);
        }
        let ws = workspaces[0];
        let foreign = workspaces[1];
        let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES ($1,1,'Comment fixture','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
        let other:Uuid=db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES ($1,1,'Private fixture','member',$2) RETURNING id", &[&foreign,&user]).await.unwrap().get(0);
        let app = router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "queries-comments".into(),
            )),
            ApiState::new(pool.clone(), SECRET.to_vec()),
        );
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
    async fn comment(
        &self,
        ws: Uuid,
        issue: Uuid,
        id: Uuid,
        parent: Option<Uuid>,
        content: &str,
        seconds: i32,
    ) {
        self.pool.get().await.unwrap().execute("INSERT INTO comment(id,workspace_id,issue_id,author_type,author_id,content,parent_id,created_at,updated_at) VALUES ($1,$2,$3,'member',$4,$5,$6,'2026-11-01T00:00:00.123400Z'::timestamptz+$7::int4*interval '1 second','2026-11-01T00:00:00.123400Z'::timestamptz)", &[&id,&ws,&issue,&self.user,&content,&parent,&seconds]).await.unwrap();
    }
    async fn get(
        &self,
        issue: &str,
        query: &str,
        token: Option<&str>,
        ws: Uuid,
    ) -> (StatusCode, HeaderMap, Value) {
        let mut request = Request::builder().uri(format!(
            "/api/issues/{issue}/comments?workspace_id={ws}&{query}"
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
        self.get(&self.issue.to_string(), query, Some(&self.token), self.ws)
            .await
    }
}
fn ids(value: &Value) -> Vec<Uuid> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|value| Uuid::parse_str(value["id"].as_str().unwrap()).unwrap())
        .collect()
}

fn assert_no_orphans(body: &Value) {
    let present: HashSet<Uuid> = ids(body).into_iter().collect();
    for comment in body.as_array().unwrap() {
        if let Some(parent) = comment["parent_id"].as_str() {
            assert!(
                present.contains(&Uuid::parse_str(parent).unwrap()),
                "orphaned comment: {comment}"
            );
        }
    }
}

async fn bulk_roots(f: &Fixture, first: i32, count: i32) {
    f.pool.get().await.unwrap().execute(
        "INSERT INTO comment(id,workspace_id,issue_id,author_type,author_id,content,created_at) SELECT md5($1::uuid::text||n)::uuid,$1,$2,'member',$3,'filler','2026-11-01T00:00:00Z'::timestamptz+n*interval '1 second' FROM generate_series($4::int4,$5::int4) n",
        &[&f.ws, &f.issue, &f.user, &first, &(first + count - 1)],
    ).await.unwrap();
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_frozen_go_sql_differential_for_roots_and_thread_windows() {
    use tokio_postgres::types::Type;
    let f = Fixture::new().await;
    let mut anchors = Vec::new();
    for n in 0..12 {
        let root = Uuid::new_v4();
        f.comment(f.ws, f.issue, root, None, "root", n / 3).await;
        let mut parent = root;
        for reply in 0..5 {
            let id = Uuid::new_v4();
            f.comment(f.ws, f.issue, id, Some(parent), "nested", 10 + reply / 2)
                .await;
            parent = id;
        }
        anchors.push(parent);
    }
    let db = f.pool.get().await.unwrap();
    let roots_sql = db
        .prepare_typed(
            include_str!("queries_sprint_comments_frozen_roots.sql"),
            &[Type::UUID, Type::UUID, Type::INT4],
        )
        .await
        .unwrap();
    let roots_since_sql = db
        .prepare_typed(
            include_str!("queries_sprint_comments_frozen_roots_since.sql"),
            &[Type::UUID, Type::UUID, Type::TIMESTAMPTZ, Type::INT4],
        )
        .await
        .unwrap();
    let thread_sql = db
        .prepare_typed(
            include_str!("queries_sprint_comments_frozen_thread.sql"),
            &[
                Type::UUID,
                Type::UUID,
                Type::UUID,
                Type::BOOL,
                Type::TIMESTAMPTZ,
                Type::UUID,
                Type::INT4,
            ],
        )
        .await
        .unwrap();
    let recent_sql = db
        .prepare_typed(
            include_str!("queries_sprint_comments_frozen_recent.sql"),
            &[
                Type::UUID,
                Type::UUID,
                Type::BOOL,
                Type::TIMESTAMPTZ,
                Type::UUID,
                Type::INT4,
            ],
        )
        .await
        .unwrap();
    let expected = db
        .query(&roots_sql, &[&f.issue, &f.ws, &2001i32])
        .await
        .unwrap();
    let (status, _, actual) = f.read("roots_only=true").await;
    assert_eq!(status, StatusCode::OK, "{actual}");
    assert_eq!(
        ids(&actual),
        expected
            .iter()
            .map(|row| row.get::<_, Uuid>("id"))
            .collect::<Vec<_>>()
    );
    for (actual, expected) in actual.as_array().unwrap().iter().zip(expected) {
        assert_eq!(actual["reply_count"], expected.get::<_, i32>("reply_count"));
    }
    let since = DateTime::parse_from_rfc3339("2026-11-01T00:00:01.1234Z")
        .unwrap()
        .with_timezone(&Utc);
    let expected = db
        .query(&roots_since_sql, &[&f.issue, &f.ws, &since, &2001i32])
        .await
        .unwrap();
    assert_eq!(
        ids(&f
            .read("roots_only=true&since=2026-11-01T00:00:01.1234Z")
            .await
            .2),
        expected
            .iter()
            .map(|row| row.get::<_, Uuid>("id"))
            .collect::<Vec<_>>()
    );
    for anchor in anchors.iter().take(4) {
        for tail in [0usize, 1, 2, 5, 6] {
            let expected = db
                .query(
                    &thread_sql,
                    &[
                        anchor,
                        &f.issue,
                        &f.ws,
                        &false,
                        &None::<DateTime<Utc>>,
                        &None::<Uuid>,
                        &((tail + 1) as i32),
                    ],
                )
                .await
                .unwrap();
            let root = expected
                .iter()
                .find(|row| row.get::<_, Option<Uuid>>("parent_id").is_none())
                .unwrap()
                .get::<_, Uuid>("id");
            let mut replies: Vec<Uuid> = expected
                .iter()
                .filter(|row| row.get::<_, Option<Uuid>>("parent_id").is_some())
                .map(|row| row.get("id"))
                .collect();
            let more = replies.len() > tail;
            if more {
                replies.remove(0);
            }
            let mut wanted = vec![root];
            wanted.extend(&replies);
            let (status, headers, actual) = f.read(&format!("thread={anchor}&tail={tail}")).await;
            assert_eq!(status, StatusCode::OK, "{actual}");
            assert_eq!(ids(&actual), wanted);
            assert_eq!(
                headers.contains_key("X-Multica-Next-Before"),
                more && tail > 0
            );
        }
    }
    // Recent window: every returned row must belong to a thread the frozen Go
    // SQL picked, and the cursor must point at the head (oldest-active) thread.
    for n in [3i64, 5, 12, 13] {
        let expected = db
            .query(
                &recent_sql,
                &[
                    &f.issue,
                    &f.ws,
                    &false,
                    &None::<DateTime<Utc>>,
                    &None::<Uuid>,
                    &(n as i32),
                ],
            )
            .await
            .unwrap();
        let wanted: Vec<Uuid> = expected.iter().map(|row| row.get("id")).collect();
        let threads: HashSet<Uuid> = expected
            .iter()
            .map(|row| row.get("thread_root_id"))
            .collect();
        let (status, headers, actual) = f.read(&format!("recent={n}")).await;
        assert_eq!(status, StatusCode::OK, "{actual}");
        assert_eq!(ids(&actual), wanted, "recent={n}");
        if threads.len() >= n as usize {
            let head = expected.first().unwrap();
            assert_eq!(
                head_time(&headers["X-Multica-Next-Before"]),
                head.get::<_, DateTime<Utc>>("thread_last_activity_at")
            );
            assert_eq!(
                headers["X-Multica-Next-Before-Id"],
                head.get::<_, Uuid>("thread_root_id").to_string()
            );
        } else {
            assert!(!headers.contains_key("X-Multica-Next-Before"), "recent={n}");
        }
        // Second page via the recent cursor resumes on strictly older threads.
        if threads.len() >= n as usize && n < 12 {
            let cursor = format!(
                "recent={n}&before={}&before-id={}",
                headers["X-Multica-Next-Before"].to_str().unwrap(),
                headers["X-Multica-Next-Before-Id"].to_str().unwrap()
            );
            let at = head_time(&headers["X-Multica-Next-Before"]);
            let root: Uuid = headers["X-Multica-Next-Before-Id"]
                .to_str()
                .unwrap()
                .parse()
                .unwrap();
            let expected = db
                .query(
                    &recent_sql,
                    &[&f.issue, &f.ws, &true, &Some(at), &Some(root), &(n as i32)],
                )
                .await
                .unwrap();
            let wanted: Vec<Uuid> = expected.iter().map(|row| row.get("id")).collect();
            let page: HashSet<Uuid> = wanted.iter().copied().collect();

            let first: HashSet<Uuid> = ids(&actual).into_iter().collect();
            assert!(page.is_disjoint(&first), "recent={n} cursor overlapped");
            assert_eq!(ids(&f.read(&cursor).await.2), wanted, "recent={n} page 2");
        }
    }
}

fn head_time(raw: &axum::http::HeaderValue) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw.to_str().unwrap())
        .unwrap()
        .with_timezone(&Utc)
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_roots_scope_summary_reactions_and_private_attachments() {
    let f = Fixture::new().await;
    let root = Uuid::new_v4();
    let reply = Uuid::new_v4();
    let nested = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    f.comment(f.ws, f.issue, root, None, &"界🙂".repeat(101), 0)
        .await;
    f.comment(f.ws, f.issue, reply, Some(root), "reply", 1)
        .await;
    f.comment(f.ws, f.issue, nested, Some(reply), "nested", 2)
        .await;
    f.comment(f.foreign, f.other, foreign, None, "PRIVATE FOREIGN", 20)
        .await;
    // Malformed links must not contaminate descendant stats or reaction projection.
    f.comment(
        f.foreign,
        f.other,
        Uuid::new_v4(),
        Some(root),
        "PRIVATE CHILD",
        30,
    )
    .await;
    let db = f.pool.get().await.unwrap();
    let reaction:Uuid=db.query_one("INSERT INTO comment_reaction(workspace_id,comment_id,actor_type,actor_id,emoji) VALUES ($1,$2,'member',$3,'👍') RETURNING id", &[&f.ws,&root,&f.user]).await.unwrap().get(0);
    db.execute("INSERT INTO comment_reaction(workspace_id,comment_id,actor_type,actor_id,emoji) VALUES ($1,$2,'member',$3,'PRIVATE')", &[&f.foreign,&root,&f.user]).await.unwrap();
    assert_eq!(
        f.get(&f.issue.to_string(), "", None, f.ws).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        f.get(&f.issue.to_string(), "", Some("bad"), f.ws).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        f.get(&f.other.to_string(), "", Some(&f.token), f.ws)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Outsider',$1) RETURNING id",
            &[&format!("outsider-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        f.get(&f.issue.to_string(), "", Some(&jwt(outsider)), f.ws)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (status, _, body) = f
        .get(
            "read-1",
            "roots-only=true&summary=true",
            Some(&f.token),
            f.ws,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ids(&body), vec![root]);
    assert_eq!(body[0]["reply_count"], 2);
    assert_eq!(
        DateTime::parse_from_rfc3339(body[0]["last_activity_at"].as_str().unwrap()).unwrap(),
        DateTime::parse_from_rfc3339("2026-11-01T00:00:02Z").unwrap()
    );
    assert_eq!(body[0]["content"], "界🙂".repeat(100) + "…");
    assert_eq!(body[0]["content_truncated"], true);
    assert_eq!(body[0]["reactions"].as_array().unwrap().len(), 1);
    assert_eq!(body[0]["reactions"][0]["id"], json!(reaction));
    assert_eq!(body[0]["attachments"], json!([]));
    assert_eq!(body[0]["resolved_at"], Value::Null);
    assert!(body[0].get("source_task_id").is_none());
    assert!(body[0].get("thread_resolved").is_none());
    let (_, _, body) = f.read("").await;
    assert_eq!(body.as_array().unwrap().len(), 3);
    assert!(body[0].get("reply_count").is_none());
    assert!(body[0].get("content_truncated").is_none());
    // Roots+since is root creation, not activity time of an old thread.
    assert_eq!(
        ids(&f.read("roots_only=true&since=2026-11-01T00:00:01Z").await.2),
        Vec::<Uuid>::new()
    );
    assert_eq!(
        ids(&f.read("since=2026-11-01T00:00:01Z").await.2),
        vec![reply, nested]
    );
    db.execute("INSERT INTO attachment(workspace_id,issue_id,comment_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,$2,$3,'member',$4,'private.txt','private://secret','text/plain',1)", &[&f.ws,&f.issue,&reply,&f.user]).await.unwrap();
    assert_eq!(f.read("roots_only=true").await.0, StatusCode::OK);
    let (status, _, body) = f.read("").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!body.to_string().contains("private://"));
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_thread_cursor_since_fold_and_tombstone() {
    let f = Fixture::new().await;
    let root = Uuid::new_v4();
    let replies: Vec<Uuid> = (1..=4)
        .map(|n| Uuid::from_u128(0x81700000000040008000000000000000 + n))
        .collect();
    f.comment(f.ws, f.issue, root, None, "root", 0).await;
    for (i, id) in replies.iter().enumerate() {
        f.comment(
            f.ws,
            f.issue,
            *id,
            Some(if i == 0 { root } else { replies[i - 1] }),
            "reply",
            1,
        )
        .await;
    }
    let foreign = Uuid::new_v4();
    f.comment(f.foreign, f.other, foreign, None, "private", 10)
        .await;
    let base = format!("thread={}", replies[3]);
    let (status, headers, body) = f.read(&format!("{base}&tail=2")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ids(&body), vec![root, replies[2], replies[3]]);
    assert_eq!(
        headers["X-Multica-Next-Before"],
        "2026-11-01T00:00:01.1234Z"
    );
    assert_eq!(headers["X-Multica-Next-Before-Id"], replies[2].to_string());
    let cursor = format!(
        "{base}&tail=2&before={}&before-id={}",
        headers["X-Multica-Next-Before"].to_str().unwrap(),
        replies[2]
    );
    let (status, headers, body) = f.read(&cursor).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ids(&body), vec![root, replies[0], replies[1]]);
    assert!(!headers.contains_key("X-Multica-Next-Before"));
    let (_, headers, body) = f.read(&format!("{base}&tail=0")).await;
    assert_eq!(ids(&body), vec![root]);
    assert!(!headers.contains_key("X-Multica-Next-Before"));
    let (_, headers, body) = f
        .read(&format!("{base}&tail=2&since=2026-11-01T00:00:02Z"))
        .await;
    assert_eq!(ids(&body), vec![root]);
    assert!(!headers.contains_key("X-Multica-Next-Before"));
    assert_eq!(
        ids(&f
            .read(&format!("{base}&since=2026-11-01T00:00:02Z"))
            .await
            .2),
        Vec::<Uuid>::new()
    );
    assert_eq!(
        f.read(&format!("thread={foreign}&tail=1")).await.0,
        StatusCode::NOT_FOUND
    );
    let db = f.pool.get().await.unwrap();
    db.execute("UPDATE comment SET resolved_at='2026-11-02T00:00:00Z',resolved_by_type='member',resolved_by_id=$1 WHERE id=$2", &[&f.user,&replies[1]]).await.unwrap();
    let (_, _, body) = f.read(&format!("{base}&fold=true&summary=true")).await;
    assert_eq!(ids(&body), vec![root, replies[1]]);
    assert_eq!(body[0]["folded_count"], 3);
    assert_eq!(body[0]["thread_resolved"], true);
    assert!(body[1].get("folded_count").is_none());
    db.execute("UPDATE comment SET resolved_at='2026-11-02T00:00:00Z',resolved_by_type='member',resolved_by_id=$1 WHERE id=$2", &[&f.user,&root]).await.unwrap();
    assert_eq!(ids(&f.read("fold=true").await.2), vec![root]);
    db.execute(
        "UPDATE comment SET content='',deleted_at=now() WHERE id=$1",
        &[&root],
    )
    .await
    .unwrap();
    let (_, _, body) = f.read(&format!("{base}&tail=1")).await;
    assert_eq!(body[0]["content"], "");
    assert!(body[0].get("deleted_at").is_some());
    assert_eq!(body[1]["parent_id"], json!(replies[2]));
    for query in [
        "summary=1",
        "tail=1",
        "roots_only=true&thread=bad",
        "fold=true&tail=0",
        "before=2026-11-01T00:00:00Z",
        "recent=0",
        "thread=bad",
    ] {
        assert_eq!(f.read(query).await.0, StatusCode::BAD_REQUEST, "{query}");
    }
    let (_, headers, body) = f.read("recent=1&fold=true").await;
    assert_eq!(ids(&body), vec![root]);
    assert_eq!(
        headers["X-Multica-Next-Before"],
        "2026-11-01T00:00:01.1234Z"
    );
    assert_eq!(
        ids(&f.read("recent=1&since=2026-11-01T00:00:02Z").await.2),
        Vec::<Uuid>::new()
    );
    assert!(
        !f.read("recent=1&since=2026-11-01T00:00:02Z")
            .await
            .1
            .contains_key("X-Multica-Next-Before")
    );
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_default_exact_cap_still_folds() {
    let f = Fixture::new().await;
    let root = Uuid::new_v4();
    let reply = Uuid::new_v4();
    f.comment(f.ws, f.issue, root, None, "root", 0).await;
    f.comment(f.ws, f.issue, reply, Some(root), "resolved", 1)
        .await;
    f.pool.get().await.unwrap().execute(
        "UPDATE comment SET resolved_at='2026-11-02T00:00:00Z',resolved_by_type='member',resolved_by_id=$1 WHERE id=$2",
        &[&f.user, &reply],
    ).await.unwrap();
    bulk_roots(&f, 100, 1998).await;
    let (status, headers, body) = f.read("fold=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!headers.contains_key("X-Comments-Truncated"));
    assert_eq!(ids(&body).len(), 2000);
    assert_eq!(body[0]["thread_resolved"], true);
    assert_eq!(body[0]["folded_count"], 0);
    assert_eq!(ids(&body)[1], reply);
    assert_no_orphans(&body);
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_default_completion_restores_old_siblings_before_folding() {
    let f = Fixture::new().await;
    let root = Uuid::new_v4();
    f.comment(f.ws, f.issue, root, None, "root", 0).await;
    for n in 1..=3 {
        f.comment(f.ws, f.issue, Uuid::new_v4(), Some(root), "old sibling", n)
            .await;
    }
    bulk_roots(&f, 100, 2001).await;
    let fresh = Uuid::new_v4();
    let resolution = Uuid::new_v4();
    f.comment(f.ws, f.issue, fresh, Some(root), "fresh", 3000)
        .await;
    f.comment(f.ws, f.issue, resolution, Some(root), "resolved", 3001)
        .await;
    f.pool.get().await.unwrap().execute(
        "UPDATE comment SET resolved_at='2026-11-02T00:00:00Z',resolved_by_type='member',resolved_by_id=$1 WHERE id=$2",
        &[&f.user, &resolution],
    ).await.unwrap();
    let (status, headers, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["X-Comments-Truncated"], "true");
    assert_eq!(ids(&body).len(), 2004);
    assert!(ids(&body).contains(&root));
    assert_no_orphans(&body);
    let (status, headers, body) = f.read("fold=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["X-Comments-Truncated"], "true");
    let folded_root = body
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == json!(root))
        .unwrap();
    assert_eq!(folded_root["thread_resolved"], true);
    assert_eq!(folded_root["folded_count"], 4);
    assert!(ids(&body).contains(&resolution));
    assert!(!ids(&body).contains(&fresh));
    assert_no_orphans(&body);
    f.pool.get().await.unwrap().execute(
        "UPDATE comment SET resolved_at='2026-11-02T00:00:00Z',resolved_by_type='member',resolved_by_id=$1 WHERE id=$2",
        &[&f.user, &root],
    ).await.unwrap();
    let (status, _, body) = f.read("fold=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let folded_root = body
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == json!(root))
        .unwrap();
    assert_eq!(folded_root["folded_count"], 5);
    assert!(!ids(&body).contains(&resolution));
    assert_no_orphans(&body);
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_default_completion_is_bounded_and_drops_oversized_threads() {
    let f = Fixture::new().await;
    let root = Uuid::new_v4();
    f.comment(f.ws, f.issue, root, None, "root", 0).await;
    f.pool.get().await.unwrap().execute(
        "INSERT INTO comment(id,workspace_id,issue_id,author_type,author_id,content,parent_id,created_at) SELECT md5($1::uuid::text||n)::uuid,$2,$3,'member',$4,'old',$1,'2026-11-01T00:00:00Z'::timestamptz+n*interval '1 second' FROM generate_series(1,2000) n",
        &[&root, &f.ws, &f.issue, &f.user],
    ).await.unwrap();
    bulk_roots(&f, 5000, 2001).await;
    let fresh = Uuid::new_v4();
    f.comment(f.ws, f.issue, fresh, Some(root), "latest", 10000)
        .await;
    let (status, headers, body) = f.read("fold=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["X-Comments-Truncated"], "true");
    assert_eq!(ids(&body).len(), 1999);
    assert!(!ids(&body).contains(&root));
    assert!(!ids(&body).contains(&fresh));
    assert_no_orphans(&body);
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_default_completion_keeps_exact_context_budget() {
    let f = Fixture::new().await;
    let root = Uuid::new_v4();
    f.comment(f.ws, f.issue, root, None, "root", 0).await;
    f.pool.get().await.unwrap().execute(
        "INSERT INTO comment(id,workspace_id,issue_id,author_type,author_id,content,parent_id,created_at) SELECT md5($1::uuid::text||n)::uuid,$2,$3,'member',$4,'old',$1,'2026-11-01T00:00:00Z'::timestamptz+n*interval '1 second' FROM generate_series(1,1999) n",
        &[&root, &f.ws, &f.issue, &f.user],
    ).await.unwrap();
    bulk_roots(&f, 5000, 2001).await;
    let fresh = Uuid::new_v4();
    f.comment(f.ws, f.issue, fresh, Some(root), "latest", 10000)
        .await;
    let (status, headers, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["X-Comments-Truncated"], "true");
    assert_eq!(ids(&body).len(), 4000);
    assert!(ids(&body).contains(&root));
    assert!(ids(&body).contains(&fresh));
    assert_no_orphans(&body);
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_default_completion_rejects_foreign_parents_and_descendants() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let sibling_issue: Uuid = db.query_one(
        "INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES ($1,2,'Sibling','member',$2) RETURNING id",
        &[&f.ws, &f.user],
    ).await.unwrap().get(0);
    let sibling_root = Uuid::new_v4();
    let foreign_root = Uuid::new_v4();
    let local_root = Uuid::new_v4();
    f.comment(f.ws, sibling_issue, sibling_root, None, "other issue", 0)
        .await;
    f.comment(f.foreign, f.other, foreign_root, None, "other workspace", 0)
        .await;
    f.comment(f.ws, f.issue, local_root, None, "local", 0).await;
    let foreign_descendant = Uuid::new_v4();
    f.comment(
        f.foreign,
        f.other,
        foreign_descendant,
        Some(local_root),
        "private child",
        1,
    )
    .await;
    bulk_roots(&f, 100, 2001).await;
    let cross_issue = Uuid::new_v4();
    let cross_workspace = Uuid::new_v4();
    let valid = Uuid::new_v4();
    f.comment(
        f.ws,
        f.issue,
        cross_issue,
        Some(sibling_root),
        "invalid",
        3000,
    )
    .await;
    f.comment(
        f.ws,
        f.issue,
        cross_workspace,
        Some(foreign_root),
        "invalid",
        3001,
    )
    .await;
    f.comment(f.ws, f.issue, valid, Some(local_root), "valid", 3003)
        .await;
    let cycle = [Uuid::new_v4(), Uuid::new_v4()];
    f.comment(f.ws, f.issue, cycle[0], None, "cycle", 3004)
        .await;
    f.comment(f.ws, f.issue, cycle[1], Some(cycle[0]), "cycle", 3005)
        .await;
    db.execute(
        "UPDATE comment SET parent_id=$1 WHERE id=$2",
        &[&cycle[1], &cycle[0]],
    )
    .await
    .unwrap();
    let (status, headers, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["X-Comments-Truncated"], "true");
    assert!(ids(&body).contains(&local_root));
    assert!(ids(&body).contains(&valid));
    for invalid in [
        sibling_root,
        foreign_root,
        foreign_descendant,
        cross_issue,
        cross_workspace,
        cycle[0],
        cycle[1],
    ] {
        assert!(
            !ids(&body).contains(&invalid),
            "foreign or orphaned comment {invalid}"
        );
    }
    assert_no_orphans(&body);
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_default_completion_prunes_ancestor_and_descendant_depth_overflow() {
    let f = Fixture::new().await;
    let old_chain_root = Uuid::new_v4();
    let descendant_root = Uuid::new_v4();
    f.comment(f.ws, f.issue, old_chain_root, None, "old chain", 0)
        .await;
    f.comment(f.ws, f.issue, descendant_root, None, "descendant chain", 0)
        .await;
    let mut parent = old_chain_root;
    for n in 1..=65 {
        let next = Uuid::new_v4();
        f.comment(f.ws, f.issue, next, Some(parent), "ancestor", n)
            .await;
        parent = next;
    }
    let mut descendant_parent = descendant_root;
    for n in 1..=65 {
        let next = Uuid::new_v4();
        f.comment(
            f.ws,
            f.issue,
            next,
            Some(descendant_parent),
            "descendant",
            n,
        )
        .await;
        descendant_parent = next;
    }
    bulk_roots(&f, 100, 2001).await;
    let deep_reply = Uuid::new_v4();
    let shallow_reply = Uuid::new_v4();
    f.comment(f.ws, f.issue, deep_reply, Some(parent), "deep", 3000)
        .await;
    f.comment(
        f.ws,
        f.issue,
        shallow_reply,
        Some(descendant_root),
        "shallow",
        3001,
    )
    .await;
    let (status, headers, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["X-Comments-Truncated"], "true");
    for absent in [old_chain_root, descendant_root, deep_reply, shallow_reply] {
        assert!(!ids(&body).contains(&absent), "incomplete thread {absent}");
    }
    assert_no_orphans(&body);
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_default_completion_accepts_shared_ancestor_at_depth_limit() {
    let f = Fixture::new().await;
    let root = Uuid::new_v4();
    f.comment(f.ws, f.issue, root, None, "root", 0).await;
    let mut parent = root;
    for n in 1..=63 {
        let next = Uuid::new_v4();
        f.comment(f.ws, f.issue, next, Some(parent), "old", n).await;
        parent = next;
    }
    bulk_roots(&f, 100, 2001).await;
    let fresh = [Uuid::new_v4(), Uuid::new_v4()];
    for (i, reply) in fresh.iter().enumerate() {
        f.comment(f.ws, f.issue, *reply, Some(parent), "new", 3000 + i as i32)
            .await;
    }
    let (status, headers, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["X-Comments-Truncated"], "true");
    assert_eq!(ids(&body).len(), 2064);
    assert!(ids(&body).contains(&root));
    assert!(fresh.iter().all(|reply| ids(&body).contains(reply)));
    assert_no_orphans(&body);
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_caps_concurrency_and_snapshot_read_only() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    db.execute("INSERT INTO comment(id,workspace_id,issue_id,author_type,author_id,content,created_at) SELECT md5($1::uuid::text||n)::uuid,$1,$2,'member',$3,'cap row','2026-11-01T00:00:00Z'::timestamptz+n*interval '1 second' FROM generate_series(1,2000) n", &[&f.ws,&f.issue,&f.user]).await.unwrap();
    let (status, headers, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ids(&body).len(), 2000);
    assert!(!headers.contains_key("X-Comments-Truncated"));
    let extra = Uuid::new_v4();
    f.comment(f.ws, f.issue, extra, None, "latest", 4000).await;
    let (status, headers, body) = f.read("").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers["X-Comments-Truncated"], "true");
    assert_eq!(ids(&body).len(), 2000);
    assert_eq!(ids(&body).last(), Some(&extra));
    let (_, headers, body) = f.read("roots_only=true").await;
    assert_eq!(headers["X-Comments-Truncated"], "true");
    assert_eq!(ids(&body).len(), 2000);
    assert_eq!(ids(&body).last(), Some(&extra));
    let (_, headers, body) = f.read("roots_only=true&since=2026-10-31T00:00:00Z").await;
    assert_eq!(headers["X-Comments-Truncated"], "true");
    assert!(!ids(&body).contains(&extra));
    let root = Uuid::new_v4();
    f.comment(f.ws, f.issue, root, None, "thread", 5000).await;
    db.execute("INSERT INTO comment(id,workspace_id,issue_id,author_type,author_id,content,parent_id,created_at) SELECT md5($1::uuid::text||n)::uuid,$2,$3,'member',$4,'reply',$1,'2026-11-02T00:00:00Z'::timestamptz+n*interval '1 second' FROM generate_series(1,2000) n", &[&root,&f.ws,&f.issue,&f.user]).await.unwrap();
    db.execute("UPDATE comment SET resolved_at=now(),resolved_by_type='member',resolved_by_id=$2 WHERE id=$1", &[&root, &f.user])
        .await
        .unwrap();
    let (status, headers, body) = f.read(&format!("thread={root}&fold=true")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ids(&body).len(), 2000);
    assert_eq!(headers["X-Comments-Truncated"], "true");
    assert!(body[0].get("thread_resolved").is_none());
    let (status, headers, body) = f.read("recent=1&fold=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ids(&body), vec![root]);
    assert_eq!(body[0]["thread_resolved"], true);
    assert_eq!(body[0]["folded_count"], 2000);
    assert!(!headers.contains_key("X-Comments-Truncated"));
    let (status, headers, body) = f.read("recent=1").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ids(&body).len(), 2001);
    assert_eq!(ids(&body)[0], root);
    assert_eq!(headers["X-Multica-Next-Before-Id"], root.to_string());
    assert!(!headers.contains_key("X-Comments-Truncated"));
    let before: DateTime<Utc> = db
        .query_one("SELECT updated_at FROM issue WHERE id=$1", &[&f.issue])
        .await
        .unwrap()
        .get(0);
    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let app = f.app.clone();
        let token = f.token.clone();
        let url = format!(
            "/api/issues/{}/comments?workspace_id={}&thread={root}&tail=2",
            f.issue, f.ws
        );
        requests.spawn(async move {
            let response = app
                .oneshot(
                    Request::builder()
                        .uri(url)
                        .header("authorization", format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        });
    }
    while let Some(result) = requests.join_next().await {
        result.unwrap();
    }
    assert_eq!(
        db.query_one("SELECT updated_at FROM issue WHERE id=$1", &[&f.issue])
            .await
            .unwrap()
            .get::<_, DateTime<Utc>>(0),
        before
    );
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_recent_activity_ranking_precedes_since_filter() {
    let f = Fixture::new().await;
    let roots: Vec<Uuid> = (1..=3)
        .map(|n| Uuid::from_u128(0x7a510000000040008000000000000000 + n))
        .collect();
    let replies: Vec<Uuid> = (1..=3)
        .map(|n| Uuid::from_u128(0x7a520000000040008000000000000000 + n))
        .collect();
    for (n, root) in roots.iter().enumerate() {
        f.comment(f.ws, f.issue, *root, None, "root", n as i32)
            .await;
        f.comment(
            f.ws,
            f.issue,
            replies[n],
            Some(*root),
            "reply",
            if n == 0 { 10 } else { 20 },
        )
        .await;
    }
    for (query, expected, cursor) in [
        (
            "recent=2",
            vec![roots[1], replies[1], roots[2], replies[2]],
            true,
        ),
        (
            "recent=3&since=2026-11-01T00:00:15Z",
            vec![replies[1], replies[2]],
            false,
        ),
        (
            "recent=2&since=2026-11-01T00:00:15Z",
            vec![replies[1], replies[2]],
            true,
        ),
        (
            "recent=3&since=2026-11-01T00:00:21Z",
            Vec::<Uuid>::new(),
            false,
        ),
    ] {
        let (status, headers, body) = f.read(query).await;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        assert_eq!(
            headers.contains_key("X-Multica-Next-Before"),
            cursor,
            "{query}"
        );
        assert_eq!(ids(&body), expected, "{query}");
    }
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL"]
async fn comments_supplements_over_twenty_thousand_complete_and_isolated() {
    let f = Fixture::new().await;
    let root = Uuid::new_v4();
    f.comment(f.ws, f.issue, root, None, "steer", 1).await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "INSERT INTO task_supplement(task_id,workspace_id,issue_id,comment_id,author_id,client_request_id,status,created_at) SELECT md5($1::uuid::text||n)::uuid,$2,$3,$4,$5,md5($4::uuid::text||n)::uuid,'delivered','2026-11-01T01:00:00Z'::timestamptz+(n/3)*interval '1 microsecond' FROM generate_series(1,20001) n",
        &[&f.issue, &f.ws, &f.issue, &root, &f.user],
    ).await.unwrap();
    // The same comment UUID outside the selected issue/workspace cannot
    // contribute a receipt, even if malformed legacy rows exist.
    db.execute(
        "INSERT INTO task_supplement(task_id,workspace_id,issue_id,comment_id,author_id,client_request_id,status) VALUES ($1,$2,$3,$4,$5,$6,'failed')",
        &[&Uuid::new_v4(), &f.foreign, &f.other, &root, &f.user, &Uuid::new_v4()],
    ).await.unwrap();
    let expected: Vec<Uuid> = db.query(
        "SELECT task_id FROM task_supplement WHERE workspace_id=$1 AND issue_id=$2 AND comment_id=$3 ORDER BY created_at,task_id",
        &[&f.ws,&f.issue,&root],
    ).await.unwrap().iter().map(|row| row.get(0)).collect();
    assert_eq!(expected.len(), 20001);
    drop(db);
    let (status, _, body) = f.read("roots_only=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let receipts = body[0]["supplements"].as_array().unwrap();
    assert_eq!(receipts.len(), 20001);
    let actual: Vec<Uuid> = receipts
        .iter()
        .map(|receipt| Uuid::parse_str(receipt["task_id"].as_str().unwrap()).unwrap())
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(body[0]["supplement_task_id"], receipts[0]["task_id"]);
    assert_eq!(body[0]["supplement_status"], "delivered");
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn comments_recent_over_twenty_thousand_is_complete_and_cursor_isolated() {
    let f = Fixture::new().await;
    let old = Uuid::new_v4();
    let root = Uuid::new_v4();
    f.comment(f.ws, f.issue, old, None, "older thread", -100)
        .await;
    f.comment(f.ws, f.issue, root, None, "root", 0).await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "INSERT INTO comment(id,workspace_id,issue_id,author_type,author_id,content,parent_id,created_at) SELECT md5($1::uuid::text||n)::uuid,$2,$3,'member',$4,'reply',$1,'2026-11-01T00:00:01Z'::timestamptz+(n/3)*interval '1 microsecond' FROM generate_series(1,20001) n",
        &[&root,&f.ws,&f.issue,&f.user],
    ).await.unwrap();
    // Malformed cross-tenant child never joins the selected thread.
    db.execute(
        "INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content,parent_id,created_at) VALUES ($1,$2,'member',$3,'PRIVATE',$4,'2026-11-03T00:00:00Z')",
        &[&f.foreign,&f.other,&f.user,&root],
    ).await.unwrap();
    let expected:Vec<Uuid> = db.query(
        "SELECT id FROM comment WHERE workspace_id=$1 AND issue_id=$2 AND (id=$3 OR parent_id=$3) ORDER BY created_at,id",
        &[&f.ws,&f.issue,&root],
    ).await.unwrap().iter().map(|row| row.get(0)).collect();
    drop(db);
    assert_eq!(expected.len(), 20002);
    let (status, headers, body) = f.read("recent=1").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ids(&body), expected);
    assert_no_orphans(&body);
    assert_eq!(headers["X-Multica-Next-Before-Id"], root.to_string());
    let query = format!(
        "recent=1&before={}&before-id={}",
        headers["X-Multica-Next-Before"].to_str().unwrap(),
        root
    );
    let (status, _, body) = f.read(&query).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ids(&body), vec![old]);
}
