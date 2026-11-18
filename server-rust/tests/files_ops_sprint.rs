//! Disposable DB and local-object HTTP checks for attachment boundaries.
use axum::{
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{
        ApiState,
        files_ops_sprint::{
            AttachmentEvent, AttachmentEventSink, CLEANUP_MIGRATION, FeedbackEvent,
            FeedbackEventSink, LocalObjects, ObjectStore, USER_ASSETS_INDEX_MIGRATION,
            USER_ASSETS_MIGRATION, USER_ASSETS_RETENTION_MIGRATION, accept_avatar_url,
            accept_avatar_url_in_transaction, feedback_metric_kind, feedback_metric_platform,
            reconcile_cleanup, reconcile_user_assets, resolve_avatar_url, retire_replaced_avatar,
        },
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

const SECRET: &[u8] = b"files-ops-synthetic-test-secret";
#[derive(Default)]
struct Sink(Mutex<Vec<AttachmentEvent>>, Mutex<Vec<(Uuid, Uuid)>>);
impl AttachmentEventSink for Sink {
    fn publish_attachment_changed(&self, event: AttachmentEvent) {
        self.0.lock().unwrap().push(event);
    }
    fn request_wakeup_dispatch(&self, workspace_id: Uuid, issue_id: Uuid) {
        self.1.lock().unwrap().push((workspace_id, issue_id));
    }
}
#[derive(Default)]
struct FeedbackSink(Mutex<Vec<FeedbackEvent>>);
impl FeedbackEventSink for FeedbackSink {
    fn record_feedback(&self, event: FeedbackEvent) {
        self.0.lock().unwrap().push(event);
    }
}
async fn call(
    app: &axum::Router,
    method: &str,
    url: &str,
    token: Option<&str>,
    body: Vec<u8>,
    content_type: Option<&str>,
    range: Option<&str>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut builder = Request::builder().method(method).uri(url);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(ct) = content_type {
        builder = builder.header("content-type", ct);
    }
    if let Some(range) = range {
        builder = builder.header("range", range);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, headers, bytes)
}
async fn upload_with_workspace_headers(
    app: &axum::Router,
    url: &str,
    token: &str,
    body: Vec<u8>,
    slug: Option<&str>,
    id: Option<Uuid>,
) -> StatusCode {
    let mut builder = Request::builder()
        .method("POST")
        .uri(url)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "multipart/form-data; boundary=filesops");
    if let Some(slug) = slug {
        builder = builder.header("x-workspace-slug", slug);
    }
    if let Some(id) = id {
        builder = builder.header("x-workspace-id", id.to_string());
    }
    app.clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap()
        .status()
}

fn jwt(user: Uuid, expires: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":expires}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}
fn form(data: &str, issue: Option<Uuid>) -> Vec<u8> {
    form_bytes(data.as_bytes(), "note.txt", issue)
}
fn form_bytes(data: &[u8], filename: &str, issue: Option<Uuid>) -> Vec<u8> {
    form_linked(data, filename, issue, None)
}
fn form_linked(data: &[u8], filename: &str, issue: Option<Uuid>, comment: Option<Uuid>) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, id) in [("issue_id", issue), ("comment_id", comment)] {
        if let Some(id) = id {
            body.extend_from_slice(
                format!(
                    "--filesops\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{id}\r\n"
                )
                .as_bytes(),
            );
        }
    }
    body.extend_from_slice(format!(
        "--filesops\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
    ).as_bytes());
    body.extend_from_slice(data);
    body.extend_from_slice(b"\r\n--filesops--\r\n");
    body
}

async fn feedback_checks(
    app: &axum::Router,
    db: &tokio_postgres::Client,
    token: &str,
    user: Uuid,
    ws: Uuid,
    other: Uuid,
    sink: &FeedbackSink,
) {
    let url = "/api/feedback";
    let ct = Some("application/json");
    let body = serde_json::to_vec(
        &json!({"message":"  synthetic feedback ![image](https://invalid.example/a.png)  ",
        "kind":"bug", "workspace_id":ws, "url":"https://invalid.example/route?auth_code=synthetic-secret#private"}),
    )
    .unwrap();
    assert_eq!(
        call(app, "POST", url, None, body.clone(), ct, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    for bad in [
        json!({"message":" "}),
        json!({"message":"x".repeat(10_001)}),
        json!({"message":"valid", "url":"/route?".to_string() + &"a".repeat(2048)}),
        json!({"message":"valid", "url":"/route\nsecret"}),
        json!({"message":"valid", "context":{"kind":"unknown","trigger":"route","error":{"name":"Error","message":"test"}}}),
    ] {
        assert_eq!(
            call(
                app,
                "POST",
                url,
                Some(token),
                serde_json::to_vec(&bad).unwrap(),
                ct,
                None
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    for headers in [
        vec![("x-client-platform", "x".repeat(513))],
        vec![
            ("x-client-platform", "desktop".into()),
            ("x-client-platform", "spoofed".into()),
        ],
    ] {
        let mut builder = Request::builder()
            .method("POST")
            .uri(url)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json");
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::from(body.clone())).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert!(sink.0.lock().unwrap().is_empty());
    // Go decodes whatever the body is with json.NewDecoder and never looks at
    // Content-Type. Malformed/wrong-type bodies are Go's 400 "invalid request
    // body" — never axum's 415/422 pair. A valid no-Content-Type success is
    // asserted below after restoring workspace membership.
    for (bad_ct, bad_body) in [
        // Non-JSON media type: Go ignores the header and fails on the body.
        (Some("text/plain"), &b"message=not-json"[..]),
        // Explicit null is Go's zero value, so the required-field check fires.
        (ct, br#"{"message":null}"#),
        // A string field carrying a number is a decode failure.
        (ct, br#"{"message":7}"#),
        // A present but non-object context fails the decode.
        (ct, br#"{"message":"valid","context":"nope"}"#),
        // Struct-typed context field set to null keeps zero values, which
        // fails validation rather than the decoder.
        (
            ct,
            br#"{"message":"valid","context":{"kind":"desktop_route_error","trigger":"route","error":null}}"#,
        ),
    ] {
        assert_eq!(
            call(app, "POST", url, Some(token), bad_body.to_vec(), bad_ct, None)
                .await
                .0,
            StatusCode::BAD_REQUEST,
            "{}",
            String::from_utf8_lossy(bad_body)
        );
    }
    // The 64 KiB body cap is Go's MaxBytesReader: exceeding it is a decode
    // failure, answered as 400 rather than axum's 413.
    let oversized = serde_json::to_vec(&json!({"message": "x".repeat(64 * 1024)})).unwrap();
    assert_eq!(
        call(app, "POST", url, Some(token), oversized, ct, None)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert!(sink.0.lock().unwrap().is_empty());
    assert_eq!(
        call(
            app,
            "POST",
            url,
            Some(token),
            serde_json::to_vec(&json!({"message":"foreign","workspace_id":other})).unwrap(),
            ct,
            None
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    // A feedback submission must not survive membership revocation while it
    // waits for a workspace member lock. No row or telemetry may be emitted.
    db.batch_execute("BEGIN").await.unwrap();
    db.query_one(
        "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 FOR UPDATE",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let pending = tokio::spawn({
        let app = app.clone();
        let token = token.to_owned();
        let body = body.clone();
        async move {
            call(&app, "POST", url, Some(&token), body, ct, None)
                .await
                .0
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !pending.is_finished(),
        "feedback did not wait for membership lock"
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &user],
    )
    .await
    .unwrap();
    db.batch_execute("COMMIT").await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::FORBIDDEN);
    assert!(sink.0.lock().unwrap().is_empty());
    assert_eq!(
        db.query_one("SELECT count(*) FROM feedback WHERE user_id=$1", &[&user])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    db.batch_execute("CREATE FUNCTION files_ops_feedback_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic feedback insert failure'; END $$; CREATE TRIGGER files_ops_feedback_reject BEFORE INSERT ON feedback FOR EACH ROW EXECUTE FUNCTION files_ops_feedback_reject()").await.unwrap();
    assert_eq!(
        call(app, "POST", url, Some(token), body.clone(), ct, None)
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(sink.0.lock().unwrap().is_empty());
    db.batch_execute("DROP TRIGGER files_ops_feedback_reject ON feedback; DROP FUNCTION files_ops_feedback_reject()").await.unwrap();
    let (status, _, bytes) = call(app, "POST", url, Some(token), body.clone(), ct, None).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let response: Value = serde_json::from_slice(&bytes).unwrap();
    let id: Uuid = response["id"].as_str().unwrap().parse().unwrap();
    let row = db
        .query_one(
            "SELECT message,metadata,workspace_id FROM feedback WHERE id=$1 AND user_id=$2",
            &[&id, &user],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, String>(0),
        "synthetic feedback ![image](https://invalid.example/a.png)"
    );
    let metadata: Value = row.get(1);
    assert_eq!(metadata["url"], "https://invalid.example/route");
    assert!(!metadata.to_string().contains("synthetic-secret"));
    assert!(metadata.get("context").is_none());
    assert_eq!(row.get::<_, Option<Uuid>>(2), Some(ws));
    assert_eq!(sink.0.lock().unwrap()[0].id, id);
    assert_eq!(sink.0.lock().unwrap()[0].kind, "bug");
    assert_eq!(feedback_metric_kind(&sink.0.lock().unwrap()[0].kind), "bug");
    assert!(sink.0.lock().unwrap()[0].has_images);
    // Go preserves the category in the analytics event, but lowercases it for
    // the Prometheus label; missing/blank still defaults to "general" before
    // both sinks receive it. No raw free-form category may become a label.
    let mixed_case = serde_json::to_vec(&json!({
        "message":"diagnostic", "kind":" Bug ", "context":{
            "kind":"desktop_route_error", "trigger":"route-errorElement",
            "error":{"name":"TypeError","message":"synthetic","stack":""}
        }
    }))
    .unwrap();
    let (case_status, _, case_body) =
        call(app, "POST", url, Some(token), mixed_case, None, None).await;
    assert_eq!(case_status, StatusCode::CREATED);
    let case_id: Uuid = serde_json::from_slice::<Value>(&case_body).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let case_metadata: Value = db
        .query_one("SELECT metadata FROM feedback WHERE id=$1", &[&case_id])
        .await
        .unwrap()
        .get(0);
    assert_eq!(case_metadata["context"]["kind"], "desktop_route_error");
    assert_eq!(case_metadata["context"]["error"]["name"], "TypeError");
    assert!(case_metadata["context"]["error"].get("stack").is_none());
    assert_eq!(sink.0.lock().unwrap()[1].kind, "Bug");
    assert_eq!(feedback_metric_kind(&sink.0.lock().unwrap()[1].kind), "bug");
    assert_eq!(
        feedback_metric_platform(&sink.0.lock().unwrap()[1].platform),
        "unknown"
    );
    // Cross-instance atomic cap: 12 parallel requests after two successes leave
    // exactly ten persisted rows, not one successful count-check per request.
    let results = futures_util::future::join_all(
        (0..12).map(|_| call(app, "POST", url, Some(token), body.clone(), ct, None)),
    )
    .await;
    assert_eq!(
        results
            .iter()
            .filter(|r| r.0 == StatusCode::CREATED)
            .count(),
        8
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| r.0 == StatusCode::TOO_MANY_REQUESTS)
            .count(),
        4
    );
    let count: i64 = db
        .query_one("SELECT count(*) FROM feedback WHERE user_id=$1", &[&user])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 10);
    assert_eq!(sink.0.lock().unwrap().len(), 10);
    db.execute("DELETE FROM feedback WHERE user_id=$1", &[&user])
        .await
        .unwrap();
}

async fn comment_checks(
    app: &axum::Router,
    db: &tokio_postgres::Client,
    token: &str,
    (user, ws, issue): (Uuid, Uuid, Uuid),
    sink: &Sink,
) {
    let comment: Uuid = db.query_one(
        "INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content) VALUES($1,$2,'member',$3,'attachment owner') RETURNING id",
        &[&ws, &issue, &user],
    ).await.unwrap().get(0);
    db.execute("INSERT INTO comment_reaction(comment_id,workspace_id,actor_type,actor_id,emoji) VALUES($1,$2,'member',$3,'heart')", &[&comment, &ws, &user]).await.unwrap();
    let path = format!("/api/upload-file?workspace_id={ws}");
    let ct = Some("multipart/form-data; boundary=filesops");
    let form = || form_linked(b"comment-private", "note.txt", None, Some(comment));
    let before_events = sink.0.lock().unwrap().len();
    // Unimplemented attachment owners must not silently produce a standalone
    // upload. Duplicate linkage and conflicting selectors are ambiguous.
    for field in [
        "chat_session_id",
        "chat_message_id",
        "task_id",
        "source_context_id",
    ] {
        let mut body = form();
        body.truncate(body.len() - b"--filesops--\r\n".len());
        body.extend_from_slice(format!(
            "--filesops\r\nContent-Disposition: form-data; name=\"{field}\"\r\n\r\n{}\r\n--filesops--\r\n",
            Uuid::now_v7()
        ).as_bytes());
        assert_eq!(
            call(app, "POST", &path, Some(token), body, ct, None)
                .await
                .0,
            StatusCode::SERVICE_UNAVAILABLE,
            "unsupported {field}"
        );
    }
    for field in ["issue_id", "comment_id"] {
        let mut body = form_linked(b"comment-private", "note.txt", Some(issue), Some(comment));
        body.truncate(body.len() - b"--filesops--\r\n".len());
        body.extend_from_slice(format!(
            "--filesops\r\nContent-Disposition: form-data; name=\"{field}\"\r\n\r\n{}\r\n--filesops--\r\n",
            Uuid::now_v7()
        ).as_bytes());
        assert_eq!(
            call(app, "POST", &path, Some(token), body, ct, None)
                .await
                .0,
            StatusCode::BAD_REQUEST,
            "duplicate {field}"
        );
    }
    let slug: String = db
        .query_one("SELECT slug FROM workspace WHERE id=$1", &[&ws])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        call(
            app,
            "POST",
            &format!(
                "/api/upload-file?workspace_slug={slug}&workspace_id={}",
                Uuid::now_v7()
            ),
            Some(token),
            form(),
            ct,
            None
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    for (url, header_slug, header_id) in [
        (format!("{path}&workspace_id={ws}"), None, None),
        (
            format!("{path}&workspace_slug={slug}"),
            Some("different-workspace"),
            None,
        ),
        (
            format!("{path}&workspace_id={ws}"),
            None,
            Some(Uuid::now_v7()),
        ),
    ] {
        assert_eq!(
            upload_with_workspace_headers(app, &url, token, form(), header_slug, header_id).await,
            StatusCode::BAD_REQUEST,
            "ambiguous workspace selector {url}"
        );
    }
    for (target, body, expected) in [
        (
            path.clone(),
            form_linked(b"wrong", "bad.txt", Some(Uuid::now_v7()), Some(comment)),
            StatusCode::FORBIDDEN,
        ),
        (
            format!("/api/upload-file?workspace_id={}", Uuid::now_v7()),
            form(),
            StatusCode::FORBIDDEN,
        ),
        (
            "/api/upload-file".to_owned(),
            form(),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        assert_eq!(
            call(app, "POST", &target, Some(token), body, ct, None)
                .await
                .0,
            expected
        );
    }
    // The owner lock serializes a tombstone against an upload, even if its
    // earlier existence check raced. A tombstone must never gain attachments.
    db.batch_execute("BEGIN").await.unwrap();
    db.query_one("SELECT id FROM issue WHERE id=$1 FOR UPDATE", &[&issue])
        .await
        .unwrap();
    let (rejected, ()) = tokio::join!(
        call(app, "POST", &path, Some(token), form(), ct, None),
        async {
            db.execute(
                "UPDATE comment SET deleted_at=now() WHERE id=$1",
                &[&comment],
            )
            .await
            .unwrap();
            db.batch_execute("COMMIT").await.unwrap();
        }
    );
    assert_eq!(rejected.0, StatusCode::FORBIDDEN);
    assert_eq!(sink.0.lock().unwrap().len(), before_events);
    db.execute(
        "UPDATE comment SET deleted_at=NULL WHERE id=$1",
        &[&comment],
    )
    .await
    .unwrap();
    let revisions = db.query_one("SELECT i.revision,c.revision FROM issue i JOIN comment c ON c.issue_id=i.id WHERE c.id=$1", &[&comment]).await.unwrap();
    let issue_revision: i64 = revisions.get(0);
    let comment_revision: i64 = revisions.get(1);
    // A revision failure rolls back attachment insert and emits nothing. The
    // pre-written object remains represented by its durable cleanup intent.
    db.batch_execute("CREATE FUNCTION files_ops_comment_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic comment bump failure'; END $$; CREATE TRIGGER files_ops_comment_reject BEFORE UPDATE ON comment FOR EACH ROW EXECUTE FUNCTION files_ops_comment_reject()").await.unwrap();
    assert_eq!(
        call(app, "POST", &path, Some(token), form(), ct, None)
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.batch_execute("DROP TRIGGER files_ops_comment_reject ON comment; DROP FUNCTION files_ops_comment_reject()").await.unwrap();
    let row = db.query_one("SELECT i.revision,c.revision,(SELECT count(*) FROM attachment WHERE comment_id=c.id) FROM issue i JOIN comment c ON c.issue_id=i.id WHERE c.id=$1", &[&comment]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), issue_revision);
    assert_eq!(row.get::<_, i64>(1), comment_revision);
    assert_eq!(row.get::<_, i64>(2), 0);
    assert_eq!(sink.0.lock().unwrap().len(), before_events);
    let (status, _, bytes) = call(app, "POST", &path, Some(token), form(), ct, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let att: Value = serde_json::from_slice(&bytes).unwrap();
    let id: Uuid = att["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(att["issue_id"], issue.to_string());
    assert_eq!(att["comment_id"], comment.to_string());
    {
        let events = sink.0.lock().unwrap();
        let event = events.last().unwrap();
        assert_eq!(event.issue_revision, issue_revision + 1);
        assert_eq!(event.comment_revision, comment_revision + 1);
        assert_eq!(event.comment_id, Some(comment));
        let projection = event.comment.as_ref().unwrap();
        assert_eq!(projection["id"], comment.to_string());
        assert_eq!(projection["revision"], comment_revision + 1);
        assert_eq!(projection["attachments"][0]["id"], id.to_string());
        assert_eq!(projection["reactions"][0]["emoji"], "heart");
    }
    // Tombstoning a comment revokes private and signed reads without relying
    // on the asynchronous attachment deletion/cleanup path.
    let (status, _, meta) = call(
        app,
        "GET",
        &format!("/api/attachments/{id}/download-metadata"),
        Some(token),
        vec![],
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let signed = serde_json::from_slice::<Value>(&meta).unwrap()["download_url"]
        .as_str()
        .unwrap()
        .to_owned();
    db.execute(
        "UPDATE comment SET deleted_at=now() WHERE id=$1",
        &[&comment],
    )
    .await
    .unwrap();
    for (url, auth) in [
        (format!("/api/attachments/{id}/download"), Some(token)),
        (signed.clone(), None),
        (
            format!("/api/attachments/{id}?workspace_id={ws}"),
            Some(token),
        ),
    ] {
        assert_eq!(
            call(app, "GET", &url, auth, vec![], None, None).await.0,
            StatusCode::NOT_FOUND,
            "tombstoned comment still served {url}"
        );
    }
    let (_, _, body) = call(
        app,
        "GET",
        &format!("/api/issues/{issue}/attachments?workspace_id={ws}"),
        Some(token),
        vec![],
        None,
        None,
    )
    .await;
    assert!(!String::from_utf8_lossy(&body).contains(&id.to_string()));
    db.execute(
        "UPDATE comment SET deleted_at=NULL WHERE id=$1",
        &[&comment],
    )
    .await
    .unwrap();
    assert_eq!(
        call(app, "GET", &signed, None, vec![], None, None).await.0,
        StatusCode::OK
    );
    // A foreign-workspace issue cannot keep a capability alive even when
    // the attachment row and bytes still exist. Use a real owner so the
    // disposable DB's legacy foreign key remains satisfied.
    let foreign_ws: Uuid = db.query_one(
        "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Foreign', $1, 'FOPS') RETURNING id",
        &[&format!("files-foreign-{}", Uuid::new_v4())],
    ).await.unwrap().get(0);
    let foreign_issue: Uuid = db.query_one(
        "INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Foreign','member',$2,1) RETURNING id",
        &[&foreign_ws, &user],
    ).await.unwrap().get(0);
    db.execute(
        "UPDATE attachment SET issue_id=$1 WHERE id=$2",
        &[&foreign_issue, &id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(app, "GET", &signed, None, vec![], None, None).await.0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE attachment SET issue_id=$1 WHERE id=$2",
        &[&issue, &id],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM issue WHERE id=$1", &[&foreign_issue])
        .await
        .unwrap();
    db.execute("DELETE FROM workspace WHERE id=$1", &[&foreign_ws])
        .await
        .unwrap();
    let deletion = format!("/api/attachments/{id}?workspace_id={ws}");
    // Delete rollback retains DB row, bytes and revisions; no event or cleanup.
    db.batch_execute("CREATE FUNCTION files_ops_delete_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic delete failure'; END $$; CREATE TRIGGER files_ops_delete_reject BEFORE DELETE ON attachment FOR EACH ROW EXECUTE FUNCTION files_ops_delete_reject()").await.unwrap();
    assert_eq!(
        call(app, "DELETE", &deletion, Some(token), vec![], None, None)
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.batch_execute("DROP TRIGGER files_ops_delete_reject ON attachment; DROP FUNCTION files_ops_delete_reject()").await.unwrap();
    assert_eq!(sink.0.lock().unwrap().len(), before_events + 1);
    let (status, _, bytes) = call(
        app,
        "GET",
        &format!("/api/attachments/{id}/download"),
        Some(token),
        vec![],
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, b"comment-private");
    let (a, b) = tokio::join!(
        call(app, "DELETE", &deletion, Some(token), vec![], None, None),
        call(app, "DELETE", &deletion, Some(token), vec![], None, None)
    );
    assert!(
        (a.0 == StatusCode::NO_CONTENT && b.0 == StatusCode::NOT_FOUND)
            || (b.0 == StatusCode::NO_CONTENT && a.0 == StatusCode::NOT_FOUND)
    );
    {
        let events = sink.0.lock().unwrap();
        assert_eq!(events.len(), before_events + 2);
        let event = events.last().unwrap();
        assert_eq!(event.issue_revision, issue_revision + 2);
        assert_eq!(event.comment_revision, comment_revision + 2);
        let projection = event.comment.as_ref().unwrap();
        assert_eq!(projection["attachments"], json!([]));
        assert_eq!(projection["reactions"][0]["emoji"], "heart");
    }
    db.execute(
        "DELETE FROM comment_reaction WHERE comment_id=$1",
        &[&comment],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM comment WHERE id=$1", &[&comment])
        .await
        .unwrap();
}

async fn single_connection_asset_lifecycle(
    config: Config,
    db: &tokio_postgres::Client,
    objects: Arc<LocalObjects>,
    token: &str,
) {
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(1)
            .build()
            .unwrap(),
    );
    let state = ApiState::new(pool.clone(), SECRET.to_vec()).with_files_storage(objects.clone());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state,
    );
    let (status, _, bytes) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        call(
            &app,
            "POST",
            "/api/upload-file",
            Some(token),
            form("single connection", None),
            Some("multipart/form-data; boundary=filesops"),
            None,
        ),
    )
    .await
    .expect("user upload retained a connection while waiting for another slot");
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let asset: Value = serde_json::from_slice(&bytes).unwrap();
    let id: Uuid = asset["id"].as_str().unwrap().parse().unwrap();
    let key = objects
        .key_from_url(asset["url"].as_str().unwrap())
        .unwrap();
    assert_eq!(objects.read(&key, 0, 64).unwrap(), b"single connection");
    // A new explicit deletion must clear an older cooldown and reconcile now,
    // without retaining the only DB connection after its transaction commits.
    db.execute(
        "UPDATE user_asset_object SET cleanup_checked_at=clock_timestamp()+interval '1 hour' WHERE id=$1",
        &[&id],
    )
    .await
    .unwrap();
    let deletion = format!("/api/user-assets/{id}");
    let (status, _, _) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        call(&app, "DELETE", &deletion, Some(token), vec![], None, None),
    )
    .await
    .expect("user retirement retained its connection while invoking reconciliation");
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        db.query_one("SELECT count(*) FROM user_asset_object WHERE id=$1", &[&id])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0,
        "new retirement did not reset the cleanup cooldown"
    );
    assert!(objects.read(&key, 0, 1).is_err());
    assert_eq!(
        call(&app, "DELETE", &deletion, Some(token), vec![], None, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(pool.status().available, 1);
}

async fn parent_deletion_cleanup(
    db: &tokio_postgres::Client,
    state: &ApiState,
    objects: &LocalObjects,
    user: Uuid,
) {
    // Reapplying the owned migration must not duplicate enqueue triggers.
    db.batch_execute(CLEANUP_MIGRATION).await.unwrap();
    assert_eq!(
        db.query_one("SELECT count(*) FROM pg_trigger WHERE tgrelid='attachment'::regclass AND tgname='files_ops_sprint_attachment_cleanup'", &[])
            .await.unwrap().get::<_, i64>(0),
        1
    );
    let ws: Uuid = db.query_one(
        "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Cascade',$1,'CASCADE') RETURNING id",
        &[&format!("files-cascade-{}", Uuid::new_v4())],
    ).await.unwrap().get(0);
    let issue: Uuid = db.query_one(
        "INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Cascade','member',$2,1) RETURNING id",
        &[&ws, &user],
    ).await.unwrap().get(0);
    let comment: Uuid = db.query_one(
        "INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content) VALUES($1,$2,'member',$3,'cascade owner') RETURNING id",
        &[&ws, &issue, &user],
    ).await.unwrap().get(0);
    let comment_id = Uuid::now_v7();
    let issue_id = Uuid::now_v7();
    let standalone_id = Uuid::now_v7();
    let payload = b"cascade-private";
    for (id, owner_issue, owner_comment) in [
        (comment_id, Some(issue), Some(comment)),
        (issue_id, Some(issue), None),
        (standalone_id, None, None),
    ] {
        let key = format!("workspaces/{ws}/{id}.txt");
        let url = objects.object_url(&key).unwrap();
        objects
            .write(&key, payload, "text/plain", "cascade.txt")
            .unwrap();
        db.execute(
            "INSERT INTO attachment(id,workspace_id,issue_id,comment_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,$2,$3,$4,'member',$5,'cascade.txt',$6,'text/plain',$7)",
            &[&id, &ws, &owner_issue, &owner_comment, &user, &url, &(payload.len() as i64)],
        ).await.unwrap();
    }
    // The trigger queues work in the deleting transaction, not external IO.
    // Another connection cannot reclaim its still-uncommitted intent.
    db.batch_execute("BEGIN").await.unwrap();
    db.execute("DELETE FROM comment WHERE id=$1", &[&comment])
        .await
        .unwrap();
    assert!(
        db.query_one(
            "SELECT ready FROM attachment_object_cleanup WHERE attachment_id=$1",
            &[&comment_id],
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    assert_eq!(reconcile_cleanup(state, 100).await.unwrap(), 0);
    db.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM attachment_object_cleanup WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert!(
        db.query_opt("SELECT id FROM attachment WHERE id=$1", &[&comment_id])
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        objects
            .read(&format!("workspaces/{ws}/{comment_id}.txt"), 0, 64)
            .unwrap(),
        payload
    );

    for (delete, owner, id) in [
        ("DELETE FROM comment WHERE id=$1", comment, comment_id),
        ("DELETE FROM issue WHERE id=$1", issue, issue_id),
        ("DELETE FROM workspace WHERE id=$1", ws, standalone_id),
    ] {
        let key = format!("workspaces/{ws}/{id}.txt");
        let url = objects.object_url(&key).unwrap();
        // A committed deletion recaptures OLD's actual ownership and URL,
        // overriding a poisoned intent and clearing any previous cooldown.
        db.execute(
            "INSERT INTO attachment_object_cleanup(attachment_id,workspace_id,object_url,ready,cleanup_checked_at) VALUES($1,$2,'/invalid-object',false,clock_timestamp()+interval '1 hour')",
            &[&id, &ws],
        ).await.unwrap();
        assert_eq!(
            reconcile_cleanup(state, 100).await.unwrap(),
            0,
            "live attachment must not be reclaimed"
        );
        db.execute(delete, &[&owner]).await.unwrap();
        assert!(
            db.query_opt("SELECT id FROM attachment WHERE id=$1", &[&id])
                .await
                .unwrap()
                .is_none()
        );
        let intent = db.query_one(
            "SELECT workspace_id,object_url,ready,cleanup_checked_at IS NULL FROM attachment_object_cleanup WHERE attachment_id=$1", &[&id],
        ).await.unwrap();
        assert_eq!(intent.get::<_, Uuid>(0), ws);
        assert_eq!(intent.get::<_, String>(1), url);
        assert!(intent.get::<_, bool>(2));
        assert!(intent.get::<_, bool>(3));
        assert_eq!(
            objects.read(&key, 0, 64).unwrap(),
            payload,
            "trigger must not delete bytes before reconciliation"
        );
        assert_eq!(reconcile_cleanup(state, 100).await.unwrap(), 1);
        assert!(objects.read(&key, 0, 1).is_err());
        assert!(
            db.query_opt(
                "SELECT attachment_id FROM attachment_object_cleanup WHERE attachment_id=$1",
                &[&id]
            )
            .await
            .unwrap()
            .is_none()
        );
    }
    assert_eq!(reconcile_cleanup(state, 100).await.unwrap(), 0);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback database multica_zbsi76"]
async fn uploads_downloads_capability_privacy_and_cleanup() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi76"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config.clone(), NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    db.batch_execute(CLEANUP_MIGRATION).await.unwrap();
    db.batch_execute(USER_ASSETS_MIGRATION).await.unwrap();
    db.batch_execute(USER_ASSETS_INDEX_MIGRATION).await.unwrap();
    db.batch_execute(USER_ASSETS_RETENTION_MIGRATION)
        .await
        .unwrap();
    let suffix = Uuid::now_v7().simple().to_string();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Files',$1) RETURNING id",
            &[&format!("files-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Other',$1) RETURNING id",
            &[&format!("other-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Files',$1,'FILES') RETURNING id",
            &[&format!("files-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Other',$1,'OTHER') RETURNING id",
            &[&format!("other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Files','member',$2,1) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let root = std::env::temp_dir().join(format!("files-ops-test-{suffix}"));
    let objects = Arc::new(LocalObjects::new(root.clone(), None).unwrap());
    let sink = Arc::new(Sink::default());
    let feedback_sink = Arc::new(FeedbackSink::default());
    let state = ApiState::new(pool.clone(), SECRET.to_vec())
        .with_files_storage(objects.clone())
        .with_attachment_events(sink.clone())
        .with_feedback_events(feedback_sink.clone());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state.clone(),
    );
    let token = jwt(user, chrono::Utc::now().timestamp() + 3600);
    let foreign = jwt(outsider, chrono::Utc::now().timestamp() + 3600);
    single_connection_asset_lifecycle(config, &db, objects.clone(), &token).await;
    parent_deletion_cleanup(&db, &state, &objects, user).await;
    // Deletion wins while the upload waits on its owner row. A still-valid JWT
    // must not turn the pending intent into a ready asset or write its bytes.
    let removed: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Removed',$1) RETURNING id",
            &[&format!("removed-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let removed_token = jwt(removed, chrono::Utc::now().timestamp() + 3600);
    db.batch_execute("BEGIN").await.unwrap();
    db.query_one(
        "SELECT id FROM \"user\" WHERE id=$1 FOR UPDATE",
        &[&removed],
    )
    .await
    .unwrap();
    let pending = tokio::spawn({
        let app = app.clone();
        async move {
            call(
                &app,
                "POST",
                "/api/upload-file",
                Some(&removed_token),
                form("deleted-owner", None),
                Some("multipart/form-data; boundary=filesops"),
                None,
            )
            .await
            .0
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let intents: i64 = db
                .query_one(
                    "SELECT count(*) FROM user_asset_object WHERE user_id=$1",
                    &[&removed],
                )
                .await
                .unwrap()
                .get(0);
            if intents > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("upload did not reach the owner lock");
    db.execute("DELETE FROM \"user\" WHERE id=$1", &[&removed])
        .await
        .unwrap();
    db.batch_execute("COMMIT").await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM user_asset_object WHERE user_id=$1 AND ready",
            &[&removed],
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    feedback_checks(&app, &db, &token, user, ws, other, &feedback_sink).await;
    let path = format!("/api/upload-file?workspace_id={ws}");
    let ct = "multipart/form-data; boundary=filesops";
    // Revoke membership after the request's initial check, while it waits for
    // the issue lock. It must revalidate inside the upload transaction.
    let original_revision: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let original_intents: i64 = db
        .query_one("SELECT count(*) FROM attachment_object_cleanup", &[])
        .await
        .unwrap()
        .get(0);
    db.batch_execute("BEGIN").await.unwrap();
    db.query_one("SELECT id FROM issue WHERE id=$1 FOR UPDATE", &[&issue])
        .await
        .unwrap();
    let pending = tokio::spawn({
        let app = app.clone();
        let path = path.clone();
        let token = token.clone();
        async move {
            call(
                &app,
                "POST",
                &path,
                Some(&token),
                form("revoked", Some(issue)),
                Some(ct),
                None,
            )
            .await
            .0
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let count: i64 = db
                .query_one("SELECT count(*) FROM attachment_object_cleanup", &[])
                .await
                .unwrap()
                .get(0);
            if count > original_intents {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("upload did not reach the owner lock");
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &user],
    )
    .await
    .unwrap();
    db.batch_execute("COMMIT").await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::FORBIDDEN);
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, i64>(0),
        original_revision
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM attachment WHERE issue_id=$1",
            &[&issue]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert!(sink.0.lock().unwrap().is_empty());
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    // A no-workspace human upload must persist an owned ledger row, without
    // granting an unauthenticated raw-URL read. Only a valid signed image URL
    // can be used publicly, and deleting the ledger revokes it immediately.
    let png = b"\x89PNG\r\n\x1a\nsynthetic-avatar";
    let (status, _, bytes) = call(
        &app,
        "POST",
        "/api/upload-file",
        None,
        form_bytes(png, "profile.png", None),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let (status, _, bytes) = call(
        &app,
        "POST",
        "/api/upload-file",
        Some(&token),
        form_bytes(png, "profile.png", None),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let uploaded: Value = serde_json::from_slice(&bytes).unwrap();
    let user_asset: Uuid = uploaded["id"].as_str().unwrap().parse().unwrap();
    let user_raw = uploaded["url"].as_str().unwrap();
    assert_eq!(user_raw, format!("/uploads/users/{user}/{user_asset}.png"));
    assert_eq!(
        call(&app, "GET", user_raw, None, vec![], None, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let user_served = resolve_avatar_url(&state, user_raw).await.unwrap();
    assert_eq!(
        call(&app, "GET", &user_served, None, vec![], None, None)
            .await
            .0,
        StatusCode::OK
    );
    // A signed avatar is not a size-only trust shortcut. A poisoned ledger
    // cannot publish a truncated user image, and an empty raster is not valid.
    for stale in [png.len() as i64 - 1, 0] {
        db.execute(
            "UPDATE user_asset_object SET size_bytes=$1 WHERE id=$2",
            &[&stale, &user_asset],
        )
        .await
        .unwrap();
        let (status, _, body) = call(&app, "GET", &user_served, None, vec![], None, None).await;
        assert_ne!(status, StatusCode::OK);
        assert_ne!(body, png);
    }
    db.execute(
        "UPDATE user_asset_object SET size_bytes=$1 WHERE id=$2",
        &[&(png.len() as i64), &user_asset],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "GET", &user_served, None, vec![], None, None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        accept_avatar_url(&state, &user_served, "", user, None)
            .await
            .unwrap(),
        user_raw
    );
    // Validation inside a caller transaction cannot checkout a second DB
    // connection: production pools may have only one idle connection.
    let single_pool = Arc::new(
        Pool::builder(Manager::new(
            Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap(),
            NoTls,
        ))
        .max_size(1)
        .build()
        .unwrap(),
    );
    let single_state =
        ApiState::new(single_pool.clone(), SECRET.to_vec()).with_files_storage(objects.clone());
    let mut single_db = single_pool.get().await.unwrap();
    let single_tx = single_db.transaction().await.unwrap();
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            accept_avatar_url_in_transaction(
                &single_state,
                &single_tx,
                &user_served,
                "",
                user,
                None,
            ),
        )
        .await
        .expect("transactional avatar validation tried to checkout a second connection")
        .unwrap(),
        user_raw
    );
    assert!(
        accept_avatar_url_in_transaction(
            &single_state,
            &single_tx,
            &user_served,
            "",
            outsider,
            None,
        )
        .await
        .is_err(),
        "a different user cannot bind the asset",
    );
    single_tx.rollback().await.unwrap();
    drop(single_db);
    // Adoption must hold the asset row through commit. A simultaneous retire
    // cannot revoke the object after preflight but before the new reference.
    let mut adoption_db = pool.get().await.unwrap();
    let adoption = adoption_db.transaction().await.unwrap();
    adoption
        .query_one("SELECT id FROM \"user\" WHERE id=$1 FOR UPDATE", &[&user])
        .await
        .unwrap();
    assert_eq!(
        accept_avatar_url_in_transaction(&state, &adoption, &user_served, "", user, None)
            .await
            .unwrap(),
        user_raw
    );
    adoption
        .execute(
            "UPDATE \"user\" SET avatar_url=$1 WHERE id=$2",
            &[&user_raw, &user],
        )
        .await
        .unwrap();
    let attempted_delete = {
        let app = app.clone();
        let token = token.clone();
        tokio::spawn(async move {
            call(
                &app,
                "DELETE",
                &format!("/api/user-assets/{user_asset}"),
                Some(&token),
                vec![],
                None,
                None,
            )
            .await
            .0
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    assert!(!attempted_delete.is_finished());
    adoption.commit().await.unwrap();
    assert_eq!(attempted_delete.await.unwrap(), StatusCode::CONFLICT);
    db.execute("UPDATE \"user\" SET avatar_url=NULL WHERE id=$1", &[&user])
        .await
        .unwrap();
    // A copied private user-object URL or a valid signed projection is not
    // authority for a different human to publish the asset as their avatar.
    assert!(
        accept_avatar_url(&state, user_raw, user_raw, outsider, None)
            .await
            .is_err()
    );
    assert!(
        accept_avatar_url(&state, user_raw, "", outsider, None)
            .await
            .is_err()
    );
    assert!(
        accept_avatar_url(&state, &user_served, "", outsider, None)
            .await
            .is_err()
    );
    assert!(
        accept_avatar_url(
            &state,
            &format!("/uploads/operators/{user_asset}.png"),
            "",
            user,
            None
        )
        .await
        .is_err()
    );
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM user_asset_object WHERE id=$1 AND user_id=$2 AND ready",
            &[&user_asset, &user],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
    // Non-image standalone uploads have an authenticated download path but
    // cannot be signed as public avatars or exposed through /uploads/users.
    let (status, _, body) = call(
        &app,
        "POST",
        "/api/upload-file",
        Some(&token),
        form_bytes(b"private notes", "notes.txt", None),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text_asset: Value = serde_json::from_slice(&body).unwrap();
    let text_id: Uuid = text_asset["id"].as_str().unwrap().parse().unwrap();
    let text_url = format!("/api/user-assets/{text_id}/download");
    assert_eq!(text_asset["download_url"], text_url);
    assert_eq!(
        call(
            &app,
            "GET",
            text_asset["url"].as_str().unwrap(),
            None,
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert!(
        resolve_avatar_url(&state, text_asset["url"].as_str().unwrap())
            .await
            .is_err()
    );
    let (status, headers, body) =
        call(&app, "GET", &text_url, Some(&token), vec![], None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"private notes");
    assert_eq!(headers["cache-control"], "no-store");
    // Go's text signature accepts non-ASCII bytes; this user-owned upload
    // remains private, but must retain a previewable text MIME in its ledger.
    let vietnamese = "Ghi chú tiếng Việt 📝".as_bytes();
    let (status, _, body) = call(
        &app,
        "POST",
        "/api/upload-file",
        Some(&token),
        form_bytes(vietnamese, "ghi-chu.txt", None),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let international: Value = serde_json::from_slice(&body).unwrap();
    let international_id: Uuid = international["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        db.query_one(
            "SELECT content_type FROM user_asset_object WHERE id=$1",
            &[&international_id],
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "text/plain; charset=utf-8"
    );
    let international_url = international["download_url"].as_str().unwrap();
    let (status, headers, bytes) = call(
        &app,
        "GET",
        international_url,
        Some(&token),
        vec![],
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    assert_eq!(bytes, vietnamese);
    assert_eq!(
        call(
            &app,
            "GET",
            international_url,
            Some(&foreign),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert!(
        resolve_avatar_url(&state, international["url"].as_str().unwrap())
            .await
            .is_err()
    );
    // A stale size must never return a successful, truncated download, even
    // for a middle range that happens to have the right number of bytes.
    db.execute(
        "UPDATE user_asset_object SET size_bytes=size_bytes-1 WHERE id=$1",
        &[&text_id],
    )
    .await
    .unwrap();
    for range in [None, Some("bytes=1-3")] {
        let (status, _, bytes) =
            call(&app, "GET", &text_url, Some(&token), vec![], None, range).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_ne!(bytes, b"private notes");
    }
    db.execute(
        "UPDATE user_asset_object SET size_bytes=0 WHERE id=$1",
        &[&text_id],
    )
    .await
    .unwrap();
    let (status, _, bytes) = call(&app, "GET", &text_url, Some(&token), vec![], None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_ne!(bytes, b"private notes");
    db.execute(
        "UPDATE user_asset_object SET size_bytes=$2 WHERE id=$1",
        &[&text_id, &13_i64],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "GET", &text_url, Some(&foreign), vec![], None, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    // Explicit retirement revokes both private downloads and signed public
    // avatars. Auth, owner scope, bound avatar and poisoned-key rules apply.
    let (status, _, body) = call(
        &app,
        "POST",
        "/api/upload-file",
        Some(&token),
        form_bytes(b"retire me", "old.txt", None),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let retire: Value = serde_json::from_slice(&body).unwrap();
    let retire_id: Uuid = retire["id"].as_str().unwrap().parse().unwrap();
    let retire_path = format!("/api/user-assets/{retire_id}");
    assert_eq!(
        call(&app, "DELETE", &retire_path, None, vec![], None, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &retire_path,
            Some(&foreign),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &retire_path,
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &retire_path,
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{retire_path}/download"),
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM user_asset_object WHERE id=$1",
            &[&retire_id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.execute(
        "UPDATE \"user\" SET avatar_url=$1 WHERE id=$2",
        &[&user_raw, &user],
    )
    .await
    .unwrap();
    let active_path = format!("/api/user-assets/{user_asset}");
    assert_eq!(
        call(
            &app,
            "DELETE",
            &active_path,
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(&app, "GET", &user_served, None, vec![], None, None)
            .await
            .0,
        StatusCode::OK
    );
    db.execute("UPDATE \"user\" SET avatar_url=NULL WHERE id=$1", &[&user])
        .await
        .unwrap();
    db.execute(
        "UPDATE workspace SET avatar_url=$1 WHERE id=$2",
        &[&user_raw, &ws],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &active_path,
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    db.execute("UPDATE workspace SET avatar_url=NULL WHERE id=$1", &[&ws])
        .await
        .unwrap();
    // A shared user avatar cannot be deleted by its uploader while any other
    // human has a raw or signed projection, even though the owner unbound it.
    db.execute(
        "UPDATE \"user\" SET avatar_url=$1 WHERE id=$2",
        &[&user_raw, &outsider],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &active_path,
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    db.execute(
        "UPDATE \"user\" SET avatar_url=$1 WHERE id=$2",
        &[&user_served, &outsider],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &active_path,
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert!(
        !retire_replaced_avatar(&state, user, user_raw, "new avatar")
            .await
            .unwrap()
    );
    db.execute(
        "UPDATE \"user\" SET avatar_url=NULL WHERE id=$1",
        &[&outsider],
    )
    .await
    .unwrap();
    // Replacement cleanup only retires the caller's old upload after the
    // committed avatar update; foreign assets and unchanged values are inert.
    assert!(
        !retire_replaced_avatar(&state, outsider, user_raw, "new avatar")
            .await
            .unwrap()
    );
    assert!(
        !retire_replaced_avatar(&state, user, user_raw, user_raw)
            .await
            .unwrap()
    );
    let (status, _, body) = call(
        &app,
        "POST",
        "/api/upload-file",
        Some(&token),
        form_bytes(png, "old.png", None),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let old: Value = serde_json::from_slice(&body).unwrap();
    let old_id: Uuid = old["id"].as_str().unwrap().parse().unwrap();
    let old_raw = old["url"].as_str().unwrap();
    let old_signed = resolve_avatar_url(&state, old_raw).await.unwrap();
    db.execute(
        "UPDATE \"user\" SET avatar_url=$1 WHERE id=$2",
        &[&old_raw, &user],
    )
    .await
    .unwrap();
    assert!(
        !retire_replaced_avatar(&state, user, old_raw, "new avatar")
            .await
            .unwrap()
    );
    db.execute("UPDATE \"user\" SET avatar_url=NULL WHERE id=$1", &[&user])
        .await
        .unwrap();
    assert!(
        retire_replaced_avatar(&state, user, &old_signed, "new avatar")
            .await
            .unwrap()
    );
    assert_eq!(
        call(&app, "GET", &old_signed, None, vec![], None, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM user_asset_object WHERE id=$1",
            &[&old_id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    // Inject an unlink failure after commitment: the row remains revoked and
    // the reconciler can retry without granting a stale URL.
    let (status, _, body) = call(
        &app,
        "POST",
        "/api/upload-file",
        Some(&token),
        form_bytes(b"blocked", "locked.txt", None),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let blocked: Value = serde_json::from_slice(&body).unwrap();
    let blocked_id: Uuid = blocked["id"].as_str().unwrap().parse().unwrap();
    let blocked_path = format!("/api/user-assets/{blocked_id}");
    let blocked_url = blocked["url"].as_str().unwrap();
    let blocked_key = objects.key_from_url(blocked_url).unwrap();
    db.execute(
        "UPDATE user_asset_object SET object_url='/uploads/operators/poison.txt' WHERE id=$1",
        &[&blocked_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &blocked_path,
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        !db.query_one(
            "SELECT deleting FROM user_asset_object WHERE id=$1",
            &[&blocked_id]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    db.execute(
        "UPDATE user_asset_object SET object_url=$1 WHERE id=$2",
        &[&blocked_url, &blocked_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &blocked_path,
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{blocked_path}/download"),
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert!(objects.read(&blocked_key, 0, 1).is_err());
    let (status, _, body) = call(
        &app,
        "POST",
        "/api/upload-file",
        Some(&token),
        form_bytes(b"retry", "retry.txt", None),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let retry: Value = serde_json::from_slice(&body).unwrap();
    let retry_id: Uuid = retry["id"].as_str().unwrap().parse().unwrap();
    let retry_key = objects
        .key_from_url(retry["url"].as_str().unwrap())
        .unwrap();
    let retry_path = root.join(&retry_key);
    std::fs::remove_file(&retry_path).unwrap();
    std::fs::create_dir(&retry_path).unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/user-assets/{retry_id}"),
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert!(
        db.query_one(
            "SELECT deleting FROM user_asset_object WHERE id=$1",
            &[&retry_id]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/user-assets/{retry_id}/download"),
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    std::fs::remove_dir(&retry_path).unwrap();
    // A failed unlink must not hammer the provider on every scheduler pass.
    assert_eq!(reconcile_user_assets(&state, 100).await.unwrap(), 0);
    assert!(
        db.query_one(
            "SELECT deleting FROM user_asset_object WHERE id=$1",
            &[&retry_id]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    db.execute("UPDATE user_asset_object SET cleanup_checked_at=clock_timestamp()-interval '6 minutes' WHERE id=$1", &[&retry_id]).await.unwrap();
    assert_eq!(reconcile_user_assets(&state, 100).await.unwrap(), 1);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM user_asset_object WHERE id=$1",
            &[&retry_id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let private_url = format!("/api/user-assets/{user_asset}/download");
    assert_eq!(uploaded["download_url"], private_url);
    assert_eq!(
        call(&app, "GET", &private_url, None, vec![], None, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &private_url,
            Some(&foreign),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (status, headers, bytes) = call(
        &app,
        "GET",
        &private_url,
        Some(&token),
        vec![],
        None,
        Some("bytes=0-7"),
    )
    .await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(bytes, &png[..8]);
    assert!(
        headers["content-disposition"]
            .to_str()
            .unwrap()
            .starts_with("attachment")
    );
    assert_eq!(headers["content-range"], format!("bytes 0-7/{}", png.len()));
    assert_eq!(
        call(
            &app,
            "GET",
            &private_url,
            Some(&token),
            vec![],
            None,
            Some("bytes=999-")
        )
        .await
        .0,
        StatusCode::RANGE_NOT_SATISFIABLE
    );
    // Even a poisoned ledger row cannot pivot an authenticated user download
    // into an operator object or an unrelated user/key.
    db.execute(
        "UPDATE user_asset_object SET object_url='/uploads/operators/secret.png' WHERE id=$1",
        &[&user_asset],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "GET", &private_url, Some(&token), vec![], None, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE user_asset_object SET object_url=$1 WHERE id=$2",
        &[&user_raw, &user_asset],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE user_asset_object SET ready=false WHERE id=$1",
        &[&user_asset],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "GET", &private_url, Some(&token), vec![], None, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(reconcile_user_assets(&state, 10).await.unwrap(), 0);
    db.execute("UPDATE user_asset_object SET ready=false,created_at=now()-interval '11 minutes' WHERE id=$1", &[&user_asset]).await.unwrap();
    // A corrupted cleanup intent cannot delete another object or silently
    // discard the ledger row; fixing the URL makes it retryable.
    let victim_key = format!("users/{user}/{}.png", Uuid::now_v7());
    objects
        .write(&victim_key, b"victim", "image/png", "victim.png")
        .unwrap();
    let victim_url = objects.object_url(&victim_key).unwrap();
    db.execute(
        "UPDATE user_asset_object SET object_url=$1 WHERE id=$2",
        &[&victim_url, &user_asset],
    )
    .await
    .unwrap();
    assert_eq!(reconcile_user_assets(&state, 10).await.unwrap(), 0);
    assert_eq!(objects.read(&victim_key, 0, 16).unwrap(), b"victim");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM user_asset_object WHERE id=$1",
            &[&user_asset]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.execute(
        "UPDATE user_asset_object SET object_url=$1 WHERE id=$2",
        &[&user_raw, &user_asset],
    )
    .await
    .unwrap();
    assert_eq!(reconcile_user_assets(&state, 10).await.unwrap(), 0);
    db.execute("UPDATE user_asset_object SET cleanup_checked_at=clock_timestamp()-interval '6 minutes' WHERE id=$1", &[&user_asset]).await.unwrap();
    assert_eq!(reconcile_user_assets(&state, 10).await.unwrap(), 1);
    objects.remove(&victim_key).unwrap();
    assert!(
        objects
            .read(&format!("users/{user}/{user_asset}.png"), 0, 1)
            .is_err()
    );
    assert_eq!(
        call(&app, "GET", &private_url, Some(&token), vec![], None, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "GET", &user_served, None, vec![], None, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (status, _, bytes) = call(
        &app,
        "POST",
        "/api/upload-file",
        Some(&token),
        form_bytes(b"<svg onload='alert(1)'/>", "unsafe.svg", None),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let svg: Value = serde_json::from_slice(&bytes).unwrap();
    let svg_url = svg["url"].as_str().unwrap();
    assert_eq!(
        call(&app, "GET", svg_url, None, vec![], None, None).await.0,
        StatusCode::NOT_FOUND
    );
    assert!(resolve_avatar_url(&state, svg_url).await.is_err());
    // Standalone workspace images are avatar-class. Neither a guessed raw
    // storage URL nor a formerly valid signature can publish bound content.
    let png = b"\x89PNG\r\n\x1a\nsynthetic-avatar";
    let (status, _, bytes) = call(
        &app,
        "POST",
        &path,
        Some(&token),
        form_bytes(png, "avatar.png", None),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let avatar: Value = serde_json::from_slice(&bytes).unwrap();
    let avatar_id: Uuid = avatar["id"].as_str().unwrap().parse().unwrap();
    let raw = avatar["url"].as_str().unwrap();
    let key = objects.key_from_url(raw).unwrap();
    let served = resolve_avatar_url(&state, raw).await.unwrap();
    assert!(served.starts_with("/api/avatars/"));
    assert_eq!(
        accept_avatar_url(&state, &served, "", user, Some(ws))
            .await
            .unwrap(),
        raw
    );
    // Raster formats supported by avatar signing must retain their sniffed
    // MIME at upload, in the ledger and on the capability response.
    for (bytes, filename, mime) in [
        (&b"BM\0\0\0\0\0\0\0\0\0\0\0\0"[..], "image.bmp", "image/bmp"),
        (&b"\0\0\x01\0\x01\0"[..], "image.ico", "image/x-icon"),
        (
            &b"\0\0\0\x14ftypavif\0\0\0\0mif1"[..],
            "image.avif",
            "image/avif",
        ),
    ] {
        let (status, _, body) = call(
            &app,
            "POST",
            &path,
            Some(&token),
            form_bytes(bytes, filename, None),
            Some(ct),
            None,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{filename}: {}",
            String::from_utf8_lossy(&body)
        );
        let uploaded: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(uploaded["content_type"], mime, "{filename}");
        let signed = resolve_avatar_url(&state, uploaded["url"].as_str().unwrap())
            .await
            .unwrap();
        let (status, headers, served) = call(&app, "GET", &signed, None, vec![], None, None).await;
        assert_eq!(status, StatusCode::OK, "{filename}");
        assert_eq!(headers["content-type"], mime, "{filename}");
        assert_eq!(served, bytes, "{filename}");
    }
    // A workspace avatar may be reused by a member in that workspace, but
    // neither an outsider nor a different workspace can claim its storage URL.
    assert!(
        accept_avatar_url(&state, raw, "", outsider, Some(ws))
            .await
            .is_err()
    );
    assert!(
        accept_avatar_url(&state, &served, "", user, Some(other))
            .await
            .is_err()
    );
    assert!(
        accept_avatar_url(&state, &served, &served, user, Some(other))
            .await
            .is_err()
    );
    assert!(
        accept_avatar_url(
            &state,
            "/api/avatars/bad/workspaces/key",
            "",
            user,
            Some(ws)
        )
        .await
        .is_err()
    );
    assert!(accept_avatar_url(&state, raw, "", user, None).await.is_ok());
    assert_eq!(resolve_avatar_url(&state, &served).await.unwrap(), served);
    let (status, headers, body) = call(&app, "GET", &served, None, vec![], None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, png);
    assert_eq!(headers["content-type"], "image/png");
    assert_eq!(headers["content-disposition"], "inline");
    assert_eq!(headers["cache-control"], "no-store");
    for stale in [png.len() as i64 - 1, 0, png.len() as i64 + 1] {
        db.execute(
            "UPDATE attachment SET size_bytes=$1 WHERE id=$2",
            &[&stale, &avatar_id],
        )
        .await
        .unwrap();
        let (status, _, body) = call(&app, "GET", &served, None, vec![], None, None).await;
        assert_ne!(status, StatusCode::OK);
        assert_ne!(body, png);
    }
    db.execute(
        "UPDATE attachment SET size_bytes=$1 WHERE id=$2",
        &[&(png.len() as i64), &avatar_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "GET", &served, None, vec![], None, None).await.0,
        StatusCode::OK
    );
    // Avatar access is capability-only even if unrelated auth headers are bad.
    assert_eq!(
        call(&app, "GET", &served, Some("invalid"), vec![], None, None)
            .await
            .0,
        StatusCode::OK
    );
    let bad = served.replace("/api/avatars/", "/api/avatars/x");
    assert_eq!(
        call(&app, "GET", &bad, None, vec![], None, None).await.0,
        StatusCode::NOT_FOUND
    );
    let (status, headers, body) = call(&app, "GET", raw, None, vec![], None, None).await;
    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(
        headers["location"].to_str().unwrap(),
        format!("/api/attachments/{avatar_id}/download")
    );
    assert_ne!(body, png);
    assert!(resolve_avatar_url(&state, "emoji:star").await.is_ok());
    db.execute(
        "UPDATE attachment SET issue_id=$1 WHERE id=$2",
        &[&issue, &avatar_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "GET", &served, None, vec![], None, None).await.0,
        StatusCode::NOT_FOUND
    );
    assert!(
        accept_avatar_url(&state, raw, "", user, Some(ws))
            .await
            .is_err()
    );
    assert!(resolve_avatar_url(&state, raw).await.is_err());
    // An orphan task/source-context binding is still private: do not rely on
    // chat_session_id alone when protecting downloads or public avatars.
    db.execute(
        "UPDATE attachment SET issue_id=NULL, task_id=$1 WHERE id=$2",
        &[&Uuid::new_v4(), &avatar_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "GET", &served, None, vec![], None, None).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/attachments/{avatar_id}/download-metadata"),
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE attachment SET task_id=NULL, source_context_id=$1 WHERE id=$2",
        &[&Uuid::new_v4(), &avatar_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "GET", &served, None, vec![], None, None).await.0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE attachment SET source_context_id=NULL, content_type='image/svg+xml' WHERE id=$1",
        &[&avatar_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "GET", &served, None, vec![], None, None).await.0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE attachment SET content_type='image/png' WHERE id=$1",
        &[&avatar_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "GET", &served, None, vec![], None, None).await.0,
        StatusCode::OK
    );
    db.execute("DELETE FROM attachment WHERE id=$1", &[&avatar_id])
        .await
        .unwrap();
    assert_eq!(
        call(&app, "GET", &served, None, vec![], None, None).await.0,
        StatusCode::NOT_FOUND
    );
    // Even a direct DB deletion leaves durable storage cleanup, without
    // relying on the HTTP delete handler or a test-only manual unlink.
    assert_eq!(reconcile_cleanup(&state, 100).await.unwrap(), 1);
    assert!(objects.read(&key, 0, 1).is_err());
    assert!(sink.0.lock().unwrap().is_empty());
    assert_eq!(
        call(
            &app,
            "POST",
            &path,
            None,
            form("private", Some(issue)),
            Some(ct),
            None
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &path,
            Some(&foreign),
            form("private", Some(issue)),
            Some(ct),
            None
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/upload-file?workspace_id={other}"),
            Some(&token),
            form("private", Some(issue)),
            Some(ct),
            None
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, _, bytes) = call(
        &app,
        "POST",
        &path,
        Some(&token),
        form("hello-private", Some(issue)),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let att: Value = serde_json::from_slice(&bytes).unwrap();
    let id: Uuid = att["id"].as_str().unwrap().parse().unwrap();
    let preview_url = format!("/api/attachments/{id}/content?workspace_id={ws}");
    let (status, headers, body) =
        call(&app, "GET", &preview_url, Some(&token), vec![], None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"hello-private");
    assert_eq!(headers["cache-control"], "no-store");
    // Preview eligibility must normalize legacy media types and path-bearing
    // filenames exactly like the frozen Go whitelist, without relaxing the
    // authenticated read or the verified object-size check.
    for (kind, filename) in [
        ("Application/JSON; Charset=UTF-8", "payload"),
        ("application/octet-stream", r"nested\MAIN.GO"),
        ("application/octet-stream", "nested/Dockerfile"),
    ] {
        db.execute(
            "UPDATE attachment SET content_type=$1, filename=$2 WHERE id=$3",
            &[&kind, &filename, &id],
        )
        .await
        .unwrap();
        let (status, headers, body) =
            call(&app, "GET", &preview_url, Some(&token), vec![], None, None).await;
        assert_eq!(status, StatusCode::OK, "{kind}/{filename}");
        assert_eq!(headers["cache-control"], "no-store");
        assert_eq!(body, b"hello-private");
    }
    db.execute(
        "UPDATE attachment SET content_type='text/plain; charset=utf-8', filename='private.txt' WHERE id=$1",
        &[&id],
    )
    .await
    .unwrap();
    for (size, expected_status) in [
        (12_i64, StatusCode::BAD_GATEWAY),
        (0, StatusCode::BAD_GATEWAY),
        (3 << 20, StatusCode::PAYLOAD_TOO_LARGE),
    ] {
        db.execute(
            "UPDATE attachment SET size_bytes=$1 WHERE id=$2",
            &[&size, &id],
        )
        .await
        .unwrap();
        let (status, _, body) =
            call(&app, "GET", &preview_url, Some(&token), vec![], None, None).await;
        assert_eq!(status, expected_status);
        assert_ne!(body, b"hello-private");
    }
    db.execute("UPDATE attachment SET size_bytes=13 WHERE id=$1", &[&id])
        .await
        .unwrap();
    assert_eq!(
        call(&app, "GET", &preview_url, Some(&token), vec![], None, None)
            .await
            .0,
        StatusCode::OK
    );
    // A poisoned attachment row cannot pivot metadata, previews, download,
    // or deletion into another object even for a valid workspace member.
    let victim_key = format!("workspaces/{ws}/{}.txt", Uuid::now_v7());
    objects
        .write(&victim_key, b"victim-private", "text/plain", "victim.txt")
        .unwrap();
    let victim_url = objects.object_url(&victim_key).unwrap();
    db.execute(
        "UPDATE attachment SET url=$1 WHERE id=$2",
        &[&victim_url, &id],
    )
    .await
    .unwrap();
    let (list_status, _, list_body) = call(
        &app,
        "GET",
        &format!("/api/issues/{issue}/attachments?workspace_id={ws}"),
        Some(&token),
        vec![],
        None,
        None,
    )
    .await;
    assert_eq!(list_status, StatusCode::OK);
    assert!(!String::from_utf8_lossy(&list_body).contains(&victim_url));
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/attachments/{id}/download-metadata"),
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/attachments/{id}/download"),
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/attachments/{id}/content?workspace_id={ws}"),
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/attachments/{id}?workspace_id={ws}"),
            Some(&token),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(objects.read(&victim_key, 0, 32).unwrap(), b"victim-private");
    assert_eq!(
        db.query_one("SELECT count(*) FROM attachment WHERE id=$1", &[&id])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    db.execute(
        "UPDATE attachment SET url=$1 WHERE id=$2",
        &[&att["url"].as_str().unwrap(), &id],
    )
    .await
    .unwrap();
    objects.remove(&victim_key).unwrap();
    assert_eq!(att["content_type"], "text/plain; charset=utf-8");
    assert_eq!(att["size_bytes"], 13);
    let revision: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(sink.0.lock().unwrap()[0].issue_revision, revision);
    let metadata_url = format!("/api/attachments/{id}/download-metadata");
    assert_eq!(
        call(
            &app,
            "GET",
            &metadata_url,
            Some(&foreign),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&ws, &outsider],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/attachments/{id}?workspace_id={ws}"),
            Some(&foreign),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &outsider],
    )
    .await
    .unwrap();
    let (status, metadata_headers, bytes) =
        call(&app, "GET", &metadata_url, Some(&token), vec![], None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(metadata_headers["cache-control"], "no-store");
    assert_eq!(metadata_headers["referrer-policy"], "no-referrer");
    let metadata: Value = serde_json::from_slice(&bytes).unwrap();
    let signed = metadata["download_url"].as_str().unwrap();
    let forced = metadata["attachment_download_url"].as_str().unwrap();
    assert_ne!(signed, forced);
    for candidate in [
        signed.replace("sig=", "sig=0"),
        signed.replace("exp=", "e%78p="),
        signed.replace("&sig=", "&dl=1&sig="),
        signed.replace("&sig=", "&sig=%"),
        format!("{signed}&unknown=1"),
    ] {
        let (status, headers, bytes) =
            call(&app, "GET", &candidate, None, vec![], None, None).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{candidate}");
        assert_eq!(headers["cache-control"], "no-store");
        assert_eq!(headers["referrer-policy"], "no-referrer");
        assert_ne!(bytes, b"hello-private", "invalid link exposed bytes");
    }
    let (status, headers, body) =
        call(&app, "GET", signed, None, vec![], None, Some("bytes=6-12")).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(&body, b"private");
    assert_eq!(headers["content-range"], "bytes 6-12/13");
    assert_eq!(headers["cache-control"], "no-store");
    for range in ["bytes=0-1,3-4", "bytes=bad", "items=0-4"] {
        let (status, headers, body) =
            call(&app, "GET", signed, None, vec![], None, Some(range)).await;
        assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE, "{range}");
        assert_eq!(headers["content-range"], "bytes */13");
        assert_ne!(body, b"hello-private", "invalid range exposed full object");
    }
    // Two separate Range headers are not a single authorized byte interval.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(signed)
                .header("range", "bytes=0-1")
                .header("range", "bytes=3-4")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(response.headers()["content-range"], "bytes */13");
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_ne!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .as_ref(),
        b"hello-private".as_slice(),
    );
    let (status, headers, body) = call(&app, "GET", forced, None, vec![], None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body, b"hello-private");
    assert!(
        headers["content-disposition"]
            .to_str()
            .unwrap()
            .starts_with("attachment;")
    );
    assert_eq!(
        call(&app, "GET", signed, None, vec![], None, Some("bytes=30-"))
            .await
            .0,
        StatusCode::RANGE_NOT_SATISFIABLE
    );
    let content = format!("/api/attachments/{id}/content?workspace_id={ws}");
    assert_eq!(
        call(&app, "GET", &content, Some(&foreign), vec![], None, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (status, headers, body) =
        call(&app, "GET", &content, Some(&token), vec![], None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body, b"hello-private");
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    let deletion = format!("/api/attachments/{id}?workspace_id={ws}");
    assert_eq!(
        call(
            &app,
            "DELETE",
            &deletion,
            Some(&foreign),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    // A revoked uploader cannot remove an attachment or publish events even
    // when the old JWT is still valid. Restore membership before real deletion.
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let revision_before: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let events_before = sink.0.lock().unwrap().len();
    assert_eq!(
        call(&app, "DELETE", &deletion, Some(&token), vec![], None, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, i64>(0),
        revision_before
    );
    assert_eq!(sink.0.lock().unwrap().len(), events_before);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    // A previous cleanup record must not postpone a new explicit deletion.
    let object_url: String = db
        .query_one("SELECT url FROM attachment WHERE id=$1", &[&id])
        .await
        .unwrap()
        .get(0);
    let object_key = objects.key_from_url(&object_url).unwrap();
    db.execute(
        "INSERT INTO attachment_object_cleanup(attachment_id,workspace_id,object_url,ready,cleanup_checked_at) VALUES($1,$2,$3,false,clock_timestamp()+interval '1 hour')",
        &[&id, &ws, &object_url],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "DELETE", &deletion, Some(&token), vec![], None, None)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&app, "GET", signed, None, vec![], None, None).await.0,
        StatusCode::NOT_FOUND
    );
    let remaining: i64 = db
        .query_one(
            "SELECT count(*) FROM attachment_object_cleanup WHERE attachment_id=$1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        remaining, 0,
        "new deletion did not reset the cleanup cooldown"
    );
    assert!(objects.read(&object_key, 0, 1).is_err());
    assert_eq!(sink.0.lock().unwrap().len(), 2);
    assert_eq!(
        sink.1.lock().unwrap().as_slice(),
        &[(ws, issue), (ws, issue)]
    );
    // Two simultaneous deletes serialize through the owner row. Exactly one
    // wins, one sees 404, and only the committed mutation emits an event.
    let (status, _, bytes) = call(
        &app,
        "POST",
        &path,
        Some(&token),
        form("concurrent", Some(issue)),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let concurrent: Value = serde_json::from_slice(&bytes).unwrap();
    let concurrent_url = format!(
        "/api/attachments/{}?workspace_id={ws}",
        concurrent["id"].as_str().unwrap()
    );
    let (first, second) = tokio::join!(
        call(
            &app,
            "DELETE",
            &concurrent_url,
            Some(&token),
            vec![],
            None,
            None
        ),
        call(
            &app,
            "DELETE",
            &concurrent_url,
            Some(&token),
            vec![],
            None,
            None
        )
    );
    assert!(
        (first.0 == StatusCode::NO_CONTENT && second.0 == StatusCode::NOT_FOUND)
            || (second.0 == StatusCode::NO_CONTENT && first.0 == StatusCode::NOT_FOUND)
    );
    assert_eq!(sink.0.lock().unwrap().len(), 4);
    assert_eq!(sink.1.lock().unwrap().len(), 4);
    let expired = jwt(user, chrono::Utc::now().timestamp() - 3600);
    assert_eq!(
        call(
            &app,
            "GET",
            &metadata_url,
            Some(&expired),
            vec![],
            None,
            None
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    // A failed DB insert must leave a durable orphan intent, not a successful
    // response with an untracked object. Reconcile against synthetic storage.
    db.batch_execute("CREATE FUNCTION files_ops_test_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic attachment insert failure'; END $$; CREATE TRIGGER files_ops_test_reject BEFORE INSERT ON attachment FOR EACH ROW EXECUTE FUNCTION files_ops_test_reject()").await.unwrap();
    let failed = call(
        &app,
        "POST",
        &path,
        Some(&token),
        form("must-be-cleaned", Some(issue)),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(
        failed.0,
        StatusCode::INTERNAL_SERVER_ERROR,
        "{}",
        String::from_utf8_lossy(&failed.2)
    );
    eprintln!(
        "failed-upload response: {}",
        String::from_utf8_lossy(&failed.2)
    );
    db.batch_execute(
        "DROP TRIGGER files_ops_test_reject ON attachment; DROP FUNCTION files_ops_test_reject()",
    )
    .await
    .unwrap();
    let orphan = db
        .query_one(
            "SELECT attachment_id,object_url FROM attachment_object_cleanup WHERE workspace_id=$1 ORDER BY created_at DESC, attachment_id DESC LIMIT 1",
            &[&ws],
        )
        .await
        .unwrap();
    let orphan_id: Uuid = orphan.get(0);
    let orphan_url: String = orphan.get(1);
    assert!(
        objects
            .read(&objects.key_from_url(&orphan_url).unwrap(), 0, 50)
            .is_ok()
    );
    let created: i64 = db
        .query_one("SELECT count(*) FROM attachment WHERE id=$1", &[&orphan_id])
        .await
        .unwrap()
        .get(0);
    assert_eq!(created, 0);
    let victim_key = format!("workspaces/{ws}/{}.txt", Uuid::now_v7());
    objects
        .write(&victim_key, b"victim", "text/plain", "victim.txt")
        .unwrap();
    let victim_url = objects.object_url(&victim_key).unwrap();
    db.execute(
        "UPDATE attachment_object_cleanup SET object_url=$1, ready=true WHERE attachment_id=$2",
        &[&victim_url, &orphan_id],
    )
    .await
    .unwrap();
    let later_id = Uuid::new_v4();
    let later_key = format!("workspaces/{ws}/{later_id}.txt");
    objects
        .write(&later_key, b"later", "text/plain", "later.txt")
        .unwrap();
    let later_url = objects.object_url(&later_key).unwrap();
    db.execute("INSERT INTO attachment_object_cleanup(attachment_id,workspace_id,object_url,ready) VALUES($1,$2,$3,true)", &[&later_id,&ws,&later_url]).await.unwrap();
    assert_eq!(reconcile_cleanup(&state, 1).await.unwrap(), 0);
    assert_eq!(reconcile_cleanup(&state, 1).await.unwrap(), 1);
    assert!(objects.read(&later_key, 0, 1).is_err());
    assert_eq!(objects.read(&victim_key, 0, 16).unwrap(), b"victim");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM attachment_object_cleanup WHERE attachment_id=$1",
            &[&orphan_id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.execute(
        "UPDATE attachment_object_cleanup SET object_url=$1 WHERE attachment_id=$2",
        &[&orphan_url, &orphan_id],
    )
    .await
    .unwrap();
    objects.remove(&victim_key).unwrap();
    db.execute(
        "UPDATE attachment_object_cleanup SET ready=true WHERE attachment_id=$1",
        &[&orphan_id],
    )
    .await
    .unwrap();
    // Repairing a poisoned intent does not bypass the retry delay by itself.
    assert_eq!(reconcile_cleanup(&state, 1).await.unwrap(), 0);
    db.execute("UPDATE attachment_object_cleanup SET cleanup_checked_at=clock_timestamp()-interval '6 minutes' WHERE attachment_id=$1", &[&orphan_id]).await.unwrap();
    assert_eq!(
        reconcile_cleanup(
            &ApiState::new(pool.clone(), SECRET.to_vec()).with_files_storage(objects.clone()),
            1
        )
        .await
        .unwrap(),
        1
    );
    assert!(
        objects
            .read(&objects.key_from_url(&orphan_url).unwrap(), 0, 50)
            .is_err()
    );
    assert_eq!(sink.0.lock().unwrap().len(), 4);
    assert_eq!(
        reconcile_cleanup(
            &ApiState::new(pool.clone(), SECRET.to_vec()).with_files_storage(objects.clone()),
            1
        )
        .await
        .unwrap(),
        0
    );
    assert!(objects.key_from_url(att["url"].as_str().unwrap()).is_ok());
    assert!(
        objects
            .read(
                &objects.key_from_url(att["url"].as_str().unwrap()).unwrap(),
                0,
                50
            )
            .is_err()
    );
    comment_checks(&app, &db, &token, (user, ws, issue), &sink).await;
    db.execute(
        "UPDATE attachment_object_cleanup SET ready=true WHERE workspace_id=$1",
        &[&ws],
    )
    .await
    .unwrap();
    reconcile_cleanup(&state, 100).await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM attachment_object_cleanup WHERE workspace_id=$1",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    // A deleted owner's upload may still be used by another account. Neither
    // stale-owner cleanup nor a concurrent reconciler may delete that avatar.
    let (status, _, body) = call(
        &app,
        "POST",
        "/api/upload-file",
        Some(&token),
        form_bytes(png, "shared.png", None),
        Some(ct),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let shared: Value = serde_json::from_slice(&body).unwrap();
    let shared_id: Uuid = shared["id"].as_str().unwrap().parse().unwrap();
    let shared_url = shared["url"].as_str().unwrap();
    let shared_signed = resolve_avatar_url(&state, shared_url).await.unwrap();
    db.execute(
        "UPDATE \"user\" SET avatar_url=$1 WHERE id=$2",
        &[&shared_signed, &outsider],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE workspace SET avatar_url=$1 WHERE id=$2",
        &[&shared_url, &ws],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap();
    db.execute("DELETE FROM \"user\" WHERE id=$1", &[&user])
        .await
        .unwrap();
    // A signed avatar remains usable for the surviving holder, but the raw
    // upload URL never becomes a public file endpoint.
    assert_eq!(
        call(&app, "GET", &shared_signed, None, vec![], None, None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        resolve_avatar_url(&state, &shared_signed).await.unwrap(),
        shared_signed
    );
    // A pinned orphan at the front of a bounded batch cannot starve a later
    // reclaimable object across scheduler passes.
    let missing_owner = Uuid::new_v4();
    let newer_orphan = Uuid::new_v4();
    let newer_key = format!("users/{missing_owner}/{newer_orphan}.png");
    objects
        .write(&newer_key, png, "image/png", "orphan.png")
        .unwrap();
    let newer_url = objects.object_url(&newer_key).unwrap();
    db.execute("INSERT INTO user_asset_object(id,user_id,object_url,filename,content_type,size_bytes,ready,created_at) VALUES($1,$2,$3,'orphan.png','image/png',$4,true,now()-interval '1 day')", &[&newer_orphan,&missing_owner,&newer_url,&(png.len() as i64)]).await.unwrap();
    db.execute(
        "UPDATE user_asset_object SET created_at=now()-interval '2 days' WHERE id=$1",
        &[&shared_id],
    )
    .await
    .unwrap();
    assert_eq!(reconcile_user_assets(&state, 1).await.unwrap(), 0);
    assert_eq!(reconcile_user_assets(&state, 1).await.unwrap(), 1);
    assert!(objects.read(&newer_key, 0, 1).is_err());
    reconcile_user_assets(&state, 100).await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM user_asset_object WHERE id=$1",
            &[&shared_id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1,
        "orphan cleanup removed an avatar still used by another account"
    );
    assert_eq!(
        objects
            .read(&format!("users/{user}/{shared_id}.png"), 0, 1)
            .unwrap()
            .len(),
        1
    );
    db.execute(
        "UPDATE \"user\" SET avatar_url=NULL WHERE id=$1",
        &[&outsider],
    )
    .await
    .unwrap();
    reconcile_user_assets(&state, 100).await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM user_asset_object WHERE id=$1",
            &[&shared_id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1,
        "orphan cleanup removed a workspace-bound avatar"
    );
    db.execute("UPDATE workspace SET avatar_url=NULL WHERE id=$1", &[&ws])
        .await
        .unwrap();
    // The referenced orphan cooled down; once unbound, a later eligible pass
    // reclaims it without hammering storage in the interim.
    db.execute("UPDATE user_asset_object SET cleanup_checked_at=clock_timestamp()-interval '6 minutes' WHERE id=$1", &[&shared_id]).await.unwrap();
    assert_eq!(
        call(&app, "GET", &shared_signed, None, vec![], None, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert!(resolve_avatar_url(&state, &shared_signed).await.is_err());
    reconcile_user_assets(&state, 100).await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM user_asset_object WHERE id=$1",
            &[&shared_id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0,
        "unbound orphan was not reclaimed"
    );
    db.execute("DELETE FROM workspace WHERE id=$1 OR id=$2", &[&ws, &other])
        .await
        .unwrap();
    db.execute("DELETE FROM \"user\" WHERE id=$1", &[&outsider])
        .await
        .unwrap();
    // The earlier orphan pass reclaimed the remaining ownerless assets.
    assert_eq!(reconcile_user_assets(&state, 100).await.unwrap(), 0);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM user_asset_object WHERE id=$1",
            &[&text_id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert!(
        objects
            .read(&format!("users/{user}/{text_id}.txt"), 0, 1)
            .is_err()
    );
    std::fs::remove_dir_all(root).unwrap();
}
