//! Response enrichment must not roll back a committed comment mutation.
use super::*;

async fn fault(db: &tokio_postgres::Client, table: &str, column: &str, fails: bool) {
    let columns: Vec<String> = db.query(
        "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position",
        &[&table],
    ).await.unwrap().iter().map(|row| {
        let name: String = row.get(0);
        if name == column { format!("issue_write_comment_read_failure(\"{name}\") AS \"{name}\"") }
        else { format!("\"{name}\"") }
    }).collect();
    db.batch_execute(&format!(
        "ALTER TABLE {table} RENAME TO issue_write_{table}_backing; CREATE VIEW {table} AS SELECT {} FROM issue_write_{table}_backing",
        columns.join(","),
    )).await.unwrap();
    let result = db
        .query(&format!("SELECT {column} FROM {table} LIMIT 1"), &[])
        .await;
    if fails {
        assert_eq!(
            result.unwrap_err().as_db_error().unwrap().message(),
            "synthetic comment projection failure"
        );
    } else {
        assert!(result.is_ok());
    }
}

async fn restore(db: &tokio_postgres::Client, table: &str) {
    db.batch_execute(&format!(
        "DROP VIEW {table}; ALTER TABLE issue_write_{table}_backing RENAME TO {table}",
    ))
    .await
    .unwrap();
}

fn projection(comment: &Value, reactions_fail: bool, attachments_fail: bool) {
    assert_eq!(
        comment["reactions"].as_array().unwrap().len(),
        usize::from(!reactions_fail),
        "{comment}"
    );
    assert_eq!(
        comment["attachments"].as_array().unwrap().len(),
        usize::from(!attachments_fail),
        "{comment}"
    );
}

pub(super) async fn run(
    db: &deadpool_postgres::Object,
    app: &Router,
    token: &str,
    sink: &Arc<Sink>,
    runtime: &Arc<Runtime>,
    scope: (Uuid, Uuid),
) {
    let (ws, member) = scope;
    db.batch_execute("CREATE FUNCTION issue_write_comment_read_failure(text) RETURNS text LANGUAGE plpgsql STABLE AS $$ BEGIN RAISE EXCEPTION 'synthetic comment projection failure'; END $$").await.unwrap();
    for (reactions_fail, attachments_fail) in
        [(true, false), (false, true), (true, true), (false, false)]
    {
        let require_commit = !reactions_fail && !attachments_fail;
        if require_commit {
            // Local actor stamps exist only inside the write transaction. A
            // savepoint fallback inside that transaction would lose nonempty
            // projections here, so this case pins the actual commit boundary.
            db.batch_execute("CREATE OR REPLACE FUNCTION issue_write_comment_read_failure(text) RETURNS text LANGUAGE plpgsql STABLE AS $$ BEGIN IF NULLIF(current_setting('multica.actor_type',true),'') IS NOT NULL THEN RAISE EXCEPTION 'projection before commit'; END IF; RETURN $1; END $$").await.unwrap();
        }
        let issue: Uuid = db.query_one(
            "INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Projection isolation','member',$2,(SELECT COALESCE(MAX(number),0)+1 FROM issue WHERE workspace_id=$1)) RETURNING id",
            &[&ws,&member],
        ).await.unwrap().get(0);
        let mut comments = vec![];
        for parent in [None, Some(0)] {
            let parent_id = parent.map(|i| comments[i]);
            let id: Uuid = db.query_one("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content,type,parent_id) VALUES ($1,$2,'member',$3,'before','comment',$4) RETURNING id", &[&ws,&issue,&member,&parent_id]).await.unwrap().get(0);
            comments.push(id);
            db.execute("INSERT INTO comment_reaction(workspace_id,comment_id,actor_type,actor_id,emoji) VALUES ($1,$2,'member',$3,'check')", &[&ws,&id,&member]).await.unwrap();
            db.execute("INSERT INTO attachment(workspace_id,issue_id,comment_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,$2,$3,'member',$4,'existing.txt','synthetic://projection-existing','text/plain',2)", &[&ws,&issue,&id,&member]).await.unwrap();
        }
        let (root, reply) = (comments[0], comments[1]);
        let pending: Uuid = db.query_one("INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,$2,'member',$3,'pending.txt','synthetic://projection-pending','text/plain',2) RETURNING id", &[&ws,&issue,&member]).await.unwrap().get(0);
        let issue_revision: i64 = db
            .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get(0);
        let root_revision: i64 = db
            .query_one("SELECT revision FROM comment WHERE id=$1", &[&root])
            .await
            .unwrap()
            .get(0);
        let reply_revision: i64 = db
            .query_one("SELECT revision FROM comment WHERE id=$1", &[&reply])
            .await
            .unwrap()
            .get(0);
        let resolution_events = sink.resolved.lock().unwrap().len();
        let changed_events = sink.changed.lock().unwrap().len();
        let created_events = sink.created.lock().unwrap().len();
        let created_actions = runtime.0.lock().unwrap().len();
        let changed_actions = runtime.1.lock().unwrap().len();
        if reactions_fail || require_commit {
            fault(db, "comment_reaction", "emoji", reactions_fail).await;
        }
        if attachments_fail || require_commit {
            fault(db, "attachment", "filename", attachments_fail).await;
        }
        let resolution = |id| format!("/api/comments/{id}/resolve?workspace_id={ws}");
        let edit = |id| format!("/api/comments/{id}?workspace_id={ws}");

        for expected_events in [resolution_events + 1, resolution_events + 1] {
            let (status, body) = call(app, "POST", &resolution(root), token, json!({})).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "resolve must survive projection failure: {body}"
            );
            projection(&body, reactions_fail, attachments_fail);
            assert_eq!(body["revision"], root_revision + 1);
            assert_eq!(body["resolved_by_id"], json!(member));
            assert!(!body["resolved_at"].is_null());
            assert_eq!(sink.resolved.lock().unwrap().len(), expected_events);
        }
        let (status, body) = call(app, "POST", &resolution(reply), token, json!({})).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        projection(&body, reactions_fail, attachments_fail);
        assert_eq!(body["revision"], reply_revision + 1);
        {
            let events = sink.resolved.lock().unwrap();
            assert_eq!(events.len(), resolution_events + 3);
            for (event, id, kind, revision) in [
                (
                    &events[resolution_events + 1],
                    root,
                    "comment:unresolved",
                    root_revision + 2,
                ),
                (
                    &events[resolution_events + 2],
                    reply,
                    "comment:resolved",
                    reply_revision + 1,
                ),
            ] {
                assert_eq!(event.event_type, kind);
                assert_eq!(event.workspace_id, ws);
                assert_eq!(event.actor_type, "member");
                assert_eq!(event.actor_id, member);
                assert_eq!(event.payload["comment"]["id"], json!(id));
                assert_eq!(event.payload["comment"]["revision"], revision);
                projection(&event.payload["comment"], reactions_fail, attachments_fail);
            }
        }
        for _ in 0..2 {
            let (status, body) = call(app, "DELETE", &resolution(reply), token, json!({})).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            projection(&body, reactions_fail, attachments_fail);
            assert_eq!(body["revision"], reply_revision + 2);
            assert!(body["resolved_at"].is_null());
            assert_eq!(sink.resolved.lock().unwrap().len(), resolution_events + 4);
        }
        let (status, body) = call(
            app,
            "PUT",
            &edit(reply),
            token,
            json!({"content":"after projection fault","expected_revision":reply_revision + 2}),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "edit must survive projection failure: {body}"
        );
        projection(&body, reactions_fail, attachments_fail);
        assert_eq!(body["revision"], reply_revision + 3);
        assert_eq!(body["issue_revision"], issue_revision + 1);
        assert_eq!(body["content"], "after projection fault");
        assert_eq!(runtime.1.lock().unwrap().len(), changed_actions + 1);
        {
            let events = sink.changed.lock().unwrap();
            assert_eq!(events.len(), changed_events + 1);
            assert_eq!(events.last().unwrap().payload["comment"], body);
        }
        let path = format!("/api/issues/{issue}/comments?workspace_id={ws}");
        let (status, body) = call(
            app,
            "POST",
            &path,
            token,
            json!({"content":"created despite projection fault","attachment_ids":[pending]}),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "create must survive projection failure: {body}"
        );
        assert_eq!(body["reactions"], json!([]));
        assert_eq!(
            body["attachments"].as_array().unwrap().len(),
            usize::from(!attachments_fail)
        );
        assert_eq!(body["issue_revision"], issue_revision + 2);
        let created = Uuid::parse_str(body["id"].as_str().unwrap()).unwrap();
        assert_eq!(
            db.query_one("SELECT comment_id FROM attachment WHERE id=$1", &[&pending])
                .await
                .unwrap()
                .get::<_, Option<Uuid>>(0),
            Some(created)
        );
        assert_eq!(
            db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
                .await
                .unwrap()
                .get::<_, i64>(0),
            issue_revision + 2
        );
        assert_eq!(runtime.0.lock().unwrap().len(), created_actions + 1);
        {
            let events = sink.created.lock().unwrap();
            assert_eq!(events.len(), created_events + 1);
            assert_eq!(events.last().unwrap().payload["comment"], body);
        }
        let (status, body) = call(app, "DELETE", &edit(root), token, json!({})).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
        assert_eq!(runtime.1.lock().unwrap().len(), changed_actions + 2);
        {
            let actions = runtime.1.lock().unwrap();
            let action = actions.last().unwrap();
            assert!(action.deleted);
            assert_eq!(
                action.deleted_attachment_urls,
                vec!["synthetic://projection-existing"]
            );
        }
        {
            let events = sink.changed.lock().unwrap();
            assert_eq!(events.len(), changed_events + 2);
            let comment = &events.last().unwrap().payload["comment"];
            assert!(comment["deleted_at"].is_string());
            assert_eq!(comment["content"], "");
            assert_eq!(comment["reactions"], json!([]));
            assert_eq!(comment["attachments"], json!([]));
        }
        let durable = db.query_one("SELECT revision,content,deleted_at IS NOT NULL AS deleted,NOT EXISTS(SELECT 1 FROM attachment WHERE comment_id=$1) AS attachments_removed,NOT EXISTS(SELECT 1 FROM comment_reaction WHERE comment_id=$1) AS reactions_removed FROM comment WHERE id=$1", &[&root]).await.unwrap();
        assert_eq!(durable.get::<_, i64>("revision"), root_revision + 3);
        assert_eq!(durable.get::<_, String>("content"), "");
        assert!(durable.get::<_, bool>("deleted"));
        assert!(durable.get::<_, bool>("attachments_removed"));
        assert!(durable.get::<_, bool>("reactions_removed"));
        assert_eq!(
            db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
                .await
                .unwrap()
                .get::<_, i64>(0),
            issue_revision + 3
        );
        // Deferred failure occurs at COMMIT, not at UPDATE. No event may leak.
        db.batch_execute(&format!("CREATE FUNCTION issue_write_comment_write_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic comment write failure'; END $$; CREATE CONSTRAINT TRIGGER issue_write_comment_write_failure AFTER UPDATE ON comment DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (OLD.id='{reply}'::uuid) EXECUTE FUNCTION issue_write_comment_write_failure()" )).await.unwrap();
        let failed = call(app, "POST", &resolution(reply), token, json!({})).await;
        assert_eq!(
            failed,
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error":"failed to resolve comment"})
            )
        );
        assert_eq!(
            db.query_one("SELECT revision FROM comment WHERE id=$1", &[&reply])
                .await
                .unwrap()
                .get::<_, i64>(0),
            reply_revision + 3
        );
        assert_eq!(sink.resolved.lock().unwrap().len(), resolution_events + 4);
        db.batch_execute("DROP TRIGGER issue_write_comment_write_failure ON comment; DROP FUNCTION issue_write_comment_write_failure()").await.unwrap();
        if reactions_fail || require_commit {
            restore(db, "comment_reaction").await;
        }
        if attachments_fail || require_commit {
            restore(db, "attachment").await;
        }
        let (status, body) = call(app, "POST", &resolution(reply), token, json!({})).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        projection(&body, false, false);
    }
    db.batch_execute("DROP FUNCTION issue_write_comment_read_failure(text)")
        .await
        .unwrap();
}
