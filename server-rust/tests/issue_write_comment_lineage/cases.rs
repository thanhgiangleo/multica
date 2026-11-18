//! Source-task provenance stays with the action, never the runtime owner.
use super::*;
use sha2::{Digest, Sha256};

async fn task_token(
    db: &tokio_postgres::Client,
    task: Uuid,
    agent: Uuid,
    ws: Uuid,
    member: Uuid,
) -> String {
    let token = format!("mat_lineage_{}", Uuid::new_v4());
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&member]).await.unwrap();
    token
}

async fn comment(db: &tokio_postgres::Client, ws: Uuid, issue: Uuid, member: Uuid) -> Uuid {
    db.query_one("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content,type) VALUES($1,$2,'member',$3,'synthetic root','comment') RETURNING id", &[&ws,&issue,&member]).await.unwrap().get(0)
}

async fn revision(db: &tokio_postgres::Client, issue: Uuid) -> i64 {
    db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0)
}

pub(super) async fn run(
    db: &tokio_postgres::Client,
    app: &Router,
    member_token: &str,
    sink: &Sink,
    runtime: &Runtime,
    (ws, member, originator): (Uuid, Uuid, Uuid),
) {
    let source: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Lineage source','member',$2,8301) RETURNING id", &[&ws,&member]).await.unwrap().get(0);
    let target: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Lineage target','member',$2,8302) RETURNING id", &[&ws,&member]).await.unwrap().get(0);
    let runtime_id: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Lineage runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,'Lineage author','cloud',$2,$3) RETURNING id", &[&ws,&member,&runtime_id]).await.unwrap().get(0);
    let trigger = comment(db, ws, source, member).await;
    let coalesced = comment(db, ws, source, member).await;
    let unrelated = comment(db, ws, source, member).await;
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,trigger_comment_id,coalesced_comment_ids,originator_user_id,accountable_user_id) VALUES($1,$2,$3,'running',$4,$5,$6,$6) RETURNING id", &[&agent,&runtime_id,&source,&trigger,&vec![coalesced],&originator]).await.unwrap().get(0);
    let token = task_token(db, task, agent, ws, member).await;
    let path = |issue: Uuid| format!("/api/issues/{issue}/comments?workspace_id={ws}");
    let edit = |id: Uuid| format!("/api/comments/{id}?workspace_id={ws}");
    let source_path = path(source);
    let target_path = path(target);

    // Reply guards bind only the task's own issue. A rejected write has no effects.
    let before = revision(db, source).await;
    let created_before = sink.created.lock().unwrap().len();
    let calls_before = runtime.0.lock().unwrap().len();
    for (parent, message) in [
        (
            None,
            format!(
                "comment-triggered tasks cannot create top-level comments; set parent_id (--parent) to {trigger} or a coalesced comment id"
            ),
        ),
        (
            Some(unrelated),
            format!(
                "parent_id {unrelated} is not a comment this task may reply under; set parent_id (--parent) to {trigger} or a coalesced comment id"
            ),
        ),
    ] {
        let mut body = json!({"content":"must not save"});
        if let Some(parent) = parent {
            body["parent_id"] = json!(parent);
        }
        assert_eq!(
            call(app, "POST", &source_path, &token, body).await,
            (StatusCode::CONFLICT, json!({"error":message}))
        );
    }
    assert_eq!(revision(db, source).await, before);
    assert_eq!(sink.created.lock().unwrap().len(), created_before);
    assert_eq!(runtime.0.lock().unwrap().len(), calls_before);
    for parent in [trigger, coalesced] {
        let result = call(
            app,
            "POST",
            &source_path,
            &token,
            json!({"content":"covered reply","parent_id":parent}),
        )
        .await;
        assert_eq!(result.0, StatusCode::CREATED, "{result:?}");
        assert_eq!(result.1["parent_id"], json!(parent));
        assert_eq!(result.1["source_task_id"], json!(task));
        assert_eq!(result.1["author_type"], "agent");
        assert_eq!(result.1["author_id"], json!(agent));
        let action = runtime.0.lock().unwrap().last().unwrap().clone();
        assert_eq!(action.source_task_id, Some(task));
        assert_eq!(action.member_originator, None);
        assert_eq!(action.actor_id, agent);
    }
    // Evaluation of another task must not suppress this run.
    let irrelevant: Uuid = db.query_one("INSERT INTO activity_log(workspace_id,issue_id,actor_type,actor_id,action,details) VALUES($1,$2,'agent',$3,'squad_leader_evaluated',$4) RETURNING id", &[&ws,&source,&agent,&json!({"outcome":"no_action","task_id":Uuid::new_v4()})]).await.unwrap().get(0);
    assert_eq!(
        call(
            app,
            "POST",
            &source_path,
            &token,
            json!({"content":"other evaluation","parent_id":trigger})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    db.execute(
        "UPDATE activity_log SET details=$2 WHERE id=$1",
        &[&irrelevant, &json!({"outcome":"no_action","task_id":task})],
    )
    .await
    .unwrap();
    let denied = call(
        app,
        "POST",
        &source_path,
        &token,
        json!({"content":"no action","parent_id":trigger}),
    )
    .await;
    assert_eq!(
        denied,
        (
            StatusCode::CONFLICT,
            json!({"error":"squad leader recorded no_action; comments are not allowed for this task"})
        )
    );

    // The same run can coordinate elsewhere without adopting that issue's creator.
    let cross = call(
        app,
        "POST",
        &target_path,
        &token,
        json!({"content":"cross issue"}),
    )
    .await;
    assert_eq!(cross.0, StatusCode::CREATED, "{cross:?}");
    assert_eq!(cross.1["source_task_id"], json!(task));
    let cross_id = Uuid::parse_str(cross.1["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        runtime.0.lock().unwrap().last().unwrap().member_originator,
        None
    );
    let audit_row = db
        .query_one(
            "SELECT source_task_id,author_id FROM comment WHERE id=$1",
            &[&cross_id],
        )
        .await
        .unwrap();
    assert_eq!(audit_row.get::<_, Option<Uuid>>(0), Some(task));
    assert_eq!(audit_row.get::<_, Uuid>(1), agent);

    // Concurrent replies reopen a root idempotently and stamp the follow-up tx.
    let root = comment(db, ws, target, member).await;
    db.execute("UPDATE comment SET resolved_at=now(),resolved_by_type='member',resolved_by_id=$2 WHERE id=$1", &[&root,&member]).await.unwrap();
    db.batch_execute("CREATE TABLE issue_write_lineage_audit(comment_id uuid, actor_type text, actor_id text, source_task_id text); CREATE FUNCTION issue_write_lineage_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO issue_write_lineage_audit VALUES(NEW.id,current_setting('multica.actor_type',true),current_setting('multica.actor_id',true),current_setting('multica.source_task_id',true)); RETURN NEW; END $$; CREATE TRIGGER issue_write_lineage_audit AFTER UPDATE OF resolved_at ON comment FOR EACH ROW EXECUTE FUNCTION issue_write_lineage_audit();").await.unwrap();
    let root_revision: i64 = db
        .query_one("SELECT revision FROM comment WHERE id=$1", &[&root])
        .await
        .unwrap()
        .get(0);
    let issue_revision = revision(db, target).await;
    let (left, right) = tokio::join!(
        call(
            app,
            "POST",
            &target_path,
            &token,
            json!({"content":"left","parent_id":root})
        ),
        call(
            app,
            "POST",
            &target_path,
            &token,
            json!({"content":"right","parent_id":root})
        ),
    );
    for result in [&left, &right] {
        assert_eq!(result.0, StatusCode::CREATED, "{result:?}");
        assert_eq!(result.1["source_task_id"], json!(task));
    }
    assert_eq!(revision(db, target).await, issue_revision + 2);
    let root_row = db
        .query_one(
            "SELECT revision,resolved_at FROM comment WHERE id=$1",
            &[&root],
        )
        .await
        .unwrap();
    assert_eq!(root_row.get::<_, i64>(0), root_revision + 1);
    assert!(
        root_row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>(1)
            .is_none()
    );
    let audit = db
        .query(
            "SELECT * FROM issue_write_lineage_audit WHERE comment_id=$1",
            &[&root],
        )
        .await
        .unwrap();
    assert!(!audit.is_empty());
    for row in audit {
        assert_eq!(row.get::<_, String>("actor_type"), "agent");
        assert_eq!(row.get::<_, String>("actor_id"), agent.to_string());
        assert_eq!(row.get::<_, String>("source_task_id"), task.to_string());
    }
    db.batch_execute("DROP TRIGGER issue_write_lineage_audit ON comment; DROP FUNCTION issue_write_lineage_audit(); DROP TABLE issue_write_lineage_audit;").await.unwrap();

    // Assignment-triggered runs have no reply-parent restriction.
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    let next_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,originator_user_id,accountable_user_id) VALUES($1,$2,$3,'running',$4,$4) RETURNING id", &[&agent,&runtime_id,&source,&originator]).await.unwrap().get(0);
    let next_token = task_token(db, next_task, agent, ws, member).await;
    assert_eq!(
        call(
            app,
            "POST",
            &source_path,
            &next_token,
            json!({"content":"assignment comment"})
        )
        .await
        .0,
        StatusCode::CREATED
    );

    // Unchanged text keeps old lineage; a new authoring task restamps changed text.
    let cross_revision = cross.1["revision"].as_i64().unwrap();
    let calls_before = runtime.1.lock().unwrap().len();
    let same = call(
        app,
        "PUT",
        &edit(cross_id),
        &next_token,
        json!({"content":"cross issue","expected_revision":cross_revision}),
    )
    .await;
    assert_eq!(same.0, StatusCode::OK, "{same:?}");
    assert_eq!(same.1["source_task_id"], json!(task));
    assert_eq!(same.1["revision"], json!(cross_revision));
    assert_eq!(runtime.1.lock().unwrap().len(), calls_before);
    let changed = call(
        app,
        "PUT",
        &edit(cross_id),
        &next_token,
        json!({"content":"new authoring action","expected_revision":cross_revision}),
    )
    .await;
    assert_eq!(changed.0, StatusCode::OK, "{changed:?}");
    assert_eq!(changed.1["source_task_id"], json!(next_task));
    let action = runtime.1.lock().unwrap().last().unwrap().comment.clone();
    assert_eq!(action.member_originator, None);
    assert_eq!(action.source_task_id, Some(next_task));
    assert_eq!(action.actor_id, agent);

    // Manage rights do not confer the original agent author's delegation rights.
    assert_eq!(
        call(
            app,
            "PUT",
            &edit(cross_id),
            member_token,
            json!({"content":"member cannot edit"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE member SET role='admin' WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &member],
    )
    .await
    .unwrap();
    let admin_noop = call(
        app,
        "PUT",
        &edit(cross_id),
        member_token,
        json!({"content":"new authoring action"}),
    )
    .await;
    assert_eq!(admin_noop.0, StatusCode::OK, "{admin_noop:?}");
    assert_eq!(admin_noop.1["source_task_id"], json!(next_task));
    let admin_edit = call(
        app,
        "PUT",
        &edit(cross_id),
        member_token,
        json!({"content":"admin action","expected_revision":changed.1["revision"]}),
    )
    .await;
    assert_eq!(admin_edit.0, StatusCode::OK, "{admin_edit:?}");
    assert!(admin_edit.1.get("source_task_id").is_none());
    assert_eq!(admin_edit.1["author_id"], json!(agent));
    let action = runtime.1.lock().unwrap().last().unwrap().comment.clone();
    assert_eq!(action.actor_type, "member");
    assert_eq!(action.actor_id, member);
    assert_eq!(action.member_originator, Some(member));
    assert_eq!(action.source_task_id, None);

    // A stale modern edit cannot restamp provenance or emit runtime work.
    let changed_before = sink.changed.lock().unwrap().len();
    let calls_before = runtime.1.lock().unwrap().len();
    let stale = call(
        app,
        "PUT",
        &edit(cross_id),
        &next_token,
        json!({"content":"stale agent action","expected_revision":changed.1["revision"]}),
    )
    .await;
    assert_eq!(stale.0, StatusCode::CONFLICT, "{stale:?}");
    assert_eq!(sink.changed.lock().unwrap().len(), changed_before);
    assert_eq!(runtime.1.lock().unwrap().len(), calls_before);
    assert!(
        db.query_one(
            "SELECT source_task_id FROM comment WHERE id=$1",
            &[&cross_id]
        )
        .await
        .unwrap()
        .get::<_, Option<Uuid>>(0)
        .is_none()
    );
    let winner_revision = admin_edit.1["revision"].as_i64().unwrap();
    let edit_path = edit(cross_id);
    let (agent_race, admin_race) = tokio::join!(
        call(
            app,
            "PUT",
            &edit_path,
            &next_token,
            json!({"content":"agent race","expected_revision":winner_revision})
        ),
        call(
            app,
            "PUT",
            &edit_path,
            member_token,
            json!({"content":"admin race","expected_revision":winner_revision})
        ),
    );
    assert_eq!(
        [agent_race.0, admin_race.0]
            .iter()
            .filter(|s| **s == StatusCode::OK)
            .count(),
        1
    );
    assert_eq!(
        [agent_race.0, admin_race.0]
            .iter()
            .filter(|s| **s == StatusCode::CONFLICT)
            .count(),
        1
    );
    let row = db
        .query_one(
            "SELECT content,source_task_id,revision FROM comment WHERE id=$1",
            &[&cross_id],
        )
        .await
        .unwrap();
    let expected_source = if agent_race.0 == StatusCode::OK {
        Some(next_task)
    } else {
        None
    };
    assert_eq!(
        row.get::<_, Option<Uuid>>("source_task_id"),
        expected_source
    );
    assert_eq!(
        row.get::<_, String>("content"),
        if expected_source.is_some() {
            "agent race"
        } else {
            "admin race"
        }
    );
    assert_eq!(row.get::<_, i64>("revision"), winner_revision + 1);
    assert_eq!(runtime.1.lock().unwrap().len(), calls_before + 1);
    db.execute(
        "UPDATE member SET role='member' WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &member],
    )
    .await
    .unwrap();

    // Client-supplied identity fields cannot replace the authenticated actor.
    for (auth, expected_source, author_type, author_id) in [
        (member_token, None, "member", member),
        (next_token.as_str(), Some(next_task), "agent", agent),
    ] {
        let response = app.clone().oneshot(Request::builder().method("POST").uri(&target_path)
            .header("authorization",format!("Bearer {auth}"))
            .header("x-agent-id",Uuid::new_v4().to_string())
            .header("x-task-id",Uuid::new_v4().to_string())
            .header("x-actor-type","system")
            .body(Body::from(json!({"content":"identity headers","source_task_id":Uuid::new_v4(),"author_type":"system","author_id":originator}).to_string())).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let result: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(result["author_type"], author_type);
        assert_eq!(result["author_id"], json!(author_id));
        assert_eq!(
            result
                .get("source_task_id")
                .and_then(Value::as_str)
                .map(|v| Uuid::parse_str(v).unwrap()),
            expected_source
        );
    }
    let before = revision(db, target).await;
    assert_eq!(
        call(
            app,
            "POST",
            &format!(
                "/api/issues/{target}/comments?workspace_id={}",
                Uuid::new_v4()
            ),
            &next_token,
            json!({"content":"wrong workspace"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(revision(db, target).await, before);

    // A best-effort Go no_action lookup failure must not abort the reply tx.
    // A view forces SELECT to fail while preserving the real activity rows.
    db.execute(
        "UPDATE activity_log SET details=$2 WHERE id=$1",
        &[
            &irrelevant,
            &json!({"outcome":"no_action","task_id":next_task}),
        ],
    )
    .await
    .unwrap();
    let attachment: Uuid = db.query_one("INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,$2,'agent',$3,'lineage.txt','synthetic://lineage','text/plain',7) RETURNING id", &[&ws,&source,&agent]).await.unwrap().get(0);
    let before = revision(db, source).await;
    let created_before = sink.created.lock().unwrap().len();
    let calls_before = runtime.0.lock().unwrap().len();
    db.batch_execute("ALTER TABLE activity_log RENAME TO issue_write_lineage_activity; CREATE FUNCTION issue_write_lineage_lookup_failure() RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$ BEGIN RAISE EXCEPTION 'synthetic no_action lookup failure'; END $$; CREATE VIEW activity_log AS SELECT * FROM issue_write_lineage_activity WHERE issue_write_lineage_lookup_failure();").await.unwrap();
    let lookup_error = db.query_one("SELECT EXISTS(SELECT 1 FROM activity_log WHERE issue_id=$1 AND actor_type='agent' AND actor_id=$2 AND action='squad_leader_evaluated' AND details->>'outcome'='no_action' AND details->>'task_id'=$3::text)", &[&source,&agent,&next_task.to_string()]).await.unwrap_err();
    let result = call(
        app,
        "POST",
        &source_path,
        &next_token,
        json!({"content":"lookup failure still saves","attachment_ids":[attachment]}),
    )
    .await;
    db.batch_execute("DROP VIEW activity_log; DROP FUNCTION issue_write_lineage_lookup_failure(); ALTER TABLE issue_write_lineage_activity RENAME TO activity_log;").await.unwrap();
    assert_eq!(
        lookup_error.as_db_error().unwrap().message(),
        "synthetic no_action lookup failure"
    );
    assert_eq!(revision(db, source).await, before + 1);
    assert_eq!(sink.created.lock().unwrap().len(), created_before + 1);
    assert_eq!(runtime.0.lock().unwrap().len(), calls_before + 1);
    let saved = Uuid::parse_str(
        result.1["id"]
            .as_str()
            .unwrap_or("00000000-0000-0000-0000-000000000000"),
    )
    .unwrap();
    assert_eq!(
        db.query_one(
            "SELECT comment_id FROM attachment WHERE id=$1",
            &[&attachment]
        )
        .await
        .unwrap()
        .get::<_, Option<Uuid>>(0),
        Some(saved)
    );
    assert_eq!(
        result.0,
        StatusCode::CREATED,
        "best-effort no_action lookup: {result:?}"
    );
    assert_eq!(result.1["source_task_id"], json!(next_task));

    // Expired or revoked task credentials cannot mutate state or reach dispatch.
    let before = revision(db, target).await;
    let created_before = sink.created.lock().unwrap().len();
    let calls_before = runtime.0.lock().unwrap().len();
    let hash = hex::encode(Sha256::digest(next_token.as_bytes()));
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 hour' WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            app,
            "POST",
            &target_path,
            &next_token,
            json!({"content":"expired task"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("DELETE FROM task_token WHERE token_hash=$1", &[&hash])
        .await
        .unwrap();
    assert_eq!(
        call(
            app,
            "POST",
            &target_path,
            &next_token,
            json!({"content":"revoked task"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(revision(db, target).await, before);
    assert_eq!(sink.created.lock().unwrap().len(), created_before);
    assert_eq!(runtime.0.lock().unwrap().len(), calls_before);
}
