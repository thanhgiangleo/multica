use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    db: &tokio_postgres::Client,
    app: &axum::Router,
    sink: &Sink,
    ws: Uuid,
    foreign_ws: Uuid,
    user: Uuid,
    token: &str,
    agent: Uuid,
) {
    let reviewer: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Workflow reviewer',$1) RETURNING id",
            &[&format!("reviewer-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Foreign reviewer',$1) RETURNING id",
            &[&format!(
                "foreign-reviewer-{}@invalid.example",
                Uuid::new_v4()
            )],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member'),($3,$4,'member')",
        &[&ws, &reviewer, &foreign_ws, &outsider],
    )
    .await
    .unwrap();
    let workflow: Uuid = db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'Member handoffs','todo',$2) RETURNING id", &[&ws,&json!([
        {"status_key":"todo"},
        {"status_key":"in_progress","handler":{"type":"member","id":user}},
        {"status_key":"in_review","handler":{"type":"creator"}},
        {"status_key":"blocked","handler":{"type":"project_lead"}},
        {"status_key":"done","handler":{"type":"member","id":outsider}}
    ])]).await.unwrap().get(0);
    let project: Uuid = db.query_one("INSERT INTO project(workspace_id,title,workflow_id,lead_type,lead_id) VALUES($1,'Member handoffs',$2,'member',$3) RETURNING id", &[&ws,&workflow,&reviewer]).await.unwrap().get(0);
    let mut ids = Vec::new();
    for (index, creator, triage) in [
        (0, reviewer, None::<&str>),
        (1, reviewer, None),
        (2, outsider, None),
        (3, reviewer, Some("pending")),
    ] {
        ids.push(db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,project_id,triage_state,status) VALUES($1,'Member workflow','member',$2,$3,$4,$5,'todo') RETURNING id", &[&ws,&creator,&(11800+index),&project,&triage]).await.unwrap().get::<_,Uuid>(0));
    }
    let single = ids[0];
    let path = format!("/api/issues/{single}?workspace_id={ws}");
    for (status, target, changed) in [
        ("in_progress", user, true),
        ("in_review", reviewer, true),
        ("blocked", reviewer, false),
    ] {
        let result = call(app, "PUT", &path, token, json!({"status":status})).await;
        assert_eq!(result.0, StatusCode::OK, "{result:?}");
        assert_eq!(result.1["assignee_id"], json!(target));
        let event = sink.updates.lock().unwrap().last().unwrap().clone();
        assert_eq!(event.actor_id, user);
        assert_eq!(event.payload["assignee_changed"], changed);
        assert_eq!(event.payload["status_changed"], true);
        assert_eq!(
            event.payload["workflow_handoff"],
            json!({"workflow":"Member handoffs","to_type":"member","to_id":target,"run":"false"})
        );
        assert_eq!(event.payload["issue"], result.1);
    }
    let before_revision: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&single])
        .await
        .unwrap()
        .get(0);
    let noop = call(app, "PUT", &path, token, json!({"status":"blocked"})).await;
    assert_eq!(noop.0, StatusCode::OK, "{noop:?}");
    assert_eq!(noop.1["revision"], before_revision);
    assert!(
        sink.updates
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .payload
            .get("workflow_handoff")
            .is_none()
    );
    let chosen = call(
        app,
        "PUT",
        &path,
        token,
        json!({"status":"in_progress","assignee_type":"member","assignee_id":reviewer}),
    )
    .await;
    assert_eq!(chosen.0, StatusCode::OK, "{chosen:?}");
    assert_eq!(chosen.1["assignee_id"], json!(reviewer));
    assert!(
        sink.updates
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .payload
            .get("workflow_handoff")
            .is_none()
    );

    let before = sink.updates.lock().unwrap().len();
    let rejected = call(
        app,
        "PUT",
        &path,
        token,
        json!({"status":"done","title":"must roll back"}),
    )
    .await;
    assert_eq!(
        rejected,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"assignee_id does not refer to a member of this workspace"})
        )
    );
    assert_eq!(sink.updates.lock().unwrap().len(), before);
    let row = db
        .query_one(
            "SELECT title,status,assignee_id FROM issue WHERE id=$1",
            &[&single],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("title"), "Member workflow");
    assert_eq!(row.get::<_, String>("status"), "in_progress");
    assert_eq!(row.get::<_, Option<Uuid>>("assignee_id"), Some(reviewer));

    let batch = format!("/api/issues/batch-update?workspace_id={ws}");
    let result = call(
        app,
        "POST",
        &batch,
        token,
        json!({"issue_ids":&ids[1..],"updates":{"status":"in_review"}}),
    )
    .await;
    assert_eq!(result, (StatusCode::OK, json!({"updated":2})), "{result:?}");
    let events = sink.updates.lock().unwrap()[before..].to_vec();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].payload["workflow_handoff"]["to_id"],
        json!(reviewer)
    );
    assert_eq!(events[0].payload["assignee_changed"], true);
    assert!(events[1].payload.get("workflow_handoff").is_none());
    assert_eq!(events[1].payload["assignee_changed"], false);
    let rows = db.query("SELECT id,status,assignee_id,revision FROM issue WHERE id=ANY($1::uuid[]) ORDER BY number", &[&ids[1..].to_vec()]).await.unwrap();
    assert_eq!(
        rows[0].get::<_, Option<Uuid>>("assignee_id"),
        Some(reviewer)
    );
    assert_eq!(rows[1].get::<_, String>("status"), "todo");
    assert_eq!(rows[1].get::<_, i64>("revision"), 1);
    assert_eq!(rows[2].get::<_, Option<Uuid>>("assignee_id"), None);
    let before = sink.updates.lock().unwrap().len();
    let rejected = call(
        app,
        "POST",
        &batch,
        token,
        json!({"issue_ids":&ids[1..],"updates":{"status":"done","title":"must not apply"}}),
    )
    .await;
    // Triage suppresses even a configured handler; the other two fail the member gate.
    assert_eq!(
        rejected,
        (StatusCode::OK, json!({"updated":1})),
        "{rejected:?}"
    );
    assert_eq!(sink.updates.lock().unwrap().len(), before + 1);

    db.execute(
        "UPDATE project SET lead_type=NULL,lead_id=NULL WHERE id=$1",
        &[&project],
    )
    .await
    .unwrap();
    let unresolvable = call(app, "PUT", &path, token, json!({"status":"blocked"})).await;
    assert_eq!(unresolvable.0, StatusCode::OK, "{unresolvable:?}");
    assert_eq!(unresolvable.1["assignee_id"], json!(reviewer));
    assert!(
        sink.updates
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .payload
            .get("workflow_handoff")
            .is_none()
    );
    call(app, "PUT", &path, token, json!({"status":"todo"})).await;
    db.execute(
        "UPDATE project SET lead_type='agent',lead_id=$1 WHERE id=$2",
        &[&agent, &project],
    )
    .await
    .unwrap();
    let before = sink.updates.lock().unwrap().len();
    let unavailable = call(app, "PUT", &path, token, json!({"status":"blocked"})).await;
    assert_eq!(
        unavailable.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "{unavailable:?}"
    );
    assert_eq!(sink.updates.lock().unwrap().len(), before);
}
