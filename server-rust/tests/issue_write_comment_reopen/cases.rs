use super::*;

pub(super) async fn run(
    db: &deadpool_postgres::Object,
    app: &Router,
    token: &str,
    sink: &Arc<Sink>,
    runtime: &Arc<Runtime>,
    scope: (Uuid, Uuid),
) {
    let (ws, member) = scope;
    let issue: Uuid = db.query_one(
        "INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Reopen isolation','member',$2,(SELECT COALESCE(MAX(number),0)+1 FROM issue WHERE workspace_id=$1)) RETURNING id",
        &[&ws,&member],
    ).await.unwrap().get(0);
    let path = format!("/api/issues/{issue}/comments?workspace_id={ws}");
    let (status, root) = call(app, "POST", &path, token, json!({"content":"root"})).await;
    assert_eq!(status, StatusCode::CREATED);
    let root_id = Uuid::parse_str(root["id"].as_str().unwrap()).unwrap();
    db.execute(
        "UPDATE comment SET resolved_at=now(),resolved_by_type='member',resolved_by_id=$1 WHERE id=$2",
        &[&member,&root_id],
    ).await.unwrap();
    let before: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let created_before = sink.created.lock().unwrap().len();
    let resolution_before = sink.resolved.lock().unwrap().len();
    let runtime_before = runtime.0.lock().unwrap().len();
    let attachment: Uuid = db.query_one(
        "INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,$2,'member',$3,'reply.txt','synthetic://reopen-reply','text/plain',2) RETURNING id",
        &[&ws,&issue,&member],
    ).await.unwrap().get(0);

    db.execute("CREATE FUNCTION zbsi66_refuse_reopen() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic root reopen failure'; END $$", &[]).await.unwrap();
    db.execute(
        &format!("CREATE TRIGGER zbsi66_refuse_reopen BEFORE UPDATE ON comment FOR EACH ROW WHEN (OLD.id = '{root_id}'::uuid AND OLD.resolved_at IS NOT NULL AND NEW.resolved_at IS NULL) EXECUTE FUNCTION zbsi66_refuse_reopen()"),
        &[],
    ).await.unwrap();
    let (status, reply) = call(
        app,
        "POST",
        &path,
        token,
        json!({"content":"reply survives reopen failure","parent_id":root_id,"attachment_ids":[attachment]}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{reply}");
    let reply_id = Uuid::parse_str(reply["id"].as_str().unwrap()).unwrap();
    assert_eq!(reply["attachments"][0]["id"], json!(attachment));
    assert_eq!(
        db.query_one(
            "SELECT comment_id FROM attachment WHERE id=$1",
            &[&attachment]
        )
        .await
        .unwrap()
        .get::<_, Option<Uuid>>(0),
        Some(reply_id)
    );
    assert_eq!(
        db.query_one("SELECT count(*) FROM comment WHERE id=$1", &[&reply_id])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, i64>(0),
        before + 1
    );
    assert!(
        db.query_one(
            "SELECT resolved_at IS NOT NULL FROM comment WHERE id=$1",
            &[&root_id]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    assert_eq!(sink.created.lock().unwrap().len(), created_before + 1);
    assert_eq!(sink.resolved.lock().unwrap().len(), resolution_before);
    assert_eq!(runtime.0.lock().unwrap().len(), runtime_before + 1);
    db.execute("DROP TRIGGER zbsi66_refuse_reopen ON comment", &[])
        .await
        .unwrap();
    db.execute("DROP FUNCTION zbsi66_refuse_reopen()", &[])
        .await
        .unwrap();

    let (status, nested) = call(
        app,
        "POST",
        &path,
        token,
        json!({"content":"nested reply reopens root","parent_id":reply_id}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{nested}");
    assert_eq!(sink.resolved.lock().unwrap().len(), resolution_before + 1);
    assert!(
        db.query_one("SELECT resolved_at FROM comment WHERE id=$1", &[&root_id])
            .await
            .unwrap()
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>(0)
            .is_none()
    );
    let events = sink.resolved.lock().unwrap();
    let event = events.last().unwrap();
    assert_eq!(event.actor_type, "member");
    assert_eq!(event.actor_id, member);
    assert_eq!(event.workspace_id, ws);
    assert_eq!(event.event_type, "comment:unresolved");
    assert_eq!(event.payload["comment"]["id"], json!(root_id));
    assert!(event.payload["comment"].get("attachments").is_none());
    assert!(event.payload["comment"].get("reactions").is_none());
}
