//! Label read failures are post-commit, not reasons to undo an assignment.
use super::*;

pub async fn verify(
    app: &axum::Router,
    token: &str,
    db: &tokio_postgres::Client,
    ws: Uuid,
    issue: Uuid,
    label: Uuid,
    sink: &Sink,
) {
    let path = format!("/api/issues/{issue}/labels?workspace_id={ws}");
    let detach = format!("/api/issues/{issue}/labels/{label}?workspace_id={ws}");
    let sentinel: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,resource_type,name,color) VALUES ($1,'issue','Read failure sentinel','#ffffff') RETURNING id", &[&ws]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO issue_to_label(issue_id,label_id) VALUES ($1,$2)",
        &[&issue, &sentinel],
    )
    .await
    .unwrap();
    let revision: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let events = sink.2.lock().unwrap().len();
    // Only the response projection reads name. Validation still locks and reads
    // resource_type normally. A sentinel makes even a successful detach fail
    // its list read, without relying on scheduler timing or a mock DB client.
    let columns: Vec<String> = db.query("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='issue_label' ORDER BY ordinal_position", &[]).await.unwrap().iter().map(|r| {
        let column: String = r.get(0);
        if column == "name" { "issue_write_label_read_failure(name) AS name".into() } else { format!("\"{column}\"") }
    }).collect();
    db.batch_execute(&format!(r#"
        CREATE FUNCTION issue_write_label_read_failure(text) RETURNS text LANGUAGE plpgsql STABLE AS $$
        BEGIN RAISE EXCEPTION 'synthetic label list failure'; END $$;
        ALTER TABLE issue_label RENAME TO issue_write_label_backing;
        CREATE VIEW issue_label AS SELECT {} FROM issue_write_label_backing;
    "#, columns.join(","))).await.unwrap();
    assert!(
        db.query_one(
            "SELECT resource_type FROM issue_label WHERE id=$1 AND workspace_id=$2 FOR SHARE",
            &[&label, &ws]
        )
        .await
        .is_ok(),
        "fault injection must not affect validation"
    );
    let failure = db.query("SELECT l.* FROM issue_label l JOIN issue_to_label il ON il.label_id=l.id WHERE il.issue_id=$1 AND l.workspace_id=$2 AND l.resource_type='issue' ORDER BY lower(l.name)", &[&issue,&ws]).await.unwrap_err();
    assert_eq!(
        failure.as_db_error().unwrap().message(),
        "synthetic label list failure"
    );
    for (method, url, expected_revision, present) in [
        ("POST", path.as_str(), revision, true),
        ("DELETE", detach.as_str(), revision + 1, false),
        ("DELETE", detach.as_str(), revision + 1, false),
        ("POST", path.as_str(), revision + 2, true),
    ] {
        let result = call(app, method, url, token, json!({"label_id":label})).await;
        assert_eq!(
            result,
            (StatusCode::OK, json!({})),
            "{method} label projection failure"
        );
        let row = db.query_one("SELECT revision,EXISTS(SELECT 1 FROM issue_to_label WHERE issue_id=$1 AND label_id=$2) AS attached FROM issue WHERE id=$1", &[&issue,&label]).await.unwrap();
        assert_eq!(row.get::<_, i64>("revision"), expected_revision);
        assert_eq!(row.get::<_, bool>("attached"), present);
        assert_eq!(
            sink.2.lock().unwrap().len(),
            events,
            "never broadcast an empty replacement after a failed list read"
        );
    }
    db.batch_execute("DROP VIEW issue_label; ALTER TABLE issue_write_label_backing RENAME TO issue_label; DROP FUNCTION issue_write_label_read_failure(text)").await.unwrap();
    db.execute(
        "DELETE FROM issue_to_label WHERE issue_id=$1 AND label_id=$2",
        &[&issue, &sentinel],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM issue_label WHERE id=$1", &[&sentinel])
        .await
        .unwrap();
    let recovered = call(app, "POST", &path, token, json!({"label_id":label})).await;
    assert_eq!(recovered.0, StatusCode::OK);
    assert_eq!(recovered.1["labels"][0]["id"], json!(label));
    assert!(recovered.1.get("issue_revision").is_none());
    assert_eq!(sink.2.lock().unwrap().len(), events);

    for body in [
        format!(r#"{{"LABEL_ID":"{label}"}}"#),
        format!(r#"{{"label_id":"{label}","label_id":null}}"#),
        format!(r#"{{"label_id":"bad","LaBeL_iD":"{label}"}}"#),
        format!(r#"{{"label_id":"{label}"}} trailing junk"#),
        format!(r#"{{"ignored":1e400,"label_id":"{label}"}}"#),
    ] {
        let result = raw_call(app, "POST", &path, token, body.as_bytes()).await;
        assert_eq!(result.0, StatusCode::OK, "{body}: {result:?}");
        assert!(result.1.get("issue_revision").is_none());
    }
    for (body, message) in [
        (
            r#"{"label_id":false,"label_id":"valid-later"}"#,
            "invalid request body",
        ),
        (r#"{"LABEL_ID":null}"#, "label_id is required"),
        (r#"{"label_id":"x","LABEL_ID":""}"#, "label_id is required"),
        (r#"{"label_id":"x","LABEL_ID":"bad"}"#, "invalid label_id"),
        (
            r#"{"label_id":"x","LABEL_ID":1e400}"#,
            "invalid request body",
        ),
    ] {
        assert_eq!(
            raw_call(app, "POST", &path, token, body.as_bytes()).await,
            (StatusCode::BAD_REQUEST, json!({"error":message})),
            "{body}"
        );
    }
    assert_eq!(sink.2.lock().unwrap().len(), events);
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, i64>(0),
        revision + 2
    );
}
