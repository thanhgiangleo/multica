//! Raw HTTP request regressions; no JSON Value round-trip may erase duplicates.
use super::*;

struct Children;
impl IssueChildRuntime for Children {
    fn process_child_events(&self, _: Uuid, _: Vec<Uuid>) {}
}

async fn raw_call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: impl Into<Vec<u8>>,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.into()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

pub(super) async fn run(
    db: &tokio_postgres::Client,
    pool: Arc<Pool>,
    sink: Arc<Sink>,
    ws: Uuid,
    user: Uuid,
    token: &str,
) {
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Decoder','member',$2,1000) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let other:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Untouched','member',$2,1001) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let target:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Target','member',$2,1002) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let attach:Uuid=db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,'member',$2,'decoder.txt','synthetic://decoder','text/plain',1) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let app = router_with_api(
        health,
        ApiState::new(pool, b"update-test-secret".to_vec())
            .with_issue_event_sink(sink.clone())
            .with_issue_child_runtime(Arc::new(Children)),
    );
    let path = format!("/api/issues/{issue}?workspace_id={ws}");
    let batch = format!("/api/issues/batch-update?workspace_id={ws}");
    let first = raw_call(
        &app,
        "PUT",
        &path,
        token,
        br#"{"TITLE":"Decoded","priority":"low","PRIORITY":"high","expected_revision":1}"#,
    )
    .await;
    assert_eq!(first.0, StatusCode::OK, "{first:?}");
    assert_eq!(first.1["title"], "Decoded");
    assert_eq!(first.1["priority"], "high");
    assert_eq!(first.1["revision"], 2);
    let null_title = raw_call(
        &app,
        "PUT",
        &path,
        token,
        br#"{"title":"ignored","TITLE":null}"#,
    )
    .await;
    assert_eq!(null_title.0, StatusCode::OK, "{null_title:?}");
    assert_eq!(null_title.1["title"], "Decoded");
    assert_eq!(null_title.1["revision"], 2);
    let linked = raw_call(
        &app,
        "PUT",
        &path,
        token,
        format!(r#"{{"attachment_ids":["{attach}"],"ATTACHMENT_IDS":[null]}}"#),
    )
    .await;
    assert_eq!(linked.0, StatusCode::OK, "{linked:?}");
    assert_eq!(
        db.query_one("SELECT issue_id FROM attachment WHERE id=$1", &[&attach])
            .await
            .unwrap()
            .get::<_, Option<Uuid>>(0),
        Some(issue)
    );
    assert_eq!(linked.1["revision"], 3);
    let dated = raw_call(
        &app,
        "PUT",
        &path,
        token,
        br#"{"start_date":null,"START_DATE":"2026-11-03"}"#,
    )
    .await;
    assert_eq!(dated.0, StatusCode::OK, "{dated:?}");
    assert_eq!(dated.1["start_date"], "2026-11-03");
    let folded_null = raw_call(&app, "PUT", &path, token, br#"{"START_DATE":null}"#).await;
    assert_eq!(folded_null.0, StatusCode::OK);
    assert_eq!(folded_null.1["start_date"], "2026-11-03");
    assert_eq!(folded_null.1["revision"], dated.1["revision"]);
    let before = sink.updates.lock().unwrap().len();
    for (body, status) in [
        (
            br#"{"title":false,"title":"bad overwrite"}"#.as_slice(),
            StatusCode::BAD_REQUEST,
        ),
        (
            br#"{"stage":"bad","stage":null}"#.as_slice(),
            StatusCode::BAD_REQUEST,
        ),
        (
            br#"{"duplicate_of_issue_id":false}"#.as_slice(),
            StatusCode::BAD_REQUEST,
        ),
        (
            br#"{"title":"trailing"} {}"#.as_slice(),
            StatusCode::BAD_REQUEST,
        ),
        (
            br#"{"title":"trailing"} junk"#.as_slice(),
            StatusCode::BAD_REQUEST,
        ),
        (
            br#"{"title":"must not commit","stop_previous_assignee_runs":true,"STOP_PREVIOUS_ASSIGNEE_RUNS":[]}"#
                .as_slice(),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let rejected = raw_call(&app, "PUT", &path, token, body).await;
        assert_eq!(rejected.0, status, "{body:?}: {rejected:?}");
    }
    assert_eq!(sink.updates.lock().unwrap().len(), before);
    assert_eq!(
        db.query_one("SELECT title FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, String>(0),
        "Decoded"
    );
    // Duplicate Updates objects merge into one Go value struct. The raw map is
    // still last exact-key wins, so it must not be used to decode typed values.
    let merged=raw_call(&app,"POST",&batch,token,format!(r#"{{"issue_ids":["{issue}","{other}"],"ISSUE_IDS":[null],"updates":{{"title":"Batch decoded","priority":"low"}},"UPDATES":{{"description":"Merged","PRIORITY":"high"}}}}"#)).await;
    assert_eq!(merged, (StatusCode::OK, json!({"updated":1})));
    let row = db
        .query_one(
            "SELECT title,description,priority FROM issue WHERE id=$1",
            &[&issue],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "Batch decoded");
    assert_eq!(row.get::<_, Option<String>>(1).as_deref(), Some("Merged"));
    assert_eq!(row.get::<_, String>(2), "high");
    assert_eq!(
        db.query_one("SELECT title FROM issue WHERE id=$1", &[&other])
            .await
            .unwrap()
            .get::<_, String>(0),
        "Untouched"
    );
    let null_struct = raw_call(
        &app,
        "POST",
        &batch,
        token,
        format!(r#"{{"issue_ids":["{issue}"],"updates":{{"title":"Retained"}},"updates":null}}"#),
    )
    .await;
    assert_eq!(null_struct, (StatusCode::OK, json!({"updated":1})));
    assert_eq!(
        db.query_one("SELECT title FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, String>(0),
        "Retained"
    );
    let invalid = raw_call(
        &app,
        "POST",
        &batch,
        token,
        format!(
            r#"{{"issue_ids":["{issue}"],"updates":{{"title":false,"TITLE":"must not write"}}}}"#
        ),
    )
    .await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST);
    let mut utf8 = br#"{"TITLE":"bad "#.to_vec();
    utf8.extend_from_slice(&[0xe2, 0x82]);
    utf8.extend_from_slice(br#""}"#);
    let repaired = raw_call(&app, "PUT", &path, token, utf8).await;
    assert_eq!(repaired.0, StatusCode::OK, "{repaired:?}");
    assert_eq!(repaired.1["title"], "bad \u{fffd}\u{fffd}");
    // Lowercase key enables the write, but the final folded value picks target.
    let duplicate = raw_call(
        &app,
        "PUT",
        &path,
        token,
        format!(r#"{{"duplicate_of_issue_id":"{other}","DUPLICATE_OF_ISSUE_ID":"{target}"}}"#),
    )
    .await;
    assert_eq!(duplicate.0, StatusCode::OK, "{duplicate:?}");
    assert_eq!(
        db.query_one(
            "SELECT duplicate_of_issue_id FROM issue WHERE id=$1",
            &[&issue]
        )
        .await
        .unwrap()
        .get::<_, Option<Uuid>>(0),
        Some(target)
    );
    let after = sink.updates.lock().unwrap().len();
    let clear_null = raw_call(
        &app,
        "PUT",
        &path,
        token,
        format!(r#"{{"duplicate_of_issue_id":"{target}","DUPLICATE_OF_ISSUE_ID":null}}"#),
    )
    .await;
    assert_eq!(clear_null.0, StatusCode::BAD_REQUEST, "{clear_null:?}");
    assert!(
        clear_null.1["error"]
            .as_str()
            .unwrap()
            .contains("cannot be null")
    );
    assert_eq!(sink.updates.lock().unwrap().len(), after);
}
