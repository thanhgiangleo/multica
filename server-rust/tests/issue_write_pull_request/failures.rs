use super::*;

pub async fn run(
    db: &Client,
    app: &axum::Router,
    runtime: &Runtime,
    ws: Uuid,
    user: Uuid,
    pr: Uuid,
    token: &str,
) {
    let race_issue = make_issue(db, ws, user, 43, "Concurrent PR links").await;
    let race_link = format!("/api/issues/{race_issue}/pull-requests?workspace_id={ws}");
    let race_unlink = format!("/api/issues/{race_issue}/pull-requests/{pr}?workspace_id={ws}");
    let merge_before = runtime.merges.lock().unwrap().len();

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..6 {
        let app = app.clone();
        let path = race_link.clone();
        let token = token.to_owned();
        tasks.spawn(async move {
            call(
                &app,
                "POST",
                &path,
                &token,
                json!({"pull_request_id": pr.to_string()}),
            )
            .await
            .0
        });
    }
    while let Some(result) = tasks.join_next().await {
        assert_eq!(result.unwrap(), StatusCode::OK);
    }
    assert_eq!(runtime.merges.lock().unwrap().len(), merge_before + 1);
    assert_eq!(runtime.observed_links.lock().unwrap().last(), Some(&1));

    let merge_before = runtime.merges.lock().unwrap().len();
    for _ in 0..6 {
        let app = app.clone();
        let path = race_unlink.clone();
        let token = token.to_owned();
        tasks.spawn(async move { call(&app, "DELETE", &path, &token, json!({})).await.0 });
    }
    while let Some(result) = tasks.join_next().await {
        assert_eq!(result.unwrap(), StatusCode::OK);
    }
    assert_eq!(runtime.merges.lock().unwrap().len(), merge_before + 1);
    assert_eq!(runtime.observed_links.lock().unwrap().last(), Some(&0));
    assert!(link_row(db, race_issue, pr).await.is_none());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_pull_request_exclusion WHERE issue_id=$1 AND pull_request_id=$2",
            &[&race_issue, &pr],
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );

    let rollback_issue = make_issue(db, ws, user, 44, "PR unlink rollback").await;
    let partial_issue = make_issue(db, ws, user, 45, "PR link autocommit").await;
    db.execute(
        "INSERT INTO issue_pull_request(issue_id,pull_request_id,linked_by_type,linked_by_id) VALUES($1,$2,'member',$3)",
        &[&rollback_issue, &pr, &user],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO issue_pull_request_exclusion(issue_id,pull_request_id,workspace_id) VALUES($1,$2,$3)",
        &[&partial_issue, &pr, &ws],
    )
    .await
    .unwrap();
    db.batch_execute(
        "CREATE FUNCTION zbsi66_pr_exclusion_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic exclusion failure'; END $$;
         CREATE TRIGGER zbsi66_pr_exclusion_failure BEFORE INSERT OR UPDATE OR DELETE ON issue_pull_request_exclusion FOR EACH ROW EXECUTE FUNCTION zbsi66_pr_exclusion_failure()",
    )
    .await
    .unwrap();

    let events_before = runtime.changed.lock().unwrap().len();
    let merge_before = runtime.merges.lock().unwrap().len();
    assert_eq!(
        call(
            app,
            "DELETE",
            &format!("/api/issues/{rollback_issue}/pull-requests/{pr}?workspace_id={ws}"),
            token,
            json!({})
        )
        .await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to unlink pull request"})
        )
    );
    assert!(link_row(db, rollback_issue, pr).await.is_some());
    assert_eq!(runtime.changed.lock().unwrap().len(), events_before);
    assert_eq!(runtime.merges.lock().unwrap().len(), merge_before);

    // Go link has two autocommit statements, not a single transaction.
    assert_eq!(
        call(
            app,
            "POST",
            &format!("/api/issues/{partial_issue}/pull-requests?workspace_id={ws}"),
            token,
            json!({"pull_request_id":pr.to_string()})
        )
        .await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to link pull request"})
        )
    );
    assert!(link_row(db, partial_issue, pr).await.is_some());
    assert_eq!(runtime.changed.lock().unwrap().len(), events_before);
    assert_eq!(runtime.merges.lock().unwrap().len(), merge_before);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_pull_request_exclusion WHERE issue_id=$1 AND pull_request_id=$2",
            &[&partial_issue, &pr],
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.batch_execute("DROP TRIGGER zbsi66_pr_exclusion_failure ON issue_pull_request_exclusion; DROP FUNCTION zbsi66_pr_exclusion_failure()")
        .await
        .unwrap();

    // Activity failure is best effort, after the automation setting commits.
    db.batch_execute(
        "CREATE FUNCTION zbsi66_pr_activity_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic activity failure'; END $$;
         CREATE TRIGGER zbsi66_pr_activity_failure BEFORE INSERT ON activity_log FOR EACH ROW EXECUTE FUNCTION zbsi66_pr_activity_failure()",
    )
    .await
    .unwrap();
    let activity_before = runtime.activity.lock().unwrap().len();
    let changed_before = runtime.changed.lock().unwrap().len();
    assert_eq!(
        call(
            app,
            "PUT",
            &format!("/api/issues/{partial_issue}/pr-auto-complete?workspace_id={ws}"),
            token,
            json!({"disabled":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(runtime.activity.lock().unwrap().len(), activity_before);
    assert_eq!(runtime.changed.lock().unwrap().len(), changed_before + 1);
    assert!(
        db.query_one(
            "SELECT auto_complete_disabled FROM issue_pr_automation WHERE issue_id=$1",
            &[&partial_issue]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    db.batch_execute("DROP TRIGGER zbsi66_pr_activity_failure ON activity_log; DROP FUNCTION zbsi66_pr_activity_failure()")
        .await
        .unwrap();
}
