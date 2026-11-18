use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    db: &tokio_postgres::Client,
    app: &axum::Router,
    sink: &Sink,
    ws: Uuid,
    user: Uuid,
    agent: Uuid,
    token: &str,
) {
    let root: Uuid = db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES($1,8840,'Subscriber decode root','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let child: Uuid = db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id,parent_issue_id) VALUES($1,8841,'Subscriber decode child','member',$2,$3) RETURNING id", &[&ws,&user,&root]).await.unwrap().get(0);
    let subscribe = format!("/api/issues/OWN-8840/subscribe?workspace_id={ws}");
    let raw_user = user.to_string().to_uppercase();
    let before = sink.subscriptions.lock().unwrap().len();
    for body in [
        format!(r#"{{"USER_ID":"{raw_user}","USER_TYPE":"member"}}"#),
        format!(r#"{{"user_id":"{raw_user}","user_id":17,"user_type":"member"}}"#),
        format!(
            r#"{{"user_id":"{raw_user}","USER_ID":{{}},"user_type":9,"USER_TYPE":"member"}} trailing"#
        ),
        format!(r#"{{"unknown":1e10000,"user_id":"{raw_user}","user_type":"member"}}"#),
    ] {
        assert_eq!(
            call_raw(app, "POST", &subscribe, token, body.clone()).await,
            (StatusCode::OK, json!({"subscribed":true})),
            "{body}"
        );
        let event = sink.subscriptions.lock().unwrap().last().unwrap().clone();
        assert_eq!(event.workspace_id, ws);
        assert_eq!((event.actor_type, event.actor_id), ("agent", agent));
        assert_eq!(
            event.payload,
            json!({"issue_id":"OWN-8840","user_type":"member","user_id":raw_user,"reason":"manual"})
        );
        assert_eq!(db.query_one("SELECT count(*) FROM issue_subscriber WHERE issue_id=$1 AND user_type='member' AND user_id=$2 AND unsubscribed_at IS NULL", &[&root,&user]).await.unwrap().get::<_,i64>(0),1);
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM issue_subscriber WHERE issue_id=$1 AND user_id=$2",
                &[&root, &agent]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            0
        );
    }
    // A later valid but foreign target must not fall back to the caller.
    let missing = Uuid::new_v4();
    assert_eq!(
        call_raw(
            app,
            "POST",
            &subscribe,
            token,
            format!(r#"{{"user_id":"{raw_user}","USER_ID":"{missing}","user_type":"member"}}"#)
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(sink.subscriptions.lock().unwrap().len(), before + 4);
    let body = format!(r#"{{"user_id":"{raw_user}","USER_ID":false,"user_type":"member"}}"#);
    let unsubscribe = format!("/api/issues/OWN-8840/unsubscribe?workspace_id={ws}");
    assert_eq!(
        call_raw(app, "POST", &unsubscribe, token, body.clone()).await,
        (StatusCode::OK, json!({"subscribed":false}))
    );
    let event = sink.subscriptions.lock().unwrap().last().unwrap().clone();
    assert_eq!(event.payload["user_id"], user.to_string());
    assert_eq!(event.payload["issue_id"], "OWN-8840");
    assert_eq!(db.query_one("SELECT opt_out_scope FROM issue_subscriber WHERE issue_id=$1 AND user_type='member' AND user_id=$2", &[&root,&user]).await.unwrap().get::<_,String>(0),"issue");
    db.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason) VALUES($1,'member',$2,'manual')", &[&child,&user]).await.unwrap();
    let before_tree = sink.subscriptions.lock().unwrap().len();
    let tree = format!("/api/issues/OWN-8840/unsubscribe/subtree?workspace_id={ws}");
    assert_eq!(
        call_raw(app, "POST", &tree, token, body).await,
        (StatusCode::OK, json!({"subscribed":false}))
    );
    let events = sink.subscriptions.lock().unwrap()[before_tree..].to_vec();
    assert_eq!(events.len(), 2);
    for id in [root, child] {
        assert!(events.iter().any(|e| e.event_type == "subscriber:removed"
            && e.payload["issue_id"] == id.to_string()
            && e.payload["user_id"] == user.to_string()
            && e.actor_id == agent));
        assert_eq!(db.query_one("SELECT opt_out_scope FROM issue_subscriber WHERE issue_id=$1 AND user_type='member' AND user_id=$2", &[&id,&user]).await.unwrap().get::<_,String>(0),"subtree");
    }
    // GetAgentInWorkspace excludes hidden system agents, even when owned by
    // the caller. None of the three routes may mutate their subscriptions.
    let system: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,kind) VALUES($1,'Hidden subscriber target','cloud',$2,'system') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    for issue in [root, child] {
        db.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason) VALUES($1,'agent',$2,'manual')", &[&issue,&system]).await.unwrap();
    }
    let snapshot = || async {
        db.query_one("SELECT jsonb_agg(to_jsonb(s) ORDER BY issue_id) FROM issue_subscriber s WHERE user_type='agent' AND user_id=$1", &[&system]).await.unwrap().get::<_, Value>(0)
    };
    let rows = snapshot().await;
    let before_hidden = sink.subscriptions.lock().unwrap().len();
    for path in [&subscribe, &unsubscribe, &tree] {
        assert_eq!(
            call(
                app,
                "POST",
                path,
                token,
                json!({"user_type":"agent","user_id":system})
            )
            .await,
            (
                StatusCode::FORBIDDEN,
                json!({"error":"target user is not a member of this workspace"})
            ),
            "system agent target: {path}"
        );
        assert_eq!(
            snapshot().await,
            rows,
            "rejected target changed rows: {path}"
        );
        assert_eq!(sink.subscriptions.lock().unwrap().len(), before_hidden);
    }
    db.execute(
        "DELETE FROM issue_subscriber WHERE user_type='agent' AND user_id=$1",
        &[&system],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            app,
            "POST",
            &subscribe,
            token,
            json!({"user_type":"agent","user_id":system})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_subscriber WHERE user_type='agent' AND user_id=$1",
            &[&system]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(sink.subscriptions.lock().unwrap().len(), before_hidden);
    // Same owner/workspace and archived state do not hide a user-kind agent
    // from the subscriber API; that API does not dispatch agent work.
    db.execute(
        "UPDATE agent SET kind='user',archived_at=now() WHERE id=$1",
        &[&system],
    )
    .await
    .unwrap();
    for (path, subscribed) in [(&subscribe, true), (&unsubscribe, false), (&tree, false)] {
        assert_eq!(
            call(
                app,
                "POST",
                path,
                token,
                json!({"user_type":"agent","user_id":system})
            )
            .await,
            (StatusCode::OK, json!({"subscribed":subscribed})),
            "user-kind agent target: {path}"
        );
    }
    assert!(sink.subscriptions.lock().unwrap().len() > before_hidden);
    // Even a syntactically incomplete request is deliberately ignored by Go;
    // no prefix field may leak into the mutation's resolved target.
    assert_eq!(
        call_raw(
            app,
            "POST",
            &subscribe,
            token,
            format!(r#"{{"user_id":"{raw_user}","user_type":"member","#)
        )
        .await,
        (StatusCode::OK, json!({"subscribed":true}))
    );
    let event = sink.subscriptions.lock().unwrap().last().unwrap().clone();
    assert_eq!(event.payload["user_type"], "agent");
    assert_eq!(event.payload["user_id"], agent.to_string());
    assert_eq!(db.query_one("SELECT opt_out_scope FROM issue_subscriber WHERE issue_id=$1 AND user_type='member' AND user_id=$2", &[&root,&user]).await.unwrap().get::<_,String>(0),"subtree");
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&root])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
}
