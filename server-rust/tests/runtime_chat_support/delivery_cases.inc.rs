// Included by runtime_chat_daemon.rs to share its synthetic DB/WS fixture.
impl Fixture {
    async fn supplement(&self, task: Uuid, author_type: &str, content: &str) -> Uuid {
        let db = self.pool.get().await.unwrap();
        db.execute("INSERT INTO task_supplement_capability(task_id,workspace_id,issue_id,capability) VALUES ($1,$2,$3,'task-supplement-v1') ON CONFLICT DO NOTHING", &[&task,&self.ws,&self.issue]).await.unwrap();
        let comment = Uuid::new_v4();
        let author = if author_type == "agent" {
            self.agent
        } else {
            self.owner
        };
        db.execute("INSERT INTO comment(id,workspace_id,issue_id,author_type,author_id,content) VALUES ($1,$2,$3,$4,$5,$6)", &[&comment,&self.ws,&self.issue,&author_type,&author,&content]).await.unwrap();
        db.execute("INSERT INTO task_supplement(task_id,workspace_id,issue_id,comment_id,author_id,client_request_id,status) VALUES ($1,$2,$3,$4,$5,$4,'pending')", &[&task,&self.ws,&self.issue,&comment,&author]).await.unwrap();
        comment
    }
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn resume_pin_preserves_terminal_guards_and_advances_only_cancelled_chat_pointer() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    for status in [
        "dispatched",
        "running",
        "waiting_local_directory",
        "queued",
        "failed",
        "completed",
    ] {
        let task = f.task(status, true).await;
        let (code, body) = post(
            &f.app,
            &format!("/api/daemon/tasks/{task}/session"),
            Some(&f.token),
            r#"{"session_id":"resume","work_dir":"/synthetic/work"}"#,
        )
        .await;
        assert_eq!(code, StatusCode::NO_CONTENT, "{status}: {body}");
        let row = db
            .query_one(
                "SELECT session_id,work_dir FROM agent_task_queue WHERE id=$1",
                &[&task],
            )
            .await
            .unwrap();
        let expected = matches!(status, "dispatched" | "running");
        assert_eq!(
            row.get::<_, Option<String>>("session_id").is_some(),
            expected
        );
        assert_eq!(row.get::<_, Option<String>>("work_dir").is_some(), expected);
    }
    assert!(
        db.query_one(
            "SELECT session_id FROM chat_session WHERE id=$1",
            &[&f.chat]
        )
        .await
        .unwrap()
        .get::<_, Option<String>>(0)
        .is_none()
    );
    let task = f.task("dispatched", false).await;
    let path = format!("/api/daemon/tasks/{task}/session");
    assert_eq!(
        post(
            &f.app,
            &path,
            Some(&f.token),
            r#"{"WORK_DIR":"first","work_dir":null} second"#
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        post(
            &f.app,
            &path,
            Some(&f.token),
            r#"{"session_id":"first","SESSION_ID":"second","session_id":null}"#
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let row = db
        .query_one(
            "SELECT session_id,work_dir FROM agent_task_queue WHERE id=$1",
            &[&task],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "second");
    assert_eq!(row.get::<_, String>(1), "first");
    for body in [
        "",
        "{}",
        "null",
        "[]",
        r#"{"session_id":1,"session_id":"valid"}"#,
    ] {
        assert_eq!(
            post(&f.app, &path, Some(&f.token), body).await.0,
            StatusCode::BAD_REQUEST,
            "{body}"
        );
    }
    // A late cancelled pin repairs the resume pointer. Second pin cannot
    // overwrite a session already recorded on the cancelled run.
    let cancelled = f.task("cancelled", true).await;
    let path = format!("/api/daemon/tasks/{cancelled}/session");
    assert_eq!(
        post(
            &f.app,
            &path,
            Some(&f.token),
            r#"{"session_id":"cancelled-resume","work_dir":"cancelled-work"}"#
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        post(
            &f.app,
            &path,
            Some(&f.token),
            r#"{"session_id":"overwrite","work_dir":"overwrite"}"#
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let row = db
        .query_one(
            "SELECT session_id,runtime_id,work_dir FROM chat_session WHERE id=$1",
            &[&f.chat],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "cancelled-resume");
    assert_eq!(row.get::<_, Uuid>(1), f.runtime);
    assert_eq!(row.get::<_, String>(2), "cancelled-work");
    // Newer recorded sessions prevent a straggler from rewinding the chat.
    let old = f.task("cancelled", true).await;
    db.execute(
        "UPDATE agent_task_queue SET created_at=now()-interval '1 hour' WHERE id=$1",
        &[&old],
    )
    .await
    .unwrap();
    assert_eq!(
        post(
            &f.app,
            &format!("/api/daemon/tasks/{old}/session"),
            Some(&f.token),
            r#"{"session_id":"too-old"}"#
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT session_id FROM chat_session WHERE id=$1",
            &[&f.chat]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "cancelled-resume"
    );
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn resume_pin_is_atomic_and_concurrent_cancelled_pins_choose_one_resume() {
    let f = Fixture::new().await;
    let task = f.task("cancelled", true).await;
    let path = format!("/api/daemon/tasks/{task}/session");
    let (one, two) = tokio::join!(
        post(
            &f.app,
            &path,
            Some(&f.token),
            r#"{"session_id":"one","work_dir":"one"}"#
        ),
        post(
            &f.app,
            &path,
            Some(&f.token),
            r#"{"session_id":"two","work_dir":"two"}"#
        )
    );
    assert_eq!(one.0, StatusCode::NO_CONTENT);
    assert_eq!(two.0, StatusCode::NO_CONTENT);
    let db = f.pool.get().await.unwrap();
    let row = db.query_one("SELECT t.session_id,t.work_dir,cs.session_id AS chat_session,cs.work_dir AS chat_work FROM agent_task_queue t JOIN chat_session cs ON cs.id=t.chat_session_id WHERE t.id=$1", &[&task]).await.unwrap();
    let winner: String = row.get("session_id");
    assert!(matches!(winner.as_str(), "one" | "two"));
    for column in ["work_dir", "chat_session", "chat_work"] {
        assert_eq!(row.get::<_, String>(column), winner);
    }
    let rollback = f.task("cancelled", true).await;
    let suffix = rollback.simple();
    db.batch_execute(&format!("CREATE FUNCTION pin_fail_{suffix}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='{}'::uuid THEN RAISE EXCEPTION 'synthetic pin rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER pin_fail_{suffix} BEFORE UPDATE ON chat_session FOR EACH ROW EXECUTE FUNCTION pin_fail_{suffix}();",f.chat)).await.unwrap();
    let result = post(
        &f.app,
        &format!("/api/daemon/tasks/{rollback}/session"),
        Some(&f.token),
        r#"{"session_id":"must-rollback"}"#,
    )
    .await;
    db.batch_execute(&format!(
        "DROP TRIGGER pin_fail_{suffix} ON chat_session; DROP FUNCTION pin_fail_{suffix}();"
    ))
    .await
    .unwrap();
    assert_eq!(result.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(result.1["error"], "pin session failed");
    assert!(
        db.query_one(
            "SELECT session_id FROM agent_task_queue WHERE id=$1",
            &[&rollback]
        )
        .await
        .unwrap()
        .get::<_, Option<String>>(0)
        .is_none()
    );
    assert_eq!(
        db.query_one(
            "SELECT session_id FROM chat_session WHERE id=$1",
            &[&f.chat]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        winner
    );
}

async fn wait_for_lock(pool: &Pool, locker: i32) {
    for _ in 0..100 {
        let db = pool.get().await.unwrap();
        if db
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
                &[&locker],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("request never reached lock barrier");
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn delivery_auth_fails_closed_and_rechecks_runtime_owner_after_lock_wait() {
    let f = Fixture::new().await;
    let task = f.task("running", false).await;
    let comment = f.supplement(task, "member", "private supplement").await;
    let pin = format!("/api/daemon/tasks/{task}/session");
    let claim = format!("/api/daemon/tasks/{task}/supplements/claim");
    let ack = format!("/api/daemon/tasks/{task}/supplements/{comment}/ack");
    for (path, body) in [
        (&pin, r#"{"session_id":"denied"}"#),
        (&claim, "{}"),
        (&ack, r#"{"delivered":true}"#),
    ] {
        assert_eq!(
            post(&f.app, path, None, body).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            post(&f.app, path, Some(&jwt(f.peer)), body).await.0,
            StatusCode::NOT_FOUND
        );
        let response = f
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("cookie", format!("multica_session={}", jwt(f.owner)))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    // Hold the runtime before the handler freezes it. Its initial lookup sees
    // the old owner, then the locked re-read must see the committed new owner.
    let mut db = f.pool.get().await.unwrap();
    let pid: i32 = db
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM agent_runtime WHERE id=$1 FOR UPDATE",
        &[&f.runtime],
    )
    .await
    .unwrap();
    let app = f.app.clone();
    let token = f.token.clone();
    let request = tokio::spawn(async move {
        post(&app, &pin, Some(&token), r#"{"session_id":"stale-owner"}"#).await
    });
    wait_for_lock(&f.pool, pid).await;
    tx.execute(
        "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1",
        &[&f.runtime, &f.peer],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(request.await.unwrap().0, StatusCode::NOT_FOUND);
    assert!(
        db.query_one(
            "SELECT session_id FROM agent_task_queue WHERE id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, Option<String>>(0)
        .is_none()
    );
    assert_eq!(
        post(&f.app, &claim, Some(&f.token), "{}").await.0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1",
        &[&f.runtime, &f.owner],
    )
    .await
    .unwrap();
    db.execute(
        "DELETE FROM daemon_token WHERE token_hash=$1",
        &[&hex::encode(Sha256::digest(f.token.as_bytes()))],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&f.app, &claim, Some(&f.token), "{}").await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM task_supplement WHERE task_id=$1 AND comment_id=$2",
            &[&task, &comment]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn supplement_claim_negotiates_author_and_has_single_delivery_under_concurrency() {
    let f = Fixture::new().await;
    let task = f.task("running", false).await;
    let agent_comment = f.supplement(task, "agent", "agent additional input").await;
    let member_comment = f
        .supplement(task, "member", "member additional input")
        .await;
    let path = format!("/api/daemon/tasks/{task}/supplements/claim");
    let (status, body) = post(&f.app, &path, Some(&f.token), "malformed").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["comment_id"], member_comment.to_string());
    assert_eq!(body["author_type"], "member");
    assert_eq!(body["content"], "member additional input");
    assert_eq!(body["author_name"], "Preparation");
    assert_eq!(post(&f.app, &path, Some(&f.token), "{}").await.1, json!({}));
    let db = f.pool.get().await.unwrap();
    db.execute("UPDATE agent SET name='  ' WHERE id=$1", &[&f.agent])
        .await
        .unwrap();
    let offered = r#"{"capabilities":[0,"task-supplement-author-v1"]}"#;
    let (one, two) = tokio::join!(
        post(&f.app, &path, Some(&f.token), offered),
        post(&f.app, &path, Some(&f.token), offered)
    );
    assert_eq!(one.0, StatusCode::OK);
    assert_eq!(two.0, StatusCode::OK);
    let winners: Vec<_> = [&one.1, &two.1]
        .into_iter()
        .filter(|b| b.get("comment_id").is_some())
        .collect();
    assert_eq!(winners.len(), 1);
    assert_eq!(winners[0]["comment_id"], agent_comment.to_string());
    assert_eq!(winners[0]["author_name"], "an agent");
    assert_eq!(
        db.query_one(
            "SELECT attempt_count FROM task_supplement WHERE task_id=$1 AND comment_id=$2",
            &[&task, &agent_comment]
        )
        .await
        .unwrap()
        .get::<_, i32>(0),
        1
    );
    let pending = f.supplement(task, "member", "pending after terminal").await;
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&f.app, &path, Some(&f.token), offered).await.1,
        json!({})
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM task_supplement WHERE task_id=$1 AND comment_id=$2",
            &[&task, &pending]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    let no_cap = f.task("running", false).await;
    f.supplement(no_cap, "member", "capability removed").await;
    db.execute(
        "DELETE FROM task_supplement_capability WHERE task_id=$1",
        &[&no_cap],
    )
    .await
    .unwrap();
    assert_eq!(
        post(
            &f.app,
            &format!("/api/daemon/tasks/{no_cap}/supplements/claim"),
            Some(&f.token),
            "{}"
        )
        .await
        .1,
        json!({})
    );
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn supplement_claim_skips_locked_receipts_and_orders_equal_timestamps_by_id() {
    let f = Fixture::new().await;
    let task = f.task("running", false).await;
    let mut ids = [
        f.supplement(task, "member", "one").await,
        f.supplement(task, "member", "two").await,
        f.supplement(task, "member", "three").await,
    ];
    ids.sort();
    let mut db = f.pool.get().await.unwrap();
    db.execute(
        "UPDATE task_supplement SET created_at='2020-01-01T00:00:00Z' WHERE task_id=$1",
        &[&task],
    )
    .await
    .unwrap();
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT comment_id FROM task_supplement WHERE task_id=$1 AND comment_id=$2 FOR UPDATE",
        &[&task, &ids[0]],
    )
    .await
    .unwrap();
    let path = format!("/api/daemon/tasks/{task}/supplements/claim");
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        post(&f.app, &path, Some(&f.token), "{}"),
    )
    .await
    .unwrap();
    assert_eq!(response.0, StatusCode::OK);
    assert_eq!(response.1["comment_id"], ids[1].to_string());
    tx.commit().await.unwrap();
    assert_eq!(
        post(&f.app, &path, Some(&f.token), "{}").await.1["comment_id"],
        ids[0].to_string()
    );
    assert_eq!(
        post(&f.app, &path, Some(&f.token), "{}").await.1["comment_id"],
        ids[2].to_string()
    );
    assert_eq!(post(&f.app, &path, Some(&f.token), "{}").await.1, json!({}));
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn supplement_ack_does_not_resurrect_unattempted_or_other_run_receipts() {
    let f = Fixture::new().await;
    let task = f.task("running", false).await;
    let comment = f.supplement(task, "member", "not attempted").await;
    let other = f.task("running", false).await;
    let db = f.pool.get().await.unwrap();
    db.execute("UPDATE task_supplement SET status='failed',failure_reason='turn_ended' WHERE task_id=$1 AND comment_id=$2", &[&task,&comment]).await.unwrap();
    for target in [task, other] {
        let path = format!("/api/daemon/tasks/{target}/supplements/{comment}/ack");
        assert_eq!(
            post(&f.app, &path, Some(&f.token), r#"{"delivered":true}"#)
                .await
                .0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            post(&f.app, &path, Some(&f.token), r#"{"error":"timeout"}"#)
                .await
                .0,
            StatusCode::CONFLICT
        );
    }
    let row = db.query_one("SELECT status,attempt_count,delivered_at FROM task_supplement WHERE task_id=$1 AND comment_id=$2", &[&task,&comment]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, i32>(1), 0);
    assert!(
        row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(2)
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn supplement_ack_is_idempotent_preserves_late_success_and_redacts_failure() {
    let f = Fixture::new().await;
    let task = f.task("running", false).await;
    let comment = f.supplement(task, "member", "additional input").await;
    let path = format!("/api/daemon/tasks/{task}/supplements/{comment}/ack");
    assert_eq!(
        post(&f.app, &path, Some(&f.token), r#"{"delivered":true}"#)
            .await
            .0,
        StatusCode::CONFLICT
    );
    let claim = format!("/api/daemon/tasks/{task}/supplements/claim");
    post(&f.app, &claim, Some(&f.token), "{}").await;
    assert_eq!(
        post(&f.app, &path, Some(&f.token), r#"{"delivered":"yes"}"#)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let db = f.pool.get().await.unwrap();
    // Application-side settlement can win the task lock before provider's
    // acknowledgement arrives; an attempted delivery can still succeed.
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    db.execute("UPDATE task_supplement SET status='failed',failure_reason='turn_ended' WHERE task_id=$1 AND comment_id=$2", &[&task,&comment]).await.unwrap();
    let (code, first) = post(&f.app, &path, Some(&f.token), r#"{"delivered":true}"#).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(first["supplement_status"], "delivered");
    assert!(first.get("supplement_failure_reason").is_none());
    let second = post(&f.app, &path, Some(&f.token), r#"{"delivered":true}"#).await;
    assert_eq!(second.1, first);
    assert_eq!(
        post(&f.app, &path, Some(&f.token), "{}").await.0,
        StatusCode::CONFLICT
    );
    let task = f.task("running", false).await;
    let comment = f.supplement(task, "member", "another input").await;
    post(
        &f.app,
        &format!("/api/daemon/tasks/{task}/supplements/claim"),
        Some(&f.token),
        "{}",
    )
    .await;
    let path = format!("/api/daemon/tasks/{task}/supplements/{comment}/ack");
    let response = post(
        &f.app,
        &path,
        Some(&f.token),
        r#"{"error":"private /home/person API secret tiếng Việt"}"#,
    )
    .await;
    assert_eq!(response.0, StatusCode::OK);
    assert_eq!(response.1["supplement_failure_reason"], "provider_rejected");
    assert_eq!(
        post(&f.app, &path, Some(&f.token), r#"{"delivered":true}"#)
            .await
            .0,
        StatusCode::CONFLICT
    );
    for (raw, expected) in [
        (r#"" \u0000timeout ""#, "timeout"),
        (r#""turn_not_started""#, "turn_not_started"),
    ] {
        db.execute(
            "UPDATE task_supplement SET status='delivering' WHERE task_id=$1 AND comment_id=$2",
            &[&task, &comment],
        )
        .await
        .unwrap();
        let response = post(
            &f.app,
            &path,
            Some(&f.token),
            &format!("{{\"error\":{raw}}}"),
        )
        .await;
        assert_eq!(response.1["supplement_failure_reason"], expected);
    }
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn supplement_ack_broadcasts_committed_comment_with_all_receipts_only_in_workspace() {
    let f = Fixture::new().await;
    let task = f.task("running", false).await;
    let comment = f.supplement(task, "member", "receipt fanout").await;
    let other_task = f.task("running", false).await;
    let db = f.pool.get().await.unwrap();
    db.execute("INSERT INTO task_supplement(task_id,workspace_id,issue_id,comment_id,author_id,client_request_id,status,created_at) VALUES ($1,$2,$3,$4,$5,$4,'pending',now()+interval '1 hour')", &[&other_task,&f.ws,&f.issue,&comment,&f.owner]).await.unwrap();
    post(
        &f.app,
        &format!("/api/daemon/tasks/{task}/supplements/claim"),
        Some(&f.token),
        "{}",
    )
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let app = f.app.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let url = format!("ws://{address}/ws?workspace_id={}", f.ws);
    let mut owner = socket(&url, f.owner).await;
    let mut peer = socket(&url, f.peer).await;
    // Same user in another workspace cannot receive this receipt.
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&f.foreign, &f.owner],
    )
    .await
    .unwrap();
    let mut foreign = socket(
        &format!("ws://{address}/ws?workspace_id={}", f.foreign),
        f.owner,
    )
    .await;
    let response = post(
        &f.app,
        &format!("/api/daemon/tasks/{task}/supplements/{comment}/ack"),
        Some(&f.token),
        r#"{"delivered":true}"#,
    )
    .await;
    assert_eq!(response.0, StatusCode::OK);
    for ws in [&mut owner, &mut peer] {
        let event = frame(ws).await;
        assert_eq!(event["type"], "comment:updated");
        let snapshot = &event["payload"]["comment"];
        assert_eq!(snapshot["id"], comment.to_string());
        assert_eq!(snapshot["content"], "receipt fanout");
        assert_eq!(snapshot["supplements"].as_array().unwrap().len(), 2);
        assert_eq!(snapshot["supplements"][0]["status"], "delivered");
        assert_eq!(snapshot["supplements"][1]["status"], "pending");
        assert_eq!(snapshot["supplement_task_id"], task.to_string());
        assert_eq!(snapshot["attachments"], json!([]));
        assert_eq!(snapshot["reactions"], json!([]));
        assert_eq!(
            db.query_one(
                "SELECT status FROM task_supplement WHERE task_id=$1 AND comment_id=$2",
                &[&task, &comment]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "delivered"
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), foreign.next())
            .await
            .is_err()
    );
    owner.close(None).await.unwrap();
    peer.close(None).await.unwrap();
    foreign.close(None).await.unwrap();
    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
