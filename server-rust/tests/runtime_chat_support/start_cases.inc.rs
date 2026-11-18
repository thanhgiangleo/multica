// Start tests share the synthetic daemon fixture, not a live agent executable.
fn start_path(id: Uuid) -> String {
    format!("/api/daemon/tasks/{id}/start")
}
async fn start_body(f: &Fixture, id: Uuid, caps: &[&str]) -> String {
    let dispatched: chrono::DateTime<chrono::Utc> = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT dispatched_at FROM agent_task_queue WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0);
    json!({"runtime_id":f.runtime,"dispatched_at":dispatched.to_rfc3339_opts(chrono::SecondsFormat::Micros,true),"capabilities":caps}).to_string()
}
async fn start_row(f: &Fixture, id: Uuid) -> Value {
    f.pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT to_jsonb(t) FROM agent_task_queue t WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0)
}
#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn start_negotiates_only_supported_exact_turns_and_legacy_is_single_winner() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    for provider in [
        "codex",
        "claude",
        "grok",
        "cursor",
        "kiro",
        "pi",
        "omp",
        "synthetic",
    ] {
        db.execute(
            "UPDATE agent_runtime SET provider=$2 WHERE id=$1",
            &[&f.runtime, &provider],
        )
        .await
        .unwrap();
        for chat in [false, true] {
            let id = f.task("waiting_local_directory", chat).await;
            db.execute("UPDATE agent_task_queue SET wait_reason='held',prepare_lease_expires_at=now()+interval '45 seconds',result='{\"kept\":true}' WHERE id=$1",&[&id]).await.unwrap();
            let body = start_body(&f, id, &["task-supplement-v1", "pi-process-cleanup-v1"]).await;
            let (code, result) = post(&f.app, &start_path(id), Some(&f.token), &body).await;
            assert_eq!(code, StatusCode::OK, "{provider} {chat}: {result}");
            assert_eq!(result["status"], "running");
            assert!(!result["started_at"].is_null());
            assert_eq!(result["result"]["kept"], true);
            let supplement =
                !chat && matches!(provider, "codex" | "claude" | "grok" | "cursor" | "kiro");
            let cleanup = matches!(provider, "pi" | "omp");
            assert_eq!(
                result.get("supplement_capability").is_some(),
                supplement,
                "{provider} {chat}"
            );
            assert_eq!(result.get("process_cleanup_capability").is_some(), cleanup);
            let before = start_row(&f, id).await;
            assert!(before["wait_reason"].is_null());
            assert!(before["prepare_lease_expires_at"].is_null());
            // A replay may not renegotiate capabilities or overwrite started_at.
            let replay = start_body(&f, id, &[]).await;
            assert_eq!(
                post(&f.app, &start_path(id), Some(&f.token), &replay)
                    .await
                    .1,
                result
            );
            assert_eq!(start_row(&f, id).await, before);
            assert_eq!(
                post(&f.app, &start_path(id), Some(&f.token), "{}").await.0,
                StatusCode::BAD_REQUEST
            );
        }
    }
    let id = f.task("dispatched", false).await;
    let path = start_path(id);
    let (a, b) = tokio::join!(
        post(&f.app, &path, Some(&f.token), ""),
        post(&f.app, &path, Some(&f.token), "null")
    );
    let mut codes = [a.0.as_u16(), b.0.as_u16()];
    codes.sort();
    assert_eq!(codes, [200, 400]);
    assert!(a.1.get("supplement_capability").is_none());
    assert!(b.1.get("supplement_capability").is_none());
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn start_claim_replays_publish_once_after_commit_and_keep_chat_private() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let id = f.task("waiting_local_directory", true).await;
    db.execute("UPDATE agent SET status='idle' WHERE id=$1", &[&f.agent])
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = f.app.clone();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let url = format!("ws://127.0.0.1:{port}/ws?workspace_id={}", f.ws);
    let mut owner = socket(&url, f.owner).await;
    let mut peer = socket(&url, f.peer).await;
    let body = start_body(&f, id, &[]).await;
    let path = start_path(id);
    let (a, b) = tokio::join!(
        post(&f.app, &path, Some(&f.token), &body),
        post(&f.app, &path, Some(&f.token), &body)
    );
    assert_eq!(a.0, StatusCode::OK, "{:?}", a.1);
    assert_eq!(a, b);
    assert_eq!(frame(&mut owner).await["type"], "agent:status");
    assert_eq!(frame(&mut peer).await["type"], "agent:status");
    let event = frame(&mut owner).await;
    assert_eq!(event["type"], "task:running");
    assert_eq!(
        event["payload"],
        json!({"task_id":id,"agent_id":f.agent,"issue_id":"","status":"running","chat_session_id":f.chat})
    );
    assert_eq!(start_row(&f, id).await["status"], "running");
    for s in [&mut owner, &mut peer] {
        assert!(
            tokio::time::timeout(Duration::from_millis(80), s.next())
                .await
                .is_err(),
            "no duplicate/private replay event"
        );
        s.close(None).await.unwrap();
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn start_auth_generation_and_terminal_fences_never_create_capabilities() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let id = f.task("dispatched", false).await;
    db.execute(
        "UPDATE agent_runtime SET provider='codex' WHERE id=$1",
        &[&f.runtime],
    )
    .await
    .unwrap();
    let path = start_path(id);
    let body = start_body(&f, id, &["task-supplement-v1"]).await;
    for token in [None, Some("mat_no"), Some("mct_no"), Some("mdt_no")] {
        assert_eq!(
            post(&f.app, &path, token, &body).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        post(&f.app, &path, Some(&jwt(f.peer)), &body).await.0,
        StatusCode::NOT_FOUND
    );
    let cookie = f
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&path)
                .header("cookie", format!("multica_auth={}", jwt(f.owner)))
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cookie.status(), StatusCode::UNAUTHORIZED);
    let before = start_row(&f, id).await;
    for invalid in [
        "[]".to_owned(),
        "{\"capabilities\":[1]}".to_owned(),
        json!({"runtime_id":f.runtime}).to_string(),
        json!({"runtime_id":f.runtime,"dispatched_at":"2026-11-02T01:02:03.123456789Z"})
            .to_string(),
    ] {
        assert_eq!(
            post(&f.app, &path, Some(&f.token), &invalid).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(start_row(&f, id).await, before);
    db.execute("UPDATE agent_task_queue SET dispatched_at=dispatched_at+interval '1 microsecond' WHERE id=$1",&[&id]).await.unwrap();
    assert_eq!(
        post(&f.app, &path, Some(&f.token), &body).await.0,
        StatusCode::CONFLICT
    );
    let terminal_body = start_body(&f, id, &["task-supplement-v1"]).await;
    for status in ["queued", "completed", "cancelled", "failed"] {
        db.execute(
            "UPDATE agent_task_queue SET status=$2 WHERE id=$1",
            &[&id, &status],
        )
        .await
        .unwrap();
        assert_eq!(
            post(&f.app, &path, Some(&f.token), &terminal_body).await.0,
            StatusCode::CONFLICT
        );
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM task_supplement_capability WHERE task_id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.execute(
        "UPDATE daemon_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&hex::encode(Sha256::digest(f.token.as_bytes()))],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&f.app, &path, Some(&f.token), &body).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn start_rechecks_claim_terminal_and_owner_after_database_lock_waits() {
    for mutation in ["terminal", "generation", "owner"] {
        let f = Fixture::new().await;
        let mut db = f.pool.get().await.unwrap();
        let id = f.task("dispatched", false).await;
        let body = start_body(&f, id, &["task-supplement-v1"]).await;
        let path = start_path(id);
        let pid: i32 = db
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        let tx = db.transaction().await.unwrap();
        match mutation {
            "terminal" => {
                tx.execute(
                    "UPDATE agent_task_queue SET status='cancelled' WHERE id=$1",
                    &[&id],
                )
                .await
                .unwrap();
            }
            "generation" => {
                tx.execute("UPDATE agent_task_queue SET dispatched_at=dispatched_at+interval '1 microsecond' WHERE id=$1",&[&id]).await.unwrap();
            }
            _ => {
                tx.execute(
                    "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1",
                    &[&f.runtime, &f.peer],
                )
                .await
                .unwrap();
            }
        }
        let app = f.app.clone();
        let token = f.token.clone();
        let request = tokio::spawn(async move { post(&app, &path, Some(&token), &body).await });
        wait_for_lock(&f.pool, pid).await;
        tx.commit().await.unwrap();
        let result = request.await.unwrap();
        assert_eq!(
            result.0,
            if mutation == "owner" {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::CONFLICT
            },
            "{mutation}: {result:?}"
        );
        let row = start_row(&f, id).await;
        assert!(row["started_at"].is_null());
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM task_supplement_capability WHERE task_id=$1",
                &[&id]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            0
        );
    }
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn start_cleanup_fences_requeue_exact_claim_and_declare_unintegrated_branches() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let stopping = f.task("cancelled", false).await;
    db.execute("UPDATE agent_task_queue SET completed_at=now(),result='{\"process_cleanup_pending\":true}' WHERE id=$1",&[&stopping]).await.unwrap();
    let id = f.task("dispatched", false).await;
    let body = start_body(&f, id, &["task-supplement-v1"]).await;
    let result = post(&f.app, &start_path(id), Some(&f.token), &body).await;
    assert_eq!(
        result,
        (
            StatusCode::CONFLICT,
            json!({"error":"previous run is still stopping; task was requeued"})
        )
    );
    let row = start_row(&f, id).await;
    assert_eq!(row["status"], "queued");
    assert!(row["dispatched_at"].is_null());
    assert!(row["started_at"].is_null());
    let blocked = f.task("dispatched", false).await;
    db.execute(
        "UPDATE agent_task_queue SET completed_at=now()-interval '35 seconds' WHERE id=$1",
        &[&stopping],
    )
    .await
    .unwrap();
    let before = start_row(&f, blocked).await;
    let body = start_body(&f, blocked, &[]).await;
    assert_eq!(
        post(&f.app, &start_path(blocked), Some(&f.token), &body)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        start_row(&f, blocked).await,
        before,
        "terminal cleanup is NOT falsely recorded as integrated"
    );
    db.execute(
        "UPDATE agent_task_queue SET result='{}' WHERE id=$1",
        &[&stopping],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE agent_runtime SET provider='qoder_cloud' WHERE id=$1",
        &[&f.runtime],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&f.app, &start_path(blocked), Some(&f.token), &body).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"task start requires Qoder issue-lifecycle integration"})
        )
    );
    assert_eq!(start_row(&f, blocked).await, before);
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn start_capability_insert_failure_rolls_back_task_and_agent_without_events() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let id = f.task("dispatched", false).await;
    db.execute(
        "UPDATE agent_runtime SET provider='codex' WHERE id=$1",
        &[&f.runtime],
    )
    .await
    .unwrap();
    db.execute("UPDATE agent SET status='idle' WHERE id=$1", &[&f.agent])
        .await
        .unwrap();
    let name = format!("zbsi67_start_{}", id.simple());
    db.batch_execute(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.task_id='{id}' THEN RAISE EXCEPTION 'synthetic start rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER {name} BEFORE INSERT ON task_supplement_capability FOR EACH ROW EXECUTE FUNCTION {name}()" )).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = f.app.clone();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut ws = socket(
        &format!("ws://127.0.0.1:{port}/ws?workspace_id={}", f.ws),
        f.owner,
    )
    .await;
    let before = start_row(&f, id).await;
    let body = start_body(&f, id, &["task-supplement-v1"]).await;
    assert_eq!(
        post(&f.app, &start_path(id), Some(&f.token), &body).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to start task"})
        )
    );
    assert_eq!(start_row(&f, id).await, before);
    assert_eq!(
        db.query_one("SELECT status FROM agent WHERE id=$1", &[&f.agent])
            .await
            .unwrap()
            .get::<_, String>(0),
        "idle"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(80), ws.next())
            .await
            .is_err()
    );
    db.batch_execute(&format!(
        "DROP TRIGGER {name} ON task_supplement_capability; DROP FUNCTION {name}()"
    ))
    .await
    .unwrap();
    ws.close(None).await.unwrap();
    server.abort();
    let _ = server.await;
}
