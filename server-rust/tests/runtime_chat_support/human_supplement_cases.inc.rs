// Included by runtime_chat_supplement.rs; all credentials and rows are synthetic.
#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn machine_tokens_revoked_pat_and_cookie_csrf_cannot_bypass_human_gate() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let machine = format!("mat_supplement_{}", f.task);
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(machine.as_bytes())),&f.task,&f.agent,&f.workspace,&f.author]).await.unwrap();
    let denied = f.post_with_token(&machine, false, None).await;
    assert_eq!(denied.0, StatusCode::FORBIDDEN, "{}", denied.1);
    assert_eq!(
        denied.1["error"],
        "this endpoint is only available to human actors"
    );
    for token in [
        "mdt_synthetic-not-human",
        "mcn_synthetic-unconfigured",
        "mul_missing",
    ] {
        assert_eq!(
            f.post_with_token(token, false, None).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    let pat = format!("mul_supplement_{}", f.task);
    let pat_id: Uuid = db.query_one("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'Supplement',$2,'mul_supplement') RETURNING id", &[&f.author,&hex::encode(Sha256::digest(pat.as_bytes()))]).await.unwrap().get(0);
    assert_eq!(
        f.post_with_token(&pat, false, None).await.0,
        StatusCode::CREATED
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE id=$1",
        &[&pat_id],
    )
    .await
    .unwrap();
    assert_eq!(
        f.post_with_token(&pat, false, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE personal_access_token SET revoked=false,expires_at=now()-interval '1 second' WHERE id=$1", &[&pat_id]).await.unwrap();
    assert_eq!(
        f.post_with_token(&pat, false, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    let token = jwt(f.author);
    for csrf in [None, Some("invalid")] {
        let denied = f.post_with_token(&token, true, csrf).await;
        assert_eq!(denied.0, StatusCode::FORBIDDEN);
        assert_eq!(denied.1["error"], "CSRF validation failed");
    }
    let nonce = b"supplement-csrf-synthetic";
    let mut mac = Hmac::<Sha256>::new_from_slice(token.as_bytes()).unwrap();
    mac.update(nonce);
    let csrf = format!(
        "{}.{}",
        hex::encode(nonce),
        hex::encode(mac.finalize().into_bytes())
    );
    assert_eq!(
        f.post_with_token(&token, true, Some(&csrf)).await.0,
        StatusCode::CREATED
    );
    assert_eq!(f.receipt_count().await, 2);
}

type SupplementSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
async fn supplement_frame(socket: &mut SupplementSocket) -> Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = frame {
            return serde_json::from_str(&text).unwrap();
        }
    }
}
async fn supplement_socket(url: &str, user: Uuid) -> SupplementSocket {
    let (mut socket, _) = connect_async(url).await.unwrap();
    socket
        .send(Message::Text(
            json!({"type":"auth","payload":{"token":jwt(user)}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(supplement_frame(&mut socket).await["type"], "auth_ack");
    socket
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn committed_events_replay_silence_and_cross_workspace_isolation() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
        &[&f.other_workspace, &f.other],
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = f.app.clone();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let url = format!("ws://{address}/ws?workspace_id={}", f.workspace);
    let mut owner = supplement_socket(&url, f.author).await;
    let mut member = supplement_socket(&url, f.other).await;
    let mut foreign = supplement_socket(
        &format!("ws://{address}/ws?workspace_id={}", f.other_workspace),
        f.other,
    )
    .await;
    let request = Uuid::new_v4();
    let created = f.submit(f.author, request).await;
    assert_eq!(created.0, StatusCode::CREATED, "{}", created.1);
    let event = supplement_frame(&mut owner).await;
    assert_eq!(event["type"], "comment:created");
    assert_eq!(event["payload"]["comment"], created.1);
    assert_eq!(
        event["payload"]["issue_revision"],
        created.1["issue_revision"]
    );
    // Issue comments are workspace-visible, unlike owner-private chat events.
    assert_eq!(
        supplement_frame(&mut member).await["payload"]["comment"]["id"],
        created.1["id"]
    );
    let comment = Uuid::parse_str(created.1["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        db.query_one(
            "SELECT status FROM task_supplement WHERE comment_id=$1",
            &[&comment]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    assert_eq!(f.submit(f.author, request).await.0, StatusCode::OK);
    assert!(
        tokio::time::timeout(Duration::from_millis(80), owner.next())
            .await
            .is_err(),
        "idempotent create must not publish twice"
    );
    db.execute(
        "UPDATE task_supplement SET status='failed',failure_reason='timeout' WHERE comment_id=$1",
        &[&comment],
    )
    .await
    .unwrap();
    let retry = format!("{}/{comment}/retry", f.path());
    assert_eq!(
        f.post(f.author, &retry, "", f.workspace).await.0,
        StatusCode::OK
    );
    let updated = supplement_frame(&mut owner).await;
    assert_eq!(updated["type"], "comment:updated");
    assert_eq!(
        updated["payload"]["comment"]["supplements"][0]["status"],
        "pending"
    );
    assert_eq!(
        supplement_frame(&mut member).await["type"],
        "comment:updated"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(80), foreign.next())
            .await
            .is_err(),
        "foreign workspace must not receive issue comments"
    );
    for socket in [&mut owner, &mut member, &mut foreign] {
        socket.close(None).await.unwrap();
    }
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

async fn wait_for_blocked_statement(pool: &Pool, prefix: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let db = pool.get().await.unwrap();
        loop {
            let waiting: bool = db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1)", &[&format!("{prefix}%")]).await.unwrap().get(0);
            if waiting { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("request did not reach expected lock wait");
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn lock_waits_recheck_terminal_owner_and_atomic_insert_failure() {
    let f = Arc::new(Fixture::new().await);
    let mut db = f.pool.get().await.unwrap();
    let before: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&f.issue])
        .await
        .unwrap()
        .get(0);
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE",
        &[&f.task],
    )
    .await
    .unwrap();
    let sender = f.clone();
    let send = tokio::spawn(async move { sender.submit(sender.author, Uuid::new_v4()).await });
    wait_for_blocked_statement(
        &f.pool,
        "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE",
    )
    .await;
    tx.execute(
        "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1",
        &[&f.task],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let denied = send.await.unwrap();
    assert_eq!(denied.0, StatusCode::CONFLICT);
    assert_eq!(denied.1["code"], "task_supplement_turn_ended");
    db.execute(
        "UPDATE agent_task_queue SET status='running',completed_at=NULL WHERE id=$1",
        &[&f.task],
    )
    .await
    .unwrap();
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&f.agent, &f.other],
    )
    .await
    .unwrap();
    let sender = f.clone();
    let send = tokio::spawn(async move { sender.submit(sender.author, Uuid::new_v4()).await });
    wait_for_blocked_statement(
        &f.pool,
        "SELECT id FROM agent WHERE id=(SELECT agent_id FROM agent_task_queue",
    )
    .await;
    tx.commit().await.unwrap();
    assert_eq!(send.await.unwrap().1["code"], "invocation_not_allowed");
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&f.agent, &f.author],
    )
    .await
    .unwrap();
    // Fail the final receipt insert after its CTE has touched issue and comment.
    // Only this fixture's task is affected, even when other tests run in parallel.
    let constraint = format!("supplement_failure_{}", f.task.simple());
    db.batch_execute(&format!(
        "ALTER TABLE task_supplement ADD CONSTRAINT {constraint} CHECK (task_id <> '{}') NOT VALID",
        f.task
    ))
    .await
    .unwrap();
    let failed = f.submit(f.author, Uuid::new_v4()).await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{}", failed.1);
    db.batch_execute(&format!(
        "ALTER TABLE task_supplement DROP CONSTRAINT {constraint}"
    ))
    .await
    .unwrap();
    assert_eq!(f.receipt_count().await, 0);
    let row = db.query_one("SELECT revision,(SELECT count(*) FROM comment WHERE issue_id=$1) AS comments FROM issue WHERE id=$1", &[&f.issue]).await.unwrap();
    assert_eq!(row.get::<_, i64>("revision"), before);
    assert_eq!(row.get::<_, i64>("comments"), 1);
    assert_eq!(
        f.submit(f.author, Uuid::new_v4()).await.0,
        StatusCode::CREATED
    );
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn issue_key_body_limits_and_author_scoped_idempotency() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    db.execute("UPDATE issue SET number=137 WHERE id=$1", &[&f.issue])
        .await
        .unwrap();
    db.execute(
        "UPDATE agent SET permission_mode='public_to' WHERE id=$1",
        &[&f.agent],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'member',$2)", &[&f.agent,&f.other]).await.unwrap();
    let request = Uuid::new_v4();
    let body = json!({"content":"through key", "client_request_id":request}).to_string();
    let keyed = f
        .post(
            f.author,
            &format!("/api/issues/sup-137/tasks/{}/supplements", f.task),
            &body,
            f.workspace,
        )
        .await;
    assert_eq!(keyed.0, StatusCode::CREATED, "{}", keyed.1);
    let conflicting = f.submit(f.other, request).await;
    assert_eq!(conflicting.0, StatusCode::CONFLICT, "{}", conflicting.1);
    assert_eq!(
        conflicting.1["error"],
        "client_request_id is already in use"
    );
    let peer_request = Uuid::new_v4();
    let peer = f.submit(f.other, peer_request).await;
    assert_eq!(peer.0, StatusCode::CREATED, "{}", peer.1);
    assert_ne!(peer.1["id"], keyed.1["id"]);
    assert_eq!(f.submit(f.other, peer_request).await.1["id"], peer.1["id"]);
    for content in ["é".repeat(32769), "x".repeat(512 * 1024)] {
        let response = f
            .post(
                f.author,
                &f.path(),
                &json!({"content":content,"client_request_id":Uuid::new_v4()}).to_string(),
                f.workspace,
            )
            .await;
        assert_eq!(response.0, StatusCode::BAD_REQUEST, "{}", response.1);
    }
    assert_eq!(f.receipt_count().await, 2);
}
