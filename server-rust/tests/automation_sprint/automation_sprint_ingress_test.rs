//! Included by automation_sprint_definitions, sharing its disposable DB fixture.
use super::*;
use axum::extract::ConnectInfo;
use hmac::{Hmac, Mac};
use std::net::SocketAddr;

fn signed(raw: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(b"synthetic-signing-credential").unwrap();
    mac.update(raw);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

async fn send(
    app: &axum::Router,
    credential: &str,
    raw: &[u8],
    key: &str,
    signature: Option<&str>,
    peer: [u8; 4],
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(format!("/api/webhooks/autopilots/{credential}"))
        .header("Content-Type", "application/json; charset=utf-8")
        .header("X-Workspace-ID", Uuid::new_v4().to_string())
        .header("Authorization", "Bearer invalid-browser-auth-must-not-run")
        .header("X-GitHub-Event", "pull_request")
        .header("X-GitHub-Delivery", key)
        .header("X-Forwarded-For", Uuid::new_v4().to_string());
    if let Some(signature) = signature {
        request = request.header("X-Hub-Signature-256", signature);
    }
    let mut request = request.body(Body::from(raw.to_vec())).unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::from((peer, 4242))));
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let data = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&data).unwrap_or_else(|_| {
            panic!(
                "non-JSON webhook: {status} {}",
                String::from_utf8_lossy(&data)
            )
        }),
    )
}

// Keep credentials and workspace explicit at this shared fixture boundary.
#[allow(clippy::too_many_arguments)]
pub(super) async fn verify(
    app: &axum::Router,
    db: &tokio_postgres::Client,
    pool: &Arc<Pool>,
    ap: Uuid,
    trigger: Uuid,
    old: &str,
    credential: &str,
    ws: Uuid,
    member_token: &str,
) {
    let peer = [127, 0, 0, 71];
    let raw = br#"{"action":"opened","private":"synthetic"}"#;
    let signature = signed(raw);
    assert_eq!(
        send(app, old, raw, "rotated", Some(&signature), peer)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(app, "awt_unknown", raw, "unknown", Some(&signature), peer)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    for (i, bytes) in [
        b"".as_slice(),
        b"null",
        b"7",
        b"true",
        b"\"string\"",
        b"{}{}",
    ]
    .iter()
    .enumerate()
    {
        let result = send(
            app,
            credential,
            bytes,
            &format!("shape-{i}"),
            Some(&signed(bytes)),
            peer,
        )
        .await;
        assert_eq!(result.0, StatusCode::BAD_REQUEST, "{result:?}");
    }
    let huge = vec![b' '; 256 * 1024 + 1];
    let result = send(app, credential, &huge, "huge", None, peer).await;
    assert_eq!(result.0, StatusCode::PAYLOAD_TOO_LARGE, "{result:?}");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM webhook_delivery WHERE autopilot_id=$1",
            &[&ap]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let missing = send(app, credential, raw, "signature-retry", None, peer).await;
    assert_eq!(missing.0, StatusCode::UNAUTHORIZED, "{missing:?}");
    assert_eq!(missing.1["reason"], "missing_signature");
    let invalid = send(
        app,
        credential,
        raw,
        "signature-retry",
        Some("sha256=00"),
        peer,
    )
    .await;
    assert_eq!(invalid.0, StatusCode::UNAUTHORIZED, "{invalid:?}");
    assert_eq!(invalid.1["reason"], "invalid_signature");
    let valid = send(
        app,
        credential,
        raw,
        "signature-retry",
        Some(&signature),
        peer,
    )
    .await;
    assert_eq!(valid.0, StatusCode::SERVICE_UNAVAILABLE, "{valid:?}");
    assert!(valid.1.get("run_id").is_none());
    let delivery = Uuid::parse_str(valid.1["delivery_id"].as_str().unwrap()).unwrap();
    let stored = db
        .query_one("SELECT * FROM webhook_delivery WHERE id=$1", &[&delivery])
        .await
        .unwrap();
    assert_eq!(stored.get::<_, Uuid>("workspace_id"), ws);
    assert_eq!(stored.get::<_, Uuid>("trigger_id"), trigger);
    assert_eq!(stored.get::<_, String>("signature_status"), "valid");
    assert_eq!(stored.get::<_, String>("status"), "queued");
    assert_eq!(
        stored.get::<_, String>("event"),
        "github.pull_request.opened"
    );
    assert_eq!(stored.get::<_, Vec<u8>>("raw_body"), raw);
    assert_eq!(stored.get::<_, String>("content_type"), "application/json");
    assert_eq!(stored.get::<_, i32>("response_status"), 503);
    let selected: Value = stored.get("selected_headers");
    assert_eq!(selected["x-hub-signature-256-present"], true);
    assert!(selected.get("authorization").is_none());
    assert!(!selected.to_string().contains(&signature));
    assert!(!selected.to_string().contains(credential));
    let (first, second) = tokio::join!(
        send(app, credential, raw, "parallel", Some(&signature), peer),
        send(app, credential, raw, "parallel", Some(&signature), peer)
    );
    assert_eq!(first.0, StatusCode::SERVICE_UNAVAILABLE, "{first:?}");
    assert_eq!(second.0, StatusCode::SERVICE_UNAVAILABLE, "{second:?}");
    assert_eq!(first.1["delivery_id"], second.1["delivery_id"]);
    let row=db.query_one("SELECT count(*),max(attempt_count) FROM webhook_delivery WHERE trigger_id=$1 AND dedupe_key='parallel'", &[&trigger]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, i32>(1), 2);
    let tp = format!("/api/autopilots/{ap}/triggers/{trigger}/");
    assert_eq!(
        request(
            app,
            "PATCH",
            &tp,
            ws,
            Some(member_token),
            json!({"enabled":false})
        )
        .await
        .0,
        StatusCode::OK
    );
    let disabled = send(app, credential, raw, "disabled", Some(&signature), peer).await;
    assert_eq!(disabled.0, StatusCode::OK, "{disabled:?}");
    assert_eq!(disabled.1["reason"], "trigger_disabled");
    let duplicate = send(app, credential, raw, "disabled", Some(&signature), peer).await;
    assert_eq!(duplicate.0, StatusCode::OK, "{duplicate:?}");
    assert_eq!(duplicate.1["status"], "duplicate");
    // A queued delivery without admission remains unavailable, even after pause.
    let queued = send(app, credential, raw, "parallel", Some(&signature), peer).await;
    assert_eq!(queued.0, StatusCode::SERVICE_UNAVAILABLE, "{queued:?}");
    assert_eq!(request(app,"PATCH",&tp,ws,Some(member_token),json!({"enabled":true,"event_filters":[{"event":"pull_request","actions":["closed"]},{"event":"pull_request","actions":["merged"]}]})).await.0,StatusCode::OK);
    let filtered = send(app, credential, raw, "filtered", Some(&signature), peer).await;
    assert_eq!(filtered.0, StatusCode::OK, "{filtered:?}");
    assert_eq!(filtered.1["reason"], "event_filtered");
    let envelope = br#"{"event":"github.pull_request","eventPayload":{"state":"merged"}}"#;
    let allowed = send(
        app,
        credential,
        envelope,
        "allowed-later-filter",
        Some(&signed(envelope)),
        peer,
    )
    .await;
    assert_eq!(allowed.0, StatusCode::SERVICE_UNAVAILABLE, "{allowed:?}");
    assert_eq!(
        request(
            app,
            "PATCH",
            &tp,
            ws,
            Some(member_token),
            json!({"event_filters":[]})
        )
        .await
        .0,
        StatusCode::OK
    );
    let path = format!("/api/autopilots/{ap}/");
    assert_eq!(
        request(
            app,
            "PATCH",
            &path,
            ws,
            Some(member_token),
            json!({"status":"paused"})
        )
        .await
        .0,
        StatusCode::OK
    );
    let paused = send(app, credential, raw, "paused", Some(&signature), peer).await;
    assert_eq!(paused.0, StatusCode::OK, "{paused:?}");
    assert_eq!(paused.1["reason"], "autopilot_paused");
    assert_eq!(
        request(
            app,
            "PATCH",
            &path,
            ws,
            Some(member_token),
            json!({"status":"active"})
        )
        .await
        .0,
        StatusCode::OK
    );
    // Atomic delivery + acknowledgement persistence under an injected DB error.
    db.batch_execute("CREATE FUNCTION automation_sprint_reject_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.response_status IS NOT NULL THEN RAISE EXCEPTION 'synthetic acknowledgement failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER automation_sprint_reject_ack BEFORE UPDATE ON webhook_delivery FOR EACH ROW EXECUTE FUNCTION automation_sprint_reject_ack();").await.unwrap();
    let rollback = send(app, credential, raw, "ack-rollback", Some(&signature), peer).await;
    assert_eq!(
        rollback.0,
        StatusCode::INTERNAL_SERVER_ERROR,
        "{rollback:?}"
    );
    assert_eq!(db.query_one("SELECT count(*) FROM webhook_delivery WHERE trigger_id=$1 AND dedupe_key='ack-rollback'", &[&trigger]).await.unwrap().get::<_,i64>(0),0);
    db.batch_execute("DROP TRIGGER automation_sprint_reject_ack ON webhook_delivery; DROP FUNCTION automation_sprint_reject_ack();").await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM autopilot_run WHERE autopilot_id=$1",
            &[&ap]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    // Unknown-token debt gates before DB and cannot be reset by forged XFF.
    let abusive = [127, 0, 0, 72];
    for _ in 0..30 {
        assert_eq!(
            send(app, "awt_unknown", raw, "debt", None, abusive).await.0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        send(app, credential, raw, "blocked", Some(&signature), abusive)
            .await
            .0,
        StatusCode::TOO_MANY_REQUESTS
    );
    // Legitimate NAT traffic does not consume bad-credential debt.
    for i in 0..31 {
        assert_eq!(
            send(
                app,
                credential,
                raw,
                &format!("nat-{i}"),
                Some(&signature),
                [127, 0, 0, 73]
            )
            .await
            .0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    // Two fresh routers model separate processes. Only one of two concurrent
    // requests may consume the last slot; rejected requests leave no row.
    let recent: i64 = db.query_one(
        "SELECT count(*) FROM webhook_delivery WHERE trigger_id=$1 AND replayed_from_delivery_id IS NULL AND created_at>now()-interval '60 seconds'",
        &[&trigger],
    ).await.unwrap().get(0);
    assert!(recent < 60, "fixture unexpectedly saturated: {recent}");
    db.execute(
        "INSERT INTO webhook_delivery(workspace_id,autopilot_id,trigger_id,provider,status,raw_body) SELECT $1,$2,$3,'github','ignored','{}'::bytea FROM generate_series(1,$4::bigint)",
        &[&ws,&ap,&trigger,&(59-recent)],
    ).await.unwrap();
    let replica = || {
        router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "ingress-replica".into(),
            )),
            ApiState::new(pool.clone(), SECRET.to_vec()),
        )
    };
    let a = replica();
    let b = replica();
    let (first, second) = tokio::join!(
        send(
            &a,
            credential,
            raw,
            "replica-a",
            Some(&signature),
            [127, 0, 0, 74]
        ),
        send(
            &b,
            credential,
            raw,
            "replica-b",
            Some(&signature),
            [127, 0, 0, 75]
        ),
    );
    let statuses = [first.0, second.0];
    assert_eq!(
        statuses
            .iter()
            .filter(|s| **s == StatusCode::SERVICE_UNAVAILABLE)
            .count(),
        1,
        "{first:?} {second:?}"
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|s| **s == StatusCode::TOO_MANY_REQUESTS)
            .count(),
        1,
        "{first:?} {second:?}"
    );
    assert_eq!(
        db.query_one("SELECT count(*) FROM webhook_delivery WHERE trigger_id=$1 AND replayed_from_delivery_id IS NULL AND created_at>now()-interval '60 seconds'", &[&trigger]).await.unwrap().get::<_,i64>(0),
        60,
    );
    automation_sprint_worker_test::verify(db, pool, ap, trigger, ws).await;
    println!(
        "automation_sprint ingress: public URL auth, signature, size/shape, workspace derivation, ignored/filter, concurrent dedupe, retry, rollback, rate debt passed; active admission remains 503 (no runtime)"
    );
}
