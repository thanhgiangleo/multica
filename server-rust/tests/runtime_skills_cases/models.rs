use super::*;
use multica_server::api::runtime_skills_sprint_models::claim_pending_model_list;

#[allow(clippy::too_many_arguments)]
pub async fn check(
    app: &axum::Router,
    pool: &Pool,
    runtime: Uuid,
    private_runtime: Uuid,
    ws: Uuid,
    owner: Uuid,
    admin: Uuid,
    member: Uuid,
    machine: &str,
    wrong: &str,
) {
    let db = pool.get().await.unwrap();
    for schema in [
        include_str!("../../runtime_skills_sprint_migrations/004_model_table.sql"),
        include_str!("../../runtime_skills_sprint_migrations/005_model_id_index.sql"),
        include_str!("../../runtime_skills_sprint_migrations/006_model_catalog_index.sql"),
        include_str!("../../runtime_skills_sprint_migrations/007_model_pending_index.sql"),
    ] {
        db.batch_execute(schema).await.unwrap();
    }
    let path = format!("/api/runtimes/{runtime}/models");
    let token = jwt(member);
    assert_eq!(
        call(app, "POST", &path, None, Some(ws), "").await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            app,
            "POST",
            &format!("/api/runtimes/{private_runtime}/models"),
            Some(&jwt(admin)),
            Some(ws),
            ""
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE agent_runtime SET status='offline' WHERE id=$1",
        &[&runtime],
    )
    .await
    .unwrap();
    assert_eq!(
        call(app, "POST", &path, Some(&token), Some(ws), "").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    db.execute(
        "UPDATE agent_runtime SET status='online' WHERE id=$1",
        &[&runtime],
    )
    .await
    .unwrap();
    let created = call(app, "POST", &path, Some(&token), Some(ws), "").await;
    assert_eq!(created.0, StatusCode::OK, "{:?}", created.1);
    assert_eq!(created.1["status"], "pending");
    assert_eq!(created.1["supported"], true);
    assert!(created.1.get("models").is_none());
    let request = Uuid::parse_str(created.1["id"].as_str().unwrap()).unwrap();
    let poll = format!("{path}/{request}");
    let result = format!("/api/daemon/runtimes/{runtime}/models/{request}/result");
    assert_eq!(
        call(app, "POST", &result, Some(wrong), None, "{}").await.0,
        StatusCode::NOT_FOUND
    );
    let (left, right) = tokio::join!(claim_pending_model_list(&**db, runtime), async {
        let other = pool.get().await.unwrap();
        claim_pending_model_list(&**other, runtime).await
    });
    assert_eq!(
        [left.unwrap(), right.unwrap()]
            .iter()
            .filter(|v| v.is_some())
            .count(),
        1
    );
    assert_eq!(
        call(app, "GET", &poll, Some(&token), Some(ws), "").await.1["status"],
        "running"
    );
    assert_eq!(
        call(
            app,
            "POST",
            &result,
            Some(machine),
            None,
            r#"{"supported":"no"}"#
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let body = r#"{"status":"completed","models":[{"id":"real-model","label":"Real","default":true,"thinking":{"supported_levels":[{"value":"high","label":"High"}],"default_level":"high"},"service_tiers":[{"id":"fast","name":"Fast"}],"supports_explicit_standard_service_tier":true,"ignored":"not-projected"}],"unavailable_models":[{"id":"future-model","label":"Future","reason":"upgrade ghp_abcdefghijklmnopqrstuvwxyz0123456789"}]}"#;
    assert_eq!(
        call(app, "POST", &result, Some(machine), None, body)
            .await
            .0,
        StatusCode::OK
    );
    let completed = call(app, "GET", &poll, Some(&token), Some(ws), "").await;
    assert_eq!(completed.1["status"], "completed");
    assert_eq!(completed.1["supported"], true);
    assert_eq!(completed.1["models"][0]["default"], true);
    assert_eq!(
        completed.1["models"][0]["thinking"]["default_level"],
        "high"
    );
    assert!(completed.1["models"][0].get("ignored").is_none());
    assert_eq!(completed.1["unavailable_models"][0]["id"], "future-model");
    assert!(
        !completed
            .1
            .to_string()
            .contains("abcdefghijklmnopqrstuvwxyz")
    );
    assert_eq!(
        call(app, "POST", &result, Some(machine), None, "bad JSON")
            .await
            .0,
        StatusCode::OK
    );
    let cached = call(app, "POST", &path, Some(&token), Some(ws), "").await;
    assert_eq!(cached.1["cached"], true);
    assert_eq!(cached.1["status"], "completed");
    assert!(cached.1.get("cached_at").is_some());
    assert_eq!(
        call(
            app,
            "GET",
            &format!("{path}/{}", cached.1["id"].as_str().unwrap()),
            Some(&token),
            Some(ws),
            ""
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    // An aged usable catalog serves immediately, queues one pending refresh, then coalesces.
    db.execute("UPDATE runtime_rpc_model_catalog SET stored_at=now()-interval '61 seconds' WHERE runtime_id=$1", &[&runtime]).await.unwrap();
    assert_eq!(
        call(app, "POST", &path, Some(&token), Some(ws), "").await.1["cached"],
        true
    );
    assert_eq!(
        call(app, "POST", &path, Some(&token), Some(ws), "").await.1["cached"],
        true
    );
    let pending: i64 = db
        .query_one(
            "SELECT count(*) FROM runtime_rpc_model_list WHERE runtime_id=$1 AND status='pending'",
            &[&runtime],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(pending, 1);
    let refresh = claim_pending_model_list(&**db, runtime)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(call(app,"POST",&format!("/api/daemon/runtimes/{runtime}/models/{refresh}/result"),Some(machine),None,r#"{"status":"completed","fallback":true,"models":[{"id":"fallback","label":"Fallback"}]}"#).await.0,StatusCode::OK);
    assert_eq!(
        call(app, "POST", &path, Some(&token), Some(ws), "").await.1["models"][0]["id"],
        "real-model"
    );
    // Force bypasses cache. Non-completed status takes Go's failed branch; cache stays intact.
    let force = call(
        app,
        "POST",
        &format!("{path}?force=true"),
        Some(&token),
        Some(ws),
        "",
    )
    .await
    .1;
    assert_eq!(force["status"], "pending");
    let forced = force["id"].as_str().unwrap();
    assert_eq!(
        call(
            app,
            "POST",
            &format!("/api/daemon/runtimes/{runtime}/models/{forced}/result"),
            Some(machine),
            None,
            r#"{"status":"running","error":"token=synthetic-failure-secret"}"#
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app,
            "GET",
            &format!("{path}/{forced}"),
            Some(&token),
            Some(ws),
            ""
        )
        .await
        .1["status"],
        "failed"
    );
    // Authoritative unsupported result invalidates an old catalog; unavailable stays advisory.
    let forced = call(
        app,
        "POST",
        &format!("{path}?force=true"),
        Some(&token),
        Some(ws),
        "",
    )
    .await
    .1;
    let forced = forced["id"].as_str().unwrap();
    assert_eq!(call(app,"POST",&format!("/api/daemon/runtimes/{runtime}/models/{forced}/result"),Some(machine),None,r#"{"status":"completed","supported":false,"models":[],"unavailable_models":[{"id":"advisory"}]}"#).await.0,StatusCode::OK);
    assert!(
        db.query_opt(
            "SELECT runtime_id FROM runtime_rpc_model_catalog WHERE runtime_id=$1",
            &[&runtime]
        )
        .await
        .unwrap()
        .is_none()
    );
    // Remove revalidation work so timeout/claim checks address exactly one request.
    db.execute("DELETE FROM runtime_rpc_model_list WHERE runtime_id=$1 AND status IN ('pending','running')", &[&runtime]).await.unwrap();
    let pending = call(app, "POST", &path, Some(&token), Some(ws), "").await.1;
    let pending_id = Uuid::parse_str(pending["id"].as_str().unwrap()).unwrap();
    db.execute(
        "UPDATE runtime_rpc_model_list SET created_at=now()-interval '31 seconds' WHERE id=$1",
        &[&pending_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            app,
            "GET",
            &format!("{path}/{pending_id}"),
            Some(&token),
            Some(ws),
            ""
        )
        .await
        .1["error"],
        "daemon did not respond within 30 seconds"
    );
    let running = call(app, "POST", &path, Some(&token), Some(ws), "").await.1;
    let running_id = Uuid::parse_str(running["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        claim_pending_model_list(&**db, runtime).await.unwrap(),
        Some(running_id)
    );
    db.execute(
        "UPDATE runtime_rpc_model_list SET started_at=now()-interval '61 seconds' WHERE id=$1",
        &[&running_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            app,
            "POST",
            &format!("/api/daemon/runtimes/{runtime}/models/{running_id}/result"),
            Some(machine),
            None,
            "bad JSON"
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app,
            "GET",
            &format!("{path}/{running_id}"),
            Some(&token),
            Some(ws),
            ""
        )
        .await
        .1["status"],
        "timeout"
    );
    // Cache SQL failures must not abort successful request persistence (savepoint boundary).
    db.batch_execute("CREATE FUNCTION model_cache_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'DB_PASSWORD=must-not-leak'; END $$; CREATE TRIGGER model_cache_test_fail BEFORE INSERT OR UPDATE ON runtime_rpc_model_catalog FOR EACH ROW EXECUTE FUNCTION model_cache_test_fail();").await.unwrap();
    let fresh = call(app, "POST", &path, Some(&jwt(owner)), Some(ws), "")
        .await
        .1;
    let fresh = fresh["id"].as_str().unwrap();
    let report = call(
        app,
        "POST",
        &format!("/api/daemon/runtimes/{runtime}/models/{fresh}/result"),
        Some(machine),
        None,
        body,
    )
    .await;
    assert_eq!(report.0, StatusCode::OK, "{:?}", report.1);
    assert_eq!(
        call(
            app,
            "GET",
            &format!("{path}/{fresh}"),
            Some(&token),
            Some(ws),
            ""
        )
        .await
        .1["status"],
        "completed"
    );
    db.batch_execute("DROP TRIGGER model_cache_test_fail ON runtime_rpc_model_catalog; DROP FUNCTION model_cache_test_fail();").await.unwrap();
    // Cache read errors fall back to live discovery, not a failed transaction.
    db.batch_execute("ALTER TABLE runtime_rpc_model_catalog RENAME TO model_cache_hidden")
        .await
        .unwrap();
    assert_eq!(
        call(app, "POST", &path, Some(&token), Some(ws), "").await.1["status"],
        "pending"
    );
    db.batch_execute("ALTER TABLE model_cache_hidden RENAME TO runtime_rpc_model_catalog")
        .await
        .unwrap();
    // Request persistence errors must roll back and surface a sanitized 500.
    db.batch_execute("CREATE FUNCTION model_request_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'DB_PASSWORD=must-not-leak'; END $$; CREATE TRIGGER model_request_test_fail BEFORE UPDATE ON runtime_rpc_model_list FOR EACH ROW WHEN (NEW.status='completed') EXECUTE FUNCTION model_request_test_fail();").await.unwrap();
    let pending = call(app, "POST", &path, Some(&token), Some(ws), "").await.1;
    let id = pending["id"].as_str().unwrap();
    let failed = call(
        app,
        "POST",
        &format!("/api/daemon/runtimes/{runtime}/models/{id}/result"),
        Some(machine),
        None,
        body,
    )
    .await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!failed.1.to_string().contains("must-not-leak"));
    assert_eq!(
        call(
            app,
            "GET",
            &format!("{path}/{id}"),
            Some(&token),
            Some(ws),
            ""
        )
        .await
        .1["status"],
        "pending"
    );
    db.batch_execute("DROP TRIGGER model_request_test_fail ON runtime_rpc_model_list; DROP FUNCTION model_request_test_fail();").await.unwrap();
    // No actual provider, daemon binary, network service, or installed CLI is invoked.
}
