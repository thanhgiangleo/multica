use super::*;
use multica_server::api::runtime_skills_sprint_local::{
    claim_pending_local_skill_list, has_pending_local_skill_list,
};

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
    outsider: Uuid,
    foreign: Uuid,
    machine: &str,
    wrong: &str,
    transport: &FakeTransport,
) {
    let db = pool.get().await.unwrap();
    for schema in [
        include_str!("../../runtime_skills_sprint_migrations/008_local_skill_list_table.sql"),
        include_str!("../../runtime_skills_sprint_migrations/009_local_skill_list_id_index.sql"),
        include_str!(
            "../../runtime_skills_sprint_migrations/010_local_skill_list_runtime_index.sql"
        ),
    ] {
        db.batch_execute(schema).await.unwrap();
    }
    let path = format!("/api/runtimes/{runtime}/local-skills");
    let token = jwt(member);
    for bad in [None, Some("bad")] {
        assert_eq!(
            call(app, "POST", &path, bad, Some(ws), "").await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    for (u, w) in [(outsider, ws), (owner, foreign)] {
        assert_eq!(
            call(app, "POST", &path, Some(&jwt(u)), Some(w), "").await.0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(app, "POST", &path, Some(machine), Some(ws), "")
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            app,
            "POST",
            &format!("/api/runtimes/{private_runtime}/local-skills"),
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
    let calls = transport.0.lock().unwrap().len();
    let created = call(
        app,
        "POST",
        &path,
        Some(&token),
        Some(ws),
        "ignored invalid JSON",
    )
    .await;
    assert_eq!(created.0, StatusCode::OK, "{:?}", created.1);
    assert_eq!(transport.0.lock().unwrap().len(), calls + 1);
    assert_eq!(created.1["status"], "pending");
    assert_eq!(created.1["supported"], true);
    assert_eq!(created.1["mcp_supported"], false);
    assert!(created.1.get("skills").is_none());
    assert!(created.1.get("mcp_servers").is_none());
    assert!(created.1.get("expires_at").is_none());
    let request = Uuid::parse_str(created.1["id"].as_str().unwrap()).unwrap();
    let poll = format!("{path}/{request}");
    let result = format!("/api/daemon/runtimes/{runtime}/local-skills/{request}/result");
    assert!(has_pending_local_skill_list(&**db, runtime).await.unwrap());
    assert_eq!(
        call(
            app,
            "GET",
            &format!("{path}/bad"),
            Some(&token),
            Some(ws),
            ""
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            app,
            "GET",
            &format!("/api/runtimes/{private_runtime}/local-skills/{request}"),
            Some(&jwt(owner)),
            Some(ws),
            ""
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for bad in [None, Some("bad"), Some(&token)] {
        // User JWT is allowed by existing daemon auth for owned runtimes only;
        // member JWT cannot report to a machine they do not own.
        assert_ne!(
            call(app, "POST", &result, bad, None, "{}").await.0,
            StatusCode::OK
        );
    }
    assert_eq!(
        call(app, "POST", &result, Some(wrong), None, "{}").await.0,
        StatusCode::NOT_FOUND
    );
    let (a, b) = tokio::join!(claim_pending_local_skill_list(&**db, runtime), async {
        let other = pool.get().await.unwrap();
        claim_pending_local_skill_list(&**other, runtime).await
    });
    assert_eq!(
        [a.unwrap(), b.unwrap()]
            .iter()
            .filter(|r| r.is_some())
            .count(),
        1
    );
    assert!(!has_pending_local_skill_list(&**db, runtime).await.unwrap());
    assert_eq!(
        call(app, "GET", &poll, Some(&jwt(admin)), Some(ws), "")
            .await
            .1["status"],
        "running"
    );
    // Report schema rejects wrong known types, but discards unknown secrets.
    for invalid in [
        "bad JSON",
        r#"{"skills":[{"file_count":"many"}]}"#,
        r#"{"supported":1}"#,
    ] {
        assert_eq!(
            call(app, "POST", &result, Some(machine), None, invalid)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let body = r#"{"status":"completed","skills":[{"key":"skill-key","name":"Skill","description":"safe","source_path":"/synthetic/skills/s","provider":"synthetic","root":"plugin","plugin":"pkg","can_disable":true,"file_count":2,"content":"secret-body","files":["secret-file"]}],"mcp_servers":[{"name":"inventory","transport":"stdio","source":"project","enabled":true,"command":"secret-command","args":["secret-arg"],"url":"secret-url","headers":{"authorization":"secret-header"},"env":{"TOKEN":"secret-env"}}],"mcp_supported":true}"#;
    assert_eq!(
        call(app, "POST", &result, Some(machine), None, body)
            .await
            .0,
        StatusCode::OK
    );
    let completed = call(app, "GET", &poll, Some(&token), Some(ws), "").await;
    assert_eq!(completed.1["status"], "completed");
    assert_eq!(completed.1["supported"], true);
    assert_eq!(completed.1["mcp_supported"], true);
    assert_eq!(completed.1["skills"][0]["key"], "skill-key");
    assert_eq!(completed.1["skills"][0]["can_disable"], true);
    assert_eq!(completed.1["skills"][0]["file_count"], 2);
    assert_eq!(completed.1["mcp_servers"][0]["enabled"], true);
    let persisted = db
        .query_one(
            "SELECT skills,mcp_servers FROM runtime_rpc_local_skill_list WHERE id=$1",
            &[&request],
        )
        .await
        .unwrap();
    let persisted = json!([persisted.get::<_, Value>(0), persisted.get::<_, Value>(1)]).to_string();
    for secret in [
        "secret-body",
        "secret-file",
        "secret-command",
        "secret-arg",
        "secret-url",
        "secret-header",
        "secret-env",
    ] {
        assert!(!completed.1.to_string().contains(secret));
        assert!(!persisted.contains(secret));
    }
    assert_eq!(
        call(
            app,
            "POST",
            &result,
            Some(machine),
            None,
            "malformed terminal retry"
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(app, "GET", &poll, Some(&token), Some(ws), "").await.1,
        completed.1
    );
    // Permission is re-evaluated for every poll, even for terminal results.
    db.execute(
        "UPDATE agent_runtime SET visibility='private',shared_with='{}' WHERE id=$1",
        &[&runtime],
    )
    .await
    .unwrap();
    assert_eq!(
        call(app, "GET", &poll, Some(&token), Some(ws), "").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(app, "GET", &poll, Some(&jwt(admin)), Some(ws), "")
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE agent_runtime SET shared_with=$2 WHERE id=$1",
        &[&runtime, &vec![member]],
    )
    .await
    .unwrap();
    assert_eq!(
        call(app, "GET", &poll, Some(&token), Some(ws), "").await.0,
        StatusCode::OK
    );
    db.execute(
        "UPDATE agent_runtime SET visibility='public',shared_with='{}' WHERE id=$1",
        &[&runtime],
    )
    .await
    .unwrap();
    // Two API nodes enqueue independent requests; neither disappears from heartbeat.
    let (a, b) = tokio::join!(
        call(app, "POST", &path, Some(&token), Some(ws), ""),
        call(app, "POST", &path, Some(&token), Some(ws), "")
    );
    assert_eq!(a.0, StatusCode::OK);
    assert_eq!(b.0, StatusCode::OK);
    assert_ne!(a.1["id"], b.1["id"]);
    let one = claim_pending_local_skill_list(&**db, runtime)
        .await
        .unwrap()
        .unwrap();
    let two = claim_pending_local_skill_list(&**db, runtime)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(one, two);
    assert!(
        claim_pending_local_skill_list(&**db, runtime)
            .await
            .unwrap()
            .is_none()
    );
    let one_result = format!("/api/daemon/runtimes/{runtime}/local-skills/{one}/result");
    // First terminal report wins across concurrent API nodes.
    let good = r#"{"status":"completed","supported":false,"mcp_supported":false,"skills":null,"mcp_servers":null}"#;
    let bad = r#"{"status":"failed","error":"TOKEN=synthetic-credential"}"#;
    let (a, b) = tokio::join!(
        call(app, "POST", &one_result, Some(machine), None, good),
        call(app, "POST", &one_result, Some(machine), None, bad)
    );
    assert_eq!(a.0, StatusCode::OK);
    assert_eq!(b.0, StatusCode::OK);
    let settled = call(
        app,
        "GET",
        &format!("{path}/{one}"),
        Some(&token),
        Some(ws),
        "",
    )
    .await
    .1;
    assert!(!settled.to_string().contains("synthetic-credential"));
    assert!(["completed", "failed"].contains(&settled["status"].as_str().unwrap()));
    assert_eq!(
        call(app, "POST", &one_result, Some(machine), None, "not JSON")
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app,
            "GET",
            &format!("{path}/{one}"),
            Some(&token),
            Some(ws),
            ""
        )
        .await
        .1,
        settled
    );
    // Database errors must not be swallowed; transaction leaves request retryable.
    db.batch_execute("CREATE FUNCTION runtime_skills_sprint_local_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'TOKEN=database-secret'; END $$; CREATE TRIGGER runtime_skills_sprint_local_reject BEFORE UPDATE ON runtime_rpc_local_skill_list FOR EACH ROW EXECUTE FUNCTION runtime_skills_sprint_local_reject();").await.unwrap();
    let two_result = format!("/api/daemon/runtimes/{runtime}/local-skills/{two}/result");
    let failed = call(app, "POST", &two_result, Some(machine), None, good).await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(failed.1["error"], "failed to persist completion");
    assert!(!failed.1.to_string().contains("database-secret"));
    db.batch_execute("DROP TRIGGER runtime_skills_sprint_local_reject ON runtime_rpc_local_skill_list; DROP FUNCTION runtime_skills_sprint_local_reject();").await.unwrap();
    assert_eq!(
        call(
            app,
            "GET",
            &format!("{path}/{two}"),
            Some(&token),
            Some(ws),
            ""
        )
        .await
        .1["status"],
        "running"
    );
    assert_eq!(
        call(app, "POST", &two_result, Some(machine), None, good)
            .await
            .0,
        StatusCode::OK
    );
    let empty = call(
        app,
        "GET",
        &format!("{path}/{two}"),
        Some(&token),
        Some(ws),
        "",
    )
    .await
    .1;
    assert_eq!(empty["supported"], false);
    assert_eq!(empty["mcp_supported"], false);
    assert!(empty.get("skills").is_none());
    assert!(empty.get("mcp_servers").is_none());
    // Pending and running timeout, plus five-minute retention, without wall-clock sleeps.
    for (phase, age, message) in [
        (
            "pending",
            "created_at",
            "daemon did not respond within 3 minutes",
        ),
        (
            "running",
            "started_at",
            "daemon did not finish within 60 seconds",
        ),
    ] {
        let fresh = call(app, "POST", &path, Some(&token), Some(ws), "").await.1;
        let id = Uuid::parse_str(fresh["id"].as_str().unwrap()).unwrap();
        let sql = format!(
            "UPDATE runtime_rpc_local_skill_list SET status=$2,{age}=now()-interval '181 seconds' WHERE id=$1"
        );
        db.execute(&sql, &[&id, &phase]).await.unwrap();
        assert!(
            claim_pending_local_skill_list(&**db, runtime)
                .await
                .unwrap()
                .is_none()
        );
        let timed = call(
            app,
            "GET",
            &format!("{path}/{id}"),
            Some(&token),
            Some(ws),
            "",
        )
        .await
        .1;
        assert_eq!(timed["status"], "timeout");
        assert_eq!(timed["error"], message);
        assert_eq!(
            call(
                app,
                "POST",
                &format!("/api/daemon/runtimes/{runtime}/local-skills/{id}/result"),
                Some(machine),
                None,
                "invalid"
            )
            .await
            .0,
            StatusCode::OK
        );
        db.execute("UPDATE runtime_rpc_local_skill_list SET expires_at=now()-interval '1 second' WHERE id=$1",&[&id]).await.unwrap();
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
            .0,
            StatusCode::NOT_FOUND
        );
    }
    let calls = transport.0.lock().unwrap().len();
    db.batch_execute("CREATE FUNCTION runtime_skills_sprint_local_insert_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'TOKEN=insert-secret'; END $$; CREATE TRIGGER runtime_skills_sprint_local_insert_reject BEFORE INSERT ON runtime_rpc_local_skill_list FOR EACH ROW EXECUTE FUNCTION runtime_skills_sprint_local_insert_reject();").await.unwrap();
    assert_eq!(
        call(app, "POST", &path, Some(&token), Some(ws), "").await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        transport.0.lock().unwrap().len(),
        calls,
        "never wake before commit"
    );
    db.batch_execute("DROP TRIGGER runtime_skills_sprint_local_insert_reject ON runtime_rpc_local_skill_list; DROP FUNCTION runtime_skills_sprint_local_insert_reject();").await.unwrap();
}
