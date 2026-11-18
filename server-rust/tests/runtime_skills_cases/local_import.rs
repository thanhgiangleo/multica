use super::*;
use multica_server::api::{
    runtime_skills_sprint::{SkillEvent, SkillEventSink},
    runtime_skills_sprint_local_import::{
        claim_pending_local_skill_imports, has_pending_local_skill_imports,
    },
};
struct Events(Mutex<Vec<SkillEvent>>);
impl SkillEventSink for Events {
    fn publish(&self, event: SkillEvent) {
        self.0.lock().unwrap().push(event);
    }
}
async fn enqueue(app: &axum::Router, path: &str, owner: Uuid, ws: Uuid, body: Value) -> Uuid {
    let response = call(
        app,
        "POST",
        path,
        Some(&jwt(owner)),
        Some(ws),
        &body.to_string(),
    )
    .await;
    assert_eq!(response.0, StatusCode::OK, "{:?}", response.1);
    assert_eq!(response.1["status"], "pending");
    Uuid::parse_str(response.1["id"].as_str().unwrap()).unwrap()
}
#[allow(clippy::too_many_arguments)]
pub async fn check(
    pool: &Arc<Pool>,
    transport: &Arc<FakeTransport>,
    runtime: Uuid,
    ws: Uuid,
    owner: Uuid,
    member: Uuid,
    machine: &str,
    wrong: &str,
) {
    let db = pool.get().await.unwrap();
    for sql in [
        include_str!("../../runtime_skills_sprint_migrations/011_local_skill_import_table.sql"),
        include_str!("../../runtime_skills_sprint_migrations/012_local_skill_import_id_index.sql"),
        include_str!(
            "../../runtime_skills_sprint_migrations/013_local_skill_import_runtime_index.sql"
        ),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    let events = Arc::new(Events(Mutex::new(Vec::new())));
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let path = format!("/api/runtimes/{runtime}/local-skills/import");
    let no_events = router_with_api(
        health.clone(),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_runtime_rpc_transport(transport.clone()),
    );
    assert_eq!(
        call(&no_events, "POST", &path, Some(&jwt(owner)), Some(ws), "{}")
            .await
            .0,
        StatusCode::METHOD_NOT_ALLOWED
    );
    let no_transport = router_with_api(
        health.clone(),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_skill_event_sink(events.clone()),
    );
    assert_eq!(
        call(
            &no_transport,
            "POST",
            &path,
            Some(&jwt(owner)),
            Some(ws),
            "{}"
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let app = router_with_api(
        health,
        ApiState::new(pool.clone(), SECRET.to_vec())
            .with_runtime_rpc_transport(transport.clone())
            .with_skill_event_sink(events.clone()),
    );
    assert_eq!(
        call(&app, "POST", &path, None, Some(ws), "{}").await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "POST", &path, Some(&jwt(member)), Some(ws), "{}")
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, "POST", &path, Some(machine), Some(ws), "{}")
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    for body in [
        "{}",
        "{",
        "{\"skill_key\":\"x\",\"action\":\"delete\"}",
        "{\"skill_key\":\"x\",\"action\":\"overwrite\",\"target_skill_id\":\"bad\"}",
    ] {
        assert_eq!(
            call(&app, "POST", &path, Some(&jwt(owner)), Some(ws), body)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let name = format!("import-{runtime}");
    let calls = transport.0.lock().unwrap().len();
    let id = enqueue(
        &app,
        &path,
        owner,
        ws,
        json!({"skill_key":"  local/key  ","name":name,"description":"override"}),
    )
    .await;
    assert_eq!(transport.0.lock().unwrap().len(), calls + 1);
    assert!(
        has_pending_local_skill_imports(&**db, runtime)
            .await
            .unwrap()
    );
    let db2 = pool.get().await.unwrap();
    let (a, b) = tokio::join!(
        claim_pending_local_skill_imports(&**db, runtime, 10),
        claim_pending_local_skill_imports(&**db2, runtime, 10)
    );
    let claimed: Vec<Value> = a.unwrap().into_iter().chain(b.unwrap()).collect();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0]["id"], id.to_string());
    assert_eq!(claimed[0]["skill_key"], "local/key");
    assert!(
        !has_pending_local_skill_imports(&**db, runtime)
            .await
            .unwrap()
    );
    let report = format!("/api/daemon/runtimes/{runtime}/local-skills/import/{id}/result");
    assert_eq!(
        call(&app, "POST", &report, Some(wrong), None, "{}").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &report, Some(&jwt(member)), None, "{}")
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &report, Some(machine), None, "{")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let bundle = json!({"status":"completed","skill":{"name":"daemon-name","description":"daemon description","content":"# Safe skill","provider":"synthetic","source_path":"/synthetic/skill","files":[{"path":"ref.txt","content":"first"},{"path":"../bad","content":"skip"},{"path":"SKILL.md","content":"skip"},{"path":"ref.txt","content":"last"}],"unknown":"TOKEN=not-stored"}});
    assert_eq!(
        call(
            &app,
            "POST",
            &report,
            Some(machine),
            None,
            &bundle.to_string()
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(events.0.lock().unwrap().len(), 1);
    let view = call(
        &app,
        "GET",
        &format!("{path}/{id}"),
        Some(&jwt(owner)),
        Some(ws),
        "",
    )
    .await;
    assert_eq!(view.0, StatusCode::OK, "{:?}", view.1);
    assert_eq!(view.1["status"], "completed");
    assert_eq!(view.1["skill"]["name"], name);
    assert_eq!(view.1["skill"]["description"], "override");
    assert_eq!(view.1["skill"]["config"]["origin"]["type"], "runtime_local");
    let skill = Uuid::parse_str(view.1["skill"]["id"].as_str().unwrap()).unwrap();
    let original = db
        .query_one(
            "SELECT created_at,created_by FROM skill WHERE id=$1",
            &[&skill],
        )
        .await
        .unwrap();
    let created: chrono::DateTime<chrono::Utc> = original.get("created_at");
    let files = db
        .query(
            "SELECT path,content FROM skill_file WHERE skill_id=$1",
            &[&skill],
        )
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].get::<_, String>("content"), "last");
    assert!(!view.1.to_string().contains("not-stored"));
    assert_eq!(
        call(
            &app,
            "POST",
            &report,
            Some(machine),
            None,
            "not JSON terminal retry"
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(events.0.lock().unwrap().len(), 1);
    // Existing-name handling differs only when the caller opts into structured conflict.
    for supports in [false, true] {
        let duplicate = enqueue(
            &app,
            &path,
            owner,
            ws,
            json!({"skill_key":"key","name":name,"supports_conflict":supports}),
        )
        .await;
        let endpoint =
            format!("/api/daemon/runtimes/{runtime}/local-skills/import/{duplicate}/result");
        assert_eq!(
            call(
                &app,
                "POST",
                &endpoint,
                Some(machine),
                None,
                &bundle.to_string()
            )
            .await
            .0,
            StatusCode::OK
        );
        let poll = call(
            &app,
            "GET",
            &format!("{path}/{duplicate}"),
            Some(&jwt(owner)),
            Some(ws),
            "",
        )
        .await
        .1;
        assert_eq!(poll["status"], if supports { "conflict" } else { "failed" });
        if supports {
            assert_eq!(poll["conflict"]["existing_skill_id"], skill.to_string());
            assert_eq!(poll["conflict"]["can_overwrite"], true);
        }
    }
    assert_eq!(events.0.lock().unwrap().len(), 1);
    let overwrite = enqueue(
        &app,
        &path,
        owner,
        ws,
        json!({"skill_key":"key","name":name,"action":"overwrite","target_skill_id":skill}),
    )
    .await;
    let overwrite_endpoint =
        format!("/api/daemon/runtimes/{runtime}/local-skills/import/{overwrite}/result");
    let replacement = json!({"status":"completed","skill":{"name":name,"content":"replacement","files":[{"path":"new.txt","content":"new"}]}});
    // Request and whole skill bundle must roll back together if persistence fails.
    db.batch_execute("CREATE FUNCTION runtime_skills_sprint_import_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'TOKEN=database-secret'; END $$; CREATE TRIGGER runtime_skills_sprint_import_fail BEFORE INSERT ON skill_file FOR EACH ROW EXECUTE FUNCTION runtime_skills_sprint_import_fail();").await.unwrap();
    let failed = call(
        &app,
        "POST",
        &overwrite_endpoint,
        Some(machine),
        None,
        &replacement.to_string(),
    )
    .await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!failed.1.to_string().contains("database-secret"));
    assert_eq!(
        db.query_one("SELECT content FROM skill WHERE id=$1", &[&skill])
            .await
            .unwrap()
            .get::<_, String>(0),
        "# Safe skill"
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM runtime_rpc_local_skill_import WHERE id=$1",
            &[&overwrite]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    assert_eq!(events.0.lock().unwrap().len(), 1);
    db.batch_execute("DROP TRIGGER runtime_skills_sprint_import_fail ON skill_file; DROP FUNCTION runtime_skills_sprint_import_fail();").await.unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &overwrite_endpoint,
            Some(machine),
            None,
            &replacement.to_string()
        )
        .await
        .0,
        StatusCode::OK
    );
    let updated = db
        .query_one("SELECT * FROM skill WHERE id=$1", &[&skill])
        .await
        .unwrap();
    assert_eq!(
        updated.get::<_, chrono::DateTime<chrono::Utc>>("created_at"),
        created
    );
    assert_eq!(updated.get::<_, Option<Uuid>>("created_by"), Some(owner));
    assert_eq!(updated.get::<_, String>("name"), name);
    assert_eq!(updated.get::<_, String>("content"), "replacement");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM skill_file WHERE skill_id=$1 AND path='ref.txt'",
            &[&skill]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(events.0.lock().unwrap().len(), 2);
    assert_eq!(events.0.lock().unwrap()[1].event_type, "skill:updated");
    // No creator ownership, no fallback create or content update.
    db.execute(
        "UPDATE skill SET created_by=$2 WHERE id=$1",
        &[&skill, &member],
    )
    .await
    .unwrap();
    let lost = enqueue(
        &app,
        &path,
        owner,
        ws,
        json!({"skill_key":"key","name":name,"action":"overwrite","target_skill_id":skill}),
    )
    .await;
    let lost_endpoint = format!("/api/daemon/runtimes/{runtime}/local-skills/import/{lost}/result");
    assert_eq!(
        call(
            &app,
            "POST",
            &lost_endpoint,
            Some(machine),
            None,
            &replacement.to_string()
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{path}/{lost}"),
            Some(&jwt(owner)),
            Some(ws),
            ""
        )
        .await
        .1["status"],
        "failed"
    );
    assert_eq!(events.0.lock().unwrap().len(), 2);
    // Credentials are rejected rather than silently rewriting executable content.
    let secret = enqueue(
        &app,
        &path,
        owner,
        ws,
        json!({"skill_key":"key","name":format!("secret-{runtime}")}),
    )
    .await;
    let secret_endpoint =
        format!("/api/daemon/runtimes/{runtime}/local-skills/import/{secret}/result");
    assert_eq!(
        call(
            &app,
            "POST",
            &secret_endpoint,
            Some(machine),
            None,
            r#"{"status":"completed","skill":{"name":"s","content":"TOKEN=synthetic-secret"}}"#
        )
        .await
        .0,
        StatusCode::OK
    );
    let secret_row = db
        .query_one(
            "SELECT status,skill,error FROM runtime_rpc_local_skill_import WHERE id=$1",
            &[&secret],
        )
        .await
        .unwrap();
    assert_eq!(secret_row.get::<_, String>("status"), "failed");
    assert!(secret_row.get::<_, Option<Value>>("skill").is_none());
    assert!(
        !secret_row
            .get::<_, String>("error")
            .contains("synthetic-secret")
    );
    // Both timeout windows and retained-result expiry use database time, no sleeps.
    for running in [false, true] {
        let timeout = enqueue(&app, &path, owner, ws, json!({"skill_key":"key"})).await;
        if running {
            assert_eq!(
                claim_pending_local_skill_imports(&**db, runtime, 1)
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
        db.execute("UPDATE runtime_rpc_local_skill_import SET created_at=now()-interval '4 minutes',started_at=now()-interval '61 seconds' WHERE id=$1",&[&timeout]).await.unwrap();
        let timeout_path = format!("{path}/{timeout}");
        assert_eq!(
            call(&app, "GET", &timeout_path, Some(&jwt(owner)), Some(ws), "")
                .await
                .1["status"],
            "timeout"
        );
        db.execute("UPDATE runtime_rpc_local_skill_import SET expires_at=now()-interval '1 second' WHERE id=$1",&[&timeout]).await.unwrap();
        assert_eq!(
            call(&app, "GET", &timeout_path, Some(&jwt(owner)), Some(ws), "")
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    // Former runtime owners cannot poll new-owner private source contents.
    let transfer = enqueue(&app, &path, owner, ws, json!({"skill_key":"key"})).await;
    db.execute(
        "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1",
        &[&runtime, &member],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{path}/{transfer}"),
            Some(&jwt(owner)),
            Some(ws),
            ""
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{path}/{transfer}"),
            Some(&jwt(member)),
            Some(ws),
            ""
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let transfer_endpoint =
        format!("/api/daemon/runtimes/{runtime}/local-skills/import/{transfer}/result");
    assert_eq!(
        call(
            &app,
            "POST",
            &transfer_endpoint,
            Some(&jwt(member)),
            None,
            &replacement.to_string()
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM runtime_rpc_local_skill_import WHERE id=$1",
            &[&transfer]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    db.execute(
        "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1",
        &[&runtime, &owner],
    )
    .await
    .unwrap();
}
