use super::*;

struct QuickRuntime {
    version: Mutex<String>,
    provider: Mutex<String>,
    ready: AtomicBool,
    queued: Mutex<Vec<IssueQuickCreateQueued>>,
    prepare_calls: std::sync::atomic::AtomicUsize,
    reclassify: Mutex<Option<Uuid>>,
    pool: Arc<Pool>,
}
#[async_trait]
impl IssueQuickCreateRuntime for QuickRuntime {
    async fn prepare(
        &self,
        input: IssueQuickCreateAdmission,
    ) -> Result<IssueQuickCreatePrepared, IssueQuickCreateReadinessError> {
        self.prepare_calls.fetch_add(1, Ordering::SeqCst);
        let reclassify = self.reclassify.lock().unwrap().take();
        if let Some(agent) = reclassify {
            self.pool
                .get()
                .await
                .unwrap()
                .execute("UPDATE agent SET kind='system' WHERE id=$1", &[&agent])
                .await
                .unwrap();
        }
        if !self.ready.load(Ordering::SeqCst) {
            return Err(IssueQuickCreateReadinessError::Unavailable {
                reason: "runtime offline".into(),
                reason_code: "runtime_offline".into(),
            });
        }
        let runtime_id = input
            .runtime_id
            .ok_or(IssueQuickCreateReadinessError::LookupFailed)?;
        Ok(IssueQuickCreatePrepared {
            runtime_id,
            provider: self.provider.lock().unwrap().clone(),
            cli_version: self.version.lock().unwrap().clone(),
            runtime_mcp_overlay: Some(json!({"mcpServers":{}})),
            runtime_connected_apps: Some(json!([])),
        })
    }
    fn enqueued(&self, event: IssueQuickCreateQueued) {
        self.queued.lock().unwrap().push(event);
    }
}
struct QuickCreatePolicy {
    runtime: Arc<QuickRuntime>,
    limit: Mutex<Option<i64>>,
}
#[async_trait]
impl IssueCreateRuntime for QuickCreatePolicy {
    async fn count_policy(&self, _: Uuid) -> Result<IssueCountPolicy, ()> {
        Ok(IssueCountPolicy {
            limit: *self.limit.lock().unwrap(),
            revision: 7,
        })
    }
    fn created(&self, _: &IssueCreatedEvent) {
        panic!("unexpected issue creation")
    }
    fn quick_create_runtime(&self) -> Option<&dyn IssueQuickCreateRuntime> {
        Some(self.runtime.as_ref())
    }
}

pub async fn exercise(
    pool: &Arc<Pool>,
    ws: Uuid,
    foreign: Uuid,
    user: Uuid,
    outsider: Uuid,
    targets: (Uuid, Uuid, Uuid),
    task_token: &str,
) {
    let (agent, parent, project) = targets;
    let db = pool.get().await.unwrap();
    let runtime = Arc::new(QuickRuntime {
        version: Mutex::new("0.4.3".into()),
        provider: Mutex::new("synthetic".into()),
        ready: AtomicBool::new(true),
        queued: Mutex::new(Vec::new()),
        prepare_calls: std::sync::atomic::AtomicUsize::new(0),
        reclassify: Mutex::new(None),
        pool: pool.clone(),
    });
    let policy = Arc::new(QuickCreatePolicy {
        runtime: runtime.clone(),
        limit: Mutex::new(None),
    });
    let build = |with_runtime: bool| {
        let state = ApiState::new(pool.clone(), b"quick-test-secret".to_vec())
            .with_issue_event_sink(Arc::new(Sink::default()));
        let state = if with_runtime {
            state.with_issue_create_runtime(policy.clone())
        } else {
            state
        };
        router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            state,
        )
    };
    let app = build(true);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let token = encode(
        &Header::default(),
        &json!({"sub":user,"exp":now+3600}),
        &EncodingKey::from_secret(b"quick-test-secret"),
    )
    .unwrap();
    let path = format!("/api/issues/quick-create?workspace_id={ws}");
    // Legacy clients sent the picked day as a local-midnight instant; Go
    // normalizes an exact UTC-midnight timestamp to the calendar date.
    let input = json!({"agent_id":agent,"prompt":"  create a synthetic subissue  ","priority":"HIGH","due_date":"2026-11-30T07:00:00+07:00","project_id":project,"parent_issue_id":parent});
    let call = |app: axum::Router, path: String, token: String, body: Value| async move {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    };
    let before:i64=db.query_one("SELECT count(*) FROM agent_task_queue WHERE agent_id=$1 AND context->>'type'='quick_create'", &[&agent]).await.unwrap().get(0);
    let result = call(app.clone(), path.clone(), token.clone(), input.clone()).await;
    assert_eq!(result.0, StatusCode::ACCEPTED, "{result:?}");
    let task_id = Uuid::parse_str(result.1["task_id"].as_str().unwrap()).unwrap();
    assert_eq!(task_id.get_version_num(), 7);
    let row=db.query_one("SELECT issue_id,status,priority,context,originator_user_id,accountable_user_id,originator_source,trigger_evidence_kind,runtime_mcp_overlay,runtime_connected_apps FROM agent_task_queue WHERE id=$1", &[&task_id]).await.unwrap();
    assert_eq!(row.get::<_, Option<Uuid>>("issue_id"), None);
    assert_eq!(row.get::<_, String>("status"), "queued");
    assert_eq!(row.get::<_, i32>("priority"), 3);
    assert_eq!(row.get::<_, Option<Uuid>>("originator_user_id"), Some(user));
    assert_eq!(
        row.get::<_, Option<Uuid>>("accountable_user_id"),
        Some(user)
    );
    assert_eq!(
        row.get::<_, Option<String>>("originator_source").as_deref(),
        Some("direct_human")
    );
    assert_eq!(row.get::<_, Option<String>>("trigger_evidence_kind"), None);
    let context: Value = row.get("context");
    assert_eq!(
        context,
        json!({"type":"quick_create","prompt":"create a synthetic subissue","requester_id":user,"workspace_id":ws,"priority":"high","due_date":"2026-11-30","project_id":project,"parent_issue_id":parent})
    );
    assert_eq!(
        row.get::<_, Option<Value>>("runtime_mcp_overlay"),
        Some(json!({"mcpServers":{}}))
    );
    assert_eq!(
        row.get::<_, Option<Value>>("runtime_connected_apps"),
        Some(json!([]))
    );
    assert_eq!(runtime.queued.lock().unwrap().len(), 1);
    assert_eq!(runtime.queued.lock().unwrap()[0].task_id, task_id);
    let count = || async {
        db.query_one("SELECT count(*) FROM agent_task_queue WHERE agent_id=$1 AND context->>'type'='quick_create'", &[&agent]).await.unwrap().get::<_,i64>(0)
    };
    assert_eq!(count().await, before + 1);
    for (body, status) in [
        (
            json!({"agent_id":agent,"prompt":" "}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"agent_id":agent,"squad_id":Uuid::new_v4(),"prompt":"x"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"agent_id":agent,"prompt":"x","due_date":"2026-02-30"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"agent_id":agent,"prompt":"x","due_date":"2026-11-30T00:00:01Z"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"agent_id":agent,"prompt":"x","due_date":"2026-11-30T00:00:00"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"agent_id":agent,"prompt":"x","attachment_ids":["invalid"]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"agent_id":agent,"prompt":"x","project_id":Uuid::new_v4()}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"agent_id":agent,"prompt":"x","parent_issue_id":Uuid::new_v4()}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let response = call(app.clone(), path.clone(), token.clone(), body).await;
        assert_eq!(response.0, status, "{response:?}");
    }
    let no_runtime = call(build(false), path.clone(), token.clone(), input.clone()).await;
    assert_eq!(no_runtime.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        call(
            app.clone(),
            format!("/api/issues/quick-create?workspace_id={foreign}"),
            token.clone(),
            input.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            app.clone(),
            path.clone(),
            task_token.to_string(),
            input.clone()
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(app.clone(), path.clone(), "invalid".into(), input.clone())
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    *runtime.version.lock().unwrap() = "0.4.2".into();
    let version = call(app.clone(), path.clone(), token.clone(), input.clone()).await;
    assert_eq!(version.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(version.1["code"], "daemon_version_unsupported");
    assert_eq!(version.1["min_version"], "0.4.3");
    // strconv.Atoi in Go maps overflowing semver components to zero. An
    // oversized minor must not let an incompatible daemon pass admission.
    *runtime.version.lock().unwrap() = "0.9223372036854775808.3".into();
    let overflow = call(app.clone(), path.clone(), token.clone(), input.clone()).await;
    assert_eq!(overflow.0, StatusCode::UNPROCESSABLE_ENTITY, "{overflow:?}");
    assert_eq!(overflow.1["code"], "daemon_version_unsupported");
    assert_eq!(overflow.1["min_version"], "0.2.21");
    assert_eq!(count().await, before + 1);
    assert_eq!(runtime.queued.lock().unwrap().len(), 1);
    *runtime.version.lock().unwrap() = "0.4.3".into();
    runtime.ready.store(false, Ordering::SeqCst);
    let offline = call(app.clone(), path.clone(), token.clone(), input.clone()).await;
    assert_eq!(offline.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(offline.1["reason_code"], "runtime_offline");
    runtime.ready.store(true, Ordering::SeqCst);
    *runtime.provider.lock().unwrap() = "qoder".into();
    assert_eq!(
        call(app.clone(), path.clone(), token.clone(), input.clone())
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    *runtime.provider.lock().unwrap() = "synthetic".into();
    *policy.limit.lock().unwrap() = Some(1);
    let quota = call(app.clone(), path.clone(), token.clone(), input.clone()).await;
    assert_eq!(quota.0, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(quota.1["code"], "issue_limit_reached");
    *policy.limit.lock().unwrap() = None;
    let squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES($1,'Quick squad',$2,$3) RETURNING id", &[&ws,&agent,&user]).await.unwrap().get(0);
    let squad_result=call(app.clone(),path.clone(),token.clone(),json!({"squad_id":squad,"prompt":"Create through leader","attachment_ids":[Uuid::new_v4()]})).await;
    assert_eq!(squad_result.0, StatusCode::ACCEPTED, "{squad_result:?}");
    let squad_task = Uuid::parse_str(squad_result.1["task_id"].as_str().unwrap()).unwrap();
    let squad_context: Value = db
        .query_one(
            "SELECT context FROM agent_task_queue WHERE id=$1",
            &[&squad_task],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(squad_context["squad_id"], squad.to_string());
    assert_eq!(squad_context["attachment_ids"].as_array().unwrap().len(), 1);
    let local_runtime = runtime.queued.lock().unwrap()[0].runtime_id;
    // A system-kind target is not a workspace-visible assignee, including
    // when a valid squad resolves to that leader. Reject before readiness.
    let hidden: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id,kind) VALUES($1,'Hidden quick agent','cloud',$2,$3,'system') RETURNING id", &[&ws,&user,&local_runtime]).await.unwrap().get(0);
    let hidden_squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES($1,'Hidden quick leader',$2,$3) RETURNING id", &[&ws,&hidden,&user]).await.unwrap().get(0);
    let prepares = runtime.prepare_calls.load(Ordering::SeqCst);
    for target in [
        json!({"agent_id":hidden,"prompt":"Hidden direct"}),
        json!({"squad_id":hidden_squad,"prompt":"Hidden squad"}),
    ] {
        let response = call(app.clone(), path.clone(), token.clone(), target).await;
        assert_eq!(
            response,
            (
                StatusCode::BAD_REQUEST,
                json!({"error":"assignee_id does not refer to an agent of this workspace"})
            )
        );
        assert_eq!(runtime.prepare_calls.load(Ordering::SeqCst), prepares);
        assert_eq!(runtime.queued.lock().unwrap().len(), 2);
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM agent_task_queue WHERE agent_id=$1",
                &[&hidden]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            0
        );
    }
    // Change classification after initial authorization, before the enqueue
    // transaction. Revalidation must roll back the inserted task and callback.
    for target in [
        json!({"agent_id":agent,"prompt":"Reclassified direct"}),
        json!({"squad_id":squad,"prompt":"Reclassified squad"}),
    ] {
        *runtime.reclassify.lock().unwrap() = Some(agent);
        let response = call(app.clone(), path.clone(), token.clone(), target).await;
        db.execute("UPDATE agent SET kind='user' WHERE id=$1", &[&agent])
            .await
            .unwrap();
        assert_eq!(
            response.0,
            StatusCode::INTERNAL_SERVER_ERROR,
            "{response:?}"
        );
        assert_eq!(count().await, before + 2);
        assert_eq!(runtime.queued.lock().unwrap().len(), 2);
    }
    assert_eq!(runtime.prepare_calls.load(Ordering::SeqCst), prepares + 2);
    let private_agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,'Other private','cloud',$2,$3) RETURNING id", &[&ws,&outsider,&local_runtime]).await.unwrap().get(0);
    assert_eq!(
        call(
            app.clone(),
            path.clone(),
            token.clone(),
            json!({"agent_id":private_agent,"prompt":"No permission"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let foreign_runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Foreign quick runtime','cloud','synthetic','online') RETURNING id", &[&foreign]).await.unwrap().get(0);
    let foreign_agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,'Foreign agent','cloud',$2,$3) RETURNING id", &[&foreign,&user,&foreign_runtime]).await.unwrap().get(0);
    assert_eq!(
        call(
            app.clone(),
            path.clone(),
            token.clone(),
            json!({"agent_id":foreign_agent,"prompt":"No tenant leak"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    db.batch_execute("CREATE FUNCTION zbsi66_queue_fail() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic queue rollback'; END $$; CREATE TRIGGER zbsi66_queue_fail BEFORE INSERT ON agent_task_queue FOR EACH ROW EXECUTE FUNCTION zbsi66_queue_fail();").await.unwrap();
    let rollback = call(app.clone(), path.clone(), token.clone(), input.clone()).await;
    assert_eq!(
        rollback.0,
        StatusCode::INTERNAL_SERVER_ERROR,
        "{rollback:?}"
    );
    db.batch_execute(
        "DROP TRIGGER zbsi66_queue_fail ON agent_task_queue; DROP FUNCTION zbsi66_queue_fail();",
    )
    .await
    .unwrap();
    assert_eq!(count().await, before + 2);
    assert_eq!(runtime.queued.lock().unwrap().len(), 2);

    // The Go handler derives the requester from X-User-ID for both session
    // and PAT callers. A PAT must enqueue under that user, never a cloud bot.
    let pat = format!("mul_quick_create_{}", Uuid::new_v4());
    db.execute(
        "INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES($1,'synthetic quick-create',$2,'mul_quick')",
        &[&user, &hex::encode(Sha256::digest(pat.as_bytes()))],
    )
    .await
    .unwrap();
    let from_pat = call(
        app.clone(),
        path.clone(),
        pat.clone(),
        json!({"agent_id":agent,"prompt":"PAT quick create"}),
    )
    .await;
    assert_eq!(from_pat.0, StatusCode::ACCEPTED, "{from_pat:?}");
    let pat_task = Uuid::parse_str(from_pat.1["task_id"].as_str().unwrap()).unwrap();
    assert_eq!(pat_task.get_version_num(), 7);
    let originator: Option<Uuid> = db
        .query_one(
            "SELECT originator_user_id FROM agent_task_queue WHERE id=$1",
            &[&pat_task],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(originator, Some(user));
    assert_eq!(count().await, before + 3);
    assert_eq!(runtime.queued.lock().unwrap().len(), 3);
    let pat_hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&pat_hash],
    )
    .await
    .unwrap();
    let revoked = call(
        app.clone(),
        path.clone(),
        pat,
        json!({"agent_id":agent,"prompt":"Revoked PAT"}),
    )
    .await;
    assert_eq!(revoked.0, StatusCode::UNAUTHORIZED, "{revoked:?}");
    assert_eq!(count().await, before + 3);
    assert_eq!(runtime.queued.lock().unwrap().len(), 3);

    // The task token's owner is not its top-of-chain human. Fail closed on
    // missing workspace membership and on the owner's private-agent rights.
    let parent_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,originator_user_id,accountable_user_id,originator_source) VALUES($1,$2,'running',$3,$3,'delegation') RETURNING id", &[&agent,&local_runtime,&outsider]).await.unwrap().get(0);
    let delegated_token = format!("mat_quick_delegated_{}", Uuid::new_v4());
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(delegated_token.as_bytes())),&parent_task,&agent,&ws,&user]).await.unwrap();
    let delegated_input = json!({"agent_id":agent,"prompt":"Delegated quick-create"});
    assert_eq!(
        call(
            app.clone(),
            path.clone(),
            delegated_token.clone(),
            delegated_input.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&ws, &outsider],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            app.clone(),
            path.clone(),
            delegated_token.clone(),
            delegated_input.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE agent SET permission_mode='public_to' WHERE id=$1",
        &[&agent],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES($1,'member',$2)", &[&agent,&outsider]).await.unwrap();
    let delegated = call(
        app.clone(),
        path.clone(),
        delegated_token.clone(),
        delegated_input.clone(),
    )
    .await;
    assert_eq!(delegated.0, StatusCode::ACCEPTED, "{delegated:?}");
    let delegated_id = Uuid::parse_str(delegated.1["task_id"].as_str().unwrap()).unwrap();
    let row=db.query_one("SELECT originator_user_id,accountable_user_id,originator_source,delegated_from_task_id,context FROM agent_task_queue WHERE id=$1", &[&delegated_id]).await.unwrap();
    assert_eq!(
        row.get::<_, Option<Uuid>>("originator_user_id"),
        Some(outsider)
    );
    assert_eq!(
        row.get::<_, Option<Uuid>>("accountable_user_id"),
        Some(outsider)
    );
    assert_eq!(
        row.get::<_, Option<String>>("originator_source").as_deref(),
        Some("delegation")
    );
    assert_eq!(
        row.get::<_, Option<Uuid>>("delegated_from_task_id"),
        Some(parent_task)
    );
    assert_eq!(
        row.get::<_, Value>("context")["requester_id"],
        outsider.to_string()
    );
    assert_eq!(
        runtime.queued.lock().unwrap().last().unwrap().requester_id,
        outsider
    );
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&parent_task],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            app.clone(),
            path.clone(),
            delegated_token.clone(),
            delegated_input.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(count().await, before + 4);
    assert_eq!(runtime.queued.lock().unwrap().len(), 4);
    db.execute("UPDATE agent_task_queue SET status='running',originator_user_id=NULL,accountable_user_id=NULL,originator_source=NULL WHERE id=$1", &[&parent_task]).await.unwrap();
    assert_eq!(
        call(app, path, delegated_token, delegated_input).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(runtime.queued.lock().unwrap().len(), 4);
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &outsider],
    )
    .await
    .unwrap();
}
