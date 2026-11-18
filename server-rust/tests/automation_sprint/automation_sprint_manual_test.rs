//! HTTP preflight and persisted acknowledgement tests. Runtime adapter is synthetic.
use super::*;
use multica_server::api::{ManualDispatchFailure, ManualDispatcher, ManualJob};
use std::sync::atomic::{AtomicUsize, Ordering};

struct FakeManual {
    pool: Arc<Pool>,
    calls: AtomicUsize,
    mode: AtomicUsize,
    jobs: tokio::sync::Mutex<HashMap<String, (Uuid, Uuid)>>,
}

#[async_trait::async_trait]
impl ManualDispatcher for FakeManual {
    async fn dispatch(&self, job: ManualJob) -> Result<Uuid, ManualDispatchFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.mode.load(Ordering::SeqCst) {
            1 => return Ok(Uuid::new_v4()),
            3 => {
                return Err(ManualDispatchFailure::QuotaExceeded {
                    used: 5,
                    reserved: 2,
                    limit: 7,
                    reset_at: chrono::Utc::now() + chrono::Duration::hours(1),
                });
            }
            4 => return Err(ManualDispatchFailure::Forbidden),
            5 => return Err(ManualDispatchFailure::Inactive),
            6 => return Err(ManualDispatchFailure::Internal),
            _ => {}
        }
        let mut jobs = self.jobs.lock().await;
        if let Some((run, human)) = jobs.get(&job.idempotency_key) {
            assert_eq!(*human, job.invoker_user_id);
            return Ok(*run);
        }
        let db = self.pool.get().await.unwrap();
        assert_eq!(
            db.query_one(
                "SELECT workspace_id FROM autopilot WHERE id=$1",
                &[&job.autopilot_id]
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
            job.workspace_id
        );
        let status = if self.mode.load(Ordering::SeqCst) == 2 {
            "running"
        } else {
            "skipped"
        };
        let run:Uuid=db.query_one("INSERT INTO autopilot_run(autopilot_id,source,status,reason_code,failure_reason) VALUES ($1,'manual',$2,'synthetic_refusal','synthetic refusal') RETURNING id", &[&job.autopilot_id,&status]).await.unwrap().get(0);
        jobs.insert(job.idempotency_key, (run, job.invoker_user_id));
        Ok(run)
    }
}

fn app(pool: &Arc<Pool>, dispatcher: Option<Arc<FakeManual>>) -> axum::Router {
    let mut state = ApiState::new(pool.clone(), SECRET.to_vec());
    if let Some(dispatcher) = dispatcher {
        state = state.with_autopilot_manual_dispatcher(dispatcher);
    }
    router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "manual-synthetic".into(),
        )),
        state,
    )
}

pub(super) async fn verify(
    db: &tokio_postgres::Client,
    pool: &Arc<Pool>,
    ws: Uuid,
    agent: Uuid,
    runtime: Uuid,
    users: &[Uuid],
) {
    let ap:Uuid=db.query_one("INSERT INTO autopilot(workspace_id,title,assignee_id,status,execution_mode,created_by_type,created_by_id) VALUES ($1,'Manual synthetic',$2,'active','run_only','member',$3) RETURNING id", &[&ws,&agent,&users[1]]).await.unwrap().get(0);
    let path = format!("/api/autopilots/{ap}/trigger");
    let tokens: Vec<_> = users.iter().map(|u| token(*u)).collect();
    let fake = Arc::new(FakeManual {
        pool: pool.clone(),
        calls: AtomicUsize::new(0),
        mode: AtomicUsize::new(0),
        jobs: tokio::sync::Mutex::new(HashMap::new()),
    });
    let app = app(pool, Some(fake.clone()));
    let closed = super::automation_sprint_manual_test::app(pool, None);
    let result = replay_request(&closed, &path, ws, &tokens[1], "no-adapter").await;
    assert_eq!(result.0, StatusCode::SERVICE_UNAVAILABLE, "{result:?}");
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
    // Auth before status/key validation, including creator/admin/collaborator.
    db.execute("UPDATE autopilot SET status='paused' WHERE id=$1", &[&ap])
        .await
        .unwrap();
    let denied = replay_request(&app, &path, ws, &tokens[3], &"x".repeat(256)).await;
    assert_eq!(denied.0, StatusCode::FORBIDDEN, "{denied:?}");
    assert_eq!(denied.1["code"], "autopilot_trigger_forbidden");
    let inactive = replay_request(&app, &path, ws, &tokens[1], "inactive").await;
    assert_eq!(inactive.0, StatusCode::BAD_REQUEST, "{inactive:?}");
    db.execute("UPDATE autopilot SET status='active' WHERE id=$1", &[&ap])
        .await
        .unwrap();
    let invalid = replay_request(&app, &path, ws, &tokens[1], &"x".repeat(256)).await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{invalid:?}");
    let outsider = replay_request(&app, &path, ws, &tokens[4], "outsider").await;
    assert_eq!(outsider.0, StatusCode::NOT_FOUND, "{outsider:?}");
    let anonymous = request(&app, "POST", &path, ws, None, Value::Null).await;
    assert_eq!(anonymous.0, StatusCode::UNAUTHORIZED, "{anonymous:?}");
    assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
    // Concurrent retries pass one namespaced key, producing one stored run.
    let (a, b) = tokio::join!(
        replay_request(&app, &path, ws, &tokens[1], "retry"),
        replay_request(&app, &path, ws, &tokens[1], "retry")
    );
    assert_eq!(a.0, StatusCode::OK, "{a:?}");
    assert_eq!(a, b);
    assert_eq!(a.1["reason_code"], "synthetic_refusal");
    assert!(a.1.get("quota_reservation_id").is_none());
    assert!(a.1["trigger_id"].is_null());
    assert!(a.1["trigger_payload"].is_null());
    assert_eq!(
        fake.jobs.lock().await[&format!("manual:{ap}:retry")].1,
        users[1]
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM autopilot_run WHERE autopilot_id=$1",
            &[&ap]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    for i in [0, 2] {
        if i == 2 {
            db.execute("INSERT INTO autopilot_collaborator(autopilot_id,user_type,user_id,granted_by) VALUES ($1,'member',$2,$3)", &[&ap,&users[i],&users[1]]).await.unwrap();
        }
        let result = replay_request(&app, &path, ws, &tokens[i], &format!("member-{i}")).await;
        assert_eq!(result.0, StatusCode::OK, "{result:?}");
    }
    // Runtime owner is admin but supplies no grant to its powerless originator.
    for (index, human) in [Some(users[3]), None, Some(users[1])]
        .into_iter()
        .enumerate()
    {
        let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,originator_user_id,accountable_user_id) VALUES ($1,$2,'running',$3,$3) RETURNING id", &[&agent,&runtime,&human]).await.unwrap().get(0);
        let bearer = format!("mat_manual_synthetic_{}", Uuid::new_v4());
        let hash = hex::encode(Sha256::digest(bearer.as_bytes()));
        db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&users[0]]).await.unwrap();
        let key = format!("task-{index}");
        let result = replay_request(&app, &path, ws, &bearer, &key).await;
        if index == 2 {
            assert_eq!(result.0, StatusCode::OK, "{result:?}");
            assert_eq!(
                fake.jobs.lock().await[&format!("manual:{ap}:{key}")].1,
                users[1]
            );
        } else {
            assert_eq!(result.0, StatusCode::FORBIDDEN, "{result:?}");
            assert_eq!(
                result.1["code"],
                if human.is_none() {
                    "autopilot_trigger_no_originator"
                } else {
                    "autopilot_trigger_forbidden"
                }
            );
        }
    }
    // Misbehaving adapters cannot acknowledge nonexistent/incomplete runs.
    for (mode, status) in [
        (1, StatusCode::INTERNAL_SERVER_ERROR),
        (2, StatusCode::INTERNAL_SERVER_ERROR),
        (4, StatusCode::FORBIDDEN),
        (5, StatusCode::BAD_REQUEST),
        (6, StatusCode::INTERNAL_SERVER_ERROR),
    ] {
        fake.mode.store(mode, Ordering::SeqCst);
        let result = replay_request(&app, &path, ws, &tokens[1], &format!("mode-{mode}")).await;
        assert_eq!(result.0, status, "{result:?}");
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            assert_eq!(result.1, json!({"error":"failed to trigger autopilot"}));
        }
    }
    fake.mode.store(3, Ordering::SeqCst);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&path)
                .header("X-Workspace-ID", ws.to_string())
                .header("Authorization", format!("Bearer {}", tokens[1]))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(
        response.headers()["retry-after"]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            >= 1
    );
    let quota: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(quota["reason_code"], "quota_exceeded");
    assert_eq!(quota["used"], 5);
    assert_eq!(quota["reserved"], 2);
    assert_eq!(quota["limit"], 7);
    // Keep subsequent scheduler tests isolated from synthetic manual rules.
    db.execute("UPDATE autopilot SET status='archived' WHERE id=$1", &[&ap])
        .await
        .unwrap();
}
