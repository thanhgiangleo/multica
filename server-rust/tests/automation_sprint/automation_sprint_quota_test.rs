//! Real HTTP and quota-table reads; entitlement source is synthetic, never Cloud.
use super::*;
use multica_server::api::{
    AutopilotQuotaPolicy, AutopilotQuotaPolicyError, AutopilotQuotaPolicySource,
};
use std::sync::Mutex;

struct Policies {
    workspace: Uuid,
    policy: Mutex<Result<AutopilotQuotaPolicy, AutopilotQuotaPolicyError>>,
    calls: Mutex<Vec<Uuid>>,
    provider_pool: Option<Arc<Pool>>,
}

#[async_trait::async_trait]
impl AutopilotQuotaPolicySource for Policies {
    async fn policy(
        &self,
        workspace: Uuid,
    ) -> Result<AutopilotQuotaPolicy, AutopilotQuotaPolicyError> {
        self.calls.lock().unwrap().push(workspace);
        if workspace != self.workspace {
            return Err(AutopilotQuotaPolicyError);
        }
        if let Some(pool) = &self.provider_pool {
            pool.get()
                .await
                .unwrap()
                .query_one("SELECT 1", &[])
                .await
                .unwrap();
        }
        self.policy.lock().unwrap().clone()
    }
}

fn app(pool: &Arc<Pool>, source: Option<Arc<Policies>>) -> axum::Router {
    let mut state = ApiState::new(pool.clone(), SECRET.to_vec());
    if let Some(source) = source {
        state = state.with_autopilot_quota_policy_source(source);
    }
    router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "quota-synthetic".into(),
        )),
        state,
    )
}

pub(super) async fn verify(
    db: &tokio_postgres::Client,
    pool: &Arc<Pool>,
    ws: Uuid,
    other: Uuid,
    agent: Uuid,
    runtime: Uuid,
    users: &[Uuid],
) {
    let path = "/api/autopilots/usage";
    let bearer = token(users[3]);
    let start: chrono::DateTime<chrono::Utc> = "2026-10-31T01:02:03.123456Z".parse().unwrap();
    let end = start + chrono::Duration::hours(17);
    let policy = AutopilotQuotaPolicy {
        action: "enforce".into(),
        limit: Some(7),
        period_start: Some(start),
        period_end: Some(end),
        reset_at: Some(end),
    };
    let source = Arc::new(Policies {
        workspace: ws,
        policy: Mutex::new(Ok(policy.clone())),
        calls: Mutex::new(Vec::new()),
        provider_pool: None,
    });
    let app = app(pool, Some(source.clone()));
    let closed = super::automation_sprint_quota_test::app(pool, None);
    let result = request(&closed, "GET", path, ws, Some(&bearer), Value::Null).await;
    assert_eq!(result.0, StatusCode::SERVICE_UNAVAILABLE, "{result:?}");
    // A missing entitlement adapter must not expose operational state to an
    // outsider, or bypass the workspace guard.
    assert_eq!(
        request(
            &closed,
            "GET",
            path,
            ws,
            Some(&token(users[4])),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&closed, "GET", path, other, Some(&bearer), Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, "GET", path, ws, None, Value::Null).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(&app, "GET", path, ws, Some(&token(users[4])), Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, "GET", path, other, Some(&bearer), Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert!(source.calls.lock().unwrap().is_empty());
    let empty = request(&app, "GET", path, ws, Some(&bearer), Value::Null).await;
    assert_eq!(empty.0, StatusCode::OK, "{empty:?}");
    assert_eq!(
        empty.1,
        json!({"action":"enforce","used":0,"reserved":0,"total":0,"limit":7,"reached":false,
        "period_start":"2026-10-31T01:02:03Z","period_end":"2026-10-31T18:02:03Z","reset_at":"2026-10-31T18:02:03Z","blocked_counts":{}})
    );
    assert_eq!(db.query_one("SELECT count(*) FROM autopilot_quota_period WHERE workspace_id=$1 AND period_start=$2 AND period_end=$3", &[&ws,&start,&end]).await.unwrap().get::<_,i64>(0),0,"usage must not create periods");
    db.execute("INSERT INTO autopilot_quota_period(workspace_id,period_start,period_end,used_count,reserved_count,blocked_counts) VALUES ($1,$2,$3,5,2,$4)", &[&ws,&start,&end,&json!({"manual":3,"webhook":4})]).await.unwrap();
    db.execute("INSERT INTO autopilot_quota_period(workspace_id,period_start,period_end,used_count,reserved_count,blocked_counts) VALUES ($1,$2,$3,99,99,$4)", &[&other,&start,&end,&json!({"manual":999})]).await.unwrap();
    let adjacent = end + chrono::Duration::microseconds(1);
    db.execute("INSERT INTO autopilot_quota_period(workspace_id,period_start,period_end,used_count) VALUES ($1,$2,$3,999)", &[&ws,&start,&adjacent]).await.unwrap();
    let full = request(&app, "GET", path, ws, Some(&bearer), Value::Null).await;
    assert_eq!(full.0, StatusCode::OK, "{full:?}");
    assert_eq!(full.1["used"], 5);
    assert_eq!(full.1["reserved"], 2);
    assert_eq!(full.1["total"], 7);
    assert_eq!(full.1["reached"], true);
    assert_eq!(full.1["blocked_counts"], json!({"manual":3,"webhook":4}));
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    let tiny = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(1)
            .build()
            .unwrap(),
    );
    let same_pool = Arc::new(Policies {
        workspace: ws,
        policy: Mutex::new(Ok(policy.clone())),
        calls: Mutex::new(Vec::new()),
        provider_pool: Some(tiny.clone()),
    });
    let tiny_app = super::automation_sprint_quota_test::app(&tiny, Some(same_pool));
    let same_pool_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        request(&tiny_app, "GET", path, ws, Some(&bearer), Value::Null),
    )
    .await
    .expect("policy must not deadlock a single-connection pool");
    assert_eq!(same_pool_result.0, StatusCode::OK);
    assert_eq!(same_pool_result.1, full.1);
    let mut observe = policy.clone();
    observe.action = "observe".into();
    *source.policy.lock().unwrap() = Ok(observe);
    let observed = request(&app, "GET", path, ws, Some(&bearer), Value::Null).await;
    assert_eq!(observed.0, StatusCode::OK);
    assert!(observed.1["reached"].is_null());
    assert_eq!(observed.1["total"], 7);
    // Even absent tables must be irrelevant for off or malformed policies.
    db.batch_execute("ALTER TABLE autopilot_quota_period RENAME TO automation_sprint_hidden_quota")
        .await
        .unwrap();
    for case in 0..10 {
        let mut malformed = policy.clone();
        match case {
            0 => malformed.action = "off".into(),
            1 => malformed.action = "unknown".into(),
            2 => malformed.limit = None,
            3 => malformed.limit = Some(-1),
            4 => malformed.period_start = None,
            5 => malformed.period_end = None,
            6 => malformed.reset_at = None,
            7 => malformed.period_end = Some(start),
            8 => malformed.period_end = Some(start - chrono::Duration::seconds(1)),
            _ => malformed.action = " enforce ".into(),
        }
        *source.policy.lock().unwrap() = Ok(malformed);
        let disabled = request(&app, "GET", path, ws, Some(&bearer), Value::Null).await;
        assert_eq!(disabled.0, StatusCode::OK, "case {case}: {disabled:?}");
        assert_eq!(
            disabled.1,
            json!({"action":"off","used":null,"reserved":null,"total":null,"limit":null,"reached":null,
            "period_start":null,"period_end":null,"reset_at":null,"blocked_counts":null})
        );
    }
    *source.policy.lock().unwrap() = Ok(policy.clone());
    let broken = request(&app, "GET", path, ws, Some(&bearer), Value::Null).await;
    assert_eq!(broken.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        broken.1,
        json!({"error":"failed to load autopilot quota usage"})
    );
    db.batch_execute("ALTER TABLE automation_sprint_hidden_quota RENAME TO autopilot_quota_period")
        .await
        .unwrap();
    *source.policy.lock().unwrap() = Err(AutopilotQuotaPolicyError);
    let unavailable = request(&app, "GET", path, ws, Some(&bearer), Value::Null).await;
    assert_eq!(unavailable.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(unavailable.1, broken.1);
    *source.policy.lock().unwrap() = Ok(policy.clone());
    for (counts, status) in [
        (json!(null), StatusCode::OK),
        (json!({"manual":null}), StatusCode::OK),
        (json!({"manual":"3"}), StatusCode::INTERNAL_SERVER_ERROR),
        (json!({"manual":1.5}), StatusCode::INTERNAL_SERVER_ERROR),
        (json!([]), StatusCode::INTERNAL_SERVER_ERROR),
    ] {
        db.execute("UPDATE autopilot_quota_period SET blocked_counts=$4 WHERE workspace_id=$1 AND period_start=$2 AND period_end=$3", &[&ws,&start,&end,&counts]).await.unwrap();
        let result = request(&app, "GET", path, ws, Some(&bearer), Value::Null).await;
        assert_eq!(result.0, status, "counts {counts}: {result:?}");
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            assert_eq!(result.1, broken.1);
        }
    }
    db.execute("UPDATE autopilot_quota_period SET used_count=$4,reserved_count=1,blocked_counts='{}' WHERE workspace_id=$1 AND period_start=$2 AND period_end=$3", &[&ws,&start,&end,&i64::MAX]).await.unwrap();
    let overflow = request(&app, "GET", path, ws, Some(&bearer), Value::Null).await;
    assert_eq!(overflow.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(overflow.1, broken.1);
    db.execute("UPDATE autopilot_quota_period SET used_count=0,reserved_count=0 WHERE workspace_id=$1 AND period_start=$2 AND period_end=$3", &[&ws,&start,&end]).await.unwrap();
    let mut zero = policy.clone();
    zero.limit = Some(0);
    *source.policy.lock().unwrap() = Ok(zero);
    let zero = request(&app, "GET", path, ws, Some(&bearer), Value::Null).await;
    assert_eq!(zero.0, StatusCode::OK);
    assert_eq!(zero.1["reached"], true);
    // The schema permits legacy unattributed tasks, but never mismatched
    // accountable users. Assert the actual CHECK invariant rather than
    // treating NULL originators as rejected by PostgreSQL.
    let mismatched = db.execute(
        "INSERT INTO agent_task_queue(agent_id,runtime_id,status,originator_user_id,accountable_user_id) VALUES ($1,$2,'running',$3,$4)",
        &[&agent, &runtime, &users[4], &users[3]],
    ).await;
    assert_eq!(
        mismatched
            .unwrap_err()
            .as_db_error()
            .map(|e| e.constraint()),
        Some(Some("agent_task_queue_accountable_matches_originator")),
    );
    // Task runtime owner cannot lend membership to an outsider originator.
    for human in [None, Some(users[4]), Some(users[3])] {
        let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,originator_user_id,accountable_user_id) VALUES ($1,$2,'running',$3,$3) RETURNING id", &[&agent,&runtime,&human]).await.unwrap().get(0);
        let task_bearer = format!("mat_quota_synthetic_{}", Uuid::new_v4());
        let hash = hex::encode(Sha256::digest(task_bearer.as_bytes()));
        db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&users[0]]).await.unwrap();
        let calls = source.calls.lock().unwrap().len();
        let closed_result =
            request(&closed, "GET", path, ws, Some(&task_bearer), Value::Null).await;
        assert_eq!(
            closed_result.0,
            if human == Some(users[3]) {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::FORBIDDEN
            },
            "{closed_result:?}"
        );
        let result = request(&app, "GET", path, ws, Some(&task_bearer), Value::Null).await;
        assert_eq!(
            result.0,
            if human == Some(users[3]) {
                StatusCode::OK
            } else {
                StatusCode::FORBIDDEN
            },
            "{result:?}"
        );
        assert_eq!(
            source.calls.lock().unwrap().len(),
            calls + usize::from(human == Some(users[3]))
        );
    }
    assert!(source.calls.lock().unwrap().iter().all(|id| *id == ws));
    db.execute("DELETE FROM autopilot_quota_period WHERE (workspace_id=$1 OR workspace_id=$2) AND period_start=$3", &[&ws,&other,&start]).await.unwrap();
}
