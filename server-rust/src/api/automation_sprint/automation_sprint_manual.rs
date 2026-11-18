//! Manual invocation spends the ordering human's authority, not the runtime owner's.
use super::*;
use async_trait::async_trait;
use axum::{http::HeaderMap, response::IntoResponse, response::Response, routing::post};
use std::sync::Arc;

/// Admission must revalidate membership, collaborator grants, active status and
/// assignee invocation permissions atomically with quota/run/task admission.
/// It must preserve direct_human attribution and recover by idempotency_key after
/// HTTP cancellation or a crash. No synthetic run or no-op adapter is acceptable.
#[async_trait]
pub trait ManualDispatcher: Send + Sync + 'static {
    async fn dispatch(&self, job: ManualJob) -> Result<Uuid, ManualDispatchFailure>;
}

#[derive(Clone, Debug)]
pub struct ManualJob {
    pub workspace_id: Uuid,
    pub autopilot_id: Uuid,
    pub invoker_user_id: Uuid,
    /// Full service key, including the manual/autopilot namespace.
    pub idempotency_key: String,
}

#[derive(Clone, Debug)]
pub enum ManualDispatchFailure {
    QuotaExceeded {
        used: i64,
        reserved: i64,
        limit: i64,
        reset_at: DateTime<Utc>,
    },
    /// Admission rejected a permission or rule-state change since preflight.
    Forbidden,
    Inactive,
    Internal,
}

impl ApiState {
    pub fn with_autopilot_manual_dispatcher(
        mut self,
        dispatcher: Arc<dyn ManualDispatcher>,
    ) -> Self {
        self.automation_sprint_manual = Some(dispatcher);
        self
    }
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/autopilots/{id}/trigger", post(trigger))
}

struct ManualError(StatusCode, Value);

impl From<ApiError> for ManualError {
    fn from(error: ApiError) -> Self {
        Self(error.0, json!({"error":error.1}))
    }
}

impl IntoResponse for ManualError {
    fn into_response(self) -> Response {
        (self.0, GoJson(self.1)).into_response()
    }
}

fn refusal(no_originator: bool) -> ManualError {
    let (code, message) = if no_originator {
        (
            "autopilot_trigger_no_originator",
            "no human authorized this trigger: the calling run records no originator",
        )
    } else {
        (
            "autopilot_trigger_forbidden",
            "only the autopilot creator, a workspace admin, or a granted collaborator can trigger this autopilot",
        )
    };
    ManualError(StatusCode::FORBIDDEN, json!({"error":message,"code":code}))
}

async fn invoker(
    db: &impl GenericClient,
    ap: &Row,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
) -> Result<Uuid, ManualError> {
    let human = match actor {
        Actor::Human | Actor::Cloud => user,
        Actor::Task(task) => db.query_opt(
            "SELECT q.originator_user_id FROM agent_task_queue q JOIN agent a ON a.id=q.agent_id WHERE q.id=$1 AND q.agent_id=$2 AND a.workspace_id=$3 AND q.status NOT IN ('completed','failed','cancelled')",
            &[&task.task,&task.agent,&ws],
        ).await.map_err(|_| fail("failed to trigger autopilot"))?
            .and_then(|r| r.get::<_,Option<Uuid>>(0)).ok_or_else(|| refusal(true))?,
        Actor::Daemon => return Err(refusal(true)),
    };
    let member = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &human],
        )
        .await
        .map_err(|_| fail("failed to trigger autopilot"))?
        .ok_or_else(|| refusal(false))?;
    if !can_write(db, ap, human, &member.get::<_, String>(0)).await? {
        return Err(refusal(false));
    }
    Ok(human)
}

fn request_key(headers: &HeaderMap, ap: Uuid) -> Result<String, ApiError> {
    let key = headers
        .get("idempotency-key")
        .map(|value| value.to_str().map(str::trim))
        .transpose()
        .map_err(|_| bad("invalid Idempotency-Key"))?
        .unwrap_or("");
    if key.len() > 255 {
        return Err(bad("Idempotency-Key is too long"));
    }
    let key = if key.is_empty() {
        Uuid::new_v4().to_string()
    } else {
        key.to_owned()
    };
    Ok(format!("manual:{ap}:{key}"))
}

fn run_response(row: &Row) -> Value {
    let mut value = json!({
        "id":row.get::<_,Uuid>("id"), "autopilot_id":row.get::<_,Uuid>("autopilot_id"),
        "trigger_id":row.get::<_,Option<Uuid>>("trigger_id"), "source":row.get::<_,String>("source"),
        "status":row.get::<_,String>("status"), "issue_id":row.get::<_,Option<Uuid>>("issue_id"),
        "task_id":row.get::<_,Option<Uuid>>("task_id"), "triggered_at":time(row,"triggered_at").unwrap_or_default(),
        "completed_at":time(row,"completed_at"), "failure_reason":row.get::<_,Option<String>>("failure_reason"),
        "trigger_payload":row.get::<_,Option<Value>>("trigger_payload"), "result":row.get::<_,Option<Value>>("result"),
        "created_at":time(row,"created_at").unwrap_or_default()
    });
    if let Some(code) = row.get::<_, Option<String>>("reason_code") {
        value["reason_code"] = json!(code);
    }
    value
}

async fn trigger(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ManualError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to trigger autopilot"))?;
    let ap = load(&db, ws, &raw).await?;
    // Authorization precedes both status and key validation to avoid disclosing
    // inactive rules or accepting a machine owner's unrelated standing grants.
    let human = invoker(&db, &ap, ws, user, &actor).await?;
    if ap.get::<_, String>("status") != "active" {
        return Err(bad("autopilot is not active").into());
    }
    let ap_id: Uuid = ap.get("id");
    let key = request_key(&headers, ap_id)?;
    let dispatcher = state.automation_sprint_manual.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "autopilot admission unavailable",
        )
    })?;
    drop(db);
    let run =
        match dispatcher
            .dispatch(ManualJob {
                workspace_id: ws,
                autopilot_id: ap_id,
                invoker_user_id: human,
                idempotency_key: key,
            })
            .await
        {
            Ok(run) => run,
            Err(ManualDispatchFailure::Forbidden) => return Err(refusal(false)),
            Err(ManualDispatchFailure::Inactive) => {
                return Err(bad("autopilot is not active").into());
            }
            Err(ManualDispatchFailure::Internal) => {
                return Err(fail("failed to trigger autopilot").into());
            }
            Err(ManualDispatchFailure::QuotaExceeded {
                used,
                reserved,
                limit,
                reset_at,
            }) => {
                let retry = (reset_at - Utc::now()).num_seconds().max(1).to_string();
                return Ok((StatusCode::TOO_MANY_REQUESTS, [("retry-after",retry)], GoJson(json!({
                "reason_code":"quota_exceeded", "used":used,"reserved":reserved,"limit":limit,
                "reset_at":reset_at.to_rfc3339_opts(chrono::SecondsFormat::Secs,true),
            }))).into_response());
            }
        };
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to trigger autopilot"))?;
    // Never report success for an invented, cross-workspace or incomplete run.
    let row = db.query_opt("SELECT r.* FROM autopilot_run r JOIN autopilot a ON a.id=r.autopilot_id WHERE r.id=$1 AND r.autopilot_id=$2 AND a.workspace_id=$3 AND r.source='manual' AND r.trigger_id IS NULL", &[&run,&ap_id,&ws])
        .await.map_err(|_| fail("failed to trigger autopilot"))?.ok_or_else(|| fail("failed to trigger autopilot"))?;
    let status: String = row.get("status");
    let complete = matches!(status.as_str(), "completed" | "failed" | "skipped")
        || (status == "issue_created" && row.get::<_, Option<Uuid>>("issue_id").is_some())
        || (status == "running" && row.get::<_, Option<Uuid>>("task_id").is_some());
    if !complete {
        return Err(fail("failed to trigger autopilot").into());
    }
    Ok(GoJson(run_response(&row)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keys_are_bounded_namespaced_and_request_local() {
        let ap = Uuid::new_v4();
        let mut headers = HeaderMap::new();
        let a = request_key(&headers, ap).unwrap();
        assert_ne!(a, request_key(&headers, ap).unwrap());
        headers.insert("idempotency-key", "  retry-1  ".parse().unwrap());
        assert_eq!(
            request_key(&headers, ap).unwrap(),
            format!("manual:{ap}:retry-1")
        );
        assert_ne!(
            request_key(&headers, ap).unwrap(),
            request_key(&headers, Uuid::new_v4()).unwrap()
        );
        headers.insert("idempotency-key", "x".repeat(255).parse().unwrap());
        assert!(request_key(&headers, ap).is_ok());
        headers.insert("idempotency-key", "x".repeat(256).parse().unwrap());
        assert_eq!(
            request_key(&headers, ap).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
    }
}
