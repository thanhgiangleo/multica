//! Claim-generation-aware daemon start. Terminal cleanup and Qoder issue
//! transitions remain explicit integration dependencies, never silent success.
use super::{
    ApiError, ApiState, GoJson,
    daemon::DaemonIdentity,
    error, json,
    runtime_chat_daemon::{agent_snapshot, reconcile_agent, system_event, task_access},
    runtime_chat_delivery::Fields,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/daemon/tasks/{taskId}/start", post(start))
}
fn failed(_: tokio_postgres::Error) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, "failed to start task")
}
#[derive(Default, Debug)]
struct Request {
    capabilities: Vec<String>,
    runtime: String,
    dispatched: String,
}
impl Request {
    fn decode(bytes: &[u8]) -> Result<Self, ApiError> {
        if bytes.is_empty() {
            return Ok(Self::default());
        }
        let Some(Ok(fields)) = serde_json::Deserializer::from_slice(bytes)
            .into_iter::<Fields>()
            .next()
        else {
            return Err(error(StatusCode::BAD_REQUEST, "invalid request body"));
        };
        let mut out = Self::default();
        for (key, value) in fields.0 {
            match key.as_str() {
                "runtime_id" | "dispatched_at" if !value.is_null() => {
                    let text = value
                        .as_str()
                        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
                    if key == "runtime_id" {
                        out.runtime = text.into();
                    } else {
                        out.dispatched = text.into();
                    }
                }
                "capabilities" => {
                    out.capabilities.clear();
                    if value.is_null() {
                        continue;
                    }
                    let values = value
                        .as_array()
                        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
                    for v in values {
                        if v.is_null() {
                            out.capabilities.push(String::new());
                        } else {
                            out.capabilities.push(
                                v.as_str()
                                    .ok_or_else(|| {
                                        error(StatusCode::BAD_REQUEST, "invalid request body")
                                    })?
                                    .into(),
                            );
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(out)
    }
    fn claim(&self) -> Result<Option<(Uuid, DateTime<Utc>)>, ApiError> {
        if self.runtime.is_empty() && self.dispatched.is_empty() {
            return Ok(None);
        }
        let runtime = Uuid::parse_str(&self.runtime)
            .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid runtime_id"))?;
        let generation = DateTime::parse_from_rfc3339(&self.dispatched)
            .map_err(|_| {
                error(
                    StatusCode::BAD_REQUEST,
                    "dispatched_at must be a PostgreSQL claim timestamp",
                )
            })?
            .with_timezone(&Utc);
        if generation.timestamp_subsec_nanos() % 1000 != 0 {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "dispatched_at must be a PostgreSQL claim timestamp",
            ));
        }
        Ok(Some((runtime, generation)))
    }
    fn offers(&self, name: &str) -> bool {
        self.capabilities.iter().any(|v| v == name)
    }
}
fn same_claim(task: &Row, claim: Option<(Uuid, DateTime<Utc>)>) -> bool {
    claim.is_none_or(|(rt, generation)| {
        task.get::<_, Option<Uuid>>("runtime_id") == Some(rt)
            && task.get::<_, Option<DateTime<Utc>>>("dispatched_at") == Some(generation)
    })
}

async fn start(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task_id"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load task"))?;
    // Frozen Go authorizes before decoding. Never use machine identity as user identity.
    let (original, workspace, owner) = task_access(&**db, &identity, id).await?;
    let request = Request::decode(&bytes)?;
    let claim = request.claim()?;
    if !same_claim(&original, claim) {
        return Err(error(StatusCode::CONFLICT, "task claim is stale"));
    }
    let runtime: Option<Uuid> = original.get("runtime_id");
    let tx = db.transaction().await.map_err(failed)?;
    tx.query_opt(
        "SELECT id FROM agent_runtime WHERE id=$1 FOR SHARE",
        &[&runtime],
    )
    .await
    .map_err(failed)?;
    tx.query_opt("SELECT id FROM agent WHERE id=(SELECT agent_id FROM agent_task_queue WHERE id=$1) FOR UPDATE",&[&id]).await.map_err(failed)?;
    tx.query_opt(
        "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE",
        &[&id],
    )
    .await
    .map_err(failed)?;
    // Fresh statement snapshot after lock waits; a reclaimed generation, terminal
    // task or rebound runtime must not borrow the original authorization.
    let (current, current_workspace, current_owner) = task_access(&*tx, &identity, id).await?;
    if current_workspace != workspace
        || current_owner != owner
        || current.get::<_, Option<Uuid>>("runtime_id") != runtime
    {
        return Err(error(StatusCode::NOT_FOUND, "task not found"));
    }
    let status: String = current.get("status");
    if !same_claim(&current, claim)
        || !matches!(
            status.as_str(),
            "dispatched" | "waiting_local_directory" | "running"
        )
        || (claim.is_none() && status == "running")
    {
        return Err(error(
            if claim.is_some() {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_REQUEST
            },
            "task claim is stale or task is no longer startable",
        ));
    }
    let replay = status == "running";
    let agent: Uuid = current.get("agent_id");
    let mut changed = None;
    let mut requeued = false;
    let row = if replay {
        current
    } else {
        let cleanup = tx
            .query_one(include_str!("runtime_chat_start_cleanup.sql"), &[&id])
            .await
            .map_err(|_| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "could not verify prior process cleanup",
                )
            })?;
        if cleanup.get::<_, bool>("blocked") && !cleanup.get::<_, bool>("waiting") {
            // FailTask also settles supplements, chat draft/resume state and
            // wakeup obligations. Do not approximate that terminal transaction.
            return Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "task start requires terminal process-cleanup integration",
            ));
        }
        if cleanup.get::<_, bool>("waiting") {
            let dispatched: Option<DateTime<Utc>> = current.get("dispatched_at");
            let row=tx.query_opt("UPDATE agent_task_queue SET status='queued',dispatched_at=NULL,prepare_lease_expires_at=NULL,delivered_comment_ids='{}' WHERE id=$1 AND runtime_id=$2 AND dispatched_at=$3 AND status='dispatched' AND started_at IS NULL RETURNING *",&[&id,&runtime,&dispatched]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to defer task until process cleanup"))?.ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to defer task until process cleanup"))?;
            requeued = true;
            changed = reconcile_agent(&*tx, agent).await?;
            row
        } else {
            if current
                .get::<_, Option<String>>("runtime_provider")
                .as_deref()
                == Some("qoder_cloud")
                && current.get::<_, Option<Uuid>>("issue_id").is_some()
            {
                return Err(error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "task start requires Qoder issue-lifecycle integration",
                ));
            }
            let row = tx
                .query_opt(
                    include_str!("runtime_chat_start.sql"),
                    &[
                        &id,
                        &request.offers("task-supplement-v1"),
                        &request.offers("pi-process-cleanup-v1"),
                    ],
                )
                .await
                .map_err(failed)?
                .ok_or_else(|| {
                    error(
                        if claim.is_some() {
                            StatusCode::CONFLICT
                        } else {
                            StatusCode::BAD_REQUEST
                        },
                        "task claim is stale or task is no longer startable",
                    )
                })?;
            changed = reconcile_agent(&*tx, agent).await?;
            row
        }
    };
    let mut response = super::runtime_chat_task::response(&row, workspace);
    if let Some(capability) = row.get::<_, Option<Value>>("result").and_then(|v| {
        v["process_cleanup_capability"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    }) {
        response["process_cleanup_capability"] = json!(capability);
    }
    if row.get::<_, Option<Uuid>>("issue_id").is_some()
        && let Some(capability) = tx
            .query_opt(
                "SELECT capability FROM task_supplement_capability WHERE task_id=$1",
                &[&id],
            )
            .await
            .map_err(failed)?
    {
        response["supplement_capability"] = json!(capability.get::<_, String>(0));
    }
    let session: Option<Uuid> = row.get("chat_session_id");
    let mut payload = json!({"task_id":id,"agent_id":agent,"issue_id":row.get::<_,Option<Uuid>>("issue_id").map(|v|v.to_string()).unwrap_or_default(),"status":row.get::<_,String>("status")});
    if let Some(session) = session {
        payload["chat_session_id"] = json!(session);
    }
    tx.commit().await.map_err(failed)?;
    if let Some(changed) = changed {
        state.events.publish(system_event(
            "agent:status",
            changed.get("workspace_id"),
            None,
            None,
            json!({"agent":agent_snapshot(&changed)}),
        ));
    }
    if !replay {
        state.events.publish(system_event(
            if requeued {
                "task:queued"
            } else {
                "task:running"
            },
            workspace,
            session,
            owner,
            payload,
        ));
    }
    if requeued {
        // Pollable now; daemon task-available transport is still an integration dependency.
        return Err(error(
            StatusCode::CONFLICT,
            "previous run is still stopping; task was requeued",
        ));
    }
    Ok(GoJson(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn start_decode_keeps_go_optional_null_case_duplicates_and_trailing_values() {
        for bytes in [b"".as_slice(), b"null", b"{}"] {
            assert!(Request::decode(bytes).unwrap().claim().unwrap().is_none());
        }
        let parsed=Request::decode(br#"{"RUNTIME_ID":"x","runtime_id":null,"CAPABILITIES":[null,"task-supplement-v1"],"capabilities":null,"Dispatched_At":"y"} {}"#).unwrap();
        assert_eq!(parsed.runtime, "x");
        assert_eq!(parsed.dispatched, "y");
        assert!(parsed.capabilities.is_empty());
        for bytes in [
            b"[]".as_slice(),
            b"false",
            b"{",
            br#"{"runtime_id":1}"#,
            br#"{"capabilities":"bad"}"#,
            br#"{"capabilities":[1]}"#,
        ] {
            assert!(Request::decode(bytes).is_err(), "{bytes:?}");
        }
    }
    #[test]
    fn start_claim_requires_microseconds_and_complete_pair() {
        let rt = Uuid::new_v4().to_string();
        for generation in [
            "2026-11-02T01:02:03.123456Z",
            "2026-11-02T08:02:03.123456000+07:00",
            "2026-11-02T01:02:03Z",
        ] {
            assert!(
                Request {
                    runtime: rt.clone(),
                    dispatched: generation.into(),
                    ..Default::default()
                }
                .claim()
                .unwrap()
                .is_some()
            );
        }
        for generation in [
            "",
            "no",
            "2026-11-02T01:02:03.123456001Z",
            "2026-11-02T01:02:03",
        ] {
            assert!(
                Request {
                    runtime: rt.clone(),
                    dispatched: generation.into(),
                    ..Default::default()
                }
                .claim()
                .is_err()
            );
        }
        assert!(
            Request {
                dispatched: "2026-11-02T01:02:03Z".into(),
                ..Default::default()
            }
            .claim()
            .is_err()
        );
    }
}
