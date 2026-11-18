//! Daemon resume pins and exact-turn supplement delivery. No provider calls.
use super::{
    ApiError, ApiState, GoJson, daemon::DaemonIdentity, error, json,
    runtime_chat_daemon::task_access, runtime_chat_events::Event,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/daemon/tasks/{taskId}/session", post(pin_session))
        .route("/api/daemon/tasks/{taskId}/supplements/claim", post(claim))
        .route(
            "/api/daemon/tasks/{taskId}/supplements/{commentId}/ack",
            post(ack),
        )
}

fn failed(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}
fn task_id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task_id"))
}

// Preserve wire order, including differently-cased duplicate fields. Go's
// decoder retains other valid fields even when a known field has a type error.
pub(super) struct Fields(pub(super) Vec<(String, Value)>);
impl<'de> serde::Deserialize<'de> for Fields {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Fields;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Fields, E> {
                Ok(Fields(Vec::new()))
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Fields, M::Error> {
                let mut fields = Vec::new();
                while let Some((name, value)) = map.next_entry::<String, Value>()? {
                    fields.push((name.to_ascii_lowercase(), value));
                }
                Ok(Fields(fields))
            }
        }
        decoder.deserialize_any(Visitor)
    }
}
#[derive(Default)]
struct Request {
    session_id: String,
    work_dir: String,
    capabilities: Vec<String>,
    delivered: bool,
    error: String,
}
#[derive(Clone, Copy)]
enum Kind {
    Pin,
    Claim,
    Ack,
}
fn decode(bytes: &[u8], kind: Kind) -> (Request, bool) {
    let mut request = Request::default();
    let Some(Ok(fields)) = serde_json::Deserializer::from_slice(bytes)
        .into_iter::<Fields>()
        .next()
    else {
        return (request, false);
    };
    let mut valid = true;
    for (name, value) in fields.0 {
        let string = match (kind, name.as_str()) {
            (Kind::Pin, "session_id") => Some(&mut request.session_id),
            (Kind::Pin, "work_dir") => Some(&mut request.work_dir),
            (Kind::Ack, "error") => Some(&mut request.error),
            _ => None,
        };
        if let Some(target) = string {
            if let Some(value) = value.as_str() {
                *target = value.to_owned();
            } else if !value.is_null() {
                valid = false;
            }
        } else if matches!(kind, Kind::Ack) && name == "delivered" {
            if let Some(value) = value.as_bool() {
                request.delivered = value;
            } else if !value.is_null() {
                valid = false;
            }
        } else if matches!(kind, Kind::Claim) && name == "capabilities" {
            if value.is_null() {
                request.capabilities.clear();
            } else if let Some(values) = value.as_array() {
                request.capabilities = values
                    .iter()
                    .map(|value| {
                        if value.is_null() {
                            String::new()
                        } else if let Some(value) = value.as_str() {
                            value.to_owned()
                        } else {
                            valid = false;
                            String::new()
                        }
                    })
                    .collect();
            } else {
                valid = false;
            }
        }
    }
    (request, valid)
}

// Freeze runtime ownership before taking source/task locks. Re-check after all
// waits: authorization from a prior read must not survive a runtime rebind.
async fn lock_runtime(db: &impl GenericClient, runtime: Option<Uuid>) -> Result<(), ApiError> {
    db.query_opt(
        "SELECT id FROM agent_runtime WHERE id=$1 FOR SHARE",
        &[&runtime],
    )
    .await
    .map_err(|_| failed("failed to load task"))?;
    Ok(())
}

async fn pin_session(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let id = task_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("pin session failed"))?;
    let (before, workspace, _) = task_access(&**db, &identity, id).await?;
    let (request, valid) = decode(&bytes, Kind::Pin);
    if !valid {
        return Err(error(StatusCode::BAD_REQUEST, "invalid request body"));
    }
    if request.session_id.is_empty() && request.work_dir.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "session_id or work_dir required",
        ));
    }
    let runtime: Option<Uuid> = before.get("runtime_id");
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("pin session failed"))?;
    lock_runtime(&*tx, runtime).await?;
    // Global chat_session -> task order matches cancel/delete/finalization.
    tx.query_opt("SELECT cs.id FROM agent_task_queue t JOIN chat_session cs ON cs.id=t.chat_session_id WHERE t.id=$1 FOR UPDATE OF cs", &[&id])
        .await.map_err(|_| failed("pin session failed"))?;
    tx.query_opt(
        "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE",
        &[&id],
    )
    .await
    .map_err(|_| failed("pin session failed"))?;
    let (current, current_workspace, _) = task_access(&*tx, &identity, id).await?;
    if current_workspace != workspace || current.get::<_, Option<Uuid>>("runtime_id") != runtime {
        return Err(error(StatusCode::NOT_FOUND, "task not found"));
    }
    let session = (!request.session_id.is_empty()).then_some(request.session_id);
    let work_dir = (!request.work_dir.is_empty()).then_some(request.work_dir);
    tx.execute("UPDATE agent_task_queue SET session_id=COALESCE($2,session_id),work_dir=COALESCE($3,work_dir) WHERE id=$1 AND (status IN ('dispatched','running') OR (status='cancelled' AND session_id IS NULL))", &[&id,&session,&work_dir])
        .await.map_err(|_| failed("pin session failed"))?;
    tx.execute(include_str!("runtime_chat_pin.sql"), &[&id])
        .await
        .map_err(|_| failed("pin session failed"))?;
    tx.commit()
        .await
        .map_err(|_| failed("pin session failed"))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn claim(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let id = task_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to claim additional message"))?;
    let (before, workspace, _) = task_access(&**db, &identity, id).await?;
    // Frozen Go treats decode errors as a lost capability, not an HTTP error.
    // Its decoder can retain valid fields in an otherwise type-invalid object.
    let (request, _) = decode(&bytes[..bytes.len().min(64 << 10)], Kind::Claim);
    let include_agent = request
        .capabilities
        .iter()
        .any(|s| s == "task-supplement-author-v1");
    let runtime: Option<Uuid> = before.get("runtime_id");
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to claim additional message"))?;
    lock_runtime(&*tx, runtime).await?;
    tx.query_opt(
        "SELECT id FROM agent_task_queue WHERE id=$1 FOR SHARE",
        &[&id],
    )
    .await
    .map_err(|_| failed("failed to claim additional message"))?;
    let (current, current_workspace, _) = task_access(&*tx, &identity, id).await?;
    if current_workspace != workspace || current.get::<_, Option<Uuid>>("runtime_id") != runtime {
        return Err(error(StatusCode::NOT_FOUND, "task not found"));
    }
    let row = tx
        .query_opt(
            include_str!("runtime_chat_supplement_claim.sql"),
            &[&id, &include_agent],
        )
        .await
        .map_err(|_| failed("failed to claim additional message"))?;
    let response = row
        .map(|row| {
            json!({
                "comment_id":row.get::<_,Uuid>("comment_id"),
                "author_type":row.get::<_,String>("author_type"),
                "author_name":row.get::<_,String>("author_name"),
                "content":row.get::<_,String>("content"),
            })
        })
        .unwrap_or_else(|| json!({}));
    tx.commit()
        .await
        .map_err(|_| failed("failed to claim additional message"))?;
    Ok(GoJson(response))
}

fn failure_reason(raw: &str) -> &str {
    let reason = raw.trim_matches(|c: char| c == '\0' || c.is_whitespace());
    match reason {
        "turn_not_started" | "timeout" | "turn_ended" | "provider_rejected" => reason,
        _ => "provider_rejected",
    }
}
fn time(row: &Row, column: &str) -> Value {
    row.get::<_, Option<DateTime<Utc>>>(column)
        .map(|time| json!(time.to_rfc3339_opts(SecondsFormat::AutoSi, true)))
        .unwrap_or(Value::Null)
}
pub(super) fn receipt(row: &Row) -> Value {
    let mut value = json!({"supplement_task_id":row.get::<_,Uuid>("task_id"),"supplement_status":row.get::<_,String>("status")});
    if let Some(reason) = row.get::<_, Option<String>>("failure_reason") {
        value["supplement_failure_reason"] = json!(reason);
    }
    if !time(row, "delivered_at").is_null() {
        value["supplement_delivered_at"] = time(row, "delivered_at");
    }
    value
}

async fn ack(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path((raw, comment_raw)): Path<(String, String)>,
    bytes: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let id = task_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to acknowledge additional message"))?;
    let (before, workspace, _) = task_access(&**db, &identity, id).await?;
    let comment = Uuid::parse_str(&comment_raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid comment id"))?;
    let (request, valid) = decode(&bytes, Kind::Ack);
    if !valid {
        return Err(error(StatusCode::BAD_REQUEST, "invalid request body"));
    }
    let runtime: Option<Uuid> = before.get("runtime_id");
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to acknowledge additional message"))?;
    lock_runtime(&*tx, runtime).await?;
    tx.query_opt(
        "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE",
        &[&id],
    )
    .await
    .map_err(|_| failed("failed to acknowledge additional message"))?;
    let (current, current_workspace, _) = task_access(&*tx, &identity, id).await?;
    if current_workspace != workspace || current.get::<_, Option<Uuid>>("runtime_id") != runtime {
        return Err(error(StatusCode::NOT_FOUND, "task not found"));
    }
    let reason = request.error.replace('\0', "");
    let reason = failure_reason(&reason);
    let row = if request.delivered {
        tx.query_opt("UPDATE task_supplement SET status='delivered',delivered_at=COALESCE(delivered_at,now()),failure_reason=NULL,updated_at=now() WHERE task_id=$1 AND comment_id=$2 AND (status IN ('delivering','delivered') OR (status='failed' AND failure_reason='turn_ended' AND attempt_count>0)) RETURNING *", &[&id,&comment]).await
    } else {
        tx.query_opt("UPDATE task_supplement SET status='failed',failure_reason=$3,updated_at=now() WHERE task_id=$1 AND comment_id=$2 AND status='delivering' RETURNING *", &[&id,&comment,&reason]).await
    }.map_err(|_| failed("failed to acknowledge additional message"))?
        .ok_or_else(|| error(StatusCode::CONFLICT,"additional message is no longer deliverable"))?;
    let response = receipt(&row);
    let receipt_workspace: Uuid = row.get("workspace_id");
    tx.commit()
        .await
        .map_err(|_| failed("failed to acknowledge additional message"))?;
    // Like Go, receipt fanout is best effort and follows the committed write.
    // A read error must not turn a successful acknowledgement into a retry.
    if receipt_workspace == workspace
        && let Some(payload) = comment_snapshot(&**db, workspace, comment).await
    {
        state.events.publish(Event {
            event_type: "comment:updated".into(),
            workspace_id: workspace,
            actor_type: "system".into(),
            actor_id: Uuid::nil(),
            chat_session_id: None,
            chat_session_owner_id: None,
            payload: json!({"comment":payload}),
        });
    }
    Ok(GoJson(response))
}

// Read-only projection owned by delivery, not a second comment mutation path.
// Go's receipt event intentionally has empty attachments and reactions.
pub(super) async fn comment_snapshot(
    db: &impl GenericClient,
    workspace: Uuid,
    comment: Uuid,
) -> Option<Value> {
    let row = db
        .query_opt(
            "SELECT * FROM comment WHERE id=$1 AND workspace_id=$2",
            &[&comment, &workspace],
        )
        .await
        .ok()??;
    let mut value = json!({
        "id":comment,"issue_id":row.get::<_,Uuid>("issue_id"),"author_type":row.get::<_,String>("author_type"),
        "author_id":row.get::<_,Uuid>("author_id"),"content":row.get::<_,String>("content"),"type":row.get::<_,String>("type"),
        "parent_id":row.get::<_,Option<Uuid>>("parent_id"),"created_at":time(&row,"created_at"),"updated_at":time(&row,"updated_at"),
        "revision":row.get::<_,i64>("revision"),"resolved_at":time(&row,"resolved_at"),
        "resolved_by_type":row.get::<_,Option<String>>("resolved_by_type"),"resolved_by_id":row.get::<_,Option<Uuid>>("resolved_by_id"),
        "reactions":[],"attachments":[],
    });
    for column in ["source_task_id", "quick_action_id"] {
        if let Some(id) = row.get::<_, Option<Uuid>>(column) {
            value[column] = json!(id);
        }
    }
    if !time(&row, "deleted_at").is_null() {
        value["deleted_at"] = time(&row, "deleted_at");
    }
    // Receipt metadata is optional; preserve every run, oldest binding first.
    if let Ok(rows) = db.query("SELECT s.*,t.agent_id FROM task_supplement s LEFT JOIN agent_task_queue t ON t.id=s.task_id WHERE s.workspace_id=$1 AND s.comment_id=$2 ORDER BY s.created_at,s.task_id", &[&workspace,&comment]).await
        && !rows.is_empty()
    {
        let receipts: Vec<Value> = rows.iter().map(|row| {
            let mut r = json!({"task_id":row.get::<_,Uuid>("task_id"),"status":row.get::<_,String>("status")});
            if let Some(agent) = row.get::<_,Option<Uuid>>("agent_id") { r["agent_id"]=json!(agent); }
            if let Some(reason) = row.get::<_,Option<String>>("failure_reason") { r["failure_reason"]=json!(reason); }
            if !time(row,"delivered_at").is_null() { r["delivered_at"]=time(row,"delivered_at"); }
            r
        }).collect();
        let first = receipt(&rows[0]);
        value.as_object_mut()?.extend(first.as_object()?.clone());
        value["supplements"] = json!(receipts);
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decoder_retains_go_null_case_duplicate_and_type_error_semantics() {
        let (r, ok) = decode(
            br#"{"session_id":"first","SESSION_ID":"last","session_id":null} trailing"#,
            Kind::Pin,
        );
        assert!(ok);
        assert_eq!(r.session_id, "last");
        assert!(!decode(br#"{"session_id":3,"session_id":"ok"}"#, Kind::Pin).1);
        assert!(!decode(b"[]", Kind::Pin).1);
        assert!(decode(b"null", Kind::Pin).1);
        let (r, ok) = decode(
            br#"{"capabilities":[0,"task-supplement-author-v1",null]}"#,
            Kind::Claim,
        );
        assert!(!ok);
        assert_eq!(r.capabilities, vec!["", "task-supplement-author-v1", ""]);
        assert!(
            decode(
                br#"{"capabilities":["x"],"CAPABILITIES":null}"#,
                Kind::Claim
            )
            .0
            .capabilities
            .is_empty()
        );
        let (r, ok) = decode(
            br#"{"delivered":true,"DELIVERED":null,"error":null}"#,
            Kind::Ack,
        );
        assert!(ok);
        assert!(r.delivered);
        assert_eq!(r.error, "");
    }
    #[test]
    fn failure_reasons_never_echo_provider_diagnostics() {
        for reason in [
            "turn_not_started",
            "timeout",
            "turn_ended",
            "provider_rejected",
        ] {
            assert_eq!(failure_reason(&format!(" {reason} \n")), reason);
        }
        for reason in [
            "",
            "TIMEOUT",
            "private /home/person token secret",
            "timeout\0extra",
        ] {
            assert_eq!(failure_reason(reason), "provider_rejected");
        }
    }
}
