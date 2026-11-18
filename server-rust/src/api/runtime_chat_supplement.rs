//! Human-authored additional input is bound to an exact running issue turn.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity,
    agent_invoke::{self, InvokeTarget, Invoker},
    catalog::WorkspaceId,
    error, json,
    runtime_chat_delivery::{Fields, comment_snapshot, receipt},
    runtime_chat_events::Event,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State, rejection::BytesRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::Value;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

const CONTENT_LIMIT: usize = 64 * 1024;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}/tasks/{taskId}/supplements", post(create))
        .route(
            "/api/issues/{id}/tasks/{taskId}/supplements/{commentId}/retry",
            post(retry),
        )
        .layer(DefaultBodyLimit::max(8 * CONTENT_LIMIT))
}

struct SupplementError(Response);
impl From<ApiError> for SupplementError {
    fn from(error: ApiError) -> Self {
        Self(error.into_response())
    }
}
impl IntoResponse for SupplementError {
    fn into_response(self) -> Response {
        self.0
    }
}
fn coded(status: StatusCode, code: &str, message: &str) -> SupplementError {
    SupplementError((status, GoJson(json!({"error":message,"code":code}))).into_response())
}
fn failed(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}
fn ended() -> SupplementError {
    coded(
        StatusCode::CONFLICT,
        "task_supplement_turn_ended",
        "this run has ended",
    )
}

fn request(bytes: &[u8]) -> Result<(String, Uuid), ApiError> {
    let Some(Ok(fields)) = serde_json::Deserializer::from_slice(bytes)
        .into_iter::<Fields>()
        .next()
    else {
        return Err(error(StatusCode::BAD_REQUEST, "invalid request body"));
    };
    let (mut content, mut request) = (String::new(), String::new());
    for (key, value) in fields.0 {
        let target = match key.as_str() {
            "content" => &mut content,
            "client_request_id" => &mut request,
            _ => continue,
        };
        if let Some(text) = value.as_str() {
            *target = text.to_owned();
        } else if !value.is_null() {
            return Err(error(StatusCode::BAD_REQUEST, "invalid request body"));
        }
    }
    content = content.replace('\0', "");
    if content.len() > CONTENT_LIMIT {
        return Err(error(StatusCode::BAD_REQUEST, "content is too long"));
    }
    if content.trim().is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "content is required"));
    }
    let request = Uuid::parse_str(&request)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid client_request_id"))?;
    Ok((content, request))
}

async fn target(
    db: &impl GenericClient,
    workspace: Uuid,
    user: Uuid,
    issue: Uuid,
    task: Uuid,
) -> Result<Row, SupplementError> {
    // Membership and invocation are separate gates. An administrator may view a
    // private agent without permission to send input to that agent's process.
    if db
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&workspace, &user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "workspace not found").into());
    }
    let task = db.query_opt("SELECT t.*,i.title AS issue_title FROM agent_task_queue t JOIN issue i ON i.id=t.issue_id WHERE t.id=$1 AND t.issue_id=$2 AND i.workspace_id=$3", &[&task,&issue,&workspace]).await
        .map_err(|_| error(StatusCode::NOT_FOUND,"task not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND,"task not found"))?;
    let agent: Uuid = task.get("agent_id");
    let carrier = db
        .query_opt(
            "SELECT owner_id,permission_mode FROM agent WHERE id=$1 AND workspace_id=$2",
            &[&agent, &workspace],
        )
        .await
        .map_err(|_| {
            coded(
                StatusCode::FORBIDDEN,
                "invocation_not_allowed",
                "you cannot run this agent",
            )
        })?;
    // Go calls canInvokeAgent(ctx, agent, "member", userID, userID, ws): the
    // member acts as themselves and is also the originator. A failed allow-list
    // read denies rather than widens, and rejects land on the same generic
    // invocation_not_allowed.
    let allowed = match carrier {
        Some(carrier) => {
            let permission_mode: String = carrier.get("permission_mode");
            agent_invoke::can_invoke(
                db,
                workspace,
                InvokeTarget {
                    id: agent,
                    owner_id: carrier.get("owner_id"),
                    permission_mode: &permission_mode,
                },
                Invoker {
                    actor_type: "member",
                    actor_id: user,
                    originator: Some(user),
                },
            )
            .await
            .unwrap_or(false)
        }
        None => false,
    };
    if !allowed {
        return Err(coded(
            StatusCode::FORBIDDEN,
            "invocation_not_allowed",
            "you cannot run this agent",
        ));
    }
    Ok(task)
}

async fn existing(
    db: &impl GenericClient,
    workspace: Uuid,
    task: Uuid,
    user: Uuid,
    request: Uuid,
) -> Result<Option<Uuid>, ApiError> {
    Ok(db.query_opt("SELECT comment_id FROM task_supplement WHERE task_id=$1 AND workspace_id=$2 AND author_id=$3 AND client_request_id=$4", &[&task,&workspace,&user,&request]).await
        .map_err(|_| failed("failed to check additional message"))?.map(|r| r.get(0)))
}
async fn snapshot(db: &impl GenericClient, ws: Uuid, comment: Uuid) -> Result<Value, ApiError> {
    comment_snapshot(db, ws, comment)
        .await
        .ok_or_else(|| failed("failed to load additional message"))
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((issue_raw, task_raw)): Path<(String, String)>,
    bytes: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, GoJson<Value>), SupplementError> {
    actor.require_human()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let issue: Uuid = super::issue_read::load_issue(&db, ws, &issue_raw)
        .await?
        .get("id");
    let task = Uuid::parse_str(&task_raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task id"))?;
    target(&**db, ws, user, issue, task).await?;
    let bytes = bytes.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let (content, request) = request(&bytes)?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to create additional message"))?;
    // The next statement gets a fresh snapshot after a prior sender/terminal
    // transition releases this lock. Identical concurrent sends create one row.
    tx.query_opt("SELECT id FROM agent WHERE id=(SELECT agent_id FROM agent_task_queue WHERE id=$1) FOR SHARE", &[&task]).await.map_err(|_| failed("failed to create additional message"))?;
    tx.query_opt(
        "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE",
        &[&task],
    )
    .await
    .map_err(|_| failed("failed to create additional message"))?;
    let current = target(&*tx, ws, user, issue, task).await?;
    if let Some(comment) = existing(&*tx, ws, task, user, request).await? {
        let response = snapshot(&*tx, ws, comment).await?;
        tx.commit()
            .await
            .map_err(|_| failed("failed to create additional message"))?;
        return Ok((StatusCode::OK, GoJson(response)));
    }
    if current.get::<_, String>("status") != "running" {
        return Err(ended());
    }
    let capability = tx
        .query_opt(
            "SELECT capability FROM task_supplement_capability WHERE task_id=$1",
            &[&task],
        )
        .await
        .map_err(|_| failed("failed to check additional-message support"))?;
    if capability.is_none_or(|r| r.get::<_, String>("capability") != "task-supplement-v1") {
        return Err(coded(
            StatusCode::PRECONDITION_FAILED,
            "task_supplement_unsupported",
            "this run does not support additional messages",
        ));
    }
    let created = tx
        .query_opt(
            include_str!("runtime_chat_supplement.sql"),
            &[&task, &issue, &ws, &user, &content, &request],
        )
        .await;
    let created = match created {
        Ok(Some(row)) => row,
        Ok(None) => return Err(ended()),
        Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
            tx.rollback()
                .await
                .map_err(|_| failed("failed to create additional message"))?;
            if let Some(comment) = existing(&**db, ws, task, user, request).await? {
                return Ok((StatusCode::OK, GoJson(snapshot(&**db, ws, comment).await?)));
            }
            return Err(error(StatusCode::CONFLICT, "client_request_id is already in use").into());
        }
        Err(_) => return Err(failed("failed to create additional message").into()),
    };
    let mut response = snapshot(&*tx, ws, created.get("id")).await?;
    let revision: i64 = created.get("issue_revision");
    response["issue_revision"] = json!(revision);
    tx.commit()
        .await
        .map_err(|_| failed("failed to create additional message"))?;
    state.events.publish(Event {
        event_type:"comment:created".into(),workspace_id:ws,actor_type:"member".into(),actor_id:user,
        chat_session_id:None,chat_session_owner_id:None,
        payload:json!({"comment":response,"issue_title":current.get::<_,String>("issue_title"),"issue_revision":revision}),
    });
    // Daemons can poll the committed pending receipt. Transport wakeups require
    // the daemon WebSocket/RPC dispatcher, not a second workspace event bus.
    Ok((StatusCode::CREATED, GoJson(response)))
}

async fn retry(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((issue_raw, task_raw, comment_raw)): Path<(String, String, String)>,
) -> Result<GoJson<Value>, SupplementError> {
    actor.require_human()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let issue: Uuid = super::issue_read::load_issue(&db, ws, &issue_raw)
        .await?
        .get("id");
    let task = Uuid::parse_str(&task_raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task id"))?;
    target(&**db, ws, user, issue, task).await?;
    let comment = Uuid::parse_str(&comment_raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid comment id"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to retry additional message"))?;
    tx.query_opt("SELECT id FROM agent WHERE id=(SELECT agent_id FROM agent_task_queue WHERE id=$1) FOR SHARE", &[&task]).await.map_err(|_| failed("failed to retry additional message"))?;
    // Frozen Go takes comment before task for retries. Keep this order for
    // compatibility with comment deletion and supplement acknowledgements.
    tx.query_opt(
        "SELECT id FROM comment WHERE id=$1 AND workspace_id=$2 AND deleted_at IS NULL FOR UPDATE",
        &[&comment, &ws],
    )
    .await
    .map_err(|_| failed("failed to retry additional message"))?;
    tx.query_opt(
        "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE",
        &[&task],
    )
    .await
    .map_err(|_| failed("failed to retry additional message"))?;
    target(&*tx, ws, user, issue, task).await?;
    let row = tx.query_opt("UPDATE task_supplement s SET status='pending',failure_reason=NULL,updated_at=now() FROM agent_task_queue t WHERE s.task_id=t.id AND t.id=$1 AND t.issue_id=$2 AND t.status='running' AND s.comment_id=$3 AND s.workspace_id=$4 AND s.status='failed' AND EXISTS(SELECT 1 FROM task_supplement_capability cap WHERE cap.task_id=t.id AND cap.capability='task-supplement-v1') AND EXISTS(SELECT 1 FROM comment c WHERE c.id=$3 AND c.workspace_id=$4 AND c.deleted_at IS NULL) RETURNING s.*", &[&task,&issue,&comment,&ws]).await.map_err(|_| failed("failed to retry additional message"))?;
    let row = match row {
        Some(row) => Some(row),
        None => tx.query_opt("SELECT * FROM task_supplement WHERE task_id=$1 AND comment_id=$2 AND workspace_id=$3 AND status='pending'", &[&task,&comment,&ws]).await.map_err(|_| failed("failed to retry additional message"))?,
    }.ok_or_else(|| coded(StatusCode::CONFLICT,"task_supplement_not_retryable","this message cannot be retried"))?;
    let response = receipt(&row);
    tx.commit()
        .await
        .map_err(|_| failed("failed to retry additional message"))?;
    if let Some(comment) = comment_snapshot(&**db, ws, comment).await {
        state.events.publish(Event {
            event_type: "comment:updated".into(),
            workspace_id: ws,
            actor_type: "system".into(),
            actor_id: Uuid::nil(),
            chat_session_id: None,
            chat_session_owner_id: None,
            payload: json!({"comment":comment}),
        });
    }
    Ok(GoJson(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decode_matches_go_case_null_duplicates_byte_limits_and_nul_removal() {
        let id = Uuid::new_v4();
        let raw = format!(
            r#"{{"content":"first","CONTENT":"hi\u0000 there","content":null,"CLIENT_REQUEST_ID":"{id}"}} trailing"#
        );
        assert_eq!(request(raw.as_bytes()).unwrap(), ("hi there".into(), id));
        for raw in ["[]", "{bad", r#"{"content":false,"content":"ok"}"#] {
            assert_eq!(
                request(raw.as_bytes()).unwrap_err().1,
                "invalid request body"
            );
        }
        for raw in ["null", r#"{"content":" \u0000\n "}"#] {
            assert_eq!(
                request(raw.as_bytes()).unwrap_err().1,
                "content is required"
            );
        }
        let raw =
            json!({"content":"é".repeat(CONTENT_LIMIT/2+1),"client_request_id":id}).to_string();
        assert_eq!(
            request(raw.as_bytes()).unwrap_err().1,
            "content is too long"
        );
        let raw = json!({"content":"x".repeat(CONTENT_LIMIT),"client_request_id":id}).to_string();
        assert_eq!(request(raw.as_bytes()).unwrap().0.len(), CONTENT_LIMIT);
    }
}
