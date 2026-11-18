//! Squad leader verdicts are authorized from the running task, not the issue assignee.
use super::{
    ApiError, ApiState, GoJson, Identity, actor::Actor, catalog::WorkspaceId, error,
    issue_read::load_issue, runtime_chat_events::Event, timestamp,
};
use axum::{
    body::Bytes,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Debug, Default, Deserialize)]
struct Evaluation {
    #[serde(default, deserialize_with = "null_string")]
    outcome: String,
    #[serde(default, deserialize_with = "null_string")]
    reason: String,
}

fn null_string<'de, D: Deserializer<'de>>(decoder: D) -> Result<String, D::Error> {
    Ok(Option::<String>::deserialize(decoder)?.unwrap_or_default())
}

fn decode(body: &[u8]) -> Result<Evaluation, ApiError> {
    // Go's JSON Decoder accepts a first JSON value without requiring EOF.
    let mut decoder = serde_json::Deserializer::from_slice(body);
    Option::<Evaluation>::deserialize(&mut decoder)
        .map(Option::unwrap_or_default)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))
}

pub(super) async fn record(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(_)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let issue_id: Uuid = load_issue(&db, ws, &raw).await?.get("id");
    let input = decode(&body)?;
    if !matches!(input.outcome.as_str(), "action" | "no_action" | "failed") {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "outcome must be 'action', 'no_action', or 'failed'",
        ));
    }
    let task_id = headers
        .get("x-task-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let task_id =
        Uuid::parse_str(task_id).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task id"))?;
    // The tenant-scoped task lookup precedes every task-derived diagnostic.
    let task = db
        .query_opt(
            "SELECT q.issue_id,q.agent_id,q.is_leader_task,q.squad_id FROM agent_task_queue q \
         JOIN agent a ON a.id=q.agent_id WHERE q.id=$1 AND a.workspace_id=$2",
            &[&task_id, &ws],
        )
        .await
        .ok()
        .flatten()
        .filter(|row| row.get::<_, Option<Uuid>>("issue_id").is_some())
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "task does not belong to issue"))?;
    let agent: Uuid = task.get("agent_id");
    if !matches!(actor, Actor::Task(ref identity) if identity.agent == agent && identity.task == task_id)
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only the squad leader agent can record evaluations",
        ));
    }
    let task_issue: Uuid = task
        .get::<_, Option<Uuid>>("issue_id")
        .expect("filtered above");
    if task_issue != issue_id {
        return Err(error(
            StatusCode::BAD_REQUEST,
            format!(
                "task does not belong to issue; record the evaluation on issue {task_issue} (the issue this task is running on)"
            ),
        ));
    }
    if !task.get::<_, bool>("is_leader_task") {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "task is not a squad leader task",
        ));
    }
    let squad_id = task
        .get::<_, Option<Uuid>>("squad_id")
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "leader task has no squad_id"))?;
    let squad = db
        .query_opt(
            "SELECT leader_id FROM squad WHERE id=$1 AND workspace_id=$2",
            &[&squad_id, &ws],
        )
        .await
        .ok()
        .flatten()
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "squad not found"))?;
    if squad.get::<_, Uuid>("leader_id") != agent {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only the squad leader agent can record evaluations",
        ));
    }

    let details = json!({"squad_id":squad_id,"task_id":task_id,"outcome":input.outcome,"reason":input.reason});
    let id = Uuid::now_v7();
    let row = db
        .query_one(
            "INSERT INTO activity_log(id,workspace_id,issue_id,actor_type,actor_id,action,details) \
         VALUES($1,$2,$3,'agent',$4,'squad_leader_evaluated',$5) RETURNING created_at",
            &[&id, &ws, &issue_id, &agent, &details],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to record evaluation",
            )
        })?;
    let created_at = timestamp(row.get("created_at"));
    state.events.publish(Event {
        event_type: "activity:created".into(),
        workspace_id: ws,
        actor_type: "agent".into(),
        actor_id: agent,
        chat_session_id: None,
        chat_session_owner_id: None,
        payload: json!({"issue_id":issue_id,"entry":{
            "type":"activity","id":id,"actor_type":"agent","actor_id":agent,
            "action":"squad_leader_evaluated","details":details,"created_at":created_at,
        }}),
    });
    Ok((
        StatusCode::CREATED,
        GoJson(json!({
            "id":id,"action":"squad_leader_evaluated","created_at":created_at,
        })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_validation_and_null_go_strings() {
        assert!(matches!(
            decode(b"{").unwrap_err().0,
            StatusCode::BAD_REQUEST
        ));
        assert_eq!(
            decode(br#"{"outcome":null,"reason":null}"#).unwrap().reason,
            ""
        );
        assert_eq!(
            decode(br#"{"outcome":"no_action","extra":true}"#)
                .unwrap()
                .outcome,
            "no_action"
        );
        assert_eq!(decode(b"null").unwrap().outcome, "");
        assert!(decode(br#"{"outcome":12}"#).is_err());
    }
}
