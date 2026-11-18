//! Scheduled wakeup check-ins: only the running task that the rule started.
use super::{
    ApiError, ApiState, Identity, actor::Actor, catalog::WorkspaceId, error,
    issue_read::load_issue, issue_write::IssueWakeupActivityEvent,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/issues/{id}/wakeups/{wakeupID}/checkin", post(checkin))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckinRequest {
    note: String,
}

fn checkin_note(body: &[u8]) -> Result<String, ApiError> {
    if body.len() > 8192 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid check-in body"));
    }
    let request: CheckinRequest = serde_json::from_slice(body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid check-in body"))?;
    let note = request.note.trim().to_owned();
    if note.is_empty() || note.chars().count() > 500 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: note must contain 1–500 characters",
        ));
    }
    Ok(note)
}

pub(super) fn wakeup_preview(w: &Value) -> Value {
    let mut preview = Map::new();
    for key in [
        "id",
        "kind",
        "mode",
        "event_types",
        "agent_id",
        "timezone",
        "created_by",
    ] {
        preview.insert(key.into(), w[key].clone());
    }
    for key in [
        "condition",
        "filter_actor_type",
        "filter_actor_id",
        "filter_agent_id",
        "interval_seconds",
        "cron_expression",
        "expires_at",
    ] {
        if !w[key].is_null() {
            preview.insert(key.into(), w[key].clone());
        }
    }
    if w["condition"].is_null()
        && let Some(raw) = w["next_fire_at"].as_str()
        && let Ok(next) = DateTime::parse_from_rfc3339(raw)
    {
        preview.insert(
            "next_fire_at".into(),
            json!(next.to_utc().to_rfc3339_opts(SecondsFormat::Secs, true)),
        );
    }
    Value::Object(preview)
}

async fn checkin(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(_)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_issue, raw_wakeup)): Path<(String, String)>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup"))?;
    let issue = load_issue(&db, ws, &raw_issue).await?;
    let issue_id: Uuid = issue.get("id");
    let wakeup_id = Uuid::parse_str(&raw_wakeup)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid wakeup id"))?;
    let note = checkin_note(&body)?;
    let Actor::Task(task) = actor else {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only the run a wakeup started can check in",
        ));
    };
    let tx = db
        .transaction()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup"))?;
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup");
    let row = tx
        .query_opt(
            "SELECT status,issue_id,agent_id,COALESCE(context->>'wakeup_id','') AS wakeup_id \
         FROM agent_task_queue WHERE id=$1 FOR UPDATE",
            &[&task.task],
        )
        .await
        .map_err(|_| fail())?
        .ok_or_else(|| error(StatusCode::FORBIDDEN, "wakeup permission denied"))?;
    let task_issue: Option<Uuid> = row.get("issue_id");
    let task_agent: Uuid = row.get("agent_id");
    let bound_wakeup: String = row.get("wakeup_id");
    if task_issue != Some(issue_id)
        || task_agent != task.agent
        || bound_wakeup != wakeup_id.to_string()
    {
        return Err(error(StatusCode::FORBIDDEN, "wakeup permission denied"));
    }
    if row.get::<_, String>("status") != "running" {
        return Err(error(
            StatusCode::CONFLICT,
            "wakeup changed; refresh and retry: only a running check can check in",
        ));
    }
    let wakeup = tx.query_opt(
        "SELECT to_jsonb(w) AS data FROM issue_wakeup w WHERE w.id=$1 AND w.issue_id=$2 AND w.workspace_id=$3",
        &[&wakeup_id, &issue_id, &ws],
    ).await.map_err(|_| fail())?.ok_or_else(|| error(StatusCode::NOT_FOUND, "wakeup not found"))?;
    let w: Value = wakeup.get("data");
    if w["kind"] != "every" && w["kind"] != "cron" {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: check-ins are for scheduled checks; post a comment instead",
        ));
    }
    let now: DateTime<Utc> = tx
        .query_one("SELECT now()", &[])
        .await
        .map_err(|_| fail())?
        .get(0);
    let checkin = json!({"note":note,"at":now.to_rfc3339_opts(SecondsFormat::Secs,true)});
    tx.execute(
        "UPDATE agent_task_queue SET context=COALESCE(context,'{}'::jsonb)||jsonb_build_object('wakeup_checkin',$2::jsonb) WHERE id=$1",
        &[&task.task, &checkin],
    ).await.map_err(|_| fail())?;
    let mut preview = wakeup_preview(&w);
    if let Some(source) = w["source_task_id"].as_str()
        && let Ok(source_id) = Uuid::parse_str(source)
        && let Some(source_agent) = tx
            .query_opt(
                "SELECT agent_id FROM agent_task_queue WHERE id=$1",
                &[&source_id],
            )
            .await
            .map_err(|_| fail())?
    {
        preview["created_by_agent_id"] = json!(source_agent.get::<_, Uuid>(0));
    }
    let details = json!({"note":note,"task_id":task.task,"wakeup":preview});
    let activity = tx
        .query_one(
            "INSERT INTO activity_log(id,workspace_id,issue_id,actor_type,actor_id,action,details) \
         VALUES ($1,$2,$3,'agent',$4,'wakeup_checkin',$5) RETURNING id,created_at",
            &[&Uuid::new_v4(), &ws, &issue_id, &task.agent, &details],
        )
        .await
        .map_err(|_| fail())?;
    let activity_id: Uuid = activity.get("id");
    let created: DateTime<Utc> = activity.get("created_at");
    tx.commit().await.map_err(|_| fail())?;
    sink.publish_wakeup_activity(IssueWakeupActivityEvent {
        workspace_id: ws,
        actor_id: task.agent,
        payload: json!({
            "issue_id":issue_id,
            "entry":{
                "type":"activity", "id":activity_id, "actor_type":"agent", "actor_id":task.agent,
                "action":"wakeup_checkin", "details":details,
                "created_at":created.to_rfc3339_opts(SecondsFormat::AutoSi,true),
            }
        }),
    });
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkin_note_validation() {
        assert_eq!(checkin_note(br#"{"note":"  ok  "}"#).unwrap(), "ok");
        assert!(checkin_note(br#"{"note":" ","extra":1}"#).is_err());
        assert!(checkin_note(br#"{"note":" "}"#).is_err());
        assert!(checkin_note(br#"{"note":42}"#).is_err());
        assert!(checkin_note(br#"{}"#).is_err());
        assert!(checkin_note("{\"note\":\"á\"}".as_bytes()).is_ok());
        assert!(checkin_note(format!("{{\"note\":\"{}\"}}", "á".repeat(501)).as_bytes()).is_err());
    }
}
