//! Issue detail core with scoped source-context projection.
use super::{
    ApiError, ApiState, GoJson,
    catalog::WorkspaceId,
    error,
    issue_read::load_issue,
    queries_sprint_attachments::{for_issue, stable_requested},
    queries_sprint_issue_list::project_rows,
    queries_sprint_search_shapes::ISSUE_SELECT,
    queries_sprint_source_context, timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    routing::get,
};
use serde_json::{Value, json};
use tokio_postgres::IsolationLevel;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}", get(detail))
        .route("/api/issues/{id}/", get(detail))
}

async fn detail(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Result<GoJson<Value>, ApiError> {
    if Uuid::parse_str(&raw).is_err()
        && !raw.rsplit_once('-').is_some_and(|(prefix, number)| {
            !prefix.is_empty()
                && !number.is_empty()
                && number.bytes().all(|byte| byte.is_ascii_digit())
                && number.parse::<i32>().is_ok_and(|number| number > 0)
        })
    {
        return Err(error(StatusCode::NOT_FOUND, "issue not found"));
    }
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to get issue");
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let id: Uuid = load_issue(&db, ws, &raw).await?.get("id");
    let tx = db
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(|_| fail())?;
    tx.batch_execute("SET LOCAL statement_timeout = 15000")
        .await
        .map_err(|_| fail())?;
    // Resolve again inside the snapshot: deletion between loading the identifier
    // and opening the transaction must not produce a stale successful response.
    let rows = tx
        .query(
            &format!("{ISSUE_SELECT} WHERE i.workspace_id=$1 AND i.id=$2"),
            &[&ws, &id],
        )
        .await
        .map_err(|_| fail())?;
    if rows.is_empty() {
        return Err(error(StatusCode::NOT_FOUND, "issue not found"));
    }
    let mut value = project_rows(&*tx, ws, &rows)
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list labels"))?
        .remove(0);

    if let Some(context) = queries_sprint_source_context::for_issue(&tx, ws, id).await? {
        value["source_context"] = context;
    }
    let attachments = for_issue(
        &tx,
        ws,
        id,
        state.attachment_urls.as_ref(),
        stable_requested(&headers),
    )
    .await?;
    if !attachments.is_empty() {
        value["attachments"] = json!(attachments);
    }
    let reactions = tx
        .query(
            "SELECT id,issue_id,actor_type,actor_id,emoji,created_at FROM issue_reaction WHERE workspace_id=$1 AND issue_id=$2 ORDER BY created_at ASC",
            &[&ws, &id],
        )
        .await
        .map_err(|_| fail())?;
    if !reactions.is_empty() {
        value["reactions"] = json!(reactions.iter().map(|row| json!({
            "id":row.get::<_,Uuid>("id"), "issue_id":row.get::<_,Uuid>("issue_id"),
            "actor_type":row.get::<_,String>("actor_type"), "actor_id":row.get::<_,Uuid>("actor_id"),
            "emoji":row.get::<_,String>("emoji"), "created_at":timestamp(row.get("created_at")),
        })).collect::<Vec<_>>());
    }
    tx.commit().await.map_err(|_| fail())?;
    Ok(GoJson(value))
}
