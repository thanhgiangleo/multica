//! Workspace deletion on self-hosted databases. External teardown awaits integration.
use super::account_catalog_write_catalog_publish::{self as events, CatalogChange};
use super::{Actor, ApiState, GoJson, Identity};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::delete,
};
use serde_json::{Value, json};
use tokio_postgres::{Transaction, error::SqlState};
use uuid::Uuid;

#[path = "account_catalog_write_workspace_delete_sql.rs"]
mod sql;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/workspaces/{id}", delete(remove))
        .route("/api/workspaces/{id}/", delete(remove))
}

struct Failure(StatusCode, Value);
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let mut response = GoJson(self.1).into_response();
        *response.status_mut() = self.0;
        response
    }
}
fn fail(status: StatusCode, message: &'static str) -> Failure {
    Failure(status, json!({"error": message}))
}
fn teardown_error(err: tokio_postgres::Error) -> Failure {
    if matches!(
        err.code(),
        Some(&SqlState::LOCK_NOT_AVAILABLE | &SqlState::T_R_DEADLOCK_DETECTED)
    ) {
        fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "workspace deletion is temporarily blocked by another operation, please try again",
        )
    } else {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to delete workspace",
        )
    }
}

async fn remove(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_ws): Path<String>,
) -> Result<StatusCode, Failure> {
    if let Some(bound) = actor.workspace()
        && raw_ws != bound.to_string()
    {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "task token is bound to a different workspace",
        ));
    }
    let ws = Uuid::parse_str(&raw_ws)
        .map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid workspace id"))?;
    let db = state.pool.get().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to delete workspace",
        )
    })?;
    // Go evicts cached memberships before starting the delete transaction.
    // Never evict on a credential, actor or role rejection. Do not hold the
    // database connection while invoking an adapter that may query the pool.
    let requester = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(teardown_error)?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    if requester.get::<_, String>(0) != "owner" {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "insufficient workspace permissions",
        ));
    }
    if state.cloud.is_some() {
        return Err(fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "workspace capacity release integration unavailable",
        ));
    }
    let affected_users: Vec<Uuid> = db
        .query(
            "SELECT user_id FROM member WHERE workspace_id=$1 ORDER BY user_id",
            &[&ws],
        )
        .await
        .map_err(teardown_error)?
        .iter()
        .map(|row| row.get(0))
        .collect();
    drop(db);
    if let Some(publisher) = &state.catalog_publisher {
        for member in &affected_users {
            publisher.invalidate_membership(ws, *member).await;
        }
    }

    let mut db = state.pool.get().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to delete workspace",
        )
    })?;
    let tx = db.transaction().await.map_err(teardown_error)?;
    // Re-check after locking: an owner may have been demoted or removed while
    // the pre-teardown membership snapshot and invalidations were in flight.
    let requester = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(teardown_error)?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    if requester.get::<_, String>(0) != "owner" {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    tx.batch_execute("SET LOCAL lock_timeout='10s'")
        .await
        .map_err(teardown_error)?;
    if tx
        .query_opt("SELECT id FROM workspace WHERE id=$1 FOR UPDATE", &[&ws])
        .await
        .map_err(teardown_error)?
        .is_none()
    {
        return Err(fail(StatusCode::NOT_FOUND, "workspace not found"));
    }
    let still_owner: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 AND role='owner')", &[&ws,&user])
        .await.map_err(teardown_error)?.get(0);
    if !still_owner {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }

    // The Go Qoder preparer, live daemon notifications, object-store deletion,
    // and channel-media reconciler are not in this child. Fence their writers
    // while rejecting affected workspaces rather than dropping durable journals.
    tx.batch_execute("LOCK TABLE agent_runtime, cloud_agent_connection, issue_source_context_object_intent, channel_media_pending_object IN SHARE MODE")
        .await.map_err(teardown_error)?;
    let unsupported: bool = tx.query_one(
        "SELECT EXISTS(SELECT 1 FROM agent_runtime WHERE workspace_id=$1) \
         OR EXISTS(SELECT 1 FROM cloud_agent_connection WHERE workspace_id=$1) \
         OR EXISTS(SELECT 1 FROM issue_source_context_object_intent WHERE workspace_id=$1) \
         OR EXISTS(SELECT 1 FROM attachment WHERE workspace_id=$1 AND source_context_id IS NOT NULL) \
         OR EXISTS(SELECT 1 FROM channel_media_pending_object WHERE workspace_id=$1)", &[&ws],
    ).await.map_err(teardown_error)?.get(0);
    if unsupported {
        return Err(fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "workspace external teardown integration unavailable",
        ));
    }
    // Go's lock order: workspace, sessions, task owners, relationship graph,
    // global rollup lock, task pages, then bottom-up relationship teardown.
    tx.execute(
        "SELECT id FROM chat_session WHERE workspace_id=$1 FOR UPDATE",
        &[&ws],
    )
    .await
    .map_err(teardown_error)?;
    tx.query_one(
        "SELECT set_config('multica.workspace_teardown','on',true)",
        &[],
    )
    .await
    .map_err(teardown_error)?;
    for table in ["agent", "issue", "agent_runtime"] {
        let statement = match table {
            "agent" => "SELECT 1 FROM agent WHERE workspace_id=$1 FOR UPDATE",
            "issue" => "SELECT 1 FROM issue WHERE workspace_id=$1 FOR UPDATE",
            _ => "SELECT 1 FROM agent_runtime WHERE workspace_id=$1 FOR UPDATE",
        };
        tx.execute(statement, &[&ws])
            .await
            .map_err(teardown_error)?;
    }
    tx.execute(sql::PREPARE_WORKSPACE_DELETION_LINKS, &[&ws])
        .await
        .map_err(teardown_error)?;
    tx.execute(sql::DELETE_CHAT_PINNED_AGENTS_BY_WORKSPACE, &[&ws])
        .await
        .map_err(teardown_error)?;
    tx.query_one("SELECT pg_advisory_xact_lock(4246)", &[])
        .await
        .map_err(teardown_error)?;
    delete_tasks(&tx, ws).await?;
    for (name, statement) in sql::STEPS {
        tx.execute(*statement, &[&ws]).await.map_err(|err| {
            eprintln!("workspace teardown step {name} failed: {err}");
            teardown_error(err)
        })?;
    }
    tx.commit().await.map_err(teardown_error)?;
    drop(db);
    events::publish(
        &state,
        ws,
        user,
        CatalogChange::WorkspaceDeleted { workspace_id: ws },
    )
    .await;
    if let Some(publisher) = &state.catalog_publisher {
        publisher
            .notify_daemon_workspaces_changed(affected_users)
            .await;
    }
    Ok(StatusCode::NO_CONTENT)
}

// Same bounded, indexed keyset walk as frozen Go: each task is discovered by
// agent, issue, and runtime ownership, not just its workspace-less task row.
async fn delete_tasks(tx: &Transaction<'_>, ws: Uuid) -> Result<(), Failure> {
    for (owners, column) in [
        ("agent", "agent_id"),
        ("issue", "issue_id"),
        ("agent_runtime", "runtime_id"),
    ] {
        let owner_first = match owners {
            "agent" => "SELECT id FROM agent WHERE workspace_id=$1 ORDER BY id LIMIT 500",
            "issue" => "SELECT id FROM issue WHERE workspace_id=$1 ORDER BY id LIMIT 500",
            _ => "SELECT id FROM agent_runtime WHERE workspace_id=$1 ORDER BY id LIMIT 500",
        };
        let owner_next = match owners {
            "agent" => "SELECT id FROM agent WHERE workspace_id=$1 AND id>$2 ORDER BY id LIMIT 500",
            "issue" => "SELECT id FROM issue WHERE workspace_id=$1 AND id>$2 ORDER BY id LIMIT 500",
            _ => {
                "SELECT id FROM agent_runtime WHERE workspace_id=$1 AND id>$2 ORDER BY id LIMIT 500"
            }
        };
        let task_first = match column {
            "agent_id" => {
                "SELECT id,status FROM agent_task_queue WHERE agent_id=$1 ORDER BY id LIMIT 1000 FOR UPDATE"
            }
            "issue_id" => {
                "SELECT id,status FROM agent_task_queue WHERE issue_id=$1 ORDER BY id LIMIT 1000 FOR UPDATE"
            }
            _ => {
                "SELECT id,status FROM agent_task_queue WHERE runtime_id=$1 ORDER BY id LIMIT 1000 FOR UPDATE"
            }
        };
        let task_next = match column {
            "agent_id" => {
                "SELECT id,status FROM agent_task_queue WHERE agent_id=$1 AND id>$2 ORDER BY id LIMIT 1000 FOR UPDATE"
            }
            "issue_id" => {
                "SELECT id,status FROM agent_task_queue WHERE issue_id=$1 AND id>$2 ORDER BY id LIMIT 1000 FOR UPDATE"
            }
            _ => {
                "SELECT id,status FROM agent_task_queue WHERE runtime_id=$1 AND id>$2 ORDER BY id LIMIT 1000 FOR UPDATE"
            }
        };
        let mut owner_rows = tx
            .query(owner_first, &[&ws])
            .await
            .map_err(teardown_error)?;
        while !owner_rows.is_empty() {
            for row in &owner_rows {
                let owner: Uuid = row.get(0);
                let mut clean = false;
                for _ in 0..3 {
                    let mut tasks = tx
                        .query(task_first, &[&owner])
                        .await
                        .map_err(teardown_error)?;
                    if tasks.is_empty() {
                        clean = true;
                        break;
                    }
                    while !tasks.is_empty() {
                        let ids: Vec<Uuid> = tasks.iter().map(|task| task.get(0)).collect();
                        // No remote work may be discarded without the runtime
                        // owner first cancelling/settling it transactionally.
                        if tasks.iter().any(|task| {
                            !matches!(
                                task.get::<_, String>(1).as_str(),
                                "completed" | "failed" | "cancelled"
                            )
                        }) {
                            return Err(fail(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "workspace task teardown integration unavailable",
                            ));
                        }
                        tx.execute(sql::DETACH_TASK_BATCH_REFERENCES, &[&ids])
                            .await
                            .map_err(teardown_error)?;
                        tx.execute(sql::DELETE_TASK_BATCH, &[&ids])
                            .await
                            .map_err(teardown_error)?;
                        let cursor = *ids.last().expect("nonempty page");
                        tasks = tx
                            .query(task_next, &[&owner, &cursor])
                            .await
                            .map_err(teardown_error)?;
                    }
                }
                if !clean {
                    return Err(fail(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "workspace task teardown is temporarily blocked",
                    ));
                }
                if column == "agent_id" {
                    tx.execute(sql::DELETE_TASK_TOKENS_BY_AGENT, &[&owner])
                        .await
                        .map_err(teardown_error)?;
                }
            }
            let cursor: Uuid = owner_rows.last().expect("nonempty page").get(0);
            owner_rows = tx
                .query(owner_next, &[&ws, &cursor])
                .await
                .map_err(teardown_error)?;
        }
    }
    Ok(())
}
