//! Comment-triggered task admission under the issue write lock.
use super::super::{ApiError, error};
use axum::http::StatusCode;
use tokio_postgres::Transaction;
use uuid::Uuid;

pub(super) async fn has_no_action(
    tx: &Transaction<'_>,
    issue: Uuid,
    agent: Uuid,
    task: Uuid,
) -> Result<bool, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create comment",
        )
    };
    // Go treats a failed no_action read as advisory: it logs and still creates
    // the reply. SAVEPOINT prevents a PostgreSQL statement error from poisoning
    // the surrounding reply + attachment transaction.
    tx.batch_execute("SAVEPOINT issue_write_no_action")
        .await
        .map_err(|_| fail())?;
    let result = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM activity_log WHERE issue_id=$1 AND actor_type='agent' AND actor_id=$2 AND action='squad_leader_evaluated' AND details->>'outcome'='no_action' AND details->>'task_id'=$3::text)",
            &[&issue, &agent, &task.to_string()],
        )
        .await;
    match result {
        Ok(row) => {
            tx.batch_execute("RELEASE SAVEPOINT issue_write_no_action")
                .await
                .map_err(|_| fail())?;
            Ok(row.get(0))
        }
        Err(err) => {
            tx.batch_execute(
                "ROLLBACK TO SAVEPOINT issue_write_no_action; RELEASE SAVEPOINT issue_write_no_action",
            )
            .await
            .map_err(|_| fail())?;
            eprintln!("checking squad leader no_action evaluation failed for task {task}: {err}");
            Ok(false)
        }
    }
}
