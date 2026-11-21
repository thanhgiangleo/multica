//! Production issue-wakeup dispatcher for the default binary.
//!
//! A committed wakeup receipt is durable but inert until something dispatches
//! the rule. Go's manual-trigger handler therefore nudges the scheduler right
//! after commit — "dispatch now rather than on the next tick; the scheduler
//! retries if this attempt loses a lock race" — and ignores the error because
//! the receipt, not this attempt, is what makes the wakeup happen.
//!
//! [`PgWakeupDispatcher`] is that nudge: a real attempt against the same
//! per-rule dispatch the scheduler tick uses, never a dropped event. The
//! request carries workspace/issue/rule, but the rule id is what the
//! transaction that committed the receipt made authoritative; `dispatch`
//! re-reads the rule and verifies the issue belongs to the rule's workspace.
use crate::api::{IssueWakeupDispatcher, issue_write::IssueWakeupDispatchRequest};
use deadpool_postgres::Pool;
use std::sync::Arc;

pub struct PgWakeupDispatcher {
    pool: Arc<Pool>,
}

impl PgWakeupDispatcher {
    pub fn new(pool: Arc<Pool>) -> Self {
        Self { pool }
    }
}

impl IssueWakeupDispatcher for PgWakeupDispatcher {
    fn request_dispatch(&self, request: IssueWakeupDispatchRequest) {
        let pool = self.pool.clone();
        // The sink is synchronous; the attempt runs on the server runtime. The
        // durable receipt is what guarantees the 30s tick retries, so the
        // response never has to wait for — or fail on — this attempt.
        tokio::spawn(async move {
            if let Err(error) =
                crate::scheduler_issue_wakeup::dispatch_now(&pool, request.wakeup_id).await
            {
                eprintln!(
                    "wakeup dispatch {} for issue {} failed: {error}",
                    request.wakeup_id, request.issue_id
                );
            }
        });
    }
}
