//! Adapter boundary to the shared issue/task/catalog publishers, not a second event bus.
//! Rows are internal inputs, never wire payloads. The integrator must enrich/redact them
//! with the shared snapshot builders and preserve workspace/privacy routing.
use std::{future::Future, pin::Pin};

use serde_json::Value;
use tokio_postgres::Row;
use uuid::Uuid;

use super::account_catalog_write_workflow_remap::RemapEffects;

/// Administrative remaps publish snapshots only: no assignment notifications,
/// subscriber notifications, handoffs, dispatch, or issue status-change history.
pub enum WorkflowPublication {
    IssueSnapshot(Row),
    CancelledWakeup(Row),
    StatusCatalogCreated,
    WorkflowUpdated,
    ProjectUpdated(Value),
}

pub struct WorkflowPublicationContext {
    pub workspace_id: Uuid,
    /// Frozen Go attributes these writes to actor_type="member", actor_id=user ID.
    pub actor_user_id: Uuid,
}

/// Register only an adapter backed by the real shared publishers. No-op adapters
/// are not a supported runtime configuration. Tests may install a recording sink.
/// Delivery is awaited, after commit; Go's publishers are likewise best effort
/// and do not turn a committed mutation into a retriable HTTP failure.
pub trait WorkflowPublisher: Send + Sync {
    fn publish<'a>(
        &'a self,
        context: &'a WorkflowPublicationContext,
        publication: WorkflowPublication,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub(super) async fn publish_remap(
    publisher: &dyn WorkflowPublisher,
    context: &WorkflowPublicationContext,
    effects: RemapEffects,
) {
    for row in effects.changed {
        publisher
            .publish(context, WorkflowPublication::IssueSnapshot(row))
            .await;
    }
    for row in effects.cancelled_wakeups {
        publisher
            .publish(context, WorkflowPublication::CancelledWakeup(row))
            .await;
    }
}
