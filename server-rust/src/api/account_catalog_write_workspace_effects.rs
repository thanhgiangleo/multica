//! Workspace creation metrics delegate to the shared recorder, never a second event bus.
use serde_json::{Value, json};
use std::{future::Future, pin::Pin};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkspaceCreated {
    pub user_id: Uuid,
    pub workspace_id: Uuid,
}

impl WorkspaceCreated {
    pub fn name(&self) -> &'static str {
        "workspace_created"
    }

    /// Frozen Go carries workspace_id in the envelope, not core properties.
    /// No name, slug, email, questionnaire, or racy first-workspace flag.
    pub fn properties(&self) -> Value {
        json!({"user_id": self.user_id, "source": "manual", "is_demo": false})
    }
}

/// Adapter must increment the existing multica_workspace_created_total{source="manual"}
/// counter only. Frozen Go marks this event metrics-only; never send it to PostHog
/// or use user/workspace IDs as metric labels. Recording is best-effort and must
/// not turn a committed workspace into a retriable HTTP failure.
pub trait WorkspaceMetrics: Send + Sync {
    fn record<'a>(
        &'a self,
        event: WorkspaceCreated,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_go_workspace_metric_has_only_core_properties() {
        let event = WorkspaceCreated {
            user_id: Uuid::from_u128(1),
            workspace_id: Uuid::from_u128(2),
        };
        assert_eq!(event.name(), "workspace_created");
        assert_eq!(
            event.properties(),
            json!({"user_id": event.user_id, "source": "manual", "is_demo": false})
        );
    }
}
