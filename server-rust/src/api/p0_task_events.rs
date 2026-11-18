//! Workspace task-cancellation fanout required by P0 task and agent mutations.
//!
//! Go's TaskService publishes `task:cancelled` on the authenticated workspace
//! bus with a system actor; chat-scoped fanout keeps its owner fence in
//! `runtime_chat_events::publish_chat_audience`. Issue-bound events carry no
//! chat session and are visible to every member of the owning workspace.
use super::{
    issue_write::IssueTaskCancelledEvent,
    runtime_chat_events::{Event, Events},
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

/// Production task fanout, never a no-op: the socket rechecks workspace
/// membership before delivering any frame.
pub trait TaskCancelSink: Send + Sync {
    fn publish_task_cancelled(&self, event: IssueTaskCancelledEvent);
}
pub type SharedTaskCancelSink = Arc<dyn TaskCancelSink>;

impl TaskCancelSink for Events {
    fn publish_task_cancelled(&self, event: IssueTaskCancelledEvent) {
        self.publish(Event {
            event_type: "task:cancelled".into(),
            workspace_id: event.workspace_id,
            actor_type: "system".into(),
            actor_id: Uuid::nil(),
            chat_session_id: None,
            chat_session_owner_id: None,
            payload: json!({
                "task_id": event.task_id,
                "agent_id": event.agent_id,
                "issue_id": event.issue_id.map(|id| id.to_string()).unwrap_or_default(),
                "status": "cancelled",
            }),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn issue_and_quick_cancellation_are_workspace_scoped() {
        let (ws, member, foreign) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let issue = Uuid::new_v4();
        let events = Events::default();
        let mut local = events.subscribe_for(ws, member);
        let mut other = events.subscribe_for(Uuid::new_v4(), foreign);
        TaskCancelSink::publish_task_cancelled(
            &events,
            IssueTaskCancelledEvent {
                workspace_id: ws,
                task_id: Uuid::new_v4(),
                agent_id: Uuid::new_v4(),
                issue_id: Some(issue),
            },
        );
        let event = local.recv().await.unwrap();
        assert_eq!(event.event_type, "task:cancelled");
        assert_eq!(event.actor_type, "system");
        assert_eq!(event.actor_id, Uuid::nil());
        assert_eq!(event.payload["issue_id"], issue.to_string());
        assert_eq!(event.payload["status"], "cancelled");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), other.recv())
                .await
                .is_err()
        );
        // Quick-created tasks have no issue but still have a workspace audience.
        TaskCancelSink::publish_task_cancelled(
            &events,
            IssueTaskCancelledEvent {
                workspace_id: ws,
                task_id: Uuid::new_v4(),
                agent_id: Uuid::new_v4(),
                issue_id: None,
            },
        );
        let event = local.recv().await.unwrap();
        assert_eq!(event.payload["issue_id"], "");
        assert_eq!(event.payload["status"], "cancelled");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), other.recv())
                .await
                .is_err()
        );
    }
}
