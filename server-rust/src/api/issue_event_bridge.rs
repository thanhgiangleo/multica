//! Local issue fanout shares the authenticated chat WebSocket hub. Dispatch is
//! a required runtime dependency, not an event that may be silently dropped.
use super::{
    issue_write::*,
    runtime_chat_events::{Event, Events},
};
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

pub trait IssueWakeupDispatcher: Send + Sync {
    fn request_dispatch(&self, request: IssueWakeupDispatchRequest);
}

pub(super) struct IssueEvents {
    events: Events,
    dispatcher: Arc<dyn IssueWakeupDispatcher>,
}

impl IssueEvents {
    pub(super) fn new(events: Events, dispatcher: Arc<dyn IssueWakeupDispatcher>) -> Self {
        Self { events, dispatcher }
    }

    fn publish(&self, kind: &str, workspace: Uuid, actor_type: &str, actor: Uuid, payload: Value) {
        self.events.publish(Event {
            event_type: kind.into(),
            workspace_id: workspace,
            actor_type: actor_type.into(),
            actor_id: actor,
            chat_session_id: None,
            chat_session_owner_id: None,
            payload,
        });
    }
}

impl IssueEventSink for IssueEvents {
    fn publish_issue_deleted(&self, event: IssueDeletedEvent) {
        self.publish(
            "issue:deleted",
            event.workspace_id,
            event.actor_type,
            event.actor_id,
            json!({"issue_id": event.issue_id}),
        );
    }

    fn publish_issue_created(&self, event: IssueCreatedEvent) {
        self.publish(
            "issue:created",
            event.workspace_id,
            event.actor_type,
            event.actor_id,
            event.payload,
        );
    }

    fn publish_issue_updated(&self, event: IssueUpdatedEvent) {
        self.publish(
            "issue:updated",
            event.workspace_id,
            event.actor_type,
            event.actor_id,
            event.payload,
        );
    }

    fn publish_metadata_changed(&self, event: IssueMetadataEvent) {
        self.publish("issue_metadata:changed", event.workspace_id, event.actor_type, event.actor_id,
                     json!({"issue_id": event.issue_id, "metadata": event.metadata, "issue_revision": event.issue_revision}));
    }

    fn publish_comment_created(&self, event: IssueCommentCreatedEvent) {
        self.publish(
            "comment:created",
            event.workspace_id,
            event.actor_type,
            event.actor_id,
            event.payload,
        );
    }

    fn publish_comment_changed(&self, event: IssueCommentChangedEvent) {
        self.publish(
            event.event_type,
            event.workspace_id,
            event.actor_type,
            event.actor_id,
            event.payload,
        );
    }

    fn publish_reaction_changed(&self, event: IssueReactionEvent) {
        self.publish(
            event.event_type,
            event.workspace_id,
            event.actor_type,
            event.actor_id,
            event.payload,
        );
    }

    fn publish_assignment_changed(&self, event: IssueAssignmentEvent) {
        self.publish(
            event.event_type,
            event.workspace_id,
            event.actor_type,
            event.actor_id,
            event.payload,
        );
    }

    fn publish_subscriber_changed(&self, event: IssueSubscriberEvent) {
        self.publish(
            event.event_type,
            event.workspace_id,
            event.actor_type,
            event.actor_id,
            event.payload,
        );
    }

    fn publish_comment_resolution(&self, event: IssueCommentResolutionEvent) {
        self.publish(
            event.event_type,
            event.workspace_id,
            event.actor_type,
            event.actor_id,
            event.payload,
        );
    }

    fn publish_wakeup_activity(&self, event: IssueWakeupActivityEvent) {
        // Check-in attribution is carried by the persisted activity entry.
        let actor_type = event.payload["entry"]["actor_type"]
            .as_str()
            .unwrap_or("agent");
        self.publish(
            "activity:created",
            event.workspace_id,
            actor_type,
            event.actor_id,
            event.payload.clone(),
        );
    }

    fn publish_task_cancelled(&self, event: IssueTaskCancelledEvent) {
        // Go's taskEvent always carries issue_id and empty-string encodes a
        // quick-created task; only an empty workspace drops the event. Chat
        // task fanout keeps its owner fence in `publish_chat_audience`.
        self.publish("task:cancelled", event.workspace_id, "system", Uuid::nil(),
                     json!({"task_id":event.task_id,"agent_id":event.agent_id,"issue_id":event.issue_id.map(|id| id.to_string()).unwrap_or_default(),"status":"cancelled"}));
    }

    fn request_wakeup_dispatch(&self, request: IssueWakeupDispatchRequest) {
        self.dispatcher.request_dispatch(request);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Default)]
    struct Dispatcher(Mutex<Vec<IssueWakeupDispatchRequest>>);
    impl IssueWakeupDispatcher for Dispatcher {
        fn request_dispatch(&self, request: IssueWakeupDispatchRequest) {
            self.0.lock().unwrap().push(request);
        }
    }

    #[tokio::test]
    async fn metadata_delete_and_cancellation_use_shared_workspace_hub() {
        let (ws, actor, issue) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let events = Events::default();
        let mut member = events.subscribe_for(ws, Uuid::new_v4());
        let mut foreign = events.subscribe_for(Uuid::new_v4(), actor);
        let dispatcher = Arc::new(Dispatcher::default());
        let sink = IssueEvents::new(events.clone(), dispatcher.clone());
        sink.publish_metadata_changed(IssueMetadataEvent {
            workspace_id: ws,
            actor_type: "agent",
            actor_id: actor,
            issue_id: issue,
            metadata: json!({"ready":true}),
            issue_revision: 7,
        });
        let event = member.recv().await.unwrap();
        assert_eq!(event.event_type, "issue_metadata:changed");
        assert_eq!(event.actor_type, "agent");
        assert_eq!(event.actor_id, actor);
        assert_eq!(
            event.payload,
            json!({"issue_id":issue,"metadata":{"ready":true},"issue_revision":7})
        );
        assert_eq!(event.chat_session_id, None);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), foreign.recv())
                .await
                .is_err()
        );
        sink.publish_issue_deleted(IssueDeletedEvent {
            workspace_id: ws,
            actor_type: "member",
            actor_id: actor,
            issue_id: issue,
        });
        assert_eq!(
            member.recv().await.unwrap().payload,
            json!({"issue_id":issue})
        );
        let task = Uuid::new_v4();
        sink.publish_task_cancelled(IssueTaskCancelledEvent {
            workspace_id: ws,
            task_id: task,
            agent_id: actor,
            issue_id: Some(issue),
        });
        let event = member.recv().await.unwrap();
        assert_eq!(event.event_type, "task:cancelled");
        assert_eq!(event.actor_type, "system");
        assert_eq!(
            event.payload,
            json!({"task_id":task,"agent_id":actor,"issue_id":issue,"status":"cancelled"})
        );
        // A quick-created task has no issue but still has a workspace audience,
        // and Go encodes the missing issue as an empty string.
        sink.publish_task_cancelled(IssueTaskCancelledEvent {
            workspace_id: ws,
            task_id: task,
            agent_id: actor,
            issue_id: None,
        });
        let event = member.recv().await.unwrap();
        assert_eq!(event.event_type, "task:cancelled");
        assert_eq!(event.payload["issue_id"], "");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), foreign.recv())
                .await
                .is_err()
        );
        let request = IssueWakeupDispatchRequest {
            workspace_id: ws,
            issue_id: issue,
            wakeup_id: Uuid::new_v4(),
        };
        sink.request_wakeup_dispatch(request.clone());
        assert_eq!(*dispatcher.0.lock().unwrap(), vec![request]);
    }
}
