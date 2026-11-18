//! Shared typed event contract for runtime and chat writes. Fanout remains
//! private until a user WebSocket has verified its own workspace and identity.
use super::agents_squads_sprint::{SquadEvent, SquadEventSink};
use super::queries_sprint_inbox::{InboxEvent, InboxEventSink};
use super::queries_sprint_pins::{PinEvent, PinEventSink};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Event {
    pub event_type: String,
    pub workspace_id: Uuid,
    pub actor_type: String,
    pub actor_id: Uuid,
    pub chat_session_id: Option<Uuid>,
    pub chat_session_owner_id: Option<Uuid>,
    pub payload: serde_json::Value,
}

impl Event {
    pub fn project_updated(workspace_id: Uuid, actor_id: Uuid, project: serde_json::Value) -> Self {
        Self {
            event_type: "project:updated".into(),
            workspace_id,
            actor_type: "member".into(),
            actor_id,
            chat_session_id: None,
            chat_session_owner_id: None,
            payload: serde_json::json!({"project": project}),
        }
    }

    pub fn chat_session_read(workspace_id: Uuid, owner: Uuid, session: Uuid) -> Self {
        Self {
            event_type: "chat:session_read".into(),
            workspace_id,
            actor_type: "member".into(),
            actor_id: owner,
            chat_session_id: Some(session),
            chat_session_owner_id: Some(owner),
            payload: serde_json::json!({"chat_session_id":session}),
        }
    }

    pub fn chat_session_updated(
        workspace_id: Uuid,
        owner: Uuid,
        session: Uuid,
        title: &str,
        updated_at: &str,
        project: Option<Option<Uuid>>,
        pinned: Option<bool>,
    ) -> Self {
        let mut payload = serde_json::json!({
            "chat_session_id": session, "title": title, "updated_at": updated_at,
        });
        if let Some(project_id) = project {
            payload["project_id"] = serde_json::json!(project_id);
        }
        if let Some(pinned) = pinned {
            payload["pinned"] = serde_json::json!(pinned);
        }
        Self {
            event_type: "chat:session_updated".into(),
            workspace_id,
            actor_type: "member".into(),
            actor_id: owner,
            chat_session_id: Some(session),
            chat_session_owner_id: Some(owner),
            payload,
        }
    }

    pub fn chat_session_deleted(workspace_id: Uuid, owner: Uuid, session: Uuid) -> Self {
        Self {
            event_type: "chat:session_deleted".into(),
            workspace_id,
            actor_type: "member".into(),
            actor_id: owner,
            chat_session_id: Some(session),
            chat_session_owner_id: Some(owner),
            payload: serde_json::json!({"chat_session_id":session}),
        }
    }

    pub fn chat_session_archived(
        workspace_id: Uuid,
        owner: Uuid,
        session: Uuid,
        title: &str,
        status: &str,
        updated_at: &str,
    ) -> Self {
        let mut event =
            Self::chat_session_updated(workspace_id, owner, session, title, updated_at, None, None);
        event.payload["status"] = serde_json::json!(status);
        event
    }

    pub fn task_cancelled(
        workspace_id: Uuid,
        owner: Uuid,
        session: Uuid,
        task: Uuid,
        agent: Uuid,
    ) -> Self {
        let mut event = Self::task_queued(workspace_id, owner, session, task, agent);
        event.event_type = "task:cancelled".into();
        event.payload["status"] = serde_json::json!("cancelled");
        event
    }

    pub fn task_queued(
        workspace_id: Uuid,
        owner: Uuid,
        session: Uuid,
        task: Uuid,
        agent: Uuid,
    ) -> Self {
        Self {
            event_type: "task:queued".into(),
            workspace_id,
            actor_type: "system".into(),
            actor_id: Uuid::nil(),
            chat_session_id: Some(session),
            chat_session_owner_id: Some(owner),
            payload: serde_json::json!({
                "task_id": task, "agent_id": agent, "issue_id": "",
                "status": "queued", "chat_session_id": session,
            }),
        }
    }

    pub fn chat_message(
        workspace_id: Uuid,
        owner: Uuid,
        session: Uuid,
        message: Uuid,
        task: Uuid,
        content: &str,
        created_at: &str,
    ) -> Self {
        Self {
            event_type: "chat:message".into(),
            workspace_id,
            actor_type: "member".into(),
            actor_id: owner,
            chat_session_id: Some(session),
            chat_session_owner_id: Some(owner),
            payload: serde_json::json!({
                "chat_session_id":session, "message_id":message, "role":"user",
                "content":content, "task_id":task, "created_at":created_at,
            }),
        }
    }

    // A chat event without an owner is dropped, not widened to a workspace.
    pub fn visible_to(&self, workspace: Uuid, user: Uuid) -> bool {
        self.workspace_id == workspace
            && match self.chat_session_id {
                Some(_) => self.chat_session_owner_id == Some(user),
                None => self.chat_session_owner_id.is_none(),
            }
    }
}

#[derive(Clone, Debug)]
struct Delivery {
    event: Event,
    recipient_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub(super) struct Events(broadcast::Sender<Delivery>);

impl Default for Events {
    fn default() -> Self {
        let (sender, _) = broadcast::channel(1024);
        Self(sender)
    }
}

impl Events {
    pub fn publish(&self, event: Event) {
        self.publish_delivery(event, None);
    }

    fn publish_delivery(&self, event: Event, recipient_id: Option<Uuid>) {
        if event.chat_session_id.is_some() && event.chat_session_owner_id.is_none() {
            return;
        }
        let _ = self.0.send(Delivery {
            event,
            recipient_id,
        });
    }

    // Address creator and channel-agent owner separately; preserve the actor and
    // payload. The socket rechecks current DB access before either delivery.
    pub fn publish_chat_audience(
        &self,
        event: Event,
        creator: Uuid,
        guest_history: bool,
        channel: Option<&str>,
        agent_owner: Option<Uuid>,
    ) {
        if event.chat_session_id.is_none() {
            return;
        }
        if !guest_history {
            self.publish(Event {
                chat_session_owner_id: Some(creator),
                ..event.clone()
            });
        }
        if matches!(channel, Some("telegram" | "zalo"))
            && let Some(owner) = agent_owner
            && (guest_history || owner != creator)
        {
            self.publish(Event {
                chat_session_owner_id: Some(owner),
                ..event
            });
        }
    }

    #[allow(dead_code)] // The WebSocket handler will subscribe after authenticating.
    pub fn subscribe_for(&self, workspace: Uuid, user: Uuid) -> ScopedEvents {
        ScopedEvents {
            receiver: self.0.subscribe(),
            workspace,
            user,
        }
    }
}

impl SquadEventSink for Events {
    fn publish(&self, event: SquadEvent) {
        self.publish(Event {
            event_type: event.event_type.into(),
            workspace_id: event.workspace_id,
            actor_type: "member".into(),
            actor_id: event.actor_id,
            chat_session_id: None,
            chat_session_owner_id: None,
            payload: event.payload,
        });
    }
}

impl InboxEventSink for Events {
    fn publish(&self, notice: InboxEvent) {
        self.publish_delivery(
            Event {
                event_type: notice.kind.wire_name().into(),
                workspace_id: notice.workspace_id,
                actor_type: "member".into(),
                actor_id: notice.recipient_id,
                chat_session_id: None,
                chat_session_owner_id: None,
                payload: notice.payload,
            },
            Some(notice.recipient_id),
        );
    }
}

impl PinEventSink for Events {
    fn publish(&self, notice: PinEvent) {
        self.publish_delivery(
            Event {
                event_type: notice.kind.wire_name().into(),
                workspace_id: notice.workspace_id,
                actor_type: "member".into(),
                actor_id: notice.user_id,
                chat_session_id: None,
                chat_session_owner_id: None,
                payload: notice.payload,
            },
            Some(notice.user_id),
        );
    }
}

pub(super) struct ScopedEvents {
    receiver: broadcast::Receiver<Delivery>,
    workspace: Uuid,
    user: Uuid,
}

impl ScopedEvents {
    // A lagged subscriber must reconnect and refetch authoritative DB state.
    #[allow(dead_code)] // Called by the user WebSocket fanout once integrated.
    pub async fn recv(&mut self) -> Result<Event, broadcast::error::RecvError> {
        loop {
            let delivery = self.receiver.recv().await?;
            if delivery.recipient_id.is_none_or(|id| id == self.user)
                && delivery.event.visible_to(self.workspace, self.user)
            {
                return Ok(delivery.event);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn agent_status_is_workspace_scoped_and_preserves_actor() {
        let (workspace, actor, peer) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let events = Events::default();
        let mut own = events.subscribe_for(workspace, actor);
        let mut member = events.subscribe_for(workspace, peer);
        let mut foreign = events.subscribe_for(Uuid::new_v4(), actor);
        let payload = serde_json::json!({"agent":{"id":Uuid::new_v4()}});
        SquadEventSink::publish(
            &events,
            SquadEvent {
                workspace_id: workspace,
                actor_id: actor,
                event_type: "agent:status",
                payload: payload.clone(),
            },
        );
        for subscriber in [&mut own, &mut member] {
            let event = subscriber.recv().await.unwrap();
            assert_eq!(event.event_type, "agent:status");
            assert_eq!(event.actor_type, "member");
            assert_eq!(event.actor_id, actor);
            assert_eq!(event.payload, payload);
            assert_eq!(event.chat_session_id, None);
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), foreign.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn pin_delivery_is_scoped_to_owner_and_workspace() {
        use super::super::queries_sprint_pins::PinEventKind;
        let (workspace, owner, peer) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let events = Events::default();
        let mut own = events.subscribe_for(workspace, owner);
        let mut other_user = events.subscribe_for(workspace, peer);
        let mut other_workspace = events.subscribe_for(Uuid::new_v4(), owner);
        let payload = serde_json::json!({"item_type":"issue", "item_id":Uuid::new_v4()});
        PinEventSink::publish(
            &events,
            PinEvent {
                kind: PinEventKind::Created,
                workspace_id: workspace,
                user_id: owner,
                payload: payload.clone(),
            },
        );
        let received = own.recv().await.unwrap();
        assert_eq!(received.event_type, "pin:created");
        assert_eq!(received.payload, payload);
        assert_eq!(received.actor_id, owner);
        let sentinel =
            Event::project_updated(workspace, peer, serde_json::json!({"id":"sentinel"}));
        events.publish(sentinel.clone());
        assert_eq!(other_user.recv().await.unwrap(), sentinel);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), other_workspace.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn inbox_delivery_does_not_widen_to_workspace_or_chat_audience() {
        use super::super::queries_sprint_inbox::InboxEventKind;
        let (workspace, recipient, peer) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let events = Events::default();
        let mut own = events.subscribe_for(workspace, recipient);
        let mut other_user = events.subscribe_for(workspace, peer);
        let mut other_workspace = events.subscribe_for(Uuid::new_v4(), recipient);
        let payload = serde_json::json!({"recipient_id":recipient,"count":2});
        InboxEventSink::publish(
            &events,
            InboxEvent {
                kind: InboxEventKind::BatchRead,
                workspace_id: workspace,
                recipient_id: recipient,
                payload: payload.clone(),
            },
        );
        let event = own.recv().await.unwrap();
        assert_eq!(event.event_type, "inbox:batch-read");
        assert_eq!(event.payload, payload);
        assert_eq!(event.actor_id, recipient);
        assert_eq!(event.chat_session_id, None);
        let sentinel =
            Event::project_updated(workspace, peer, serde_json::json!({"id":"sentinel"}));
        events.publish(sentinel.clone());
        assert_eq!(other_user.recv().await.unwrap(), sentinel);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), other_workspace.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn project_update_reaches_workspace_members_only() {
        let ws = Uuid::new_v4();
        let actor = Uuid::new_v4();
        let member = Uuid::new_v4();
        let project = serde_json::json!({"id": Uuid::new_v4()});
        let events = Events::default();
        let mut scoped = events.subscribe_for(ws, member);
        let event = Event::project_updated(ws, actor, project.clone());
        assert_eq!(event.event_type, "project:updated");
        assert_eq!(event.actor_type, "member");
        assert_eq!(event.actor_id, actor);
        assert_eq!(event.payload, serde_json::json!({"project": project}));
        assert!(event.visible_to(ws, member));
        assert!(!event.visible_to(Uuid::new_v4(), actor));
        events.publish(event.clone());
        assert_eq!(scoped.recv().await.unwrap(), event);
    }

    #[test]
    fn updated_payload_preserves_omitted_vs_null_project_and_pin() {
        let (workspace, user, session) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let renamed =
            Event::chat_session_updated(workspace, user, session, "new", "now", None, None);
        assert!(renamed.payload.get("project_id").is_none());
        assert!(renamed.payload.get("pinned").is_none());
        let unassigned =
            Event::chat_session_updated(workspace, user, session, "new", "now", Some(None), None);
        assert!(unassigned.payload.get("project_id").unwrap().is_null());
        let pinned =
            Event::chat_session_updated(workspace, user, session, "new", "now", None, Some(false));
        assert_eq!(pinned.payload["pinned"], false);
        assert_eq!(pinned.event_type, "chat:session_updated");
    }

    #[test]
    fn archived_and_cancelled_events_have_owner_scope_and_go_payloads() {
        let (workspace, owner, other, session, task, agent) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let archived =
            Event::chat_session_archived(workspace, owner, session, "title", "archived", "now");
        assert_eq!(archived.event_type, "chat:session_updated");
        assert_eq!(archived.payload["status"], "archived");
        assert_eq!(archived.payload["title"], "title");
        assert!(archived.payload.get("project_id").is_none());
        let cancelled = Event::task_cancelled(workspace, owner, session, task, agent);
        assert_eq!(cancelled.event_type, "task:cancelled");
        assert_eq!(cancelled.payload["status"], "cancelled");
        assert_eq!(cancelled.payload["chat_session_id"], session.to_string());
        for event in [archived, cancelled] {
            assert!(event.visible_to(workspace, owner));
            assert!(!event.visible_to(workspace, other));
            assert!(!event.visible_to(Uuid::new_v4(), owner));
        }
    }

    #[tokio::test]
    async fn queued_event_is_visible_only_to_its_session_owner() {
        let (ws, other_ws, owner, other, session, task, agent) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let events = Events::default();
        let mut own = events.subscribe_for(ws, owner);
        let mut foreign = events.subscribe_for(other_ws, owner);
        let mut unrelated = events.subscribe_for(ws, other);
        let event = Event::task_queued(ws, owner, session, task, agent);
        events.publish(event.clone());
        assert_eq!(own.recv().await.unwrap(), event);
        assert!(!event.visible_to(other_ws, owner));
        assert!(!event.visible_to(ws, other));
        assert_eq!(event.event_type, "task:queued");
        assert_eq!(
            event.payload,
            serde_json::json!({
                "task_id":task,"agent_id":agent,"issue_id":"", "status":"queued",
                "chat_session_id":session,
            })
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), foreign.recv())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), unrelated.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn channel_chat_has_creator_and_owner_snapshots_without_rewriting_actor() {
        let (ws, creator, owner, other, session, task, agent) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let events = Events::default();
        let mut creator_events = events.subscribe_for(ws, creator);
        let mut owner_events = events.subscribe_for(ws, owner);
        let mut other_events = events.subscribe_for(ws, other);
        let event = Event::task_queued(ws, creator, session, task, agent);
        events.publish_chat_audience(event.clone(), creator, false, Some("telegram"), Some(owner));
        for receiver in [&mut creator_events, &mut owner_events] {
            let delivered = receiver.recv().await.unwrap();
            assert_eq!(delivered.payload, event.payload);
            assert_eq!(delivered.actor_id, event.actor_id);
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), other_events.recv())
                .await
                .is_err()
        );
        events.publish_chat_audience(event.clone(), creator, true, Some("zalo"), Some(owner));
        assert_eq!(
            owner_events.recv().await.unwrap().chat_session_owner_id,
            Some(owner)
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), creator_events.recv())
                .await
                .is_err()
        );
        events.publish_chat_audience(event, creator, false, None, Some(owner));
        assert_eq!(
            creator_events.recv().await.unwrap().chat_session_owner_id,
            Some(creator)
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), owner_events.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn chat_events_are_owner_scoped_and_never_widen() {
        let (ws, other_ws, owner, other, session) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let events = Events::default();
        let mut owner_events = events.subscribe_for(ws, owner);
        let mut foreign_events = events.subscribe_for(other_ws, owner);
        let mut other_user_events = events.subscribe_for(ws, other);
        let event = Event::chat_session_read(ws, owner, session);
        events.publish(event.clone());
        assert_eq!(owner_events.recv().await.unwrap(), event);
        let other_ws_event = Event::chat_session_read(other_ws, owner, Uuid::new_v4());
        let other_user_event = Event::chat_session_read(ws, other, Uuid::new_v4());
        events.publish(other_ws_event.clone());
        events.publish(other_user_event.clone());
        assert_eq!(foreign_events.recv().await.unwrap(), other_ws_event);
        assert_eq!(other_user_events.recv().await.unwrap(), other_user_event);
        assert_eq!(
            owner_events.receiver.try_recv().unwrap().event,
            other_ws_event
        );
        assert_eq!(
            owner_events.receiver.try_recv().unwrap().event,
            other_user_event
        );
        assert!(!event.visible_to(other_ws, owner));
        assert!(!event.visible_to(ws, other));
        assert_eq!(
            event.payload,
            serde_json::json!({"chat_session_id":session})
        );
        events.publish(Event {
            chat_session_owner_id: None,
            ..event
        });
        assert!(owner_events.receiver.try_recv().is_err());
    }
}
