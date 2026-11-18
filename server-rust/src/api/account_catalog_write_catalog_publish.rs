//! Post-commit adapter to the shared workspace publisher, not a second event bus.
//! Catalog handlers build these payloads from DB responses, never request bodies.
use std::{future::Future, pin::Pin};

use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use super::ApiState;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StatusAction {
    Created,
    Updated,
    Archived,
    Reordered,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WorkflowAction {
    Created,
    Updated,
    Deleted,
}

/// Wire payloads match frozen Go. Status events invalidate the whole catalog;
/// they deliberately do not carry a possibly stale status snapshot.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum CatalogChange {
    LabelCreated {
        label: Value,
    },
    LabelUpdated {
        label: Value,
    },
    LabelDeleted {
        label_id: Uuid,
    },
    PropertyCreated {
        property: Value,
    },
    PropertyUpdated {
        property: Value,
    },
    StatusChanged {
        action: StatusAction,
    },
    WorkflowChanged {
        action: WorkflowAction,
    },
    ResourceCreated {
        resource: Value,
        project_id: Uuid,
    },
    ResourceUpdated {
        resource: Value,
        project_id: Uuid,
    },
    ResourceDeleted {
        project_id: Uuid,
        resource_id: Uuid,
    },
    ProjectCreated {
        project: Value,
    },
    ProjectUpdated {
        project: Value,
    },
    ProjectDeleted {
        project_id: Uuid,
    },
    RuntimeProfileChanged {
        runtime_profile_id: Uuid,
    },
    RuntimeProfileDeleted {
        deleted_runtime_profile_id: Uuid,
    },
    WorkspaceUpdated {
        workspace: Value,
    },
    WorkspaceDeleted {
        workspace_id: Uuid,
    },
    MemberUpdated {
        member: Value,
    },
    MemberRemoved {
        member_id: Uuid,
        workspace_id: Uuid,
        user_id: Uuid,
    },
    InvitationCreated {
        invitation: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        workspace_name: Option<String>,
    },
    MemberAdded {
        member: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        workspace_name: Option<String>,
    },
    InvitationAccepted {
        invitation_id: Uuid,
        member: Value,
    },
    InvitationDeclined {
        invitation_id: Uuid,
        invitee_email: String,
    },
    InvitationRevoked {
        invitation_id: Uuid,
        invitee_email: String,
        invitee_user_id: Option<Uuid>,
    },
}

impl CatalogChange {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::LabelCreated { .. } => "label:created",
            Self::LabelUpdated { .. } => "label:updated",
            Self::LabelDeleted { .. } => "label:deleted",
            Self::PropertyCreated { .. } => "property:created",
            Self::PropertyUpdated { .. } => "property:updated",
            Self::StatusChanged { .. } => "issue_status:changed",
            Self::WorkflowChanged { .. } => "issue_workflow:changed",
            Self::ResourceCreated { .. } => "project_resource:created",
            Self::ResourceUpdated { .. } => "project_resource:updated",
            Self::ResourceDeleted { .. } => "project_resource:deleted",
            Self::ProjectCreated { .. } => "project:created",
            Self::ProjectUpdated { .. } => "project:updated",
            Self::ProjectDeleted { .. } => "project:deleted",
            Self::RuntimeProfileChanged { .. } | Self::RuntimeProfileDeleted { .. } => {
                "daemon:register"
            }
            Self::WorkspaceUpdated { .. } => "workspace:updated",
            Self::WorkspaceDeleted { .. } => "workspace:deleted",
            Self::MemberUpdated { .. } => "member:updated",
            Self::MemberRemoved { .. } => "member:removed",
            Self::InvitationCreated { .. } => "invitation:created",
            Self::MemberAdded { .. } => "member:added",
            Self::InvitationAccepted { .. } => "invitation:accepted",
            Self::InvitationDeclined { .. } => "invitation:declined",
            Self::InvitationRevoked { .. } => "invitation:revoked",
        }
    }
}

#[derive(Clone, Debug)]
pub struct CatalogPublication {
    pub workspace_id: Uuid,
    /// Go uses actor_type="member", user ID even for a task-authenticated label/resource write.
    /// Trigger-local actor/task attribution is separate and must not be replaced by this value.
    pub actor_user_id: Uuid,
    pub change: CatalogChange,
}

/// Install only an adapter backed by the existing shared publisher. Delivery is
/// awaited after commit. Like Go, a delivery failure must not make a committed
/// mutation look like a retriable HTTP error. Recording adapters are test-only.
pub trait CatalogPublisher: Send + Sync {
    /// Close the departed member's workspace sockets through the shared hub.
    /// Required before cache invalidation and member:removed publication.
    fn disconnect_member<'a>(
        &'a self,
        workspace: Uuid,
        user: Uuid,
        member: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    /// Invalidate the shared membership cache before publishing membership changes.
    /// Workspace deletion also evicts its member snapshot BEFORE teardown, matching
    /// Go; rollback may leave harmless cache misses but must not publish deletion.
    /// Required: an event-only adapter must not silently ignore role revocation.
    fn invalidate_membership<'a>(
        &'a self,
        workspace: Uuid,
        user: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    /// Notify each user's daemon connections through the existing runtime bridge.
    /// This is not a workspace broadcast and must not contain workspace settings.
    fn notify_daemon_workspaces_changed<'a>(
        &'a self,
        users: Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    /// Tell workspace daemons to reconcile this profile through the shared runtime
    /// bridge. This is NOT a broadcast of profile configuration or a task enqueue.
    /// Awaited before daemon:register, after the profile transaction releases its connection.
    fn notify_runtime_profiles_changed<'a>(
        &'a self,
        workspace: Uuid,
        profile: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    fn publish<'a>(
        &'a self,
        publication: CatalogPublication,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub(super) async fn publish(state: &ApiState, workspace: Uuid, user: Uuid, change: CatalogChange) {
    // Absence preserves the pre-integration API; it is NOT full event parity.
    if let Some(publisher) = &state.catalog_publisher {
        publisher
            .publish(CatalogPublication {
                workspace_id: workspace,
                actor_user_id: user,
                change,
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn frozen_go_event_names_and_payloads() {
        let id = Uuid::nil();
        let cases = [
            (
                CatalogChange::RuntimeProfileChanged {
                    runtime_profile_id: id,
                },
                "daemon:register",
                json!({"runtime_profile_id":id}),
            ),
            (
                CatalogChange::RuntimeProfileDeleted {
                    deleted_runtime_profile_id: id,
                },
                "daemon:register",
                json!({"deleted_runtime_profile_id":id}),
            ),
            (
                CatalogChange::WorkspaceDeleted { workspace_id: id },
                "workspace:deleted",
                json!({"workspace_id":id}),
            ),
            (
                CatalogChange::LabelCreated {
                    label: json!({"id":id}),
                },
                "label:created",
                json!({"label":{"id":id}}),
            ),
            (
                CatalogChange::LabelUpdated {
                    label: json!({"id":id}),
                },
                "label:updated",
                json!({"label":{"id":id}}),
            ),
            (
                CatalogChange::LabelDeleted { label_id: id },
                "label:deleted",
                json!({"label_id":id}),
            ),
            (
                CatalogChange::PropertyCreated {
                    property: json!({"id":id}),
                },
                "property:created",
                json!({"property":{"id":id}}),
            ),
            (
                CatalogChange::PropertyUpdated {
                    property: json!({"id":id}),
                },
                "property:updated",
                json!({"property":{"id":id}}),
            ),
            (
                CatalogChange::ResourceCreated {
                    resource: json!({"id":id}),
                    project_id: id,
                },
                "project_resource:created",
                json!({"resource":{"id":id},"project_id":id}),
            ),
            (
                CatalogChange::ResourceUpdated {
                    resource: json!({"id":id}),
                    project_id: id,
                },
                "project_resource:updated",
                json!({"resource":{"id":id},"project_id":id}),
            ),
            (
                CatalogChange::ResourceDeleted {
                    project_id: id,
                    resource_id: id,
                },
                "project_resource:deleted",
                json!({"project_id":id,"resource_id":id}),
            ),
            (
                CatalogChange::ProjectCreated {
                    project: json!({"id":id}),
                },
                "project:created",
                json!({"project":{"id":id}}),
            ),
            (
                CatalogChange::ProjectUpdated {
                    project: json!({"id":id}),
                },
                "project:updated",
                json!({"project":{"id":id}}),
            ),
            (
                CatalogChange::ProjectDeleted { project_id: id },
                "project:deleted",
                json!({"project_id":id}),
            ),
            (
                CatalogChange::WorkspaceUpdated {
                    workspace: json!({"id":id}),
                },
                "workspace:updated",
                json!({"workspace":{"id":id}}),
            ),
            (
                CatalogChange::MemberUpdated {
                    member: json!({"id":id}),
                },
                "member:updated",
                json!({"member":{"id":id}}),
            ),
            (
                CatalogChange::MemberRemoved {
                    member_id: id,
                    workspace_id: id,
                    user_id: id,
                },
                "member:removed",
                json!({"member_id":id,"workspace_id":id,"user_id":id}),
            ),
            (
                CatalogChange::InvitationCreated {
                    invitation: json!({"id":id}),
                    workspace_name: Some("Team".into()),
                },
                "invitation:created",
                json!({"invitation":{"id":id},"workspace_name":"Team"}),
            ),
            (
                CatalogChange::MemberAdded {
                    member: json!({"id":id}),
                    workspace_name: None,
                },
                "member:added",
                json!({"member":{"id":id}}),
            ),
            (
                CatalogChange::InvitationAccepted {
                    invitation_id: id,
                    member: json!({"id":id}),
                },
                "invitation:accepted",
                json!({"invitation_id":id,"member":{"id":id}}),
            ),
            (
                CatalogChange::InvitationDeclined {
                    invitation_id: id,
                    invitee_email: "recipient@invalid.example".into(),
                },
                "invitation:declined",
                json!({"invitation_id":id,"invitee_email":"recipient@invalid.example"}),
            ),
            (
                CatalogChange::InvitationRevoked {
                    invitation_id: id,
                    invitee_email: "recipient@invalid.example".into(),
                    invitee_user_id: None,
                },
                "invitation:revoked",
                json!({"invitation_id":id,"invitee_email":"recipient@invalid.example","invitee_user_id":null}),
            ),
        ];
        for (change, name, payload) in cases {
            assert_eq!(change.event_type(), name);
            assert_eq!(serde_json::to_value(change).unwrap(), payload);
        }
        for (action, name) in [
            (StatusAction::Created, "created"),
            (StatusAction::Updated, "updated"),
            (StatusAction::Archived, "archived"),
            (StatusAction::Reordered, "reordered"),
        ] {
            let change = CatalogChange::StatusChanged { action };
            assert_eq!(change.event_type(), "issue_status:changed");
            assert_eq!(
                serde_json::to_value(change).unwrap(),
                json!({"action":name})
            );
        }
    }
}
