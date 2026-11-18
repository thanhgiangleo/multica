//! Post-commit invitation effects; production transport belongs to shared integration.
use serde_json::{Value, json};
use std::{future::Future, pin::Pin};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvitationMail {
    pub recipient: String,
    pub inviter_name: String,
    pub workspace_name: String,
    pub invitation_id: Uuid,
}

/// Implementations own HTML/subject sanitization and provider delivery.
/// Handler invokes this on an independent task only after commit, like frozen Go.
pub trait InvitationMailer: Send + Sync {
    fn send<'a>(
        &'a self,
        mail: InvitationMail,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + 'a>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvitationMetric {
    Sent { invited_email_domain: String },
    Accepted { days_since_invite: i64 },
}

impl InvitationMetric {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Sent { .. } => "team_invite_sent",
            Self::Accepted { .. } => "team_invite_accepted",
        }
    }

    pub fn properties(&self) -> Value {
        match self {
            Self::Sent {
                invited_email_domain,
            } => {
                json!({"invited_email_domain": invited_email_domain, "invite_method": "email"})
            }
            Self::Accepted { days_since_invite } => json!({"days_since_invite": days_since_invite}),
        }
    }
}

/// Records server-side metrics only, not PostHog or raw recipient email.
/// The shared analytics adapter must provide the real counter transport.
pub trait InvitationMetrics: Send + Sync {
    fn record<'a>(
        &'a self,
        user_id: Uuid,
        workspace_id: Uuid,
        event: InvitationMetric,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub(super) fn email_domain(email: &str) -> String {
    email
        .rsplit_once('@')
        .map_or("", |(_, domain)| domain)
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_do_not_expose_recipient() {
        let event = InvitationMetric::Sent {
            invited_email_domain: email_domain("Person@EXAMPLE.TEST"),
        };
        assert_eq!(event.name(), "team_invite_sent");
        assert_eq!(
            event.properties(),
            json!({"invited_email_domain":"example.test","invite_method":"email"})
        );
        assert_eq!(email_domain("missing-at"), "");
        assert_eq!(email_domain("person@"), "");
        let accepted = InvitationMetric::Accepted {
            days_since_invite: 2,
        };
        assert_eq!(accepted.name(), "team_invite_accepted");
        assert_eq!(accepted.properties(), json!({"days_since_invite":2}));
    }
}
