//! Local invitation abuse budgets. Redis configuration fails closed until a shared gate is wired.
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use uuid::Uuid;

pub(super) struct Admission {
    requests: Mutex<HashMap<(usize, String), Vec<Instant>>>,
    limits: [usize; 3],
    unavailable: bool,
}

const WINDOWS: [Duration; 3] = [
    Duration::from_secs(600),
    Duration::from_secs(86_400),
    Duration::from_secs(86_400),
];

pub(super) enum Denied {
    Limited(u64),
    Unavailable,
}

impl Default for Admission {
    fn default() -> Self {
        Self {
            requests: Mutex::default(),
            limits: [10, 50, 6],
            unavailable: false,
        }
    }
}

impl Admission {
    pub(super) fn from_env() -> Self {
        let mut admission = Self::default();
        for (index, name) in [
            "RATE_LIMIT_INVITATION_ACTOR_10M",
            "RATE_LIMIT_INVITATION_WORKSPACE_24H",
            "RATE_LIMIT_INVITATION_RECIPIENT_24H",
        ]
        .iter()
        .enumerate()
        {
            if let Ok(value) = std::env::var(name)
                && let Ok(limit) = value.trim().parse::<usize>()
            {
                admission.limits[index] = limit;
            }
        }
        admission.unavailable =
            std::env::var("REDIS_URL").is_ok_and(|value| !value.trim().is_empty());
        admission
    }

    pub(super) fn consume(&self, actor: Uuid, workspace: Uuid, email: &str) -> Result<(), Denied> {
        self.consume_at(actor, workspace, email, Instant::now())
    }

    fn consume_at(
        &self,
        actor: Uuid,
        workspace: Uuid,
        email: &str,
        now: Instant,
    ) -> Result<(), Denied> {
        if self.unavailable {
            return Err(Denied::Unavailable);
        }
        let email = email.trim().to_lowercase();
        let recipient = format!("{:x}", Sha256::digest(email.as_bytes()));
        let keys = [
            (0, actor.to_string()),
            (1, workspace.to_string()),
            (2, recipient),
        ];
        let mut requests = self.requests.lock().map_err(|_| Denied::Unavailable)?;
        // Evict expired keys too: unique recipient addresses must not leave an
        // ever-growing map once their budget window has elapsed.
        requests.retain(|(dimension, _), times| {
            times.retain(|at| now.duration_since(*at) < WINDOWS[*dimension]);
            !times.is_empty()
        });
        let retry = keys
            .iter()
            .filter_map(|key| {
                let limit = self.limits[key.0];
                let times = requests.get(key)?;
                (limit > 0 && times.len() >= limit).then(|| {
                    let remaining = WINDOWS[key.0].saturating_sub(now.duration_since(times[0]));
                    remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0)
                })
            })
            .max();
        if let Some(retry) = retry {
            return Err(Denied::Limited(retry.max(1)));
        }
        // Check every gate before consuming any, so a rejection spends no other
        // dimension. The local mutex also prevents Check-to-Allow overshoot.
        for key in keys {
            if self.limits[key.0] > 0 {
                requests.entry(key).or_default().push(now);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_actor_without_spending_other_budgets() {
        let limiter = Admission::default();
        let actor = Uuid::new_v4();
        let another = Uuid::new_v4();
        let ws = Uuid::new_v4();
        for n in 0..10 {
            assert!(
                limiter
                    .consume(actor, ws, &format!("{n}@example.com"))
                    .is_ok()
            );
        }
        assert!(matches!(
            limiter.consume(actor, ws, "blocked@example.com"),
            Err(Denied::Limited(_))
        ));
        assert!(limiter.consume(another, ws, "blocked@example.com").is_ok());
    }

    #[test]
    fn recipient_is_normalized_and_shared_across_workspaces() {
        let limiter = Admission::default();
        for _ in 0..6 {
            assert!(
                limiter
                    .consume(Uuid::new_v4(), Uuid::new_v4(), " SHARED@example.com ")
                    .is_ok()
            );
        }
        assert!(matches!(
            limiter.consume(Uuid::new_v4(), Uuid::new_v4(), "shared@example.com"),
            Err(Denied::Limited(_))
        ));
        assert!(
            limiter
                .consume(Uuid::new_v4(), Uuid::new_v4(), "different@example.com")
                .is_ok()
        );
    }

    #[test]
    fn workspace_and_expiry_boundaries() {
        let limiter = Admission::default();
        let workspace = Uuid::new_v4();
        let now = Instant::now();
        for n in 0..50 {
            assert!(
                limiter
                    .consume_at(Uuid::new_v4(), workspace, &format!("{n}@example.com"), now)
                    .is_ok()
            );
        }
        assert!(matches!(
            limiter.consume_at(Uuid::new_v4(), workspace, "new@example.com", now),
            Err(Denied::Limited(86400))
        ));
        assert!(
            limiter
                .consume_at(
                    Uuid::new_v4(),
                    workspace,
                    "new@example.com",
                    now + Duration::from_secs(86_400)
                )
                .is_ok()
        );
        assert_eq!(limiter.requests.lock().unwrap().len(), 3);
    }

    #[test]
    fn disabled_gates_and_backend_failure() {
        let limiter = Admission {
            limits: [0, 0, 0],
            ..Admission::default()
        };
        for _ in 0..100 {
            assert!(
                limiter
                    .consume(Uuid::nil(), Uuid::nil(), "same@example.com")
                    .is_ok()
            );
        }
        assert!(limiter.requests.lock().unwrap().is_empty());
        let limiter = Admission {
            unavailable: true,
            ..Admission::default()
        };
        assert!(matches!(
            limiter.consume(Uuid::nil(), Uuid::nil(), "x@example.com"),
            Err(Denied::Unavailable)
        ));
    }
}
