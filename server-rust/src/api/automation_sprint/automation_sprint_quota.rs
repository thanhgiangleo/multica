//! Workspace quota facts from a trusted policy source and durable counters.
use super::*;
use async_trait::async_trait;
use std::sync::Arc;

/// The entitlement source owns interval construction. Never infer a billing
/// period, plan, or limit from local database state.
#[async_trait]
pub trait AutopilotQuotaPolicySource: Send + Sync + 'static {
    async fn policy(
        &self,
        workspace_id: Uuid,
    ) -> Result<AutopilotQuotaPolicy, AutopilotQuotaPolicyError>;
}

#[derive(Clone, Copy, Debug)]
pub struct AutopilotQuotaPolicyError;

#[derive(Clone, Debug)]
pub struct AutopilotQuotaPolicy {
    pub action: String,
    pub limit: Option<i64>,
    pub period_start: Option<DateTime<Utc>>,
    pub period_end: Option<DateTime<Utc>>,
    pub reset_at: Option<DateTime<Utc>>,
}

impl ApiState {
    pub fn with_autopilot_quota_policy_source(
        mut self,
        source: Arc<dyn AutopilotQuotaPolicySource>,
    ) -> Self {
        self.automation_sprint_quota = Some(source);
        self
    }
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/autopilots/usage", get(usage))
}

fn inactive() -> Value {
    json!({
        "action": "off", "used": null, "reserved": null, "total": null,
        "limit": null, "reached": null, "period_start": null,
        "period_end": null, "reset_at": null, "blocked_counts": null
    })
}

struct QuotaInterval {
    limit: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    reset: DateTime<Utc>,
}

fn valid(policy: &AutopilotQuotaPolicy) -> Option<QuotaInterval> {
    if !matches!(policy.action.as_str(), "enforce" | "observe") {
        return None;
    }
    let (Some(limit), Some(start), Some(end), Some(reset)) = (
        policy.limit,
        policy.period_start,
        policy.period_end,
        policy.reset_at,
    ) else {
        return None;
    };
    (limit >= 0 && start < end).then_some(QuotaInterval {
        limit,
        start,
        end,
        reset,
    })
}

fn blocked_counts(value: Value) -> Result<Value, ApiError> {
    if value.is_null() {
        return Ok(json!({}));
    }
    let counts = value
        .as_object()
        .ok_or_else(|| fail("failed to load autopilot quota usage"))?;
    let mut result = serde_json::Map::new();
    for (key, value) in counts {
        let number = if value.is_null() {
            0
        } else {
            value
                .as_i64()
                .ok_or_else(|| fail("failed to load autopilot quota usage"))?
        };
        result.insert(key.clone(), json!(number));
    }
    Ok(Value::Object(result))
}

async fn usage(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to load autopilot quota usage"))?;
    acting_member(&db, ws, user, &actor).await?;
    drop(db); // The policy source may need the same pool; do not hold a slot across I/O.
    // A missing provider is not an off policy, but never expose provider state
    // to an actor who cannot read this workspace.
    let source = state.automation_sprint_quota.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "autopilot quota policy unavailable",
        )
    })?;
    let policy = source
        .policy(ws)
        .await
        .map_err(|_| fail("failed to load autopilot quota usage"))?;
    let Some(QuotaInterval {
        limit,
        start,
        end,
        reset,
    }) = valid(&policy)
    else {
        // Disabled/malformed decisions must not access the quota table.
        return Ok(GoJson(inactive()));
    };
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to load autopilot quota usage"))?;
    let row = db.query_opt(
        "SELECT used_count,reserved_count,blocked_counts FROM autopilot_quota_period WHERE workspace_id=$1 AND period_start=$2 AND period_end=$3",
        &[&ws, &start, &end],
    ).await.map_err(|_| fail("failed to load autopilot quota usage"))?;
    let (used, reserved, blocked) = if let Some(row) = row {
        let counts: Value = row.get("blocked_counts");
        (
            row.get::<_, i64>("used_count"),
            row.get::<_, i64>("reserved_count"),
            blocked_counts(counts)?,
        )
    } else {
        (0, 0, json!({}))
    };
    let total = used
        .checked_add(reserved)
        .ok_or_else(|| fail("failed to load autopilot quota usage"))?;
    Ok(GoJson(json!({
        "action":policy.action, "used":used, "reserved":reserved, "total":total,
        "limit":limit, "reached":(policy.action == "enforce").then_some(total >= limit),
        "period_start":start.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "period_end":end.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "reset_at":reset.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "blocked_counts":blocked
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_policy_disables_without_inventing_interval() {
        let start = Utc::now();
        let mut policy = AutopilotQuotaPolicy {
            action: "enforce".into(),
            limit: Some(0),
            period_start: Some(start),
            period_end: Some(start),
            reset_at: Some(start),
        };
        assert!(valid(&policy).is_none());
        policy.period_end = Some(start + chrono::Duration::hours(1));
        assert!(valid(&policy).is_some());
        policy.action = "observe".into();
        assert!(valid(&policy).is_some());
        policy.limit = Some(-1);
        assert!(valid(&policy).is_none());
        policy.action = "off".into();
        assert!(valid(&policy).is_none());
        assert!(inactive()["blocked_counts"].is_null());
        assert_eq!(blocked_counts(json!(null)).unwrap(), json!({}));
        assert_eq!(
            blocked_counts(json!({"manual":null})).unwrap(),
            json!({"manual":0})
        );
        assert!(blocked_counts(json!({"manual":1.5})).is_err());
        assert!(blocked_counts(json!([1])).is_err());
    }
}
