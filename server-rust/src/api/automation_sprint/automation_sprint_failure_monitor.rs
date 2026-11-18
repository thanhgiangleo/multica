//! Bounded failure-rate sweep. Startup and durable event transport belong to the integrator.
use super::*;
use async_trait::async_trait;
use deadpool_postgres::Pool;
use std::sync::Arc;

const REASON: &str = "auto_paused_high_failure_rate";

fn format_lookback(seconds: i64) -> String {
    if seconds % 86_400 == 0 {
        let days = seconds / 86_400;
        return format!("{days} day{}", if days == 1 { "" } else { "s" });
    }
    if seconds % 3_600 == 0 {
        let hours = seconds / 3_600;
        return format!("{hours} hour{}", if hours == 1 { "" } else { "s" });
    }
    if seconds >= 3_600 {
        return format!(
            "{}h{}m{}s",
            seconds / 3_600,
            (seconds / 60) % 60,
            seconds % 60
        );
    }
    if seconds >= 60 {
        return format!("{}m{}s", seconds / 60, seconds % 60);
    }
    format!("{seconds}s")
}

#[derive(Clone, Copy, Debug)]
pub struct FailureMonitorConfig {
    pub lookback_seconds: i64,
    pub min_runs: i64,
    pub fail_ratio: f64,
}

impl Default for FailureMonitorConfig {
    fn default() -> Self {
        Self {
            lookback_seconds: 7 * 24 * 3600,
            min_runs: 50,
            fail_ratio: 0.9,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PauseNotice {
    pub recipient_id: Uuid,
    pub item_id: Uuid,
    pub item: Value,
}

#[derive(Debug, Clone)]
pub struct PauseTransition {
    pub workspace_id: Uuid,
    pub autopilot_id: Uuid,
    pub autopilot: Value,
    pub failed_runs: i64,
    pub total_runs: i64,
    pub notices: Vec<PauseNotice>,
}

/// Enqueue autopilot:updated (system, reason=auto_paused_high_failure_rate)
/// and one inbox:new per notice within the caller's transaction. A post-commit
/// publish is not a durable implementation of this contract.
#[async_trait]
pub trait DurablePauseEvents: Send + Sync {
    async fn enqueue(
        &self,
        tx: &Transaction<'_>,
        change: &PauseTransition,
    ) -> Result<(), &'static str>;
}

#[derive(Debug)]
pub struct FailureMonitorPage {
    pub next_after: Option<Uuid>,
    pub paused: Vec<PauseTransition>,
    pub failures: Vec<(Uuid, &'static str)>,
}

#[derive(Clone)]
pub struct FailureMonitor {
    pool: Arc<Pool>,
    events: Arc<dyn DurablePauseEvents>,
    config: FailureMonitorConfig,
}

impl FailureMonitor {
    pub fn new(
        pool: Arc<Pool>,
        events: Arc<dyn DurablePauseEvents>,
        config: FailureMonitorConfig,
    ) -> Result<Self, &'static str> {
        if config.lookback_seconds <= 0
            || config.lookback_seconds > 366 * 24 * 3600
            || config.min_runs <= 0
            || !config.fail_ratio.is_finite()
            || !(0.0..=1.0).contains(&config.fail_ratio)
        {
            return Err("invalid failure monitor configuration");
        }
        Ok(Self {
            pool,
            events,
            config,
        })
    }

    /// Cursor is over active autopilots, not just offenders. A failed row does
    /// not wedge later pages; start a fresh sweep at None after next_after=None.
    pub async fn sweep_page(
        &self,
        after: Option<Uuid>,
        limit: i64,
    ) -> Result<FailureMonitorPage, &'static str> {
        if !(1..=100).contains(&limit) {
            return Err("invalid failure monitor page size");
        }
        let db = self
            .pool
            .get()
            .await
            .map_err(|_| "failure monitor database unavailable")?;
        let ids: Vec<Uuid> = db
            .query(
                "SELECT id FROM autopilot WHERE status='active' AND ($1::uuid IS NULL OR id>$1) ORDER BY id LIMIT $2",
                &[&after, &limit],
            )
            .await
            .map_err(|_| "failure monitor scan failed")?
            .iter()
            .map(|r| r.get(0))
            .collect();
        drop(db);
        let mut result = FailureMonitorPage {
            next_after: ids.last().copied().filter(|_| ids.len() == limit as usize),
            paused: Vec::new(),
            failures: Vec::new(),
        };
        for id in ids {
            match self.pause_if_failing(id).await {
                Ok(Some(transition)) => result.paused.push(transition),
                Ok(None) => {}
                Err(err) => result.failures.push((id, err)),
            }
        }
        Ok(result)
    }

    async fn pause_if_failing(&self, id: Uuid) -> Result<Option<PauseTransition>, &'static str> {
        let mut db = self
            .pool
            .get()
            .await
            .map_err(|_| "failure monitor database unavailable")?;
        let tx = db
            .transaction()
            .await
            .map_err(|_| "failure monitor transaction failed")?;
        let Some(_) = tx
            .query_opt(
                "SELECT * FROM autopilot WHERE id=$1 AND status='active' FOR UPDATE",
                &[&id],
            )
            .await
            .map_err(|_| "failure monitor autopilot lookup failed")?
        else {
            return Ok(None);
        };
        let stats = tx
            .query_one(
                "SELECT count(*) FILTER (WHERE status IN ('completed','failed'))::bigint AS total, \
                 count(*) FILTER (WHERE status='failed')::bigint AS failed \
                 FROM autopilot_run WHERE autopilot_id=$1 \
                 AND created_at >= now() - $2::bigint * interval '1 second'",
                &[&id, &self.config.lookback_seconds],
            )
            .await
            .map_err(|_| "failure monitor run count failed")?;
        let total: i64 = stats.get("total");
        let failed: i64 = stats.get("failed");
        if total < self.config.min_runs || (failed as f64) / (total as f64) < self.config.fail_ratio
        {
            return Ok(None);
        }
        let ap = tx
            .query_one(
                "UPDATE autopilot SET status='paused',pause_reason=NULL,updated_at=now() WHERE id=$1 AND status='active' RETURNING *",
                &[&id],
            )
            .await
            .map_err(|_| "failure monitor pause failed")?;
        let ws: Uuid = ap.get("workspace_id");
        let summary = json!({
            "assignee_type": ap.get::<_, String>("assignee_type"),
            "assignee_id": ap.get::<_, Uuid>("assignee_id"),
            "status": "paused",
            "execution_mode": ap.get::<_, String>("execution_mode"),
        });
        tx.execute(
            "INSERT INTO autopilot_rule_version(autopilot_id,workspace_id,published_by_type,published_by_id,config_summary) VALUES ($1,$2,'system',NULL,$3)",
            &[&id, &ws, &summary],
        )
        .await
        .map_err(|_| "failure monitor rule version failed")?;
        let pct = ((failed as f64) / (total as f64) * 1000.0).round() / 10.0;
        let title: String = ap.get("title");
        let details = json!({
            "autopilot_id": id, "autopilot_title": title,
            "failed_runs": failed, "total_runs": total, "fail_pct": pct,
            "lookback_seconds": self.config.lookback_seconds,
            "threshold_min_runs": self.config.min_runs,
            "threshold_fail_ratio": self.config.fail_ratio,
            "reason": REASON,
        });
        let notice_title = format!("Autopilot paused: {title}");
        let notice_body = format!(
            "Auto-paused after {failed} of {total} runs failed ({pct:.1}%) in the last {}. Investigate the failures, fix the root cause, then re-enable from the autopilot page.",
            format_lookback(self.config.lookback_seconds)
        );
        let creator_type: String = ap.get("created_by_type");
        let creator: Uuid = ap.get("created_by_id");
        // Prefer current member creator, then creator agent's owner in this
        // workspace. Fall back to current workspace managers only if neither
        // resolves. Never deliver to an agent or a former member.
        let recipients = tx.query(
            "WITH primary_recipient AS ( \
               SELECT m.user_id FROM member m WHERE m.workspace_id=$1 AND $2='member' AND m.user_id=$3 \
               UNION SELECT m.user_id FROM agent a JOIN member m ON m.workspace_id=a.workspace_id AND m.user_id=a.owner_id \
               WHERE a.id=$3 AND a.workspace_id=$1 AND $2='agent' \
             ), recipients AS ( \
               SELECT user_id FROM primary_recipient \
               UNION ALL SELECT m.user_id FROM member m WHERE m.workspace_id=$1 AND m.role IN ('owner','admin') \
                 AND NOT EXISTS(SELECT 1 FROM primary_recipient) \
             ) SELECT DISTINCT user_id FROM recipients ORDER BY user_id",
            &[&ws, &creator_type, &creator],
        ).await.map_err(|_| "failure monitor recipients lookup failed")?;
        let mut notices = Vec::new();
        for recipient in recipients {
            let user: Uuid = recipient.get(0);
            let item_id = Uuid::new_v4();
            let row = tx.query_opt(
                "INSERT INTO inbox_item(id,workspace_id,recipient_type,recipient_id,type,severity,title,body,actor_type,details) \
                 SELECT $1,$2,'member',$3,'autopilot_paused','attention',$4,$5,'system',$6 \
                 WHERE EXISTS(SELECT 1 FROM member WHERE workspace_id=$2 AND user_id=$3) RETURNING id,created_at",
                &[&item_id, &ws, &user, &notice_title, &notice_body, &details],
            ).await.map_err(|_| "failure monitor inbox write failed")?;
            if let Some(row) = row {
                let created_at: DateTime<Utc> = row.get("created_at");
                notices.push(PauseNotice {
                    recipient_id: user,
                    item_id,
                    item: json!({"id":item_id,"workspace_id":ws,"recipient_type":"member",
                        "recipient_id":user,"type":"autopilot_paused","severity":"attention",
                        "issue_id":null,"title":notice_title,"body":notice_body,"actor_type":"system",
                        "actor_id":null,"details":details,"read":false,"archived":false,
                        "created_at":timestamp(created_at)}),
                });
            }
        }
        let transition = PauseTransition {
            workspace_id: ws,
            autopilot_id: id,
            autopilot: json!({"id":id,"workspace_id":ws,"title":title,
                "description":ap.get::<_,Option<String>>("description"),
                "assignee_id":ap.get::<_,Uuid>("assignee_id"),"status":"paused",
                "execution_mode":ap.get::<_,String>("execution_mode"),
                "issue_title_template":ap.get::<_,Option<String>>("issue_title_template"),
                "created_by_type":creator_type,"created_by_id":creator,
                "last_run_at":time(&ap,"last_run_at"),
                "created_at":timestamp(ap.get::<_,DateTime<Utc>>("created_at")),
                "updated_at":timestamp(ap.get::<_,DateTime<Utc>>("updated_at"))}),
            failed_runs: failed,
            total_runs: total,
            notices,
        };
        self.events.enqueue(&tx, &transition).await?;
        tx.commit()
            .await
            .map_err(|_| "failure monitor commit failed")?;
        Ok(Some(transition))
    }
}

#[cfg(test)]
mod tests {
    use super::format_lookback;

    #[test]
    fn lookback_matches_go_duration_display() {
        for (seconds, expected) in [
            (86_400, "1 day"),
            (604_800, "7 days"),
            (3_600, "1 hour"),
            (7_200, "2 hours"),
            (90, "1m30s"),
            (3_601, "1h0m1s"),
        ] {
            assert_eq!(format_lookback(seconds), expected);
        }
    }
}
