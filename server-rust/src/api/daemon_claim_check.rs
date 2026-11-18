//! Wakeup authorization is rechecked after claiming, before exposing inputs.
use deadpool_postgres::GenericClient;
use serde::Deserialize;
use serde_json::Value;
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub(super) enum CheckClaimError {
    Forbidden,
    Database,
}

#[derive(Deserialize)]
struct Source {
    #[serde(default, rename = "wakeup_id")]
    id: String,
    #[serde(default, rename = "wakeup_revision")]
    revision: i64,
}

fn source(context: Option<Value>) -> Result<Option<(Uuid, i64)>, CheckClaimError> {
    let Some(source) = context.and_then(|v| serde_json::from_value::<Source>(v).ok()) else {
        return Ok(None);
    };
    if source.id.is_empty() {
        return Ok(None);
    }
    Uuid::parse_str(&source.id)
        .map(|id| Some((id, source.revision)))
        .map_err(|_| CheckClaimError::Forbidden)
}

pub(super) async fn check_claim(
    db: &impl GenericClient,
    task: &Row,
) -> Result<(), CheckClaimError> {
    let Some((id, revision)) = source(task.get("context"))? else {
        return Ok(());
    };
    let wakeup = db.query_opt(
        "SELECT workspace_id,issue_id,agent_id,created_by,system_rule,enabled,disabled_at IS NOT NULL AS disabled,revision FROM issue_wakeup WHERE id=$1",
        &[&id],
    ).await.map_err(|_| CheckClaimError::Database)?
        .ok_or(CheckClaimError::Forbidden)?;
    let system = wakeup.get::<_, Option<String>>("system_rule").is_some();
    let agent: Uuid = task.get("agent_id");
    let member: Option<Uuid> = wakeup.get("created_by");
    if wakeup.get::<_, bool>("disabled")
        || wakeup.get::<_, i64>("revision") != revision
        || wakeup.get::<_, Option<Uuid>>("issue_id") != task.get::<_, Option<Uuid>>("issue_id")
        || (system && !wakeup.get::<_, bool>("enabled"))
        || (!system
            && (wakeup.get::<_, Option<Uuid>>("agent_id") != Some(agent)
                || member != task.get::<_, Option<Uuid>>("originator_user_id")))
    {
        return Err(CheckClaimError::Forbidden);
    }
    let workspace: Uuid = wakeup.get("workspace_id");
    let row = db.query_opt(
        "SELECT id,owner_id,permission_mode,runtime_id,archived_at IS NOT NULL AS archived FROM agent WHERE id=$1 AND workspace_id=$2",
        &[&agent, &workspace],
    ).await.map_err(|_| CheckClaimError::Database)?
        .ok_or(CheckClaimError::Forbidden)?;
    if row.get::<_, bool>("archived") || row.get::<_, Option<Uuid>>("runtime_id").is_none() {
        return Err(CheckClaimError::Forbidden);
    }
    if system {
        return Ok(());
    }
    let member = member.ok_or(CheckClaimError::Forbidden)?;
    if db
        .query_opt(
            "SELECT 1 FROM member WHERE user_id=$1 AND workspace_id=$2",
            &[&member, &workspace],
        )
        .await
        .map_err(|_| CheckClaimError::Database)?
        .is_none()
    {
        return Err(CheckClaimError::Forbidden);
    }
    if row.get::<_, Option<Uuid>>("owner_id") == Some(member) {
        return Ok(());
    }
    if row.get::<_, String>("permission_mode") != "public_to" {
        return Err(CheckClaimError::Forbidden);
    }
    // CanMemberInvokeAgent deliberately collapses invocation-read errors to denial.
    let targets = db.query("SELECT target_type,target_id FROM agent_invocation_target WHERE agent_id=$1 ORDER BY target_type ASC,created_at ASC", &[&agent])
        .await.map_err(|_| CheckClaimError::Forbidden)?;
    let is_member = db
        .query_opt(
            "SELECT 1 FROM member WHERE user_id=$1 AND workspace_id=$2",
            &[&member, &workspace],
        )
        .await
        .map_err(|_| CheckClaimError::Forbidden)?
        .is_some();
    if targets.iter().any(|target| {
        target_allows(
            target.get::<_, String>("target_type").as_str(),
            target.get("target_id"),
            member,
            is_member,
        )
    }) {
        Ok(())
    } else {
        Err(CheckClaimError::Forbidden)
    }
}

fn target_allows(kind: &str, target: Option<Uuid>, member: Uuid, is_member: bool) -> bool {
    match kind {
        "workspace" => is_member,
        "member" => target == Some(member),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn malformed_context_is_ignored_but_invalid_wakeup_is_denied() {
        for context in [
            None,
            Some(Value::Null),
            Some(json!({})),
            Some(json!({"wakeup_id":7})),
            Some(json!({"wakeup_id":"x","wakeup_revision":"bad"})),
        ] {
            assert_eq!(source(context), Ok(None));
        }
        assert_eq!(
            source(Some(json!({"wakeup_id":"bad"}))),
            Err(CheckClaimError::Forbidden)
        );
        let id = Uuid::now_v7();
        assert_eq!(
            source(Some(json!({"wakeup_id":id,"wakeup_revision":3}))),
            Ok(Some((id, 3)))
        );
    }

    #[test]
    fn invocation_targets_do_not_grant_workspace_broad_access() {
        let user = Uuid::now_v7();
        assert!(!target_allows("workspace_broad", None, user, true));
        assert!(!target_allows("workspace", None, user, false));
        assert!(target_allows("workspace", None, user, true));
        assert!(target_allows("member", Some(user), user, false));
        assert!(!target_allows("member", Some(Uuid::now_v7()), user, true));
    }
}
