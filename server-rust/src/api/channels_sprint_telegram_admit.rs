//! Conservative private-text admission, durable chat route and context snapshots.
//! This does not enqueue tasks, deliver replies, redeem invitations or handle groups.
use crate::api::channels_sprint_ingress::{ClaimError, InboundKey};
use tokio_postgres::Transaction;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub enum Admission {
    Persisted(AdmittedText),
    Dropped,
}

/// IDs and fences for a later task scheduler. Never infer a task from this
/// value alone: enqueue must lock the current route and exact guest grant again.
#[derive(Debug, PartialEq, Eq)]
pub struct AdmittedText {
    pub workspace: Uuid,
    pub installation: Uuid,
    pub agent: Uuid,
    pub session: Uuid,
    pub binding: Uuid,
    pub message: Uuid,
    pub sender: Uuid,
    pub guest_generation: Option<i64>,
    pub context_revision: i64,
    pub route_revision: i64,
    pub created_session: bool,
}

/// Only a private human text update from the authenticated poll session may
/// construct this input. No caller-supplied workspace, principal or generation.
pub(super) struct PrivateText {
    pub chat_id: i64,
    pub sender_id: i64,
    pub message_id: i64,
    pub text: String,
}

/// Until the command, pairing and entity parsers are ported, reject instead of
/// persisting potential invitation codes or interpreting commands as agent text.
/// This intentionally blocks some ordinary long identifiers.
pub(super) fn plain_text(text: &str) -> bool {
    if text.trim().is_empty()
        || text.len() > 65536
        || text.contains('/')
        || text.chars().any(char::is_control)
    {
        return false;
    }
    let lower = text.to_ascii_lowercase();
    if lower.contains("t.me") || lower.contains("telegram.me") {
        return false;
    }
    let mut run = 0;
    for byte in text.bytes().chain(std::iter::once(b' ')) {
        if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
            run += 1;
        } else {
            if run >= 32 {
                return false;
            }
            run = 0;
        }
    }
    true
}

pub(super) async fn persist_private_text(
    tx: &Transaction<'_>,
    key: &InboundKey,
    input: PrivateText,
) -> Result<Admission, ClaimError> {
    let invalid = || ClaimError::Invalid;
    let database = |_| ClaimError::Database;
    if input.chat_id <= 0
        || input.chat_id != input.sender_id
        || input.message_id <= 0
        || !plain_text(&input.text)
    {
        return Err(invalid());
    }
    let installation = tx.query_opt(
        "SELECT i.agent_id, a.owner_id, a.permission_mode FROM channel_installation i JOIN agent a ON a.id=i.agent_id AND a.workspace_id=i.workspace_id AND a.archived_at IS NULL WHERE i.id=$1 AND i.workspace_id=$2 AND i.channel_type='telegram' AND i.status='active' FOR SHARE OF i,a",
        &[&key.installation, &key.workspace],
    ).await.map_err(database)?.ok_or_else(invalid)?;
    let agent: Uuid = installation.get("agent_id");
    let owner: Option<Uuid> = installation.get("owner_id");
    let permission: String = installation.get("permission_mode");
    let external = input.sender_id.to_string();
    let member = tx.query_opt(
        "SELECT workspace_id,channel_type,multica_user_id FROM channel_user_binding WHERE installation_id=$1 AND channel_user_id=$2 FOR SHARE",
        &[&key.installation, &external],
    ).await.map_err(database)?;
    let (sender, guest_generation) = if let Some(binding) = member {
        // A stale/wrong-workspace member binding is a denial, never a guest fallback.
        if binding.get::<_, Uuid>("workspace_id") != key.workspace
            || binding.get::<_, String>("channel_type") != "telegram"
        {
            return Ok(Admission::Dropped);
        }
        let sender: Uuid = binding.get("multica_user_id");
        if tx.query_opt(
            "SELECT 1 FROM member m JOIN \"user\" u ON u.id=m.user_id AND NOT u.zbs_is_shadow WHERE m.workspace_id=$1 AND m.user_id=$2 FOR SHARE OF m,u",
            &[&key.workspace, &sender],
        ).await.map_err(database)?.is_none() {
            return Ok(Admission::Dropped);
        }
        if Some(sender) != owner && (permission != "public_to" || tx.query_opt(
            "SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND ((target_type='workspace' AND target_id=$2) OR (target_type='member' AND target_id=$3)) LIMIT 1 FOR SHARE",
            &[&agent, &key.workspace, &sender],
        ).await.map_err(database)?.is_none()) {
            return Ok(Admission::Dropped);
        }
        (sender, None)
    } else {
        let grant = tx.query_opt(
            "SELECT g.shadow_user_id,g.grant_generation FROM zbs_telegram_guest_grant g JOIN \"user\" u ON u.id=g.shadow_user_id AND u.zbs_is_shadow WHERE g.installation_id=$1 AND g.telegram_user_id=$2 AND g.workspace_id=$3 AND g.agent_id=$4 AND g.revoked_at IS NULL AND g.grant_generation>0 FOR SHARE OF g,u",
            &[&key.installation, &external, &key.workspace, &agent],
        ).await.map_err(database)?;
        let Some(grant) = grant else {
            return Ok(Admission::Dropped);
        };
        if permission != "public_to" || tx.query_opt(
            "SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND target_type='workspace' AND target_id=$2 LIMIT 1 FOR SHARE",
            &[&agent, &key.workspace],
        ).await.map_err(database)?.is_none() {
            return Ok(Admission::Dropped);
        }
        (
            grant.get("shadow_user_id"),
            Some(grant.get::<_, i64>("grant_generation")),
        )
    };

    let routing = input.chat_id.to_string();
    // Serializes first messages from this adapter. The unique route constraint
    // still fences other writers; a conflicting external writer rolls back.
    let lock_key = format!("telegram:{}:{routing}", key.installation);
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,73))",
        &[&lock_key],
    )
    .await
    .map_err(database)?;
    let current = tx.query_opt(
        "SELECT chat_session_id FROM channel_chat_session_binding WHERE installation_id=$1 AND channel_chat_id=$2 AND retired_at IS NULL",
        &[&key.installation, &routing],
    ).await.map_err(database)?;
    let (session, binding, context_revision, route_revision, created_session) = if let Some(
        current,
    ) = current
    {
        let session: Uuid = current.get("chat_session_id");
        // Match the shared append/enqueue lock order: session, route, context.
        let chat = tx.query_opt("SELECT workspace_id,agent_id,creator_id,status,zbs_source_channel_type,zbs_source_channel_chat_type FROM chat_session WHERE id=$1 FOR UPDATE", &[&session]).await.map_err(database)?.ok_or_else(invalid)?;
        let route = tx.query_opt("SELECT id,context_revision,route_revision,chat_type,channel_type,history_boundary_pending FROM channel_chat_session_binding WHERE chat_session_id=$1 AND installation_id=$2 AND channel_chat_id=$3 AND retired_at IS NULL FOR UPDATE", &[&session, &key.installation, &routing]).await.map_err(database)?.ok_or_else(invalid)?;
        // The archive/new-context state machine is owned by shared chat runtime.
        if route.get::<_, bool>("history_boundary_pending")
            || route.get::<_, i64>("context_revision") <= 0
            || route.get::<_, i64>("route_revision") <= 0
        {
            return Err(invalid());
        }
        if route.get::<_, String>("chat_type") != "p2p"
            || route.get::<_, String>("channel_type") != "telegram"
            || chat.get::<_, Uuid>("workspace_id") != key.workspace
            || chat.get::<_, Uuid>("agent_id") != agent
            || chat.get::<_, Uuid>("creator_id") != sender
            || chat.get::<_, String>("status") != "active"
            || chat
                .get::<_, Option<String>>("zbs_source_channel_type")
                .as_deref()
                != Some("telegram")
            || chat
                .get::<_, Option<String>>("zbs_source_channel_chat_type")
                .as_deref()
                != Some("p2p")
        {
            return Ok(Admission::Dropped);
        }
        (
            session,
            route.get("id"),
            route.get("context_revision"),
            route.get("route_revision"),
            false,
        )
    } else {
        let session = Uuid::new_v4();
        let title = input.text.trim().chars().take(80).collect::<String>();
        tx.execute(
            "INSERT INTO chat_session(id,workspace_id,agent_id,creator_id,title,zbs_source_channel_type,zbs_source_channel_chat_type,zbs_has_guest_history) VALUES($1,$2,$3,$4,$5,'telegram','p2p',$6)",
            &[&session, &key.workspace, &agent, &sender, &title, &guest_generation.is_some()],
        ).await.map_err(database)?;
        let row = tx.query_one(
            "INSERT INTO channel_chat_session_binding(chat_session_id,installation_id,channel_type,channel_chat_id,chat_type,route_revision) SELECT $1,$2,'telegram',$3,'p2p',COALESCE(MAX(route_revision)+1,1) FROM channel_chat_session_binding WHERE installation_id=$2 AND channel_chat_id=$3 RETURNING id,context_revision,route_revision",
            &[&session, &key.installation, &routing],
        ).await.map_err(database)?;
        tx.execute(
            "INSERT INTO channel_chat_context_generation(chat_session_id,revision) VALUES($1,$2)",
            &[&session, &row.get::<_, i64>("context_revision")],
        )
        .await
        .map_err(database)?;
        (
            session,
            row.get("id"),
            row.get("context_revision"),
            row.get("route_revision"),
            true,
        )
    };
    let context = tx.query_opt(
        "SELECT initiator_user_id,zbs_guest_shadow_user_id,zbs_guest_grant_generation,history_boundary_pending,pending_fresh FROM channel_chat_context_generation WHERE chat_session_id=$1 AND revision=$2 FOR UPDATE",
        &[&session, &context_revision],
    ).await.map_err(database)?.ok_or_else(invalid)?;
    let former: Option<i64> = context.get("zbs_guest_grant_generation");
    let former_shadow: Option<Uuid> = context.get("zbs_guest_shadow_user_id");
    let former_initiator: Option<Uuid> = context.get("initiator_user_id");
    if context.get::<_, bool>("history_boundary_pending")
        || context.get::<_, bool>("pending_fresh")
        || former.is_some() != former_shadow.is_some()
    {
        return Err(invalid());
    }
    if former.is_some_and(|previous| guest_generation != Some(previous))
        || former_shadow.is_some_and(|previous| previous != sender)
        || former_initiator.is_some_and(|previous| previous != sender)
        || (guest_generation.is_some() && !created_session && former.is_none())
    {
        // Do not let a re-pair inherit a pending task context from an old grant.
        return Ok(Admission::Dropped);
    }
    let guest_shadow = guest_generation.map(|_| sender);
    let message_id = format!("{}:{}", input.chat_id, input.message_id);
    let message = Uuid::new_v4();
    tx.execute(
        "UPDATE channel_chat_context_generation SET initiator_user_id=$3,zbs_guest_shadow_user_id=$4,zbs_guest_grant_generation=$5,last_message_id=$6,last_thread_id=NULL,last_sender_id=$7 WHERE chat_session_id=$1 AND revision=$2",
        &[&session, &context_revision, &sender, &guest_shadow, &guest_generation, &message_id, &external],
    ).await.map_err(database)?;
    tx.execute(
        "INSERT INTO chat_message(id,chat_session_id,role,content,channel_ingested,channel_context_revision,zbs_sender_user_id,zbs_sender_external_id) VALUES($1,$2,'user',$3,true,$4,$5,$6)",
        &[&message, &session, &input.text, &context_revision, &sender, &external],
    ).await.map_err(database)?;
    tx.execute("UPDATE chat_session SET updated_at=now(),zbs_has_guest_history=zbs_has_guest_history OR $2 WHERE id=$1", &[&session, &guest_generation.is_some()]).await.map_err(database)?;
    tx.execute("UPDATE channel_chat_session_binding SET last_message_id=$2,last_thread_id=NULL WHERE id=$1 AND retired_at IS NULL", &[&binding, &message_id]).await.map_err(database)?;
    Ok(Admission::Persisted(AdmittedText {
        workspace: key.workspace,
        installation: key.installation,
        agent,
        session,
        binding,
        message,
        sender,
        guest_generation,
        context_revision,
        route_revision,
        created_session,
    }))
}

#[cfg(test)]
mod tests {
    use super::plain_text;
    #[test]
    fn channels_sprint_telegram_admission_rejects_secret_and_command_text() {
        assert!(plain_text("hello from private chat"));
        for text in [
            "/new hello",
            "  /new hello",
            "try /pair code",
            "please /start Abc",
            "https://t.me/bot?start=code",
            "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789012345678",
            "A".repeat(100).as_str(),
            "hello\nworld",
        ] {
            assert!(!plain_text(text), "sensitive input should fail closed");
        }
    }
}
