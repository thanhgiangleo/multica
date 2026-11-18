//! Private Telegram terminal delivery. Runtime must call `snapshot_private_task`
//! in the enqueue transaction, then deliver only a completed task's persisted
//! assistant message. No stream, retries, attachments, or background worker.
use super::{TelegramInstaller, parse_token};
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::{GenericClient, Transaction};
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub enum ReplyError {
    Database,
    InvalidTask,
    Unsupported,
    InvalidInstallation,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ReplyOutcome {
    Delivered,
    AlreadySettled,
    Busy,
    Denied,
    Refused,
    Unknown,
}

/// Called by the trusted scheduler in the SAME transaction that inserts the
/// task. Captures route, trigger, guest, and installation generation. A task
/// without this snapshot cannot send, even when its session has a live route.
pub async fn snapshot_private_task(tx: &Transaction<'_>, task: Uuid) -> Result<(), ReplyError> {
    let row = tx.query_opt(
        "SELECT t.chat_session_id,t.agent_id,t.initiator_user_id,t.channel_context_revision,t.retry_of_task_id,t.status,s.agent_id AS session_agent_id, s.workspace_id,s.creator_id,s.status AS session_status,s.zbs_source_channel_type,s.zbs_source_channel_chat_type FROM agent_task_queue t JOIN chat_session s ON s.id=t.chat_session_id WHERE t.id=$1 FOR SHARE OF t,s",
        &[&task],
    ).await.map_err(|_| ReplyError::Database)?.ok_or(ReplyError::InvalidTask)?;
    let session: Uuid = row.get("chat_session_id");
    let revision: Option<i64> = row.get("channel_context_revision");
    let sender: Option<Uuid> = row.get("initiator_user_id");
    if row.get::<_, Uuid>("agent_id") != row.get::<_, Uuid>("session_agent_id")
        || row.get::<_, Option<Uuid>>("retry_of_task_id").is_some()
        || row.get::<_, String>("status") != "queued"
        || row.get::<_, String>("session_status") != "active"
        || row
            .get::<_, Option<String>>("zbs_source_channel_type")
            .as_deref()
            != Some("telegram")
        || row
            .get::<_, Option<String>>("zbs_source_channel_chat_type")
            .as_deref()
            != Some("p2p")
        || revision.is_none_or(|n| n <= 0)
        || sender != Some(row.get("creator_id"))
    {
        return Err(ReplyError::Unsupported);
    }
    let revision = revision.ok_or(ReplyError::Unsupported)?;
    let route = tx.query_opt(
        "SELECT b.id,b.installation_id,b.channel_chat_id,b.route_revision,b.context_revision,b.last_message_id,b.retired_at,b.history_boundary_pending,b.channel_type,b.chat_type,i.config,i.status AS install_status,i.agent_id,i.workspace_id FROM channel_chat_session_binding b JOIN channel_installation i ON i.id=b.installation_id WHERE b.chat_session_id=$1 FOR SHARE OF b,i",
        &[&session],
    ).await.map_err(|_| ReplyError::Database)?.ok_or(ReplyError::InvalidTask)?;
    let config: Value = route.get("config");
    let generation = generation(&config)?;
    let chat: String = route.get("channel_chat_id");
    private_chat(&chat)?;
    let context = tx.query_opt(
        "SELECT initiator_user_id,last_message_id,last_sender_id,last_thread_id,zbs_guest_shadow_user_id,zbs_guest_grant_generation,history_boundary_pending,pending_fresh FROM channel_chat_context_generation WHERE chat_session_id=$1 AND revision=$2 FOR SHARE",
        &[&session, &revision],
    ).await.map_err(|_| ReplyError::Database)?.ok_or(ReplyError::InvalidTask)?;
    let guest: Option<Uuid> = context.get("zbs_guest_shadow_user_id");
    let grant: Option<i64> = context.get("zbs_guest_grant_generation");
    let message: Option<String> = context.get("last_message_id");
    let external: Option<String> = context.get("last_sender_id");
    if route.get::<_, String>("channel_type") != "telegram"
        || route.get::<_, String>("chat_type") != "p2p"
        || route.get::<_, String>("install_status") != "active"
        || route.get::<_, Uuid>("agent_id") != row.get::<_, Uuid>("agent_id")
        || route.get::<_, Uuid>("workspace_id") != row.get::<_, Uuid>("workspace_id")
        || route
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("retired_at")
            .is_some()
        || route.get::<_, bool>("history_boundary_pending")
        || route.get::<_, i64>("route_revision") <= 0
        || route.get::<_, i64>("context_revision") != revision
        || context.get::<_, bool>("history_boundary_pending")
        || context.get::<_, bool>("pending_fresh")
        || context.get::<_, Option<Uuid>>("initiator_user_id") != sender
        || context.get::<_, Option<String>>("last_thread_id").is_some()
        || external.as_deref() != Some(chat.as_str())
        || guest.is_some() != grant.is_some()
        || guest.is_some_and(|g| Some(g) != sender)
        || grant.is_some_and(|g| g <= 0)
        || message
            .as_deref()
            .and_then(|s| s.split_once(':'))
            .and_then(|(c, m)| (c == chat && m.parse::<i64>().is_ok_and(|m| m > 0)).then_some(()))
            != Some(())
    {
        return Err(ReplyError::Unsupported);
    }
    if guest.is_some() {
        check_grant(
            tx,
            route.get("installation_id"),
            route.get("workspace_id"),
            route.get("agent_id"),
            &chat,
            guest,
            grant,
        )
        .await?;
    }
    let snapshot =
        json!({"installation_generation":generation.to_string(),"context_revision":revision});
    let inserted = tx.execute(
        "INSERT INTO channel_task_delivery(task_id,binding_id,installation_id,channel_type,channel_chat_id,chat_type,channel_message_id,channel_thread_id,channel_sender_id,route_revision,config,zbs_guest_shadow_user_id,zbs_guest_grant_generation) VALUES($1,$2,$3,'telegram',$4,'p2p',$5,NULL,$6,$7,$8,$9,$10) ON CONFLICT (task_id) DO NOTHING",
        &[&task,&route.get::<_, Uuid>("id"),&route.get::<_, Uuid>("installation_id"),&chat,&message,&external,&route.get::<_,i64>("route_revision"),&snapshot,&guest,&grant],
    ).await.map_err(|_| ReplyError::Database)?;
    if inserted != 1 {
        return Err(ReplyError::InvalidTask);
    }
    Ok(())
}

fn generation(config: &Value) -> Result<Uuid, ReplyError> {
    let raw = config["generation"]
        .as_str()
        .ok_or(ReplyError::InvalidInstallation)?;
    let parsed = Uuid::parse_str(raw).map_err(|_| ReplyError::InvalidInstallation)?;
    if parsed.to_string() != raw {
        return Err(ReplyError::InvalidInstallation);
    }
    Ok(parsed)
}

fn private_chat(chat: &str) -> Result<i64, ReplyError> {
    let parsed = chat.parse::<i64>().map_err(|_| ReplyError::Unsupported)?;
    if parsed <= 0 || parsed.to_string() != chat {
        return Err(ReplyError::Unsupported);
    }
    Ok(parsed)
}

async fn check_grant(
    tx: &impl GenericClient,
    installation: Uuid,
    workspace: Uuid,
    agent: Uuid,
    external: &str,
    shadow: Option<Uuid>,
    grant: Option<i64>,
) -> Result<(), ReplyError> {
    let row = tx.query_opt("SELECT 1 FROM zbs_telegram_guest_grant g JOIN \"user\" u ON u.id=g.shadow_user_id AND u.zbs_is_shadow WHERE g.installation_id=$1 AND g.workspace_id=$2 AND g.agent_id=$3 AND g.telegram_user_id=$4 AND g.shadow_user_id=$5 AND g.grant_generation=$6 AND g.revoked_at IS NULL FOR SHARE OF g,u", &[&installation,&workspace,&agent,&external,&shadow,&grant]).await.map_err(|_| ReplyError::Database)?;
    if row.is_none() {
        return Err(ReplyError::InvalidTask);
    }
    Ok(())
}

struct Target {
    installation: Uuid,
    binding: Uuid,
    chat: i64,
    reply_to: i64,
    token: String,
    text: String,
}

/// Reads only frozen task delivery, not an arbitrary session route or caller
/// payload. Lock installation through the provider call so rotate/revoke cannot
/// commit between last fence check and send. No token or answer is logged.
async fn load_target(
    tx: &Transaction<'_>,
    installer: &TelegramInstaller,
    task: Uuid,
) -> Result<Target, ReplyError> {
    let d = tx.query_opt(
        "SELECT task_id,binding_id,installation_id,channel_type,channel_chat_id,chat_type,channel_message_id,channel_thread_id,channel_sender_id,route_revision,config,zbs_guest_shadow_user_id,zbs_guest_grant_generation FROM channel_task_delivery WHERE task_id=$1 FOR SHARE",
        &[&task],
    ).await.map_err(|_| ReplyError::Database)?.ok_or(ReplyError::InvalidTask)?;
    if d.get::<_, String>("channel_type") != "telegram"
        || d.get::<_, String>("chat_type") != "p2p"
        || d.get::<_, Option<String>>("channel_thread_id").is_some()
        || d.get::<_, i64>("route_revision") <= 0
    {
        return Err(ReplyError::Unsupported);
    }
    let installation: Uuid = d.get("installation_id");
    let frozen: Value = d.get("config");
    let frozen_generation = generation(&json!({"generation":frozen["installation_generation"]}))?;
    let context_revision = frozen["context_revision"]
        .as_i64()
        .filter(|n| *n > 0)
        .ok_or(ReplyError::Unsupported)?;
    let i = tx.query_opt(
        "SELECT i.workspace_id,i.agent_id,i.config,a.owner_id,a.permission_mode FROM channel_installation i JOIN agent a ON a.id=i.agent_id AND a.workspace_id=i.workspace_id AND a.archived_at IS NULL JOIN workspace w ON w.id=i.workspace_id WHERE i.id=$1 AND i.channel_type='telegram' AND i.status='active' FOR SHARE OF i,a,w",
        &[&installation],
    ).await.map_err(|_| ReplyError::Database)?.ok_or(ReplyError::InvalidInstallation)?;
    let config: Value = i.get("config");
    if generation(&config)? != frozen_generation {
        return Err(ReplyError::InvalidInstallation);
    }
    let chat = d.get::<_, String>("channel_chat_id");
    let chat_id = private_chat(&chat)?;
    let reply = d
        .get::<_, Option<String>>("channel_message_id")
        .ok_or(ReplyError::Unsupported)?;
    let (c, m) = reply.split_once(':').ok_or(ReplyError::Unsupported)?;
    let reply_to = m.parse::<i64>().map_err(|_| ReplyError::Unsupported)?;
    if c != chat
        || reply_to <= 0
        || reply_to.to_string() != m
        || d.get::<_, Option<String>>("channel_sender_id").as_deref() != Some(chat.as_str())
    {
        return Err(ReplyError::Unsupported);
    }
    let s = tx.query_opt("SELECT s.id,s.workspace_id,s.agent_id,s.creator_id,s.status,s.zbs_source_channel_type,s.zbs_source_channel_chat_type,t.agent_id AS task_agent_id,t.status AS task_status,t.initiator_user_id,t.channel_context_revision,t.retry_of_task_id FROM agent_task_queue t JOIN chat_session s ON s.id=t.chat_session_id WHERE t.id=$1 FOR SHARE OF t,s", &[&task]).await.map_err(|_| ReplyError::Database)?.ok_or(ReplyError::InvalidTask)?;
    let session: Uuid = s.get("id");
    let principal: Option<Uuid> = s.get("initiator_user_id");
    if s.get::<_, Uuid>("workspace_id") != i.get::<_, Uuid>("workspace_id")
        || s.get::<_, Uuid>("agent_id") != i.get::<_, Uuid>("agent_id")
        || s.get::<_, Uuid>("task_agent_id") != i.get::<_, Uuid>("agent_id")
        || s.get::<_, Option<Uuid>>("retry_of_task_id").is_some()
        || s.get::<_, String>("task_status") != "completed"
        || s.get::<_, String>("status") != "active"
        || s.get::<_, Option<String>>("zbs_source_channel_type")
            .as_deref()
            != Some("telegram")
        || s.get::<_, Option<String>>("zbs_source_channel_chat_type")
            .as_deref()
            != Some("p2p")
        || s.get::<_, Option<i64>>("channel_context_revision") != Some(context_revision)
        || principal != Some(s.get("creator_id"))
    {
        return Err(ReplyError::InvalidTask);
    }
    let b = tx.query_opt("SELECT id,installation_id,channel_chat_id,channel_type,chat_type,route_revision,context_revision,retired_at,history_boundary_pending FROM channel_chat_session_binding WHERE id=$1 AND chat_session_id=$2 FOR SHARE", &[&d.get::<_,Uuid>("binding_id"),&session]).await.map_err(|_|ReplyError::Database)?.ok_or(ReplyError::InvalidTask)?;
    if b.get::<_, Uuid>("installation_id") != installation
        || b.get::<_, String>("channel_chat_id") != chat
        || b.get::<_, String>("channel_type") != "telegram"
        || b.get::<_, String>("chat_type") != "p2p"
        || b.get::<_, i64>("route_revision") != d.get::<_, i64>("route_revision")
        || b.get::<_, i64>("context_revision") != context_revision
        || b.get::<_, Option<chrono::DateTime<chrono::Utc>>>("retired_at")
            .is_some()
        || b.get::<_, bool>("history_boundary_pending")
    {
        return Err(ReplyError::InvalidTask);
    }
    let c = tx.query_opt("SELECT initiator_user_id,zbs_guest_shadow_user_id,zbs_guest_grant_generation,history_boundary_pending,pending_fresh FROM channel_chat_context_generation WHERE chat_session_id=$1 AND revision=$2 FOR SHARE", &[&session,&context_revision]).await.map_err(|_|ReplyError::Database)?.ok_or(ReplyError::InvalidTask)?;
    let guest: Option<Uuid> = d.get("zbs_guest_shadow_user_id");
    let grant: Option<i64> = d.get("zbs_guest_grant_generation");
    if guest.is_some() != grant.is_some()
        || grant.is_some_and(|g| g <= 0)
        || c.get::<_, Option<Uuid>>("initiator_user_id") != principal
        || c.get::<_, Option<Uuid>>("zbs_guest_shadow_user_id") != guest
        || c.get::<_, Option<i64>>("zbs_guest_grant_generation") != grant
        || c.get::<_, bool>("history_boundary_pending")
        || c.get::<_, bool>("pending_fresh")
    {
        return Err(ReplyError::InvalidTask);
    }
    let workspace: Uuid = i.get("workspace_id");
    let agent: Uuid = i.get("agent_id");
    if let Some(shadow) = guest {
        if Some(shadow) != principal {
            return Err(ReplyError::InvalidTask);
        }
        check_grant(tx, installation, workspace, agent, &chat, guest, grant).await?;
        let policy = tx.query_opt("SELECT 1 FROM agent a JOIN agent_invocation_target target ON target.agent_id=a.id AND target.target_type='workspace' AND target.target_id=$2 WHERE a.id=$1 AND a.permission_mode='public_to' FOR SHARE OF a,target", &[&agent,&workspace]).await.map_err(|_|ReplyError::Database)?;
        if policy.is_none() {
            return Err(ReplyError::InvalidTask);
        }
    } else {
        let sender = principal.ok_or(ReplyError::InvalidTask)?;
        let member = tx.query_opt("SELECT 1 FROM channel_user_binding b JOIN member m ON m.user_id=b.multica_user_id AND m.workspace_id=$2 JOIN \"user\" u ON u.id=m.user_id AND NOT u.zbs_is_shadow WHERE b.installation_id=$1 AND b.channel_type='telegram' AND b.workspace_id=$2 AND b.channel_user_id=$3 AND b.multica_user_id=$4 FOR SHARE OF b,m,u", &[&installation,&workspace,&chat,&sender]).await.map_err(|_|ReplyError::Database)?;
        if member.is_none() {
            return Err(ReplyError::InvalidTask);
        }
        if i.get::<_, Option<Uuid>>("owner_id") != Some(sender) {
            let permitted = tx.query_opt("SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND ((target_type='workspace' AND target_id=$3) OR (target_type='member' AND target_id=$2)) LIMIT 1 FOR SHARE", &[&agent,&sender,&workspace]).await.map_err(|_|ReplyError::Database)?;
            if i.get::<_, String>("permission_mode") != "public_to" || permitted.is_none() {
                return Err(ReplyError::InvalidTask);
            }
        }
    }
    let reply_message = tx.query_opt("SELECT content,channel_context_revision FROM chat_message WHERE task_id=$1 AND chat_session_id=$2 AND role='assistant' ORDER BY created_at DESC,id DESC LIMIT 1 FOR SHARE", &[&task,&session]).await.map_err(|_| ReplyError::Database)?;
    let message = reply_message.ok_or(ReplyError::InvalidTask)?;
    let count: i64 = tx.query_one("SELECT count(*) FROM chat_message WHERE task_id=$1 AND chat_session_id=$2 AND role='assistant'", &[&task,&session]).await.map_err(|_|ReplyError::Database)?.get(0);
    if count != 1
        || message.get::<_, Option<i64>>("channel_context_revision") != Some(context_revision)
    {
        return Err(ReplyError::Unsupported);
    }
    let text: String = message.get("content");
    if text.is_empty() || text.chars().count() > 4096 {
        return Err(ReplyError::Unsupported);
    }
    let sealed = config["bot_token_encrypted"]
        .as_str()
        .ok_or(ReplyError::InvalidInstallation)?;
    let token = String::from_utf8(
        installer
            .cipher
            .open(sealed)
            .map_err(|_| ReplyError::InvalidInstallation)?,
    )
    .map_err(|_| ReplyError::InvalidInstallation)?;
    let bot = parse_token(&token).map_err(|_| ReplyError::InvalidInstallation)?;
    if config["app_id"].as_str() != Some(bot) {
        return Err(ReplyError::InvalidInstallation);
    }
    Ok(Target {
        installation,
        binding: d.get("binding_id"),
        chat: chat_id,
        reply_to,
        token,
        text,
    })
}

/// One bounded terminal send per completed private text task. Telegram does
/// not offer sendMessage idempotency: a lost result becomes durable `unknown`,
/// never an automatic resend. External I/O happens ONLY after committed
/// `in_flight`, with installation/route/grant locks held across the call.
pub async fn deliver_completed_private_text(
    installer: &TelegramInstaller,
    pool: Arc<Pool>,
    task: Uuid,
) -> Result<ReplyOutcome, ReplyError> {
    let mut db = pool.get().await.map_err(|_| ReplyError::Database)?;
    let tx = db.transaction().await.map_err(|_| ReplyError::Database)?;
    tx.batch_execute("SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='5s'")
        .await
        .map_err(|_| ReplyError::Database)?;
    let target = match load_target(&tx, installer, task).await {
        Ok(t) => t,
        Err(ReplyError::Database) => return Err(ReplyError::Database),
        Err(_) => return Ok(ReplyOutcome::Denied),
    };
    let owner = Uuid::new_v4();
    let row = tx.query_opt(
        "INSERT INTO channel_reply_delivery(turn_id,task_id,attempt_depth,binding_id,installation_id,channel_type,chat_id,phase,send_state,owner_token,owner_expires_at) VALUES($1,$1,0,$2,$3,'telegram',$4,'terminal','none',$5,clock_timestamp()+interval '45 seconds') ON CONFLICT(turn_id) DO UPDATE SET owner_token=EXCLUDED.owner_token,owner_expires_at=EXCLUDED.owner_expires_at,updated_at=clock_timestamp() WHERE channel_reply_delivery.phase='terminal' AND channel_reply_delivery.task_id=$1 AND channel_reply_delivery.binding_id=$2 AND channel_reply_delivery.installation_id=$3 AND channel_reply_delivery.chat_id=$4 AND channel_reply_delivery.attempt_depth=0 AND (channel_reply_delivery.owner_token IS NULL OR channel_reply_delivery.owner_expires_at<=clock_timestamp()) RETURNING send_state,message_id,chunks_sent",
        &[&task,&target.binding,&target.installation,&target.chat.to_string(),&owner],
    ).await.map_err(|_|ReplyError::Database)?;
    let Some(row) = row else {
        let prior = tx
            .query_opt(
                "SELECT phase FROM channel_reply_delivery WHERE turn_id=$1",
                &[&task],
            )
            .await
            .map_err(|_| ReplyError::Database)?;
        return Ok(
            if prior.is_some_and(|r| r.get::<_, String>("phase") == "settled") {
                ReplyOutcome::AlreadySettled
            } else {
                ReplyOutcome::Busy
            },
        );
    };
    let state: String = row.get("send_state");
    if state == "in_flight" || state == "unknown" {
        tx.execute("UPDATE channel_reply_delivery SET send_state='unknown',phase='settled',settled_reason='send_result_unknown',owner_token=NULL,owner_expires_at=NULL WHERE turn_id=$1 AND owner_token=$2", &[&task,&owner]).await.map_err(|_|ReplyError::Database)?;
        tx.commit().await.map_err(|_| ReplyError::Database)?;
        return Ok(ReplyOutcome::Unknown);
    }
    if row.get::<_, i32>("chunks_sent") != 0
        || !row.get::<_, String>("message_id").is_empty()
        || state != "none"
    {
        return Ok(ReplyOutcome::Denied); // streaming/partial replies need the shared scheduler
    }
    tx.execute("UPDATE channel_reply_delivery SET send_state='in_flight',updated_at=clock_timestamp() WHERE turn_id=$1 AND owner_token=$2", &[&task,&owner]).await.map_err(|_|ReplyError::Database)?;
    tx.commit().await.map_err(|_| ReplyError::Database)?;

    let tx = db.transaction().await.map_err(|_| ReplyError::Database)?;
    tx.batch_execute("SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='5s'")
        .await
        .map_err(|_| ReplyError::Database)?;
    let target = match load_target(&tx, installer, task).await {
        Ok(t) => t,
        Err(ReplyError::Database) => return Err(ReplyError::Database),
        Err(_) => {
            tx.execute("UPDATE channel_reply_delivery SET phase='settled',settled_reason='policy_denied',send_state='unknown',owner_token=NULL,owner_expires_at=NULL WHERE turn_id=$1 AND owner_token=$2 AND send_state='in_flight'", &[&task,&owner]).await.map_err(|_|ReplyError::Database)?;
            tx.commit().await.map_err(|_| ReplyError::Database)?;
            return Ok(ReplyOutcome::Denied);
        }
    };
    let lease = tx.query_opt("SELECT 1 FROM channel_reply_delivery WHERE turn_id=$1 AND owner_token=$2 AND phase='terminal' AND send_state='in_flight' AND owner_expires_at>clock_timestamp()+interval '20 seconds' FOR UPDATE", &[&task,&owner]).await.map_err(|_|ReplyError::Database)?;
    if lease.is_none() {
        return Ok(ReplyOutcome::Unknown);
    }
    let outcome = installer.send_private_text(&target).await;
    let (result, message_id) = match outcome {
        SendResult::Accepted(message) => (ReplyOutcome::Delivered, Some(message)),
        SendResult::Refused => (ReplyOutcome::Refused, None),
        SendResult::Unknown => (ReplyOutcome::Unknown, None),
    };
    if let Some(message) = message_id {
        let id = message.to_string();
        let recorded = tx.execute("INSERT INTO channel_outbound_message(installation_id,channel_type,channel_message_id,binding_id,route_revision,task_id,outbound_kind) SELECT d.installation_id,'telegram',$2,d.binding_id,d.route_revision,$1,'reply' FROM channel_task_delivery d WHERE d.task_id=$1 ON CONFLICT(installation_id,channel_message_id) DO NOTHING", &[&task,&id]).await.map_err(|_|ReplyError::Database)?;
        if recorded != 1 {
            return Err(ReplyError::Database);
        }
        let settled = tx.execute("UPDATE channel_reply_delivery SET send_state='known',message_id=$3,chunks_sent=1,phase='settled',settled_reason='delivered',owner_token=NULL,owner_expires_at=NULL,updated_at=clock_timestamp() WHERE turn_id=$1 AND owner_token=$2 AND send_state='in_flight'", &[&task,&owner,&id]).await.map_err(|_| ReplyError::Database)?;
        if settled != 1 {
            return Err(ReplyError::Database);
        }
    } else {
        let (state, reason) = if result == ReplyOutcome::Refused {
            ("none", "send_refused")
        } else {
            ("unknown", "send_result_unknown")
        };
        tx.execute("UPDATE channel_reply_delivery SET send_state=$3,phase='settled',settled_reason=$4,owner_token=NULL,owner_expires_at=NULL,updated_at=clock_timestamp() WHERE turn_id=$1 AND owner_token=$2 AND send_state='in_flight'", &[&task,&owner,&state,&reason]).await.map_err(|_|ReplyError::Database)?;
    }
    tx.commit().await.map_err(|_| ReplyError::Database)?;
    Ok(result)
}

enum SendResult {
    Accepted(i64),
    Refused,
    Unknown,
}
impl TelegramInstaller {
    async fn send_private_text(&self, target: &Target) -> SendResult {
        let response = self.http.post(format!("{}/bot{}/sendMessage",self.origin,target.token))
            .timeout(std::time::Duration::from_secs(15))
            .header("content-type", "application/json")
            .body(json!({"chat_id":target.chat,"text":target.text,
                "reply_parameters":{"message_id":target.reply_to,"allow_sending_without_reply":true}}).to_string())
            .send().await;
        let Ok(mut response) = response else {
            return SendResult::Unknown;
        };
        let status = response.status();
        if status.is_client_error() {
            return SendResult::Refused;
        }
        if !status.is_success() || response.content_length().is_some_and(|n| n > 65536) {
            return SendResult::Unknown;
        }
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) if chunk.len() <= 65536 - bytes.len() => {
                    bytes.extend_from_slice(&chunk)
                }
                Ok(None) => break,
                _ => return SendResult::Unknown,
            }
        }
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
            return SendResult::Unknown;
        };
        if value["ok"] == false
            && value["error_code"]
                .as_u64()
                .is_some_and(|code| (400..500).contains(&code))
        {
            return SendResult::Refused;
        }
        if value["ok"] != true || value["result"]["chat"]["id"].as_i64() != Some(target.chat) {
            return SendResult::Unknown;
        }
        match value["result"]["message_id"].as_i64().filter(|id| *id > 0) {
            Some(id) => SendResult::Accepted(id),
            None => SendResult::Unknown,
        }
    }
}
