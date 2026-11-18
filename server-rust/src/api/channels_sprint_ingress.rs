//! Durable inbound idempotency for authenticated channel adapters.
//! Provider verification and chat persistence belong to the caller. No HTTP route
//! is registered here: an unauthenticated message must never acquire a claim.
use super::channels_sprint::Channel;
use deadpool_postgres::Pool;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio_postgres::Transaction;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundKey {
    pub workspace: Uuid,
    pub installation: Uuid,
    pub channel: Channel,
    pub message_id: String,
    /// Captured by the polling loop before receiving this message, never
    /// reloaded from the installation when processing a queued update.
    pub generation: Option<Uuid>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundClaim {
    pub key: InboundKey,
    pub token: Uuid,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClaimError {
    Invalid,
    Database,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClaimResult {
    Acquired(InboundClaim),
    Duplicate,
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 1024 && !id.chars().any(char::is_control)
}

fn valid_generation(key: &InboundKey) -> bool {
    key.generation.is_some()
}

/// Snapshot the installation epoch when starting a polling/stream connection.
/// Carry it with each queued update; reloading it while processing old work
/// would re-label a stale message as current. Legacy rows without an epoch fail
/// closed until an authorized installer establishes a fresh generation.
async fn connection_generation(
    pool: &Arc<Pool>,
    workspace: Uuid,
    installation: Uuid,
    channel: Channel,
) -> Result<Uuid, ClaimError> {
    let db = pool.get().await.map_err(|_| ClaimError::Database)?;
    let row = db.query_opt(
        "SELECT i.config->>'generation' FROM channel_installation i JOIN agent a ON a.id=i.agent_id AND a.workspace_id=i.workspace_id AND a.archived_at IS NULL WHERE i.id=$1 AND i.workspace_id=$2 AND i.channel_type=$3 AND i.status='active'",
        &[&installation, &workspace, &channel.database_type()],
    ).await.map_err(|_| ClaimError::Database)?.ok_or(ClaimError::Invalid)?;
    row.get::<_, Option<String>>(0)
        .and_then(|generation| {
            Uuid::parse_str(&generation)
                .ok()
                .filter(|id| id.to_string() == generation)
        })
        .ok_or(ClaimError::Invalid)
}

/// Capture before opening a Telegram long-poll connection.
pub async fn telegram_poll_generation(
    pool: &Arc<Pool>,
    workspace: Uuid,
    installation: Uuid,
) -> Result<Uuid, ClaimError> {
    connection_generation(pool, workspace, installation, Channel::Telegram).await
}

/// Capture before opening a Slack Socket Mode connection.
pub async fn slack_stream_generation(
    pool: &Arc<Pool>,
    workspace: Uuid,
    installation: Uuid,
) -> Result<Uuid, ClaimError> {
    connection_generation(pool, workspace, installation, Channel::Slack).await
}

pub async fn zalo_poll_generation(
    pool: &Arc<Pool>,
    workspace: Uuid,
    installation: Uuid,
) -> Result<Uuid, ClaimError> {
    connection_generation(pool, workspace, installation, Channel::Zalo).await
}

/// Worker captures this once before opening a WeCom stream. The authenticated
/// adapter must pass it to every inbound claim and transaction-local write.
pub async fn wecom_stream_generation(
    pool: &Arc<Pool>,
    workspace: Uuid,
    installation: Uuid,
) -> Result<Uuid, ClaimError> {
    connection_generation(pool, workspace, installation, Channel::Wecom).await
}

/// Capture before opening the authenticated Lark event stream.
pub async fn lark_stream_generation(
    pool: &Arc<Pool>,
    workspace: Uuid,
    installation: Uuid,
) -> Result<Uuid, ClaimError> {
    connection_generation(pool, workspace, installation, Channel::Lark).await
}

/// Capture before opening the authenticated DingTalk Stream connection.
pub async fn dingtalk_stream_generation(
    pool: &Arc<Pool>,
    workspace: Uuid,
    installation: Uuid,
) -> Result<Uuid, ClaimError> {
    connection_generation(pool, workspace, installation, Channel::DingTalk).await
}

/// Hold a share lock until commit while checking the same epoch captured before
/// polling. Call from task admission, durable delivery/outbox insertion, and
/// other transaction-local consumers before writing; false means roll back.
/// Provider sends cannot be made safe by this DB lock alone: sender must also
/// recheck installation status and credential epoch immediately before HTTP.
pub async fn current_in_transaction(
    tx: &Transaction<'_>,
    key: &InboundKey,
) -> Result<bool, ClaimError> {
    if !valid_generation(key) {
        return Err(ClaimError::Invalid);
    }
    if tx
        .query_opt(
            "SELECT id FROM workspace WHERE id=$1 FOR KEY SHARE",
            &[&key.workspace],
        )
        .await
        .map_err(|_| ClaimError::Database)?
        .is_none()
    {
        return Ok(false);
    }
    let generation = key.generation.map(|g| g.to_string());
    Ok(tx.query_opt(
        "SELECT 1 FROM channel_installation i JOIN agent a ON a.id=i.agent_id AND a.workspace_id=i.workspace_id AND a.archived_at IS NULL WHERE i.id=$1 AND i.workspace_id=$2 AND i.channel_type=$3 AND i.status='active' AND i.config->>'generation'=$4 FOR SHARE OF i,a",
        &[&key.installation, &key.workspace, &key.channel.database_type(), &generation],
    ).await.map_err(|_| ClaimError::Database)?.is_some())
}

/// Call only after validating provider signatures and resolving the authenticated
/// installation. Claim is scoped to one active installation and message ID. A
/// concurrent claim loses; an abandoned claim is reclaimable after 60 seconds.
/// This claim alone does not persist a message or mark it processed.
pub async fn claim_inbound(pool: &Arc<Pool>, key: InboundKey) -> Result<ClaimResult, ClaimError> {
    if !valid_id(&key.message_id) || !valid_generation(&key) {
        return Err(ClaimError::Invalid);
    }
    let mut db = pool.get().await.map_err(|_| ClaimError::Database)?;
    let tx = db.transaction().await.map_err(|_| ClaimError::Database)?;
    if !current_in_transaction(&tx, &key).await? {
        return Err(ClaimError::Invalid);
    }
    let token = Uuid::new_v4();
    let row = tx.query_opt(
        "INSERT INTO channel_inbound_message_dedup(installation_id,message_id,claim_token) VALUES($1,$2,$3) ON CONFLICT(installation_id,message_id) DO UPDATE SET received_at=now(),claim_token=EXCLUDED.claim_token WHERE channel_inbound_message_dedup.processed_at IS NULL AND channel_inbound_message_dedup.received_at < now()-interval '60 seconds' RETURNING claim_token",
        &[&key.installation, &key.message_id, &token],
    ).await.map_err(|_| ClaimError::Database)?;
    tx.commit().await.map_err(|_| ClaimError::Database)?;
    Ok(match row {
        Some(_) => ClaimResult::Acquired(InboundClaim { key, token }),
        None => ClaimResult::Duplicate,
    })
}

/// Call inside the SAME transaction that writes the authenticated chat message.
/// A false result means lost ownership, revoked installation or stale token:
/// caller MUST roll back its transaction, including the chat write. Holding the
/// installation share lock until commit fences a concurrent revoke. Publishing
/// or dispatching before the transaction commits is forbidden.
pub async fn finish_in_transaction(
    tx: &Transaction<'_>,
    claim: &InboundClaim,
) -> Result<bool, ClaimError> {
    let key = &claim.key;
    if !current_in_transaction(tx, key).await? {
        return Ok(false);
    }
    let updated = tx.execute(
        "UPDATE channel_inbound_message_dedup SET processed_at=now() WHERE installation_id=$1 AND message_id=$2 AND claim_token=$3 AND processed_at IS NULL",
        &[&key.installation, &key.message_id, &claim.token],
    ).await.map_err(|_| ClaimError::Database)?;
    Ok(updated == 1)
}

pub type InboundWork<'a, T> = Pin<Box<dyn Future<Output = Result<T, ClaimError>> + Send + 'a>>;

/// Preferred ingestion boundary. Callback may perform only transaction-local DB
/// writes: never provider requests, event publication or scheduler dispatch. It
/// is not called for a stale claim. Its writes and processed marker commit
/// together; callback/SQL failures roll back both. Publish only after Some(T).
/// None means replay/lost claim/revocation, not successful message ingestion.
pub async fn process_inbound<T, F>(
    pool: &Arc<Pool>,
    claim: &InboundClaim,
    persist: F,
) -> Result<Option<T>, ClaimError>
where
    F: for<'a, 'b> FnOnce(&'a Transaction<'b>) -> InboundWork<'a, T>,
{
    let mut db = pool.get().await.map_err(|_| ClaimError::Database)?;
    let tx = db.transaction().await.map_err(|_| ClaimError::Database)?;
    if !finish_in_transaction(&tx, claim).await? {
        return Ok(None);
    }
    let value = persist(&tx).await?;
    tx.commit().await.map_err(|_| ClaimError::Database)?;
    Ok(Some(value))
}

/// Release only after rollback of every side effect, for an infrastructure
/// failure before durable chat persistence. Epoch and claim token both fence
/// later owners. Stale/revoked claims cannot mutate dedup state.
pub async fn release_inbound(pool: &Arc<Pool>, claim: &InboundClaim) -> Result<bool, ClaimError> {
    let mut db = pool.get().await.map_err(|_| ClaimError::Database)?;
    let tx = db.transaction().await.map_err(|_| ClaimError::Database)?;
    if !current_in_transaction(&tx, &claim.key).await? {
        return Ok(false);
    }
    let deleted = tx.execute(
        "DELETE FROM channel_inbound_message_dedup WHERE installation_id=$1 AND message_id=$2 AND claim_token=$3 AND processed_at IS NULL",
        &[&claim.key.installation, &claim.key.message_id, &claim.token],
    ).await.map_err(|_| ClaimError::Database)? == 1;
    tx.commit().await.map_err(|_| ClaimError::Database)?;
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channels_sprint_stream_channels_require_explicit_generation() {
        for channel in [
            Channel::Lark,
            Channel::Slack,
            Channel::Wecom,
            Channel::DingTalk,
            Channel::Telegram,
            Channel::Zalo,
        ] {
            let mut key = InboundKey {
                workspace: Uuid::new_v4(),
                installation: Uuid::new_v4(),
                channel,
                message_id: "synthetic-message".into(),
                generation: None,
            };
            assert!(!valid_generation(&key));
            key.generation = Some(Uuid::new_v4());
            assert!(valid_generation(&key));
        }
    }
}
