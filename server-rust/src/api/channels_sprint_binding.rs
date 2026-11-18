//! Hash-only member-binding token minting for authenticated private ingress.
//! No HTTP route, provider authentication or external delivery is supplied here.
use super::{channels_sprint::Channel, channels_sprint_pair::VerifiedPrivateSender};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub enum BindingMintError {
    InvalidSender,
    InstallationUnavailable,
    AlreadyBound,
    RandomUnavailable,
    Database,
}

/// Raw bearer secrets intentionally have neither Debug nor Serialize. A fresh
/// secret may only be delivered to the verified sender privately, never to a
/// group or workspace stream. A throttled mint cannot recover an earlier secret.
pub enum BindingMint {
    Fresh {
        raw: String,
        expires_at: DateTime<Utc>,
    },
    Throttled {
        expires_at: DateTime<Utc>,
    },
}

/// Caller must authenticate provider ingress and verify private-chat identity
/// before constructing `sender`. This struct is a trust contract, not proof of
/// authentication. Browser request fields must never reach this function.
/// Installation locking serializes against revoke; WeCom's one-minute burst
/// throttle is durable and serialized per sender, including concurrent workers.
/// Only SHA-256 hashes persist. Redemption still requires human membership and,
/// for Telegram/Zalo, agent invocation permission in the same transaction.
pub async fn mint_binding(
    pool: &Arc<Pool>,
    workspace: Uuid,
    sender: &VerifiedPrivateSender,
) -> Result<BindingMint, BindingMintError> {
    // Stream adapters must use their epoch-bearing entry points. Old streams
    // cannot mint fresh links after an installation rotates or is revoked.
    let _ = (pool, workspace, sender);
    // All supported channels require a connection-pinned generation.
    Err(BindingMintError::InvalidSender)
}

/// Epoch captured before opening the authenticated Telegram long-poll connection.
pub async fn mint_telegram_binding(
    pool: &Arc<Pool>,
    workspace: Uuid,
    sender: &VerifiedPrivateSender,
    generation: Uuid,
) -> Result<BindingMint, BindingMintError> {
    if sender.channel != Channel::Telegram {
        return Err(BindingMintError::InvalidSender);
    }
    mint_inner(pool, workspace, sender, generation).await
}

/// Epoch captured before opening the authenticated Slack Socket Mode connection.
pub async fn mint_slack_binding(
    pool: &Arc<Pool>,
    workspace: Uuid,
    sender: &VerifiedPrivateSender,
    generation: Uuid,
) -> Result<BindingMint, BindingMintError> {
    if sender.channel != Channel::Slack {
        return Err(BindingMintError::InvalidSender);
    }
    mint_inner(pool, workspace, sender, generation).await
}

/// Epoch captured before opening the authenticated Zalo long-poll connection.
pub async fn mint_zalo_binding(
    pool: &Arc<Pool>,
    workspace: Uuid,
    sender: &VerifiedPrivateSender,
    generation: Uuid,
) -> Result<BindingMint, BindingMintError> {
    if sender.channel != Channel::Zalo {
        return Err(BindingMintError::InvalidSender);
    }
    mint_inner(pool, workspace, sender, generation).await
}

/// Epoch is captured before opening the authenticated WeCom stream, never
/// reloaded per message. This function is not a public/browser auth boundary.
pub async fn mint_wecom_binding(
    pool: &Arc<Pool>,
    workspace: Uuid,
    sender: &VerifiedPrivateSender,
    generation: Uuid,
) -> Result<BindingMint, BindingMintError> {
    if sender.channel != Channel::Wecom {
        return Err(BindingMintError::InvalidSender);
    }
    mint_inner(pool, workspace, sender, generation).await
}

/// Epoch captured before opening the authenticated Lark stream.
pub async fn mint_lark_binding(
    pool: &Arc<Pool>,
    workspace: Uuid,
    sender: &VerifiedPrivateSender,
    generation: Uuid,
) -> Result<BindingMint, BindingMintError> {
    if sender.channel != Channel::Lark {
        return Err(BindingMintError::InvalidSender);
    }
    mint_inner(pool, workspace, sender, generation).await
}

/// Epoch captured before opening the authenticated DingTalk stream.
pub async fn mint_dingtalk_binding(
    pool: &Arc<Pool>,
    workspace: Uuid,
    sender: &VerifiedPrivateSender,
    generation: Uuid,
) -> Result<BindingMint, BindingMintError> {
    if sender.channel != Channel::DingTalk {
        return Err(BindingMintError::InvalidSender);
    }
    mint_inner(pool, workspace, sender, generation).await
}

async fn mint_inner(
    pool: &Arc<Pool>,
    workspace: Uuid,
    sender: &VerifiedPrivateSender,
    generation: Uuid,
) -> Result<BindingMint, BindingMintError> {
    if sender.external_user_id.trim().is_empty()
        || sender.external_user_id.len() > 1024
        || sender.external_user_id.chars().any(char::is_control)
    {
        return Err(BindingMintError::InvalidSender);
    }
    let mut db = pool.get().await.map_err(|_| BindingMintError::Database)?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| BindingMintError::Database)?;
    let epoch = generation.to_string();
    if tx.query_opt(
        "SELECT 1 FROM channel_installation ci JOIN agent a ON a.id=ci.agent_id AND a.workspace_id=ci.workspace_id AND a.archived_at IS NULL WHERE ci.id=$1 AND ci.workspace_id=$2 AND ci.channel_type=$3 AND ci.status='active' AND ci.config->>'generation'=$4 FOR SHARE OF ci,a",
        &[&sender.installation,&workspace,&sender.channel.database_type(),&epoch],
    ).await.map_err(|_| BindingMintError::Database)?.is_none() {
        return Err(BindingMintError::InstallationUnavailable);
    }
    if tx.query_opt(
        "SELECT 1 FROM channel_user_binding WHERE installation_id=$1 AND workspace_id=$2 AND channel_type=$3 AND channel_user_id=$4 FOR SHARE",
        &[&sender.installation,&workspace,&sender.channel.database_type(),&sender.external_user_id],
    ).await.map_err(|_| BindingMintError::Database)?.is_some() {
        return Err(BindingMintError::AlreadyBound);
    }
    if sender.channel == Channel::Wecom {
        // Hash collisions only over-serialize unrelated senders; they never
        // reuse another sender's token because the query below uses full keys.
        let scope = format!(
            "binding:{}:{}:{}",
            workspace, sender.installation, sender.external_user_id
        );
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&scope],
        )
        .await
        .map_err(|_| BindingMintError::Database)?;
        if let Some(row) = tx.query_opt(
            "SELECT expires_at FROM channel_binding_token WHERE installation_id=$1 AND workspace_id=$2 AND channel_type=$3 AND channel_user_id=$4 AND consumed_at IS NULL AND expires_at>now() AND created_at>now()-interval '1 minute' ORDER BY created_at DESC LIMIT 1 FOR SHARE",
            &[&sender.installation,&workspace,&sender.channel.database_type(),&sender.external_user_id],
        ).await.map_err(|_| BindingMintError::Database)? {
            let expires_at = row.get("expires_at");
            tx.commit().await.map_err(|_| BindingMintError::Database)?;
            return Ok(BindingMint::Throttled { expires_at });
        }
    }
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| BindingMintError::RandomUnavailable)?;
    let raw = URL_SAFE_NO_PAD.encode(bytes);
    let hash = hex::encode(Sha256::digest(raw.as_bytes()));
    let expires_at = tx.query_one(
        "INSERT INTO channel_binding_token(token_hash,workspace_id,installation_id,channel_type,channel_user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '15 minutes') RETURNING expires_at",
        &[&hash,&workspace,&sender.installation,&sender.channel.database_type(),&sender.external_user_id],
    ).await.map_err(|_| BindingMintError::Database)?.get(0);
    tx.commit().await.map_err(|_| BindingMintError::Database)?;
    Ok(BindingMint::Fresh { raw, expires_at })
}
