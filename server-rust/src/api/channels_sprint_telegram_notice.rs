//! Static pairing notices: durable before HTTP, at-most-one attempted send.
//! Only a pending directive may be recovered. An attempted send may have reached
//! Telegram even if its process died; never retry that indeterminate outcome.
use super::{
    TelegramInstaller,
    pairing::{PairingDelivery, PairingNotice, send_notice},
    polling::PollError,
};
use crate::api::channels_sprint_ingress::{ClaimError, InboundKey, current_in_transaction};
use deadpool_postgres::{ClientWrapper, Pool};
use std::sync::Arc;
use tokio_postgres::Transaction;

pub async fn record(
    tx: &Transaction<'_>,
    key: &InboundKey,
    message: String,
    chat: i64,
    notice: &PairingNotice,
) -> Result<(), ClaimError> {
    let kind = match notice {
        PairingNotice::Paired => "paired",
        PairingNotice::Invalid => "invalid",
        PairingNotice::Unavailable => "unavailable",
        PairingNotice::Silent => return Ok(()),
    };
    let generation = key.generation.ok_or(ClaimError::Invalid)?;
    tx.execute(
        "INSERT INTO channel_pairing_notice(workspace_id,installation_id,generation,message_id,chat_id,kind) VALUES($1,$2,$3,$4,$5,$6)",
        &[&key.workspace, &key.installation, &generation, &message, &chat, &kind],
    ).await.map_err(|_| ClaimError::Database)?;
    Ok(())
}

async fn settle(pool: &Arc<Pool>, key: &InboundKey, message: &str, result: &str) {
    if let Ok(db) = pool.get().await {
        let _ = db.execute(
            "UPDATE channel_pairing_notice SET status=$4, updated_at=now() WHERE installation_id=$1 AND message_id=$2 AND generation=$3 AND status='attempted'",
            &[&key.installation, &message, &key.generation.unwrap(), &result],
        ).await;
    }
}

pub async fn deliver(
    installer: &TelegramInstaller,
    pool: &Arc<Pool>,
    lease: &ClientWrapper,
    key: &InboundKey,
    token: &str,
    message: &str,
) -> Result<PairingDelivery, PollError> {
    // A successful claim commits before the provider call so restart and replay
    // cannot send twice. The lease is independent of the pooled connection.
    lease
        .query_one("SELECT 1", &[])
        .await
        .map_err(|_| PollError::LeaseLost)?;
    let mut db = pool.get().await.map_err(|_| PollError::Database)?;
    let tx = db.transaction().await.map_err(|_| PollError::Database)?;
    tx.batch_execute("SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='5s'")
        .await
        .map_err(|_| PollError::Database)?;
    if !current_in_transaction(&tx, key).await? {
        return Err(PollError::InvalidInstallation);
    }
    let generation = key.generation.ok_or(PollError::InvalidInstallation)?;
    let row = tx.query_opt(
        "UPDATE channel_pairing_notice SET status='attempted', updated_at=now() WHERE workspace_id=$1 AND installation_id=$2 AND generation=$3 AND message_id=$4 AND status='pending' RETURNING chat_id,kind",
        &[&key.workspace, &key.installation, &generation, &message],
    ).await.map_err(|_| PollError::Database)?;
    tx.commit().await.map_err(|_| PollError::Database)?;
    let Some(row) = row else {
        return Ok(PairingDelivery::NotSent);
    };
    let chat: i64 = row.get(0);
    let kind: String = row.get(1);
    let notice = match kind.as_str() {
        "paired" => PairingNotice::Paired,
        "invalid" => PairingNotice::Invalid,
        "unavailable" => PairingNotice::Unavailable,
        _ => return Ok(PairingDelivery::NotSent),
    };

    // Revalidate with locks held across HTTP; rotation/revoke/archive cannot
    // commit while this send is in progress. Never infer a new epoch or token.
    let Ok(mut db) = pool.get().await else {
        return Ok(PairingDelivery::NotSent);
    };
    let Ok(tx) = db.transaction().await else {
        return Ok(PairingDelivery::NotSent);
    };
    if tx
        .batch_execute("SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='5s'")
        .await
        .is_err()
    {
        return Ok(PairingDelivery::NotSent);
    }
    let result = match current_in_transaction(&tx, key).await {
        Ok(true) => {
            if lease.query_one("SELECT 1", &[]).await.is_err() {
                PairingDelivery::NotSent
            } else {
                send_notice(installer, token, chat, notice.text().unwrap()).await
            }
        }
        Ok(false) | Err(ClaimError::Invalid) => PairingDelivery::Denied,
        Err(ClaimError::Database) => PairingDelivery::NotSent,
    };
    let _ = tx.rollback().await;
    let status = match result {
        PairingDelivery::Delivered => "delivered",
        PairingDelivery::Refused => "refused",
        PairingDelivery::Denied => "denied",
        _ => "unknown",
    };
    settle(pool, key, message, status).await;
    Ok(result)
}
