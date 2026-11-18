//! Private-chat guest pairing service. Adapter must verify sender and privacy
//! before calling; this module never accepts a browser-supplied user identity.
use super::channels_sprint::Channel;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use deadpool_postgres::Pool;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::{GenericClient, Transaction};
use uuid::Uuid;

pub struct VerifiedPrivateSender {
    pub channel: Channel,
    pub installation: Uuid,
    pub external_user_id: String,
    pub display_name: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum GuestPairError {
    Invalid,
    Throttled,
    AlreadyPaired,
    NotShared,
    Database,
}

#[derive(Debug, PartialEq, Eq)]
pub struct GuestPair {
    pub workspace: Uuid,
    pub agent: Uuid,
    pub shadow_user: Uuid,
    pub grant_generation: i64,
}

fn valid_code(code: &str) -> bool {
    code.len() == 43
        && code
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        && URL_SAFE_NO_PAD
            .decode(code)
            .is_ok_and(|data| data.len() == 32)
}

struct Tables {
    invites: &'static str,
    grants: &'static str,
    attempts: &'static str,
    user_column: &'static str,
    display_column: &'static str,
}
fn tables(channel: Channel) -> Result<Tables, GuestPairError> {
    let (invites, grants, attempts, user_column, display_column) = match channel {
        Channel::Telegram => (
            "zbs_telegram_guest_invite",
            "zbs_telegram_guest_grant",
            "zbs_telegram_guest_attempt",
            "telegram_user_id",
            "telegram_display_name",
        ),
        Channel::Zalo => (
            "zbs_zalo_guest_invite",
            "zbs_zalo_guest_grant",
            "zbs_zalo_guest_attempt",
            "zalo_user_id",
            "zalo_display_name",
        ),
        _ => return Err(GuestPairError::Invalid),
    };
    Ok(Tables {
        invites,
        grants,
        attempts,
        user_column,
        display_column,
    })
}
async fn charge_attempt<C: GenericClient + Sync>(
    db: &C,
    sender: &VerifiedPrivateSender,
    tables: &Tables,
) -> Result<(), GuestPairError> {
    if sender.external_user_id.trim().is_empty() {
        return Err(GuestPairError::Invalid);
    }
    let scope = format!("sender:{}:{}", sender.installation, sender.external_user_id);
    let scope_hash = hex::encode(Sha256::digest(scope.as_bytes()));
    let attempts = tables.attempts;
    let attempts: i32 = db.query_one(&format!("INSERT INTO {attempts}(scope_hash,window_start,attempts) VALUES ($1,to_timestamp(floor(extract(epoch from now())/900)*900),1) ON CONFLICT (scope_hash,window_start) DO UPDATE SET attempts={attempts}.attempts+1 RETURNING attempts"), &[&scope_hash]).await.map_err(|_|GuestPairError::Database)?.get(0);
    if attempts > 5 {
        Err(GuestPairError::Throttled)
    } else {
        Ok(())
    }
}

/// Pair only from a trusted private-chat adapter after sender authentication.
/// Rate charge precedes token lookup; invalid/replayed codes still consume budget.
/// All database failures deny pairing; no guest run may start without fence check.
pub async fn redeem_guest(
    pool: &Arc<Pool>,
    sender: &VerifiedPrivateSender,
    code: &str,
) -> Result<GuestPair, GuestPairError> {
    let tables = tables(sender.channel)?;
    let mut db = pool.get().await.map_err(|_| GuestPairError::Database)?;
    charge_attempt(&**db, sender, &tables).await?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| GuestPairError::Database)?;
    let pair = redeem_locked(&tx, sender, code, &tables).await?;
    tx.commit().await.map_err(|_| GuestPairError::Database)?;
    Ok(pair)
}

/// Connection-fenced ingress only. The caller holds the installation generation
/// lock and commits this together with its processed marker. Product denials
/// preserve the rate charge but roll back ALL redemption writes to a savepoint.
/// SQL failures must abort the outer transaction, never acknowledge an update.
pub(crate) async fn redeem_guest_in_transaction(
    tx: &Transaction<'_>,
    sender: &VerifiedPrivateSender,
    code: &str,
) -> Result<GuestPair, GuestPairError> {
    let tables = tables(sender.channel)?;
    charge_attempt(tx, sender, &tables).await?;
    tx.batch_execute("SAVEPOINT channels_sprint_pair_redeem")
        .await
        .map_err(|_| GuestPairError::Database)?;
    let result = redeem_locked(tx, sender, code, &tables).await;
    if result.is_err() {
        tx.batch_execute("ROLLBACK TO SAVEPOINT channels_sprint_pair_redeem")
            .await
            .map_err(|_| GuestPairError::Database)?;
    }
    tx.batch_execute("RELEASE SAVEPOINT channels_sprint_pair_redeem")
        .await
        .map_err(|_| GuestPairError::Database)?;
    result
}

async fn redeem_locked(
    tx: &Transaction<'_>,
    sender: &VerifiedPrivateSender,
    code: &str,
    tables: &Tables,
) -> Result<GuestPair, GuestPairError> {
    if !valid_code(code) {
        return Err(GuestPairError::Invalid);
    }
    let Tables {
        invites,
        grants,
        user_column,
        display_column,
        ..
    } = tables;
    let code_hash = hex::encode(Sha256::digest(code.as_bytes()));
    // Share lock fences rotation/revoke without upgrading the ingress generation
    // lock. Invite/grant row locks and the guarded upsert serialize contenders.
    let inst = tx.query_opt("SELECT workspace_id,agent_id FROM channel_installation WHERE id=$1 AND channel_type=$2 AND status='active' FOR SHARE", &[&sender.installation,&sender.channel.database_type()]).await.map_err(|_|GuestPairError::Database)?.ok_or(GuestPairError::Invalid)?;
    let workspace: Uuid = inst.get("workspace_id");
    let agent: Uuid = inst.get("agent_id");
    let invite=tx.query_opt(&format!("SELECT id,workspace_id,agent_id,issued_by_user_id,alias FROM {invites} WHERE installation_id=$1 AND code_hash=$2 AND consumed_at IS NULL AND revoked_at IS NULL AND expires_at>now() FOR UPDATE"), &[&sender.installation,&code_hash]).await.map_err(|_|GuestPairError::Database)?.ok_or(GuestPairError::Invalid)?;
    let issuer: Uuid = invite.get("issued_by_user_id");
    if invite.get::<_, Uuid>("workspace_id") != workspace
        || invite.get::<_, Uuid>("agent_id") != agent
    {
        return Err(GuestPairError::Invalid);
    }
    let valid_owner=tx.query_opt("SELECT 1 FROM agent a JOIN member m ON m.workspace_id=a.workspace_id AND m.user_id=a.owner_id JOIN \"user\" u ON u.id=m.user_id AND NOT u.zbs_is_shadow WHERE a.id=$1 AND a.workspace_id=$2 AND a.owner_id=$3 AND a.archived_at IS NULL FOR SHARE OF a,m,u", &[&agent,&workspace,&issuer]).await.map_err(|_|GuestPairError::Database)?.is_some();
    if !valid_owner {
        return Err(GuestPairError::Invalid);
    }
    let shared=tx.query_opt("SELECT 1 FROM agent a JOIN agent_invocation_target t ON t.agent_id=a.id WHERE a.id=$1 AND a.workspace_id=$2 AND a.permission_mode='public_to' AND t.target_type='workspace' AND t.target_id=$2 FOR SHARE OF a,t", &[&agent,&workspace]).await.map_err(|_|GuestPairError::Database)?.is_some();
    if !shared {
        return Err(GuestPairError::NotShared);
    }
    let existing_member=tx.query_opt("SELECT 1 FROM channel_user_binding WHERE installation_id=$1 AND channel_user_id=$2 LIMIT 1 FOR SHARE", &[&sender.installation,&sender.external_user_id]).await.map_err(|_|GuestPairError::Database)?.is_some();
    if existing_member {
        return Err(GuestPairError::AlreadyPaired);
    }
    let previous=tx.query_opt(&format!("SELECT shadow_user_id,workspace_id,agent_id,revoked_at FROM {grants} WHERE installation_id=$1 AND {user_column}=$2 FOR UPDATE"), &[&sender.installation,&sender.external_user_id]).await.map_err(|_|GuestPairError::Database)?;
    let alias: String = invite.get("alias");
    let shadow = if let Some(row) = previous {
        if row.get::<_, Uuid>("workspace_id") != workspace
            || row.get::<_, Uuid>("agent_id") != agent
        {
            return Err(GuestPairError::Invalid);
        }
        if row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("revoked_at")
            .is_none()
        {
            return Err(GuestPairError::AlreadyPaired);
        }
        let shadow: Uuid = row.get("shadow_user_id");
        let valid_shadow = tx
            .query_opt(
                "SELECT 1 FROM \"user\" WHERE id=$1 AND zbs_is_shadow FOR SHARE",
                &[&shadow],
            )
            .await
            .map_err(|_| GuestPairError::Database)?
            .is_some();
        if !valid_shadow {
            return Err(GuestPairError::Invalid);
        }
        shadow
    } else {
        let shadow = Uuid::new_v4();
        let email = format!("guest.{shadow}@channel.invalid");
        tx.execute(
            "INSERT INTO \"user\"(id,name,email,zbs_is_shadow) VALUES ($1,$2,$3,true)",
            &[&shadow, &alias, &email],
        )
        .await
        .map_err(|_| GuestPairError::Database)?;
        shadow
    };
    let invite_id: Uuid = invite.get("id");
    let row=tx.query_opt(&format!("INSERT INTO {grants}(installation_id,workspace_id,agent_id,{user_column},shadow_user_id,alias,{display_column},invite_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (installation_id,{user_column}) DO UPDATE SET alias=EXCLUDED.alias,{display_column}=EXCLUDED.{display_column},invite_id=EXCLUDED.invite_id,grant_generation={grants}.grant_generation+1,granted_at=now(),revoked_at=NULL,revoked_by_user_id=NULL WHERE {grants}.revoked_at IS NOT NULL AND {grants}.workspace_id=EXCLUDED.workspace_id AND {grants}.agent_id=EXCLUDED.agent_id AND {grants}.shadow_user_id=EXCLUDED.shadow_user_id RETURNING grant_generation"), &[&sender.installation,&workspace,&agent,&sender.external_user_id,&shadow,&alias,&sender.display_name,&invite_id]).await.map_err(|_|GuestPairError::Database)?.ok_or(GuestPairError::AlreadyPaired)?;
    tx.query_opt(&format!("UPDATE {invites} SET consumed_at=now() WHERE id=$1 AND installation_id=$2 AND code_hash=$3 AND consumed_at IS NULL AND revoked_at IS NULL AND expires_at>now() RETURNING id"), &[&invite_id,&sender.installation,&code_hash]).await.map_err(|_|GuestPairError::Database)?.ok_or(GuestPairError::Invalid)?;
    Ok(GuestPair {
        workspace,
        agent,
        shadow_user: shadow,
        grant_generation: row.get("grant_generation"),
    })
}
