//! Telegram/Zalo guest invitation and grant fences; no outbound bot calls.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity,
    channels_sprint::{self, Channel, GuestCancellation, channel_time as timestamp},
    error,
};
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::get,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use getrandom::fill;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

type Params = HashMap<String, String>;

pub(super) fn routes(root: &str) -> Router<ApiState> {
    let base = format!("{root}/installations/{{installation}}");
    Router::new()
        .route(
            &format!("{base}/guest-invites"),
            get(list_invites).post(issue_invite),
        )
        .route(
            &format!("{base}/guest-invites/{{invite}}"),
            axum::routing::delete(revoke_invite),
        )
        .route(&format!("{base}/guests"), get(list_grants))
        .route(
            &format!("{base}/guests/{{shadow}}"),
            axum::routing::delete(revoke_grant),
        )
}

struct GuestAccess {
    workspace: Uuid,
    installation: Uuid,
    agent: Uuid,
    config: Value,
}

async fn guest_owner<C: GenericClient + Sync>(
    db: &C,
    channel: Channel,
    user: Uuid,
    params: &Params,
) -> Result<GuestAccess, ApiError> {
    let workspace = channels_sprint::id(params, "id")?;
    let installation = channels_sprint::id(params, "installation")?;
    // Go loads the installation without a status filter, then the agent, then
    // the owner check, then membership — in that order.
    let row = db.query_opt("SELECT agent_id,config FROM channel_installation WHERE id=$1 AND workspace_id=$2 AND channel_type=$3", &[&installation,&workspace,&channel.database_type()]).await.map_err(channels_sprint::db_error)?
        .ok_or_else(||error(StatusCode::NOT_FOUND, match channel {
            Channel::Telegram => "telegram installation not found",
            _ => "zalo installation not found",
        }))?;
    let agent: Uuid = row.get("agent_id");
    let owner = channels_sprint::agent_in_workspace(db, workspace, agent)
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "agent not found"))?;
    if owner != user {
        return Err(error(StatusCode::FORBIDDEN, "agent owner required"));
    }
    match db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&workspace, &user],
        )
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => {
            return Err(error(
                StatusCode::FORBIDDEN,
                "agent owner membership required",
            ));
        }
        Err(_) => {
            return Err(error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to check workspace membership",
            ));
        }
    }
    Ok(GuestAccess {
        workspace,
        installation,
        agent,
        config: row.get("config"),
    })
}

fn date(row: &Row, key: &str) -> Option<String> {
    row.get::<_, Option<DateTime<Utc>>>(key).map(timestamp)
}
fn invite_json(row: &Row) -> Value {
    json!({"id":row.get::<_,Uuid>("id"),"alias":row.get::<_,String>("alias"),
        "issued_by_user_id":row.get::<_,Uuid>("issued_by_user_id"),
        "created_at":timestamp(row.get::<_,DateTime<Utc>>("created_at")),
        "expires_at":timestamp(row.get::<_,DateTime<Utc>>("expires_at")),
        "consumed_at":date(row,"consumed_at"),"revoked_at":date(row,"revoked_at")})
}
fn grant_json(row: &Row, channel: Channel) -> Value {
    let mut v = json!({"shadow_user_id":row.get::<_,Uuid>("shadow_user_id"),"alias":row.get::<_,String>("alias"),
        "grant_generation":row.get::<_,i64>("grant_generation"),
        "granted_at":timestamp(row.get::<_,DateTime<Utc>>("granted_at")),"revoked_at":date(row,"revoked_at")});
    match channel {
        Channel::Telegram => {
            v["telegram_user_id"] = json!(row.get::<_, String>("telegram_user_id"));
            v["telegram_display_name"] = json!(row.get::<_, String>("telegram_display_name"));
        }
        Channel::Zalo => {
            v["zalo_user_id"] = json!(row.get::<_, String>("zalo_user_id"));
            v["zalo_display_name"] = json!(row.get::<_, String>("zalo_display_name"));
        }
        _ => unreachable!("guest routes only registered for Telegram and Zalo"),
    }
    v
}

async fn list_invites(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(channel): Extension<Channel>,
    Path(params): Path<Params>,
) -> Result<GoJson<Value>, ApiError> {
    channels_sprint::workspace_gate(&state, &actor, user, &params, false).await?;
    actor.require_human()?;
    channels_sprint::runtime_for(&state, channel, channels_sprint::Operation::Guest)?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let tx = db.transaction().await.map_err(channels_sprint::db_error)?;
    let access = guest_owner(&*tx, channel, user, &params).await?;
    let (table, _) = channel.guest_tables()?;
    let rows = tx.query(&format!("SELECT id,alias,issued_by_user_id,created_at,expires_at,consumed_at,revoked_at FROM {table} WHERE installation_id=$1 AND workspace_id=$2 ORDER BY created_at DESC LIMIT 200"), &[&access.installation,&access.workspace]).await.map_err(channels_sprint::db_error)?;
    Ok(GoJson(
        json!({"invites":rows.iter().map(invite_json).collect::<Vec<_>>()}),
    ))
}
async fn list_grants(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(channel): Extension<Channel>,
    Path(params): Path<Params>,
) -> Result<GoJson<Value>, ApiError> {
    channels_sprint::workspace_gate(&state, &actor, user, &params, false).await?;
    actor.require_human()?;
    channels_sprint::runtime_for(&state, channel, channels_sprint::Operation::Guest)?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let tx = db.transaction().await.map_err(channels_sprint::db_error)?;
    let access = guest_owner(&*tx, channel, user, &params).await?;
    let (_, table) = channel.guest_tables()?;
    let rows = tx.query(&format!("SELECT * FROM {table} WHERE installation_id=$1 AND workspace_id=$2 ORDER BY granted_at DESC LIMIT 500"), &[&access.installation,&access.workspace]).await.map_err(channels_sprint::db_error)?;
    Ok(GoJson(
        json!({"guests":rows.iter().map(|r|grant_json(r,channel)).collect::<Vec<_>>()}),
    ))
}
#[derive(Deserialize)]
struct Invite {
    alias: String,
}
async fn issue_invite(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(channel): Extension<Channel>,
    Path(params): Path<Params>,
    body: Result<Json<Invite>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    channels_sprint::workspace_gate(&state, &actor, user, &params, false).await?;
    actor.require_human()?;
    channels_sprint::runtime_for(&state, channel, channels_sprint::Operation::Guest)?;
    let Json(body) = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if body.alias.trim().is_empty() || body.alias.chars().count() > 80 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "alias must be 1–80 characters",
        ));
    }
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let tx = db.transaction().await.map_err(channels_sprint::db_error)?;
    let access = guest_owner(&*tx, channel, user, &params).await?;
    if channel == Channel::Telegram {
        let username = access
            .config
            .get("bot_username")
            .and_then(Value::as_str)
            .unwrap_or("");
        let valid = username.len() >= 5
            && username.len() <= 32
            && username.starts_with(|c: char| c.is_ascii_alphabetic())
            && username
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_');
        if !valid {
            return Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "telegram bot username unavailable",
            ));
        }
    }
    // Guest invocation is workspace-wide. Private or selectively shared agents
    // may never mint an invite that bypasses their own permission policy.
    let shared = tx.query_opt("SELECT 1 FROM agent a JOIN agent_invocation_target t ON t.agent_id=a.id WHERE a.id=$1 AND a.workspace_id=$2 AND a.permission_mode='public_to' AND t.target_type='workspace' AND t.target_id=$2 LIMIT 1 FOR SHARE OF a,t", &[&access.agent,&access.workspace]).await.map_err(channels_sprint::db_error)?.is_some();
    if !shared {
        return Err(error(
            StatusCode::CONFLICT,
            "agent_not_shared_with_workspace",
        ));
    }
    let mut raw = [0u8; 32];
    fill(&mut raw).map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "cannot generate invite code",
        )
    })?;
    let code = URL_SAFE_NO_PAD.encode(raw);
    let hash = hex::encode(Sha256::digest(code.as_bytes()));
    let (table, _) = channel.guest_tables()?;
    let row=tx.query_one(&format!("INSERT INTO {table}(id,workspace_id,installation_id,agent_id,code_hash,issued_by_user_id,alias,expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,now()+interval '24 hours') RETURNING id,alias,issued_by_user_id,created_at,expires_at,consumed_at,revoked_at"), &[&Uuid::new_v4(),&access.workspace,&access.installation,&access.agent,&hash,&user,&body.alias.trim()]).await.map_err(channels_sprint::db_error)?;
    tx.commit().await.map_err(channels_sprint::db_error)?;
    let mut payload = json!({"invite":invite_json(&row),"code":code});
    if channel == Channel::Telegram {
        let username = access
            .config
            .get("bot_username")
            .and_then(Value::as_str)
            .unwrap_or("");
        payload["deep_link"] = json!(format!("https://t.me/{username}?start={code}"));
    }
    Ok((StatusCode::CREATED, GoJson(payload)))
}
async fn revoke_invite(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(channel): Extension<Channel>,
    Path(params): Path<Params>,
) -> Result<StatusCode, ApiError> {
    channels_sprint::workspace_gate(&state, &actor, user, &params, false).await?;
    actor.require_human()?;
    channels_sprint::runtime_for(&state, channel, channels_sprint::Operation::Guest)?;
    let invite = channels_sprint::id(&params, "invite")?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let tx = db.transaction().await.map_err(channels_sprint::db_error)?;
    let access = guest_owner(&*tx, channel, user, &params).await?;
    let (table, _) = channel.guest_tables()?;
    tx.query_opt(&format!("UPDATE {table} SET revoked_at=COALESCE(revoked_at,now()),revoked_by_user_id=COALESCE(revoked_by_user_id,$4) WHERE id=$1 AND installation_id=$2 AND workspace_id=$3 RETURNING id"), &[&invite,&access.installation,&access.workspace,&user]).await.map_err(channels_sprint::db_error)?.ok_or_else(||error(StatusCode::NOT_FOUND,"guest invitation not found"))?;
    tx.commit().await.map_err(channels_sprint::db_error)?;
    Ok(StatusCode::NO_CONTENT)
}
async fn revoke_grant(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(channel): Extension<Channel>,
    Path(params): Path<Params>,
) -> Result<StatusCode, ApiError> {
    channels_sprint::workspace_gate(&state, &actor, user, &params, false).await?;
    actor.require_human()?;
    let rt = channels_sprint::runtime_for(&state, channel, channels_sprint::Operation::Guest)?;
    let shadow = channels_sprint::id(&params, "shadow")?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let tx = db.transaction().await.map_err(channels_sprint::db_error)?;
    let access = guest_owner(&*tx, channel, user, &params).await?;
    let (_, table) = channel.guest_tables()?;
    let grant=tx.query_opt(&format!("UPDATE {table} SET revoked_at=now(),revoked_by_user_id=$4 WHERE installation_id=$1 AND workspace_id=$2 AND shadow_user_id=$3 AND revoked_at IS NULL RETURNING grant_generation"), &[&access.installation,&access.workspace,&shadow,&user]).await.map_err(channels_sprint::db_error)?;
    if grant.is_none() {
        tx.query_opt(&format!("SELECT 1 FROM {table} WHERE installation_id=$1 AND workspace_id=$2 AND shadow_user_id=$3"), &[&access.installation,&access.workspace,&shadow]).await.map_err(channels_sprint::db_error)?.ok_or_else(||error(StatusCode::NOT_FOUND,"guest not found"))?;
    }
    let generation = grant.map(|row| row.get::<_, i64>("grant_generation"));
    tx.commit().await.map_err(channels_sprint::db_error)?;
    if let Some(grant_generation) = generation {
        rt.cancel_guest_tasks(GuestCancellation {
            workspace_id: access.workspace,
            installation_id: access.installation,
            shadow_user_id: shadow,
            grant_generation,
        })
        .await;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Lock in the same transaction that admits/claims a guest task or reserves an
/// outbound delivery. Lock order matches pairing/revoke: installation, grant,
/// shadow user. A plain read followed by a separate write is not a task fence.
pub async fn lock_guest_grant(
    tx: &tokio_postgres::Transaction<'_>,
    channel: Channel,
    workspace: Uuid,
    installation: Uuid,
    agent: Uuid,
    shadow: Uuid,
    generation: i64,
) -> Result<bool, tokio_postgres::Error> {
    let (_, table) = match channel.guest_tables() {
        Ok(v) => v,
        Err(_) => return Ok(false),
    };
    let active = tx.query_opt("SELECT id FROM channel_installation WHERE id=$1 AND workspace_id=$2 AND agent_id=$3 AND channel_type=$4 AND status='active' FOR SHARE", &[&installation,&workspace,&agent,&channel.database_type()]).await?.is_some();
    if !active {
        return Ok(false);
    }
    Ok(tx.query_opt(&format!("SELECT g.shadow_user_id FROM {table} g JOIN \"user\" u ON u.id=g.shadow_user_id AND u.zbs_is_shadow WHERE g.installation_id=$1 AND g.workspace_id=$2 AND g.agent_id=$3 AND g.shadow_user_id=$4 AND g.grant_generation=$5 AND g.revoked_at IS NULL FOR SHARE OF g,u"), &[&installation,&workspace,&agent,&shadow,&generation]).await?.is_some())
}

/// Call at admission, claim, retry, and *immediately before* outbound send.
/// Missing rows, revoked installs, wrong workspace/agent, shadow mismatch and
/// stale generations all fail closed. The integrator must wire these call sites.
pub async fn guest_grant_active<C: GenericClient + Sync>(
    db: &C,
    channel: Channel,
    workspace: Uuid,
    installation: Uuid,
    agent: Uuid,
    shadow: Uuid,
    generation: i64,
) -> Result<bool, tokio_postgres::Error> {
    let (_, table) = match channel.guest_tables() {
        Ok(v) => v,
        Err(_) => return Ok(false),
    };
    let row=db.query_one(&format!("SELECT EXISTS(SELECT 1 FROM {table} g JOIN channel_installation i ON i.id=g.installation_id JOIN \"user\" u ON u.id=g.shadow_user_id WHERE g.installation_id=$1 AND g.workspace_id=$2 AND g.agent_id=$3 AND g.shadow_user_id=$4 AND g.grant_generation=$5 AND g.revoked_at IS NULL AND i.status='active' AND i.channel_type=$6 AND i.workspace_id=g.workspace_id AND i.agent_id=g.agent_id AND u.zbs_is_shadow)"), &[&installation,&workspace,&agent,&shadow,&generation,&channel.database_type()]).await?;
    Ok(row.get(0))
}
