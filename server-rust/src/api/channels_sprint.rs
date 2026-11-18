//! Channel lifecycle against the frozen channel_* schema. No provider fallback.
use super::{Actor, ApiError, ApiState, GoJson, Identity, error};
use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{delete, get, post},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Channel {
    Lark,
    Slack,
    Wecom,
    DingTalk,
    Telegram,
    Zalo,
}

impl Channel {
    pub fn path(self) -> &'static str {
        match self {
            Self::Lark => "lark",
            Self::Slack => "slack",
            Self::Wecom => "wecom",
            Self::DingTalk => "dingtalk",
            Self::Telegram => "telegram",
            Self::Zalo => "zalo",
        }
    }
    pub fn database_type(self) -> &'static str {
        match self {
            Self::Lark => "feishu",
            _ => self.path(),
        }
    }
    pub(super) fn guest_tables(self) -> Result<(&'static str, &'static str), ApiError> {
        match self {
            Self::Telegram => Ok(("zbs_telegram_guest_invite", "zbs_telegram_guest_grant")),
            Self::Zalo => Ok(("zbs_zalo_guest_invite", "zbs_zalo_guest_grant")),
            _ => Err(error(StatusCode::NOT_FOUND, "guest channel not found")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChannelEvent {
    pub event_type: String,
    pub workspace_id: Uuid,
    pub actor_id: Uuid,
    pub installation_id: Uuid,
}

#[derive(Clone, Debug)]
pub struct GuestCancellation {
    pub workspace_id: Uuid,
    pub installation_id: Uuid,
    pub shadow_user_id: Uuid,
    pub grant_generation: i64,
}

/// Integrator must supply the real workspace event bus and guest-task canceller.
/// Never wire a no-op implementation in production. Revoke commits its fence
/// before best-effort cancellation; dispatch/token/delivery must check that fence.
#[async_trait]
pub trait ChannelRuntime: Send + Sync {
    fn configured(&self, channel: Channel) -> bool;
    fn publish(&self, event: ChannelEvent);
    async fn cancel_guest_tasks(&self, cancellation: GuestCancellation);
    fn telegram_installer(&self) -> Option<&super::channels_sprint_telegram::TelegramInstaller> {
        None
    }
    fn slack_installer(&self) -> Option<&super::channels_sprint_slack::SlackInstaller> {
        None
    }
    fn zalo_installer(&self) -> Option<&super::channels_sprint_zalo::ZaloInstaller> {
        None
    }
    fn dingtalk_installer(&self) -> Option<&super::channels_sprint_dingtalk::DingTalkInstaller> {
        None
    }
    fn wecom_installer(&self) -> Option<&super::channels_sprint_wecom::WecomInstaller> {
        None
    }
    fn lark_registrar(&self) -> Option<&dyn super::channels_sprint_lark_install::LarkRegistrar> {
        None
    }
}

pub type SharedChannelRuntime = Arc<dyn ChannelRuntime>;

pub(super) fn channel_time(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channels_sprint_go_times_are_utc_seconds() {
        let instant = DateTime::parse_from_rfc3339("2026-11-02T19:01:02.123+07:00")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(channel_time(instant), "2026-11-02T12:01:02Z");
        assert_eq!(Channel::Lark.database_type(), "feishu");
    }
}

pub(super) type Params = HashMap<String, String>;
/// Mirrors Go's `parseUUIDOrBadRequest`, including its per-field message.
pub(super) fn uuid_param(value: &str, field: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(value).map_err(|_| error(StatusCode::BAD_REQUEST, field))
}
pub(super) fn id(params: &Params, key: &str) -> Result<Uuid, ApiError> {
    let field = match key {
        "id" => "invalid workspace id",
        "installation" => "invalid installation id",
        "invite" => "invalid invite id",
        "shadow" => "invalid shadow user id",
        _ => "invalid resource id",
    };
    uuid_param(params.get(key).map_or("", String::as_str), field)
}
pub(super) fn db_error(_: tokio_postgres::Error) -> ApiError {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "channel database operation failed",
    )
}
#[derive(Clone, Copy)]
pub(super) enum Operation {
    Install,
    Revoke,
    Redeem,
    Guest,
}

pub(super) fn disabled(channel: Channel, operation: Operation) -> ApiError {
    let message = match (channel, operation) {
        (Channel::Lark, Operation::Install) => "lark install not configured",
        (Channel::Lark, _) => "lark integration not configured",
        (Channel::Slack, Operation::Install) => "slack integration not enabled",
        (Channel::Slack, _) => "slack integration not configured",
        (Channel::Wecom, Operation::Install | Operation::Revoke) => "wecom integration not enabled",
        (Channel::Wecom, _) => "wecom integration not configured",
        (Channel::DingTalk, Operation::Install) => "dingtalk integration not enabled",
        (Channel::DingTalk, _) => "dingtalk integration not configured",
        (Channel::Telegram, Operation::Install) => "telegram integration not enabled",
        (Channel::Telegram, Operation::Guest) => "telegram guest invitations are not configured",
        (Channel::Telegram, _) => "telegram integration not configured",
        (Channel::Zalo, Operation::Install) => "zalo integration not enabled",
        (Channel::Zalo, Operation::Guest) => "zalo guest invitations are not configured",
        (Channel::Zalo, _) => "zalo integration not configured",
    };
    error(StatusCode::FORBIDDEN, message)
}

pub(super) fn runtime_for(
    state: &ApiState,
    channel: Channel,
    operation: Operation,
) -> Result<&SharedChannelRuntime, ApiError> {
    state
        .channels
        .as_ref()
        .filter(|r| r.configured(channel))
        .ok_or_else(|| disabled(channel, operation))
}

pub(super) fn runtime(
    state: &ApiState,
    channel: Channel,
) -> Result<&SharedChannelRuntime, ApiError> {
    runtime_for(state, channel, Operation::Revoke)
}

pub(super) fn routes() -> Router<ApiState> {
    let mut routes = Router::new();
    for channel in [
        Channel::Lark,
        Channel::Slack,
        Channel::Wecom,
        Channel::DingTalk,
        Channel::Telegram,
        Channel::Zalo,
    ] {
        let root = format!("/api/workspaces/{{id}}/{}", channel.path());
        let mut group = Router::new()
            .route(&format!("{root}/installations"), get(list_installations))
            .route(
                &format!("{root}/installations/{{installation}}"),
                delete(revoke_installation),
            )
            .route(
                &format!("/api/{}/binding/redeem", channel.path()),
                post(redeem),
            );
        if channel == Channel::Lark {
            group = group.merge(super::channels_sprint_lark_install::routes(&root));
        }
        if matches!(channel, Channel::Telegram | Channel::Zalo) {
            group = group.merge(super::channels_sprint_guest::routes(&root));
        }
        if channel == Channel::Telegram {
            group = group.route(
                &format!("{root}/install"),
                post(super::channels_sprint_telegram::register),
            );
        }
        if channel == Channel::Slack {
            group = group.route(
                &format!("{root}/install/byo"),
                post(super::channels_sprint_slack::register_byo),
            );
        }
        if channel == Channel::Wecom {
            group = group.route(
                &format!("{root}/install/byo"),
                post(super::channels_sprint_wecom::register_byo)
                    .layer(axum::extract::DefaultBodyLimit::max(16 * 1024)),
            );
        }
        if channel == Channel::DingTalk {
            group = group.route(
                &format!("{root}/install/byo"),
                post(super::channels_sprint_dingtalk::register_byo),
            );
        }
        if channel == Channel::Zalo {
            group = group.route(
                &format!("{root}/install"),
                post(super::channels_sprint_zalo::register),
            );
        }
        routes = routes.merge(group.layer(Extension(channel)));
    }
    routes
}

pub(super) async fn membership<C: GenericClient + Sync>(
    db: &C,
    workspace: Uuid,
    user: Uuid,
) -> Result<String, ApiError> {
    db.query_opt("SELECT m.role FROM member m JOIN workspace w ON w.id=m.workspace_id JOIN \"user\" u ON u.id=m.user_id WHERE m.workspace_id=$1 AND m.user_id=$2 AND NOT u.zbs_is_shadow FOR SHARE OF m,w,u", &[&workspace,&user]).await.map_err(db_error)?
        .map(|r| r.get("role")).ok_or_else(|| error(StatusCode::FORBIDDEN, "workspace membership required"))
}
pub(super) fn admin(role: &str) -> bool {
    matches!(role, "owner" | "admin")
}

/// Workspace URL middleware runs before Go's channel handlers, even when the
/// channel integration is disabled. Keep that ordering for error contracts.
pub(super) async fn workspace_gate(
    state: &ApiState,
    actor: &Actor,
    user: Uuid,
    params: &Params,
    require_admin: bool,
) -> Result<Uuid, ApiError> {
    let raw = params.get("id").map_or("", String::as_str);
    if raw.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "workspace_id or workspace_slug is required",
        ));
    }
    actor.require_workspace_url(raw)?;
    let workspace = uuid_param(raw, "invalid workspace_id")?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let role = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&workspace, &user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .map(|r| r.get::<_, String>(0))
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    if require_admin && !admin(&role) {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    Ok(workspace)
}

pub(super) async fn agent_in_workspace<C: GenericClient + Sync>(
    db: &C,
    workspace: Uuid,
    agent: Uuid,
) -> Result<Uuid, ApiError> {
    db.query_opt(
        "SELECT owner_id FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user' FOR SHARE",
        &[&agent, &workspace],
    )
    .await
    .map_err(|_| error(StatusCode::NOT_FOUND, "agent not found in this workspace"))?
    .map(|row| row.get("owner_id"))
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found in this workspace"))
}

pub(super) async fn active_agent_in_workspace<C: GenericClient + Sync>(
    db: &C,
    workspace: Uuid,
    agent: Uuid,
) -> Result<Uuid, ApiError> {
    db.query_opt(
        "SELECT owner_id FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user' AND archived_at IS NULL FOR SHARE",
        &[&agent, &workspace],
    )
    .await
    .map_err(|_| error(StatusCode::NOT_FOUND, "agent not found in this workspace"))?
    .map(|row| row.get("owner_id"))
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found in this workspace"))
}

pub(super) async fn authorize_install<C: GenericClient + Sync>(
    db: &C,
    workspace: Uuid,
    agent: Uuid,
    user: Uuid,
    require_admin: bool,
) -> Result<(), ApiError> {
    let owner = active_agent_in_workspace(db, workspace, agent).await?;
    let role = membership(db, workspace, user).await?;
    if require_admin && !admin(&role) {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if !admin(&role) && owner != user {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only the agent owner can manage this agent",
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
struct Redeem {
    token: String,
}

async fn redeem(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(channel): Extension<Channel>,
    body: Result<Json<Redeem>, axum::extract::rejection::JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    runtime_for(&state, channel, Operation::Redeem)?;
    let Json(body) = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if body.token.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "token is required"));
    }
    let hash = hex::encode(Sha256::digest(body.token.as_bytes()));
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let tx = db.transaction().await.map_err(db_error)?;
    // Peek without locking the token, then lock installation first. Revoke and
    // redeem use this order, so deleting pending tokens on revoke cannot deadlock.
    let invalid = || error(StatusCode::GONE, "binding token invalid or expired");
    let token = tx.query_opt("SELECT workspace_id,installation_id FROM channel_binding_token WHERE token_hash=$1 AND channel_type=$2 AND consumed_at IS NULL AND expires_at>now()", &[&hash,&channel.database_type()]).await.map_err(db_error)?.ok_or_else(invalid)?;
    let workspace: Uuid = token.get("workspace_id");
    let installation: Uuid = token.get("installation_id");
    let inst = tx.query_opt("SELECT agent_id FROM channel_installation WHERE id=$1 AND workspace_id=$2 AND channel_type=$3 AND status='active' FOR SHARE", &[&installation,&workspace,&channel.database_type()]).await.map_err(db_error)?.ok_or_else(invalid)?;
    if tx
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&workspace, &user],
        )
        .await
        .map_err(db_error)?
        .is_none()
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "binding refused (are you a workspace member?)",
        ));
    }
    // Telegram/Zalo add invocation policy to the membership gate. Admin is not
    // an invocation bypass. Agent/target row locks serialize policy revocation.
    if matches!(channel, Channel::Telegram | Channel::Zalo) {
        let agent_id: Uuid = inst.get("agent_id");
        let agent = tx.query_opt("SELECT owner_id,permission_mode FROM agent WHERE id=$1 AND workspace_id=$2 AND archived_at IS NULL FOR SHARE", &[&agent_id,&workspace]).await.map_err(db_error)?.ok_or_else(invalid)?;
        if agent.get::<_, Uuid>("owner_id") != user {
            let allowed = agent.get::<_, String>("permission_mode") == "public_to" && tx.query_opt("SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND ((target_type='workspace' AND target_id=$2) OR (target_type='member' AND target_id=$3)) LIMIT 1 FOR SHARE", &[&agent_id,&workspace,&user]).await.map_err(db_error)?.is_some();
            if !allowed {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "you do not have permission to use this agent",
                ));
            }
        }
    }
    let token = tx.query_opt("UPDATE channel_binding_token SET consumed_at=now() WHERE token_hash=$1 AND channel_type=$2 AND workspace_id=$3 AND installation_id=$4 AND consumed_at IS NULL AND expires_at>now() RETURNING channel_user_id", &[&hash,&channel.database_type(),&workspace,&installation]).await.map_err(db_error)?.ok_or_else(invalid)?;
    let external: String = token.get("channel_user_id");
    let union = json!({});
    let bound = tx.query_opt("INSERT INTO channel_user_binding (workspace_id,multica_user_id,installation_id,channel_type,channel_user_id,config) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (installation_id,channel_user_id) DO UPDATE SET config=channel_user_binding.config || jsonb_strip_nulls(EXCLUDED.config),bound_at=now() WHERE channel_user_binding.multica_user_id=EXCLUDED.multica_user_id AND channel_user_binding.workspace_id=EXCLUDED.workspace_id AND channel_user_binding.channel_type=EXCLUDED.channel_type RETURNING id", &[&workspace,&user,&installation,&channel.database_type(),&external,&union]).await.map_err(db_error)?;
    if bound.is_none() {
        let message = match channel {
            Channel::Lark => "this Lark account is already bound to a different Multica user",
            Channel::Slack => "this Slack account is already bound to a different Multica user",
            Channel::Wecom => "this WeCom user is already bound to a different Multica user",
            Channel::DingTalk => {
                "this DingTalk account is already bound to a different Multica user"
            }
            Channel::Telegram => {
                "this Telegram account is already bound to a different Multica user"
            }
            Channel::Zalo => "this Zalo account is already bound to a different Multica user",
        };
        return Err(error(StatusCode::CONFLICT, message));
    }
    tx.commit().await.map_err(db_error)?;
    let identity_key = match channel {
        Channel::Lark => "lark_open_id",
        Channel::Slack => "slack_user_id",
        Channel::Wecom => "wecom_user_id",
        Channel::DingTalk => "dingtalk_user_id",
        Channel::Telegram => "telegram_user_id",
        Channel::Zalo => "zalo_user_id",
    };
    let mut response = json!({"workspace_id":workspace,"installation_id":installation});
    response[identity_key] = json!(external);
    Ok(GoJson(response))
}

pub(super) fn installation_json(row: &Row, channel: Channel) -> Value {
    let c: Value = row.get("config");
    let text = |key: &str| c.get(key).and_then(Value::as_str).unwrap_or("").to_owned();
    let mut value = json!({"id":row.get::<_,Uuid>("id"),"workspace_id":row.get::<_,Uuid>("workspace_id"),"agent_id":row.get::<_,Uuid>("agent_id"),"installer_user_id":row.get::<_,Uuid>("installer_user_id"),"status":row.get::<_,String>("status")});
    if channel != Channel::Wecom {
        for field in ["installed_at", "created_at", "updated_at"] {
            value[field] = json!(channel_time(row.get::<_, DateTime<Utc>>(field)));
        }
    }
    match channel {
        Channel::Lark => {
            value["app_id"] = json!(text("app_id"));
            value["bot_open_id"] = json!(text("bot_open_id"));
            value["region"] = json!(
                c.get("region")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("feishu")
            );
            if c.get("tenant_key").is_some_and(Value::is_string) {
                value["tenant_key"] = c["tenant_key"].clone();
            }
        }
        Channel::Slack => {
            value["team_id"] = json!(text("team_id"));
            value["bot_user_id"] = json!(text("bot_user_id"));
        }
        Channel::Wecom => {
            value["bot_id"] = json!(text("bot_id"));
        }
        Channel::Telegram => {
            value["bot_id"] = json!(text("app_id"));
            value["bot_username"] = json!(text("bot_username"));
        }
        Channel::Zalo => {
            // Frozen Go projects the verified routing slot as bot_id.
            value["bot_id"] = json!(text("app_id"));
            value["bot_name"] = json!(text("bot_name"));
            value["account_name"] = json!(text("account_name"));
            value["share_link"] = json!(text("share_link"));
        }
        Channel::DingTalk => {
            value["agent_available"] = json!(row.get::<_, bool>("agent_available"));
        }
    }
    value
}

async fn list_installations(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(channel): Extension<Channel>,
    Path(params): Path<Params>,
) -> Result<GoJson<Value>, ApiError> {
    let workspace = workspace_gate(&state, &actor, user, &params, false).await?;
    actor.require_human()?;
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let role = membership(&**db, workspace, user).await?;
    let configured = runtime(&state, channel).is_ok();
    if !configured {
        return Ok(GoJson(
            json!({"installations":[],"configured":false,"install_supported":false}),
        ));
    }
    let rows = db.query("SELECT ci.*, (a.id IS NOT NULL) AS agent_available FROM channel_installation ci LEFT JOIN agent a ON a.id=ci.agent_id AND a.workspace_id=ci.workspace_id AND a.archived_at IS NULL WHERE ci.workspace_id=$1 AND ci.channel_type=$2 AND ($3::bool OR $2<>'dingtalk' OR (a.id IS NOT NULL AND (a.owner_id=$4 OR (a.permission_mode='public_to' AND EXISTS(SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND ((t.target_type='workspace' AND t.target_id=$1) OR (t.target_type='member' AND t.target_id=$4))))))) ORDER BY ci.created_at DESC", &[&workspace,&channel.database_type(),&admin(&role),&user]).await.map_err(db_error)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut value = installation_json(row, channel);
        if channel == Channel::DingTalk && admin(&role) {
            let installation: Uuid = row.get("id");
            let bindings = db.query("SELECT channel_user_id FROM channel_user_binding WHERE installation_id=$1 AND workspace_id=$2 AND multica_user_id=$3 AND channel_type='dingtalk' ORDER BY bound_at DESC", &[&installation,&workspace,&user]).await.map_err(db_error)?;
            if !bindings.is_empty() {
                value["bound_dingtalk_user_ids"] = json!(
                    bindings
                        .iter()
                        .map(|r| r.get::<_, String>(0))
                        .collect::<Vec<_>>()
                );
            }
        }
        out.push(value);
    }
    // Installation providers are a separate gate; persisted lifecycle support
    // must not advertise an unwired provider as installable.
    Ok(GoJson(
        json!({"installations":out,"configured":true,"install_supported":false}),
    ))
}

async fn revoke_installation(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(channel): Extension<Channel>,
    Path(params): Path<Params>,
) -> Result<StatusCode, ApiError> {
    let workspace = workspace_gate(
        &state,
        &actor,
        user,
        &params,
        matches!(
            channel,
            Channel::Slack | Channel::Wecom | Channel::Telegram | Channel::Zalo
        ),
    )
    .await?;
    actor.require_human()?;
    let rt = runtime(&state, channel)?;
    let installation = id(&params, "installation")?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let tx = db.transaction().await.map_err(db_error)?;
    let row = tx.query_opt("SELECT ci.agent_id,a.owner_id FROM channel_installation ci LEFT JOIN agent a ON a.id=ci.agent_id AND a.workspace_id=ci.workspace_id WHERE ci.id=$1 AND ci.workspace_id=$2 AND ci.channel_type=$3 FOR UPDATE OF ci", &[&installation,&workspace,&channel.database_type()]).await.map_err(db_error)?.ok_or_else(||error(StatusCode::NOT_FOUND, match channel {
        Channel::Lark => "lark installation not found",
        Channel::Slack => "slack installation not found",
        Channel::Wecom => "wecom installation not found",
        Channel::DingTalk => "dingtalk installation not found",
        Channel::Telegram => "telegram installation not found",
        Channel::Zalo => "zalo installation not found",
    }))?;
    let role = membership(&*tx, workspace, user).await?;
    let may_manage = admin(&role)
        || (matches!(channel, Channel::Lark | Channel::DingTalk)
            && row.get::<_, Option<Uuid>>("owner_id") == Some(user));
    if !may_manage {
        return Err(error(
            StatusCode::FORBIDDEN,
            "insufficient permission to revoke installation",
        ));
    }
    tx.execute("UPDATE channel_installation SET status='revoked',updated_at=now() WHERE id=$1 AND workspace_id=$2 AND channel_type=$3", &[&installation,&workspace,&channel.database_type()]).await.map_err(db_error)?;
    // Unlike old status-only revoke, pending pairing links cannot resurrect
    // after reconnect. Lock order is installation, then token throughout.
    tx.execute("DELETE FROM channel_binding_token WHERE installation_id=$1 AND workspace_id=$2 AND channel_type=$3 AND consumed_at IS NULL", &[&installation,&workspace,&channel.database_type()]).await.map_err(db_error)?;
    // A polling/stream loop pinned to the old generation cannot finish
    // after the exclusive installation lock commits. Clear orphaned claims
    // and queued replies atomically; retain outbound history and chats.
    tx.execute(
        "DELETE FROM channel_inbound_message_dedup WHERE installation_id=$1",
        &[&installation],
    )
    .await
    .map_err(db_error)?;
    if channel == Channel::Telegram {
        tx.execute(
            "DELETE FROM channel_pairing_notice WHERE installation_id=$1 AND workspace_id=$2",
            &[&installation, &workspace],
        )
        .await
        .map_err(db_error)?;
    }
    tx.execute(
        "DELETE FROM channel_task_delivery WHERE installation_id=$1 AND channel_type=$2",
        &[&installation, &channel.database_type()],
    )
    .await
    .map_err(db_error)?;
    let mut cancellations = Vec::new();
    if matches!(channel, Channel::Telegram | Channel::Zalo) {
        let (invites, grants) = channel.guest_tables()?;
        tx.execute(&format!("UPDATE {invites} SET revoked_at=now(),revoked_by_user_id=$3 WHERE installation_id=$1 AND workspace_id=$2 AND consumed_at IS NULL AND revoked_at IS NULL"), &[&installation,&workspace,&user]).await.map_err(db_error)?;
        let revoked = tx.query(&format!("UPDATE {grants} SET revoked_at=now(),revoked_by_user_id=$3 WHERE installation_id=$1 AND workspace_id=$2 AND revoked_at IS NULL RETURNING shadow_user_id,grant_generation"), &[&installation,&workspace,&user]).await.map_err(db_error)?;
        for grant in revoked {
            cancellations.push(GuestCancellation {
                workspace_id: workspace,
                installation_id: installation,
                shadow_user_id: grant.get("shadow_user_id"),
                grant_generation: grant.get("grant_generation"),
            });
        }
    }
    tx.commit().await.map_err(db_error)?;
    for cancellation in cancellations {
        rt.cancel_guest_tasks(cancellation).await;
    }
    rt.publish(ChannelEvent {
        event_type: format!("{}_installation:revoked", channel.path()),
        workspace_id: workspace,
        actor_id: user,
        installation_id: installation,
    });
    Ok(StatusCode::NO_CONTENT)
}
