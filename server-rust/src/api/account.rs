//! Account-scoped metadata reads from the frozen Go router.
use super::{Actor, ApiError, ApiState, GoJson, Identity, error, timestamp};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/tokens", get(tokens))
        .route("/api/tokens/", get(tokens))
        .route("/api/invitations", get(invitations))
        .route("/api/invitations/{id}", get(invitation))
}

#[derive(Serialize)]
struct Token {
    id: Uuid,
    name: String,
    token_prefix: String,
    expires_at: Option<String>,
    last_used_at: Option<String>,
    created_at: String,
}

async fn tokens(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
) -> Result<GoJson<Vec<Token>>, ApiError> {
    actor.require_human()?;
    let failed = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list tokens");
    let db = state.pool.get().await.map_err(|_| failed())?;
    let rows = db
        .query(
            "SELECT id, name, token_prefix, expires_at, last_used_at, created_at \
             FROM personal_access_token WHERE user_id = $1 AND NOT revoked ORDER BY created_at DESC",
            &[&user],
        )
        .await
        .map_err(|_| failed())?;
    Ok(GoJson(
        rows.iter()
            .map(|r| Token {
                id: r.get("id"),
                name: r.get("name"),
                token_prefix: r.get("token_prefix"),
                expires_at: r
                    .get::<_, Option<DateTime<Utc>>>("expires_at")
                    .map(timestamp),
                last_used_at: r
                    .get::<_, Option<DateTime<Utc>>>("last_used_at")
                    .map(timestamp),
                created_at: timestamp(r.get("created_at")),
            })
            .collect(),
    ))
}

#[derive(Serialize)]
pub(super) struct Invitation {
    id: Uuid,
    workspace_id: Uuid,
    inviter_id: Uuid,
    invitee_email: String,
    invitee_user_id: Option<Uuid>,
    role: String,
    status: String,
    created_at: String,
    updated_at: String,
    expires_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    workspace_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inviter_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inviter_email: Option<String>,
}

pub(super) fn invitation_row(r: &tokio_postgres::Row) -> Invitation {
    Invitation {
        id: r.get("id"),
        workspace_id: r.get("workspace_id"),
        inviter_id: r.get("inviter_id"),
        invitee_email: r.get("invitee_email"),
        invitee_user_id: r.get("invitee_user_id"),
        role: r.get("role"),
        status: r.get("status"),
        created_at: timestamp(r.get("created_at")),
        updated_at: timestamp(r.get("updated_at")),
        expires_at: timestamp(r.get("expires_at")),
        workspace_name: r
            .get::<_, Option<String>>("workspace_name")
            .filter(|s| !s.is_empty()),
        inviter_name: r
            .get::<_, Option<String>>("inviter_name")
            .filter(|s| !s.is_empty()),
        inviter_email: r
            .get::<_, Option<String>>("inviter_email")
            .filter(|s| !s.is_empty()),
    }
}

pub(super) const INVITATION_COLUMNS: &str = "wi.id, wi.workspace_id, wi.inviter_id, wi.invitee_email, wi.invitee_user_id, wi.role, wi.status, wi.created_at, wi.updated_at, wi.expires_at";

async fn invitations(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Vec<Invitation>>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?;
    let row = db
        .query_opt("SELECT email FROM \"user\" WHERE id = $1", &[&user])
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?
        .ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?;
    let email: String = row.get(0);
    let sql = format!(
        "SELECT {INVITATION_COLUMNS}, w.name AS workspace_name, u.name AS inviter_name, u.email AS inviter_email \
        FROM workspace_invitation wi JOIN workspace w ON w.id = wi.workspace_id JOIN \"user\" u ON u.id = wi.inviter_id \
        WHERE wi.status = 'pending' AND (wi.invitee_user_id = $1 OR wi.invitee_email = $2) AND wi.expires_at > now() \
        ORDER BY wi.created_at DESC"
    );
    let rows = db.query(&sql, &[&user, &email]).await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list invitations",
        )
    })?;
    Ok(GoJson(rows.iter().map(invitation_row).collect()))
}

async fn invitation(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw_id): Path<String>,
) -> Result<GoJson<Invitation>, ApiError> {
    let id = Uuid::parse_str(&raw_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid invitation id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "invitation not found"))?;
    // Go checks the invitation before comparing the invitee and intentionally
    // distinguishes an unknown ID (404) from somebody else's ID (403).
    let sql = format!(
        "SELECT {INVITATION_COLUMNS}, w.name AS workspace_name, u.name AS inviter_name, u.email AS inviter_email \
        FROM workspace_invitation wi LEFT JOIN workspace w ON w.id = wi.workspace_id \
        LEFT JOIN \"user\" u ON u.id = wi.inviter_id WHERE wi.id = $1"
    );
    let row = db
        .query_opt(&sql, &[&id])
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "invitation not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "invitation not found"))?;
    let invite = invitation_row(&row);
    let email_row = db
        .query_opt("SELECT email FROM \"user\" WHERE id = $1", &[&user])
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?
        .ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?;
    let email: String = email_row.get(0);
    if email.to_lowercase() != invite.invitee_email && invite.invitee_user_id != Some(user) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "invitation does not belong to you",
        ));
    }
    Ok(GoJson(invite))
}
