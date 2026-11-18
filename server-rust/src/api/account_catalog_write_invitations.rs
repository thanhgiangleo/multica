//! Invitation mutations against the frozen Go database contract.
use super::account_catalog_write::view_preference::parse_go_uuid;
use super::account_catalog_write_catalog_publish::{CatalogChange, publish};
use super::account_catalog_write_invitation_admission::Denied;
use super::account_catalog_write_invitation_effects::{
    InvitationMail, InvitationMetric, email_domain,
};
use super::account_catalog_write_onboarding::{CompletionPath, OnboardingMetric, record};
use super::account_catalog_write_profile::decode::fields;
use super::{Actor, ApiState, GoJson, Identity, timestamp};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, post},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use tokio_postgres::{Row, Transaction, error::SqlState};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/workspaces/{id}/members", post(create))
        .route("/api/invitations/{id}/accept", post(accept))
        .route("/api/invitations/{id}/decline", post(decline))
        .route(
            "/api/workspaces/{id}/invitations/{invitation_id}",
            delete(revoke),
        )
        .layer(DefaultBodyLimit::disable())
}

struct Failure(StatusCode, &'static str);
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let mut response = GoJson(json!({"error": self.1})).into_response();
        *response.status_mut() = self.0;
        response
    }
}
fn fail(status: StatusCode, message: &'static str) -> Failure {
    Failure(status, message)
}
fn unavailable() -> Failure {
    fail(
        StatusCode::SERVICE_UNAVAILABLE,
        "invitation seat capacity integration unavailable",
    )
}
fn parse_invitation(raw: &str) -> Result<Uuid, Failure> {
    parse_go_uuid(raw).ok_or_else(|| fail(StatusCode::BAD_REQUEST, "invalid invitation id"))
}

fn go_lowercase(value: &str) -> String {
    // Go strings.ToLower maps U+0130 to one rune; Rust expands it to i + dot.
    value
        .chars()
        .map(|c| {
            if c == 'İ' {
                'i'
            } else {
                c.to_lowercase().next().unwrap_or(c)
            }
        })
        .collect()
}

#[derive(Default)]
struct InviteRequest {
    email: String,
    role: String,
}
impl<'de> Deserialize<'de> for InviteRequest {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        let mut request = Self::default();
        for (key, value) in fields(decoder, &["email", "role"])? {
            if let Some(value) = value {
                match key {
                    "email" => request.email = value,
                    "role" => request.role = value,
                    _ => unreachable!(),
                }
            }
        }
        Ok(request)
    }
}

fn rate_limited(retry: u64) -> Response {
    let mut response = GoJson(json!({
        "error": "too many invitation requests", "code": "invitation_rate_limited",
        "retry_after_seconds": retry
    }))
    .into_response();
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response.headers_mut().insert("retry-after", retry.into());
    response
}

fn limiter_unavailable() -> Response {
    let mut response = GoJson(json!({
        "error": "invitation service temporarily unavailable",
        "code": "invitation_rate_limiter_unavailable"
    }))
    .into_response();
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert("retry-after", "5".parse().unwrap());
    response
}

async fn create(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_ws): Path<String>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<Response, Failure> {
    if let Some(bound) = actor.workspace()
        && bound.to_string() != raw_ws
    {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "task token is bound to a different workspace",
        ));
    }
    let ws = parse_go_uuid(&raw_ws)
        .ok_or_else(|| fail(StatusCode::BAD_REQUEST, "invalid workspace id"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    let tx = db.transaction().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create invitation",
        )
    })?;
    // Serialize concurrent invites for one workspace. The pending unique index
    // remains a backstop, including for a writer that does not take this lock.
    let workspace = tx
        .query_opt("SELECT name FROM workspace WHERE id=$1 FOR UPDATE", &[&ws])
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    let workspace_name: String = workspace.get("name");
    let member = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    if !matches!(member.get::<_, String>("role").as_str(), "owner" | "admin") {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "insufficient workspace permissions",
        ));
    }
    let body = body.map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let request: InviteRequest = super::account_catalog_write_profile::account_request(&body)
        .map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let email = go_lowercase(request.email.trim());
    if email.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "email is required"));
    }
    let role = request.role;
    let role = if role.is_empty() {
        "member"
    } else {
        role.trim()
    };
    if !matches!(role, "owner" | "admin" | "member") {
        return Err(fail(StatusCode::BAD_REQUEST, "invalid member role"));
    }
    if role == "owner" {
        return Err(fail(StatusCode::BAD_REQUEST, "cannot invite as owner"));
    }
    if email.ends_with("@channel.invalid") {
        return Err(fail(StatusCode::BAD_REQUEST, "invalid invitee email"));
    }
    let account = tx
        .query_opt(
            "SELECT id,zbs_is_shadow AS is_shadow FROM \"user\" WHERE email=$1",
            &[&email],
        )
        .await
        .map_err(|_| {
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create invitation",
            )
        })?;
    let invitee_id: Option<Uuid> = if let Some(row) = account {
        if row.get::<_, bool>("is_shadow") {
            return Err(fail(StatusCode::BAD_REQUEST, "invalid invitee email"));
        }
        let id: Uuid = row.get("id");
        if tx
            .query_opt(
                "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &id],
            )
            .await
            .map_err(|_| {
                fail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to create invitation",
                )
            })?
            .is_some()
        {
            return Err(fail(StatusCode::CONFLICT, "user is already a member"));
        }
        Some(id)
    } else {
        None
    };
    // Cloud reservations and capacity releases must be durable. Refuse the
    // entire mutation before expiring an old row or spending abuse budgets.
    if state.cloud.is_some() {
        return Err(unavailable());
    }
    tx.execute("UPDATE workspace_invitation SET status='expired', updated_at=now() WHERE workspace_id=$1 AND invitee_email=$2 AND status='pending' AND expires_at <= now()", &[&ws, &email]).await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to create invitation"))?;
    if tx.query_opt("SELECT id FROM workspace_invitation WHERE workspace_id=$1 AND invitee_email=$2 AND status='pending'", &[&ws, &email]).await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to create invitation"))?.is_some()
    {
        // Go expires stale invitations even when a live invite exists.
        tx.commit().await.map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to create invitation"))?;
        return Err(fail(StatusCode::CONFLICT, "invitation already pending for this email"));
    }
    let denied = state.invitation_admission.consume(user, ws, &email).err();
    if let Some(denied) = denied {
        // Stale-invitation cleanup is independent from the invitation budget.
        tx.commit().await.map_err(|_| {
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create invitation",
            )
        })?;
        return Ok(match denied {
            Denied::Limited(retry) => rate_limited(retry),
            Denied::Unavailable => limiter_unavailable(),
        });
    }
    let id = Uuid::new_v4();
    let expires = Utc::now() + chrono::Duration::days(7);
    let row = tx.query_one("INSERT INTO workspace_invitation(id,workspace_id,inviter_id,invitee_email,invitee_user_id,role,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING created_at,updated_at,expires_at", &[&id, &ws, &user, &email, &invitee_id, &role, &expires]).await
        .map_err(|error| if error.code() == Some(&SqlState::UNIQUE_VIOLATION) {
            fail(StatusCode::CONFLICT, "invitation already pending for this email")
        } else { fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to create invitation") })?;
    let result = json!({
        "id": id, "workspace_id": ws, "inviter_id": user, "invitee_email": email,
        "invitee_user_id": invitee_id, "role": role, "status": "pending",
        "created_at": timestamp(row.get("created_at")),
        "updated_at": timestamp(row.get("updated_at")),
        "expires_at": timestamp(row.get("expires_at"))
    });
    tx.commit().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create invitation",
        )
    })?;
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::InvitationCreated {
            invitation: result.clone(),
            workspace_name: Some(workspace_name.clone()),
        },
    )
    .await;
    if let Some(metrics) = &state.invitation_metrics {
        metrics
            .record(
                user,
                ws,
                InvitationMetric::Sent {
                    invited_email_domain: email_domain(&email),
                },
            )
            .await;
    }
    if let Some(mailer) = &state.invitation_mailer
        && !workspace_name.is_empty()
    {
        let inviter_name = match state.pool.get().await {
            Ok(db) => db
                .query_opt("SELECT name FROM \"user\" WHERE id=$1", &[&user])
                .await
                .ok()
                .flatten()
                .map(|row| row.get::<_, String>(0)),
            Err(_) => None,
        }
        .unwrap_or_else(|| email.clone());
        let mail = InvitationMail {
            recipient: email,
            inviter_name,
            workspace_name,
            invitation_id: id,
        };
        let mailer = mailer.clone();
        // Frozen Go sends mail independently from the HTTP request. Adapters
        // own provider/template behavior; never log recipient or provider errors.
        tokio::spawn(async move {
            if mailer.send(mail).await.is_err() {
                eprintln!("failed to send invitation email");
            }
        });
    }
    let mut response = GoJson(result).into_response();
    *response.status_mut() = StatusCode::CREATED;
    Ok(response)
}

async fn invitation_for_user(
    tx: &Transaction<'_>,
    id: Uuid,
    user: Uuid,
    actor: &Actor,
) -> Result<(Row, Row), Failure> {
    let invitation = tx
        .query_opt(
            "SELECT * FROM workspace_invitation WHERE id=$1 FOR UPDATE",
            &[&id],
        )
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "invitation not found"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "invitation not found"))?;
    let account = tx
        .query_opt(
            "SELECT name,email,avatar_url FROM \"user\" WHERE id=$1",
            &[&user],
        )
        .await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?
        .ok_or_else(|| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?;
    let email: String = account.get("email");
    let invitee: Option<Uuid> = invitation.get("invitee_user_id");
    let recipient: String = invitation.get("invitee_email");
    if !go_lowercase(&email).eq(&recipient) && invitee != Some(user) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "invitation does not belong to you",
        ));
    }
    // User-scoped task credentials must not join an unrelated workspace.
    if let Some(bound) = actor.workspace()
        && bound != invitation.get::<_, Uuid>("workspace_id")
    {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "task token is bound to a different workspace",
        ));
    }
    Ok((invitation, account))
}

async fn member_response(
    tx: &Transaction<'_>,
    workspace: Uuid,
    user: Uuid,
    account: &Row,
) -> Result<Option<Value>, Failure> {
    let row = tx
        .query_opt(
            "SELECT id, role, created_at FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&workspace, &user],
        )
        .await
        .map_err(|_| {
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load membership",
            )
        })?;
    Ok(row.map(|row| {
        json!({
            "id": row.get::<_, Uuid>("id"), "workspace_id": workspace, "user_id": user,
            "role": row.get::<_, String>("role"), "created_at": timestamp(row.get("created_at")),
            "name": account.get::<_, String>("name"),
            "email": account.get::<_, String>("email"),
            "avatar_url": account.get::<_, Option<String>>("avatar_url")
        })
    }))
}

async fn accept(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, Failure> {
    let id = parse_invitation(&raw)?;
    let mut db = state.pool.get().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to accept invitation",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to accept invitation",
        )
    })?;
    let (invitation, account) = invitation_for_user(&tx, id, user, &actor).await?;
    let workspace: Uuid = invitation.get("workspace_id");
    let status: String = invitation.get("status");
    if status == "accepted" {
        let member = member_response(&tx, workspace, user, &account).await?;
        return member
            .map(GoJson)
            .ok_or_else(|| fail(StatusCode::BAD_REQUEST, "invitation is not pending"));
    }
    if status != "pending" {
        return Err(fail(StatusCode::BAD_REQUEST, "invitation is not pending"));
    }
    let expires: DateTime<Utc> = invitation.get("expires_at");
    if expires < Utc::now() {
        return Err(fail(StatusCode::GONE, "invitation has expired"));
    }
    // Cloud-backed seat reservations need a durable consume/confirm handshake.
    // No local-only join may silently bypass that protocol.
    if state.cloud.is_some() {
        return Err(unavailable());
    }
    let accepted = tx
        .query_opt(
            "UPDATE workspace_invitation SET status='accepted', updated_at=now() WHERE id=$1 AND status='pending' AND expires_at>now() RETURNING role",
            &[&id],
        )
        .await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to accept invitation"))?
        .ok_or_else(|| fail(StatusCode::GONE, "invitation has expired"))?;
    let role: String = accepted.get("role");
    let member = tx
        .query_one(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3) RETURNING id,role,created_at",
            &[&workspace, &user, &role],
        )
        .await
        .map_err(|e| {
            if e.code() == Some(&SqlState::UNIQUE_VIOLATION) {
                fail(
                    StatusCode::CONFLICT,
                    "you are already a member of this workspace",
                )
            } else {
                fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to create membership")
            }
        })?;
    // Concurrent accepts insert different memberships before updating the same
    // user. Their FK checks hold KEY SHARE on that row; FOR UPDATE would conflict
    // with the other transaction's FK lock and deadlock. No key is changed here.
    let first_completion: bool = tx
        .query_one(
            "SELECT onboarded_at IS NULL FROM \"user\" WHERE id=$1 FOR NO KEY UPDATE",
            &[&user],
        )
        .await
        .map_err(|_| {
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to mark user onboarded",
            )
        })?
        .get(0);
    tx.execute(
        "UPDATE \"user\" SET onboarded_at=COALESCE(onboarded_at,now()), updated_at=now() WHERE id=$1",
        &[&user],
    )
    .await
    .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to mark user onboarded"))?;
    let result = json!({
        "id": member.get::<_, Uuid>("id"), "workspace_id": workspace, "user_id": user,
        "role": role, "created_at": timestamp(member.get("created_at")),
        "name": account.get::<_, String>("name"), "email": account.get::<_, String>("email"),
        "avatar_url": account.get::<_, Option<String>>("avatar_url")
    });
    tx.commit().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to accept invitation",
        )
    })?;
    drop(db);
    let workspace_name = match state.pool.get().await {
        Ok(db) => db
            .query_opt("SELECT name FROM workspace WHERE id=$1", &[&workspace])
            .await
            .ok()
            .flatten()
            .map(|row| row.get::<_, String>(0)),
        Err(_) => None,
    };
    publish(
        &state,
        workspace,
        user,
        CatalogChange::MemberAdded {
            member: result.clone(),
            workspace_name,
        },
    )
    .await;
    publish(
        &state,
        workspace,
        user,
        CatalogChange::InvitationAccepted {
            invitation_id: id,
            member: result.clone(),
        },
    )
    .await;
    if let Some(publisher) = &state.catalog_publisher {
        publisher.notify_daemon_workspaces_changed(vec![user]).await;
    }
    if let Some(metrics) = &state.invitation_metrics {
        let created_at: DateTime<Utc> = invitation.get("created_at");
        metrics
            .record(
                user,
                workspace,
                InvitationMetric::Accepted {
                    days_since_invite: Utc::now().signed_duration_since(created_at).num_seconds()
                        / 86_400,
                },
            )
            .await;
    }
    if first_completion {
        record(
            &state,
            vec![OnboardingMetric::Completed {
                path: CompletionPath::InviteAccept,
            }],
        )
        .await;
    }
    Ok(GoJson(result))
}

async fn decline(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<StatusCode, Failure> {
    let id = parse_invitation(&raw)?;
    let mut db = state.pool.get().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to decline invitation",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to decline invitation",
        )
    })?;
    let (invitation, _) = invitation_for_user(&tx, id, user, &actor).await?;
    if invitation.get::<_, String>("status") != "pending" {
        return Ok(StatusCode::NO_CONTENT);
    }
    if state.cloud.is_some() {
        return Err(unavailable());
    }
    tx.execute(
        "UPDATE workspace_invitation SET status='declined', updated_at=now() WHERE id=$1 AND status='pending'",
        &[&id],
    )
    .await
    .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to decline invitation"))?;
    tx.commit().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to decline invitation",
        )
    })?;
    drop(db);
    publish(
        &state,
        invitation.get("workspace_id"),
        user,
        CatalogChange::InvitationDeclined {
            invitation_id: id,
            invitee_email: invitation.get("invitee_email"),
        },
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

async fn revoke(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_ws, raw_id)): Path<(String, String)>,
) -> Result<StatusCode, Failure> {
    if let Some(bound) = actor.workspace()
        && bound.to_string() != raw_ws
    {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "task token is bound to a different workspace",
        ));
    }
    let workspace = parse_go_uuid(&raw_ws)
        .ok_or_else(|| fail(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
    let mut db = state.pool.get().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to revoke invitation",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to revoke invitation",
        )
    })?;
    // Lock workspace before permission check, consistent with role updates.
    if tx
        .query_opt(
            "SELECT id FROM workspace WHERE id=$1 FOR UPDATE",
            &[&workspace],
        )
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .is_none()
    {
        return Err(fail(StatusCode::NOT_FOUND, "workspace not found"));
    }
    let member = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&workspace, &user],
        )
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    if !matches!(member.get::<_, String>("role").as_str(), "owner" | "admin") {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "insufficient workspace permissions",
        ));
    }
    let id = parse_invitation(&raw_id)?;
    let invitation = tx
        .query_opt(
            "SELECT invitee_email,invitee_user_id FROM workspace_invitation WHERE id=$1 AND workspace_id=$2 AND status='pending' FOR UPDATE",
            &[&id, &workspace],
        )
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "invitation not found"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "invitation not found"))?;
    if state.cloud.is_some() {
        return Err(unavailable());
    }
    tx.execute(
        "DELETE FROM workspace_invitation WHERE id=$1 AND workspace_id=$2 AND status='pending'",
        &[&id, &workspace],
    )
    .await
    .map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to revoke invitation",
        )
    })?;
    tx.commit().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to revoke invitation",
        )
    })?;
    drop(db);
    publish(
        &state,
        workspace,
        user,
        CatalogChange::InvitationRevoked {
            invitation_id: id,
            invitee_email: invitation.get("invitee_email"),
            invitee_user_id: invitation.get("invitee_user_id"),
        },
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod decoding_tests {
    use super::*;
    use crate::api::account_catalog_write_profile::account_request;

    #[test]
    fn go_invitation_email_lowercase_is_single_rune() {
        assert_eq!(go_lowercase("İ@EXAMPLE.COM"), "i@example.com");
        assert_eq!(go_lowercase("ẞ@EXAMPLE.COM"), "ß@example.com");
        assert_ne!("İ@EXAMPLE.COM".to_lowercase(), "i@example.com");
    }

    #[test]
    fn go_invitation_request_decoding() {
        let req: InviteRequest = account_request(
            br#"{"EMAIL":"first@example.com","email":null,"Role":"admin","role":null} trailing"#,
        )
        .unwrap();
        assert_eq!(req.email, "first@example.com");
        assert_eq!(req.role, "admin");
        let req: InviteRequest = account_request(b"null after").unwrap();
        assert_eq!(req.email, "");
        assert!(account_request::<InviteRequest>(br#"{"email":123}"#).is_err());
    }
}
