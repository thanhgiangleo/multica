//! Workspace share-link mutations. Cloud seat reservations remain an integration gate.
use super::account_catalog_write_catalog_publish::{self as events, CatalogChange};
use super::{
    Actor, ApiState, GoJson, Identity, account_catalog_write_profile::account_request, timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, post},
};
use chrono::{DateTime, Utc};
use getrandom::fill;
use serde_json::{Value, json};
use tokio_postgres::{Row, error::SqlState};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/workspaces/{id}/share-links", post(create))
        .route("/api/workspaces/{id}/share-links/{link_id}", delete(revoke))
        .route("/api/share-links/join", post(join))
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
fn join_failed() -> Failure {
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to join workspace",
    )
}
fn parse_ws(raw: &str, actor: &Actor) -> Result<Uuid, Failure> {
    if let Some(bound) = actor.workspace()
        && bound.to_string() != raw
    {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "task token is bound to a different workspace",
        ));
    }
    Uuid::parse_str(raw).map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid workspace id"))
}
async fn require_admin(
    db: &impl tokio_postgres::GenericClient,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
) -> Result<(), Failure> {
    let role = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .get::<_, String>(0);
    if !matches!(role.as_str(), "owner" | "admin") {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "insufficient workspace permissions",
        ));
    }
    Ok(())
}

#[path = "account_catalog_write_share_links_decode.rs"]
mod decode;
use decode::{CreateRequest, JoinRequest};

fn link_response(row: &Row) -> Value {
    let expiry: Option<DateTime<Utc>> = row.get("expires_at");
    let max_uses: Option<i32> = row.get("max_uses");
    let mut value = json!({
        "id": row.get::<_, Uuid>("id"),
        "workspace_id": row.get::<_, Uuid>("workspace_id"),
        "code": row.get::<_, String>("code"),
        "created_by": row.get::<_, Uuid>("created_by"),
        "role": row.get::<_, String>("role"),
        "use_count": row.get::<_, i32>("use_count"),
        "is_active": row.get::<_, bool>("is_active"),
        "created_at": timestamp(row.get("created_at")),
    });
    if let Some(expiry) = expiry {
        value["expires_at"] = json!(timestamp(expiry));
    }
    if let Some(max_uses) = max_uses {
        value["max_uses"] = json!(max_uses);
    }
    value
}

async fn create(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_ws): Path<String>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<(StatusCode, GoJson<Value>), Failure> {
    let ws = parse_ws(&raw_ws, &actor)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    let tx = db.transaction().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create share link",
        )
    })?;
    // Serialize rotations, including concurrent administrators and member-role changes.
    if tx
        .query_opt("SELECT id FROM workspace WHERE id=$1 FOR UPDATE", &[&ws])
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .is_none()
    {
        return Err(fail(StatusCode::NOT_FOUND, "workspace not found"));
    }
    require_admin(&*tx, ws, user, &actor).await?;
    let req: CreateRequest =
        account_request(&body.map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?)
            .map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let raw_role = &req.role;
    let role = if raw_role.is_empty() {
        "member"
    } else {
        raw_role.trim()
    };
    let role = if role.eq_ignore_ascii_case("member") {
        "member"
    } else if role.eq_ignore_ascii_case("admin") {
        "admin"
    } else {
        return Err(fail(StatusCode::BAD_REQUEST, "invalid role"));
    };
    // Go time.Duration overflows above 2_562_047 hours.
    if req
        .expires_in
        .is_some_and(|hours| !(1..=2_562_047).contains(&hours))
    {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "expires_in must be between 1 and 2562047 hours",
        ));
    }
    if req
        .max_uses
        .is_some_and(|uses| !(1..=i32::MAX as i64).contains(&uses))
    {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "max_uses must be between 1 and 2147483647",
        ));
    }
    let expiry = req
        .expires_in
        .map(|hours| Utc::now() + chrono::Duration::hours(hours));
    let max_uses = req.max_uses.map(|uses| uses as i32);
    let mut bytes = [0u8; 12];
    fill(&mut bytes).map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create share link",
        )
    })?;
    let code = hex::encode(bytes);
    tx.execute(
        "UPDATE workspace_share_link SET is_active=false WHERE workspace_id=$1 AND is_active=true",
        &[&ws],
    )
    .await
    .map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create share link",
        )
    })?;
    let row = tx.query_one("INSERT INTO workspace_share_link(workspace_id,code,created_by,role,expires_at,max_uses) VALUES($1,$2,$3,$4,$5,$6) RETURNING *",
        &[&ws, &code, &user, &role, &expiry, &max_uses]).await.map_err(|e| {
            if e.code() == Some(&SqlState::UNIQUE_VIOLATION) {
                fail(StatusCode::CONFLICT, "a share link is already active for this workspace")
            } else {
                fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to create share link")
            }
        })?;
    let result = link_response(&row);
    tx.commit().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create share link",
        )
    })?;
    Ok((StatusCode::CREATED, GoJson(result)))
}

async fn revoke(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_ws, raw_link)): Path<(String, String)>,
) -> Result<StatusCode, Failure> {
    let ws = parse_ws(&raw_ws, &actor)?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    require_admin(&**db, ws, user, &actor).await?;
    let link =
        Uuid::parse_str(&raw_link).map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid link id"))?;
    db.execute(
        "UPDATE workspace_share_link SET is_active=false WHERE id=$1 AND workspace_id=$2",
        &[&link, &ws],
    )
    .await
    .map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to revoke share link",
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn join(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<Value>, Failure> {
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "this endpoint is only available to human actors",
        ));
    }
    let req: JoinRequest =
        account_request(&body.map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?)
            .map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let code = req.code.trim();
    if code.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "code is required"));
    }
    let mut db = state.pool.get().await.map_err(|_| join_failed())?;
    let account = db
        .query_opt(
            "SELECT name,email,avatar_url FROM \"user\" WHERE id=$1",
            &[&user],
        )
        .await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?
        .ok_or_else(|| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?;
    let preview = db.query_opt("SELECT id,workspace_id FROM workspace_share_link WHERE code=$1 AND is_active=true AND (expires_at IS NULL OR expires_at>now()) AND (max_uses IS NULL OR use_count<max_uses)", &[&code]).await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "share link not found or expired"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "share link not found or expired"))?;
    let ws: Uuid = preview.get("workspace_id");
    if db
        .query_opt(
            "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| join_failed())?
        .is_some()
    {
        return Err(fail(
            StatusCode::CONFLICT,
            "you are already a member of this workspace",
        ));
    }
    // Cloud joins need a capacity intent with durable confirmation/compensation.
    if state.cloud.is_some() {
        return Err(fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "share link seat capacity integration unavailable",
        ));
    }
    let tx = db.transaction().await.map_err(|_| join_failed())?;
    if tx
        .query_opt("SELECT id FROM workspace WHERE id=$1 FOR KEY SHARE", &[&ws])
        .await
        .map_err(|_| join_failed())?
        .is_none()
    {
        return Err(fail(
            StatusCode::NOT_FOUND,
            "share link not found or expired",
        ));
    }
    let row = tx.query_opt("UPDATE workspace_share_link SET use_count=use_count+1 WHERE id=$1 AND is_active=true AND (expires_at IS NULL OR expires_at>now()) AND (max_uses IS NULL OR use_count<max_uses) RETURNING workspace_id,role", &[&preview.get::<_, Uuid>("id")]).await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "share link not found or expired"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "share link not found or expired"))?;
    let role: String = row.get("role");
    if tx
        .query_opt(
            "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| join_failed())?
        .is_some()
    {
        return Err(fail(
            StatusCode::CONFLICT,
            "you are already a member of this workspace",
        ));
    }
    let email: String = account.get("email");
    // A matching pending email invitation must be settled in this transaction;
    // otherwise joining leaves a phantom invitation and its reservation behind.
    let settled = tx.query_opt("SELECT id FROM workspace_invitation WHERE workspace_id=$1 AND invitee_email=$2 AND status='pending' LIMIT 1",
        &[&ws, &email.to_lowercase()]).await.map_err(|_| join_failed())?;
    let accepted: Option<Uuid> = if let Some(invitation) = settled {
        tx.query_opt("UPDATE workspace_invitation SET status='accepted',updated_at=now() WHERE id=$1 AND status='pending' RETURNING id", &[&invitation.get::<_, Uuid>("id")]).await
            .map_err(|_| join_failed())?.map(|row| row.get(0))
    } else {
        None
    };
    let member = tx.query_one("INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3) RETURNING id,created_at",
        &[&ws,&user,&role]).await.map_err(|e| if e.code() == Some(&SqlState::UNIQUE_VIOLATION) {
            fail(StatusCode::CONFLICT, "you are already a member of this workspace")
        } else { fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to create membership") })?;
    tx.query_one("UPDATE \"user\" SET onboarded_at=COALESCE(onboarded_at,now()), updated_at=now() WHERE id=$1 RETURNING id", &[&user]).await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to finalize onboarding"))?;
    let slug: String = tx
        .query_opt("SELECT slug FROM workspace WHERE id=$1", &[&ws])
        .await
        .map_err(|_| join_failed())?
        .map(|row| row.get(0))
        .unwrap_or_default();
    let response = json!({
        "member": {"id":member.get::<_, Uuid>("id"), "workspace_id":ws, "user_id":user,
            "role":role, "created_at":timestamp(member.get("created_at")),
            "name":account.get::<_, String>("name"), "email":email,
            "avatar_url":account.get::<_, Option<String>>("avatar_url")},
        "workspace_id":ws, "workspace_slug":slug
    });
    tx.commit().await.map_err(|_| join_failed())?;
    drop(db);
    let member = response["member"].clone();
    events::publish(
        &state,
        ws,
        user,
        CatalogChange::MemberAdded {
            member: member.clone(),
            workspace_name: None,
        },
    )
    .await;
    if let Some(invitation_id) = accepted {
        events::publish(
            &state,
            ws,
            user,
            CatalogChange::InvitationAccepted {
                invitation_id,
                member,
            },
        )
        .await;
    }
    if let Some(publisher) = &state.catalog_publisher {
        publisher.notify_daemon_workspaces_changed(vec![user]).await;
    }
    Ok(GoJson(response))
}
