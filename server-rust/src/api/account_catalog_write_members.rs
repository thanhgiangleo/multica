//! Membership mutations. Runtime-bearing departures await the shared revocation lifecycle.
use super::{Actor, ApiState, GoJson, Identity, timestamp};
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{patch, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/workspaces/{id}/members/{member_id}",
            patch(update_role).delete(remove_member),
        )
        .route(
            "/api/workspaces/{id}/members/{member_id}/",
            patch(update_role).delete(remove_member),
        )
        .route("/api/workspaces/{id}/leave", post(leave_workspace))
}

struct Failure(StatusCode, Value);
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let mut response = GoJson(self.1).into_response();
        *response.status_mut() = self.0;
        response
    }
}
fn fail(status: StatusCode, message: &'static str) -> Failure {
    Failure(status, json!({"error": message}))
}

#[derive(Deserialize)]
struct RoleRequest {
    role: Option<String>,
}

async fn update_role(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_ws, raw_member)): Path<(String, String)>,
    body: Result<Json<RoleRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<GoJson<Value>, Failure> {
    // The workspace URL is an authority boundary, not just a selector.
    if let Some(bound) = actor.workspace()
        && raw_ws != bound.to_string()
    {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "task token is bound to a different workspace",
        ));
    }
    let ws = Uuid::parse_str(&raw_ws)
        .map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid workspace id"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to update member"))?;
    // Serialize promotions/demotions on the workspace row. A last-owner check
    // outside this lock would allow two owners to demote each other concurrently.
    if tx
        .query_opt("SELECT id FROM workspace WHERE id=$1 FOR UPDATE", &[&ws])
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .is_none()
    {
        return Err(fail(StatusCode::NOT_FOUND, "workspace not found"));
    }
    let requester = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    let requester_role: String = requester.get(0);
    if requester_role != "owner" && requester_role != "admin" {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    // An agent task's runtime owner can also be a workspace admin. That does
    // not turn the task credential into a human administrator.
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "insufficient workspace permissions",
        ));
    }
    let member = Uuid::parse_str(&raw_member)
        .map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid member id"))?;
    let target = tx
        .query_opt(
            "SELECT role FROM member WHERE id=$1 AND workspace_id=$2",
            &[&member, &ws],
        )
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "member not found"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "member not found"))?;
    let old: String = target.get(0);
    let Json(req) = body.map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let role = req.role.as_deref().unwrap_or_default().trim();
    if role.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "role is required"));
    }
    if !matches!(role, "owner" | "admin" | "member") {
        return Err(fail(StatusCode::BAD_REQUEST, "invalid member role"));
    }
    if (old == "owner" || role == "owner") && requester_role != "owner" {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if old == "owner" && role != "owner" {
        let owners: i64 = tx
            .query_one(
                "SELECT count(*) FROM member WHERE workspace_id=$1 AND role='owner'",
                &[&ws],
            )
            .await
            .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to update member"))?
            .get(0);
        if owners <= 1 {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "workspace must have at least one owner",
            ));
        }
    }
    let row = tx.query_one("UPDATE member SET role=$2 WHERE id=$1 AND workspace_id=$3 RETURNING id, workspace_id, user_id, role, created_at", &[&member, &role, &ws]).await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to update member"))?;
    let target_user: Uuid = row.get("user_id");
    let account = tx
        .query_one(
            "SELECT name, email, avatar_url FROM \"user\" WHERE id=$1",
            &[&target_user],
        )
        .await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to load member"))?;
    let result = json!({
        "id": row.get::<_, Uuid>("id"), "workspace_id": ws, "user_id": target_user,
        "role": row.get::<_, String>("role"), "created_at": timestamp(row.get("created_at")),
        "name": account.get::<_, String>("name"), "email": account.get::<_, String>("email"),
        "avatar_url": account.get::<_, Option<String>>("avatar_url"),
    });
    tx.commit()
        .await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to update member"))?;
    drop(db);
    if let Some(publisher) = &state.catalog_publisher {
        publisher.invalidate_membership(ws, target_user).await;
    }
    super::account_catalog_write_catalog_publish::publish(
        &state,
        ws,
        user,
        super::account_catalog_write_catalog_publish::CatalogChange::MemberUpdated {
            member: result.clone(),
        },
    )
    .await;
    Ok(GoJson(result))
}

async fn remove_member(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((workspace, member)): Path<(String, String)>,
) -> Result<StatusCode, Failure> {
    depart(state, user, actor, workspace, Some(member)).await
}

async fn leave_workspace(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(workspace): Path<String>,
) -> Result<StatusCode, Failure> {
    depart(state, user, actor, workspace, None).await
}

async fn depart(
    state: ApiState,
    user: Uuid,
    actor: Actor,
    raw_ws: String,
    raw_member: Option<String>,
) -> Result<StatusCode, Failure> {
    if let Some(bound) = actor.workspace()
        && raw_ws != bound.to_string()
    {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "task token is bound to a different workspace",
        ));
    }
    let ws = Uuid::parse_str(&raw_ws)
        .map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid workspace id"))?;
    let removing = raw_member.is_some();
    let message = if removing {
        "failed to delete member"
    } else {
        "failed to leave workspace"
    };
    let internal = || fail(StatusCode::INTERNAL_SERVER_ERROR, message);
    let mut db = state.pool.get().await.map_err(|_| internal())?;
    let tx = db.transaction().await.map_err(|_| internal())?;
    let requester = tx
        .query_opt(
            "SELECT id,role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| internal())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    let requester_role: String = requester.get("role");
    if removing && !matches!(requester_role.as_str(), "owner" | "admin") {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "insufficient workspace permissions",
        ));
    }
    let member = match raw_member {
        Some(raw) => {
            Uuid::parse_str(&raw).map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid member id"))?
        }
        None => requester.get("id"),
    };
    let target = tx
        .query_opt(
            "SELECT user_id,role FROM member WHERE id=$1 AND workspace_id=$2",
            &[&member, &ws],
        )
        .await
        .map_err(|_| internal())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "member not found"))?;
    let target_user: Uuid = target.get("user_id");
    let target_role: String = target.get("role");
    if removing && target_role == "owner" && requester_role != "owner" {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    // Match Go LockSubscriberWrites before locking the target member. Canonical
    // UUID casts are essential: uppercase route IDs must use the same key.
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1::uuid::text),hashtext($2::uuid::text))",
        &[&ws, &target_user],
    )
    .await
    .map_err(|_| internal())?;
    // Role updates and departures take this workspace lock before checking
    // owner count. Reload roles after waiting: the unlocked reads above only
    // establish the (workspace, user) advisory lock key and error precedence.
    if tx
        .query_opt("SELECT id FROM workspace WHERE id=$1 FOR UPDATE", &[&ws])
        .await
        .map_err(|_| internal())?
        .is_none()
    {
        return Err(fail(StatusCode::NOT_FOUND, "workspace not found"));
    }
    let requester = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| internal())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    let requester_role: String = requester.get("role");
    if removing && !matches!(requester_role.as_str(), "owner" | "admin") {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    let target = tx
        .query_opt(
            "SELECT user_id,role FROM member WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&member, &ws],
        )
        .await
        .map_err(|_| internal())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "member not found"))?;
    if target.get::<_, Uuid>("user_id") != target_user {
        return Err(fail(StatusCode::NOT_FOUND, "member not found"));
    }
    let target_role: String = target.get("role");
    if removing && target_role == "owner" && requester_role != "owner" {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if target_role == "owner" {
        let owners: i64 = tx
            .query_one(
                "SELECT count(*) FROM member WHERE workspace_id=$1 AND role='owner'",
                &[&ws],
            )
            .await
            .map_err(|_| internal())?
            .get(0);
        if owners <= 1 {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "workspace must have at least one owner",
            ));
        }
    }
    if state.cloud.is_some() {
        return Err(fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "member capacity release integration unavailable",
        ));
    }
    // This provisional slice cannot archive agents, cancel tasks or revoke
    // shared runtimes. Fence dependency writers while checking for absence;
    // bounded table locks are deliberately conservative until the shared
    // runtime revoker replaces this block. No task lifecycle SQL lives here.
    tx.batch_execute("SET LOCAL lock_timeout='2s'")
        .await
        .map_err(|_| internal())?;
    tx.batch_execute("LOCK TABLE agent,agent_runtime IN SHARE MODE")
        .await
        .map_err(|_| {
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "member revocation dependencies busy",
            )
        })?;
    let dependencies: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM agent_runtime WHERE workspace_id=$1 AND (owner_id=$2 OR $2=ANY(shared_with))) OR EXISTS(SELECT 1 FROM agent WHERE workspace_id=$1 AND owner_id=$2)",
            &[&ws, &target_user],
        )
        .await
        .map_err(|_| internal())?
        .get(0);
    if dependencies {
        return Err(fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "member runtime revocation integration unavailable",
        ));
    }
    // All FK-free catalog/subscription cleanup belongs in the membership
    // transaction. Preserve public furniture and rows in other workspaces.
    for sql in [
        "UPDATE zbs_telegram_guest_invite SET revoked_at=now(),revoked_by_user_id=$2 WHERE workspace_id=$1 AND issued_by_user_id=$2 AND consumed_at IS NULL AND revoked_at IS NULL",
        "UPDATE zbs_zalo_guest_invite SET revoked_at=now(),revoked_by_user_id=$2 WHERE workspace_id=$1 AND issued_by_user_id=$2 AND consumed_at IS NULL AND revoked_at IS NULL",
        "DELETE FROM channel_user_binding WHERE workspace_id=$1 AND multica_user_id=$2",
        "DELETE FROM agent_invocation_target AS t USING agent AS a WHERE t.agent_id=a.id AND a.workspace_id=$1 AND t.target_type='member' AND t.target_id=$2",
        "DELETE FROM quick_action WHERE workspace_id=$1 AND created_by_id=$2 AND visibility='private'",
        "WITH deleted AS (DELETE FROM issue_view WHERE workspace_id=$1 AND owner_id=$2 AND visibility='private' RETURNING id) DELETE FROM pinned_item WHERE workspace_id=$1 AND item_type='view' AND item_id IN (SELECT id FROM deleted)",
        "DELETE FROM issue_view_preference WHERE workspace_id=$1 AND user_id=$2",
        "DELETE FROM issue_subscriber AS s USING issue AS i WHERE s.issue_id=i.id AND i.workspace_id=$1 AND s.user_type='member' AND s.user_id=$2",
        "DELETE FROM autopilot_subscriber AS s USING autopilot AS a WHERE s.autopilot_id=a.id AND a.workspace_id=$1 AND s.user_type='member' AND s.user_id=$2",
    ] {
        tx.execute(sql, &[&ws, &target_user])
            .await
            .map_err(|_| internal())?;
    }
    let removed = tx
        .execute(
            "DELETE FROM member WHERE id=$1 AND workspace_id=$2",
            &[&member, &ws],
        )
        .await
        .map_err(|_| internal())?;
    if removed != 1 {
        return Err(internal());
    }
    tx.commit().await.map_err(|_| internal())?;
    drop(db);
    if let Some(publisher) = &state.catalog_publisher {
        publisher.disconnect_member(ws, target_user, member).await;
        publisher.invalidate_membership(ws, target_user).await;
        super::account_catalog_write_catalog_publish::publish(
            &state,
            ws,
            user,
            super::account_catalog_write_catalog_publish::CatalogChange::MemberRemoved {
                member_id: member,
                workspace_id: ws,
                user_id: target_user,
            },
        )
        .await;
        publisher
            .notify_daemon_workspaces_changed(vec![target_user])
            .await;
    }
    Ok(StatusCode::NO_CONTENT)
}
