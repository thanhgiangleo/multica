//! Bearer-only DaemonAuth; never inherit browser cookies or task credentials.
use super::*;
use axum::{body::Bytes, routing::post};
use std::collections::HashMap;
use tokio_postgres::Row;

#[derive(Clone)]
pub(super) enum DaemonIdentity {
    User(Uuid),
    Bound {
        workspace: Uuid,
        daemon: String,
        owner: Option<Uuid>,
        hash: String,
    },
}

pub(super) fn routes(state: ApiState) -> Router<ApiState> {
    Router::new()
        .merge(super::runtime_chat_daemon::routes())
        .merge(super::p0_daemon::routes())
        .merge(super::p0_daemon_registration::routes())
        .merge(super::p0_daemon_claim::routes())
        .merge(super::p0_daemon_outcomes::routes())
        .merge(super::p0_daemon_unconfigured::routes())
        .merge(super::p0_daemon_ws::routes())
        .merge(super::runtime_skills_sprint_bundles::routes())
        .merge(if state.runtime_rpc_transport.is_some() {
            super::runtime_skills_sprint_control::daemon_routes()
        } else {
            Router::new()
        })
        .merge(
            if state.runtime_rpc_transport.is_some() && state.skill_events.is_some() {
                super::runtime_skills_sprint_local_import::daemon_routes()
            } else {
                Router::new()
            },
        )
        .route("/api/daemon/workspaces", get(workspaces))
        .route("/api/daemon/workspaces/{workspaceId}/repos", get(repos))
        .route(
            "/api/daemon/workspaces/{workspaceId}/runtime-profiles",
            get(runtime_profiles),
        )
        .route("/api/daemon/tasks/{taskId}/status", get(task_status))
        .route("/api/daemon/tasks/{taskId}/gc-check", get(task_gc_check))
        .route("/api/daemon/issues/{issueId}/gc-check", get(issue_gc_check))
        .route(
            "/api/daemon/workspaces/{workspaceId}/issues/gc-check",
            post(batch_issue_gc_check),
        )
        .route(
            "/api/daemon/chat-sessions/{sessionId}/gc-check",
            get(chat_session_gc_check),
        )
        .route(
            "/api/daemon/autopilot-runs/{runId}/gc-check",
            get(autopilot_run_gc_check),
        )
        .route(
            "/api/daemon/runtimes/{runtimeId}/tasks/claim",
            post(super::p0_daemon_claim::claim_by_runtime),
        )
        .route_layer(middleware::from_fn_with_state(state, authenticate))
}

fn bearer(headers: &HeaderMap) -> Result<&str, ApiError> {
    let header = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .filter(|h| !h.is_empty())
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "missing authorization header"))?;
    header
        .strip_prefix("Bearer ")
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid authorization format"))
}

async fn user_allowed(
    db: &impl tokio_postgres::GenericClient,
    id: Uuid,
    email: &str,
) -> Result<(), ApiError> {
    let row = db
        .query_opt("SELECT zbs_is_shadow FROM \"user\" WHERE id=$1", &[&id])
        .await
        .map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication temporarily unavailable",
            )
        })?;
    if row.is_none_or(|r| r.get::<_, bool>(0)) {
        return Err(error(StatusCode::UNAUTHORIZED, "invalid token"));
    }
    if temporarily_disabled(id, email) {
        return Err(error(StatusCode::FORBIDDEN, "account disabled"));
    }
    Ok(())
}

#[derive(Deserialize)]
struct DaemonClaims {
    #[serde(default)]
    sub: Value,
    #[serde(default)]
    email: Value,
}

async fn authenticate(
    State(state): State<ApiState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let token = bearer(request.headers())?.to_owned();
    // Do not allow mdt_ callers to inject a user identity into discovery.
    // Frozen DaemonAuth strips actor headers but leaves X-User-ID on mdt_.
    // The typed context deliberately closes that spoofing hole rather than
    // copying it; differential evidence records the security divergence.
    for key in [
        "x-user-id",
        "x-user-email",
        "x-agent-id",
        "x-task-id",
        "x-actor-source",
    ] {
        request.headers_mut().remove(key);
    }
    let mut actor = Actor::Human;
    let identity = if token.starts_with("mcn_") {
        actor = Actor::Cloud;
        DaemonIdentity::User(cloud::authenticate(&state, &token).await?)
    } else {
        let db = state.pool.get().await.map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication temporarily unavailable",
            )
        })?;
        if token.starts_with("mdt_") {
            actor = Actor::Daemon;
            let hash = hex::encode(Sha256::digest(token.as_bytes()));
            let row = db
            .query_opt(
                "SELECT workspace_id,daemon_id,owner_id,created_at FROM daemon_token WHERE token_hash=$1 AND expires_at > now()",
                &[&hash],
            )
            .await
            .map_err(|_| error(StatusCode::UNAUTHORIZED, "invalid daemon token"))?
            .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid daemon token"))?;
            let workspace: Uuid = row.get(0);
            let daemon: String = row.get(1);
            let mut owner: Option<Uuid> = row.get(2);
            if owner.is_none() && !daemon.starts_with("qoder-managed-") {
                let created_at: chrono::DateTime<Utc> = row.get(3);
                // Frozen pre-588 tokens were minted beside task tokens in one
                // transaction. Ambiguous or missing owners remain unbound.
                if let Ok(owners) = db.query(
                    "SELECT DISTINCT tt.user_id FROM task_token tt JOIN agent_task_queue q ON q.id=tt.task_id JOIN agent_runtime r ON r.id=q.runtime_id WHERE tt.workspace_id=$1 AND tt.created_at=$2 AND tt.expires_at>now() AND r.workspace_id=$1 AND r.daemon_id=$3 LIMIT 2",
                    &[&workspace, &created_at, &daemon],
                ).await && owners.len() == 1 {
                    owner = owners[0].get(0);
                }
            }
            DaemonIdentity::Bound {
                workspace,
                daemon,
                owner,
                hash,
            }
        } else if token.starts_with("mul_") {
            let hash = hex::encode(Sha256::digest(token.as_bytes()));
            let row = db.query_opt("SELECT id, user_id FROM personal_access_token WHERE token_hash=$1 AND NOT revoked AND (expires_at IS NULL OR expires_at > now())", &[&hash]).await
            .map_err(|_| error(StatusCode::UNAUTHORIZED, "invalid token"))?
            .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid token"))?;
            let id: Uuid = row.get("user_id");
            user_allowed(&**db, id, "").await?;
            let pat: Uuid = row.get("id");
            let _ = db
            .execute(
                "UPDATE personal_access_token SET last_used_at=now() WHERE id=$1 AND NOT revoked",
                &[&pat],
            )
            .await;
            DaemonIdentity::User(id)
        } else {
            // Unlike browser renewal, DaemonAuth accepts HMAC JWTs without exp.
            let mut validation = Validation::new(Algorithm::HS256);
            validation.algorithms = vec![Algorithm::HS256, Algorithm::HS384, Algorithm::HS512];
            validation.required_spec_claims.clear();
            validation.validate_nbf = true;
            validation.validate_aud = false;
            validation.leeway = 0;
            let claims = decode::<DaemonClaims>(
                &token,
                &DecodingKey::from_secret(&state.jwt_secret),
                &validation,
            )
            .map_err(|_| error(StatusCode::UNAUTHORIZED, "invalid token"))?
            .claims;
            let sub = claims
                .sub
                .as_str()
                .filter(|sub| !sub.trim().is_empty())
                .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid claims"))?;
            let id = Uuid::parse_str(sub)
                .map_err(|_| error(StatusCode::UNAUTHORIZED, "invalid token"))?;
            user_allowed(&**db, id, claims.email.as_str().unwrap_or("")).await?;
            DaemonIdentity::User(id)
        }
    };
    if let DaemonIdentity::User(id) = &identity {
        request
            .headers_mut()
            .insert("x-user-id", id.to_string().parse().unwrap());
    }
    if matches!(actor, Actor::Cloud) {
        request
            .headers_mut()
            .insert("x-actor-source", "cloud_pat".parse().unwrap());
    }
    request.extensions_mut().insert(identity);
    request.extensions_mut().insert(actor);
    Ok(next.run(request).await)
}

#[derive(Serialize)]
struct WorkspaceName {
    id: Uuid,
    name: String,
}

fn etag(rows: &[WorkspaceName]) -> String {
    // encoding/json.Marshal escapes HTML and JavaScript line separators.
    let bytes = serde_json::to_string(rows)
        .expect("fixed string/UUID projection")
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    format!("W/\"{}\"", hex::encode(Sha256::digest(bytes.as_bytes())))
}

async fn workspaces(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let message = match identity {
        DaemonIdentity::User(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list daemon workspaces",
        ),
        DaemonIdentity::Bound { .. } => (StatusCode::NOT_FOUND, "workspace not found"),
    };
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(message.0, message.1))?;
    let rows = match identity {
        DaemonIdentity::User(user) => db.query("SELECT w.id,w.name FROM member m JOIN workspace w ON w.id=m.workspace_id WHERE m.user_id=$1 ORDER BY w.id ASC", &[&user]).await,
        DaemonIdentity::Bound { workspace, .. } => db.query("SELECT id,name FROM workspace WHERE id=$1", &[&workspace]).await,
    }.map_err(|_| error(message.0, message.1))?;
    if matches!(identity, DaemonIdentity::Bound { .. }) && rows.is_empty() {
        return Err(error(StatusCode::NOT_FOUND, "workspace not found"));
    }
    let names: Vec<_> = rows
        .iter()
        .map(|row| WorkspaceName {
            id: row.get(0),
            name: row.get(1),
        })
        .collect();
    let tag = etag(&names);
    let mut response = if headers.get("if-none-match").and_then(|h| h.to_str().ok()) == Some(&tag) {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        GoJson(names).into_response()
    };
    response
        .headers_mut()
        .insert("cache-control", "private, no-cache".parse().unwrap());
    response.headers_mut().insert("etag", tag.parse().unwrap());
    Ok(response)
}

// The daemon API accepts user credentials too, but a daemon credential never
// inherits user membership. Qoder credentials must still have an active owner
// connection at every request; the daemon token alone does not grant access.
pub(super) async fn workspace_access(
    db: &impl tokio_postgres::GenericClient,
    identity: &DaemonIdentity,
    raw: &str,
) -> Result<Uuid, ApiError> {
    let raw = raw.trim();
    let id = Uuid::parse_str(raw).map_err(|_| error(StatusCode::NOT_FOUND, "not found"))?;
    match identity {
        DaemonIdentity::User(user) => {
            let member = db
                .query_opt(
                    "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                    &[&id, user],
                )
                .await
                .map_err(|_| error(StatusCode::NOT_FOUND, "not found"))?;
            if member.is_none() {
                return Err(error(StatusCode::NOT_FOUND, "not found"));
            }
        }
        DaemonIdentity::Bound {
            workspace,
            daemon,
            hash,
            ..
        } => {
            if raw != workspace.to_string() {
                return Err(error(StatusCode::NOT_FOUND, "not found"));
            }
            if daemon.starts_with("qoder-managed-") {
                let allowed: bool = db.query_one(
                    "SELECT EXISTS(SELECT 1 FROM cloud_agent_connection c JOIN daemon_token d ON d.id=c.token_id JOIN member m ON m.workspace_id=c.workspace_id AND m.user_id=c.owner_id WHERE c.provider='qoder_cloud' AND c.workspace_id=$1 AND c.enabled AND d.daemon_id=$2 AND d.token_hash=$3 AND d.expires_at>now() AND m.role IN ('owner','admin'))",
                    &[workspace, daemon, hash],
                ).await.map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "runtime authorization unavailable"))?.get(0);
                if !allowed {
                    return Err(error(StatusCode::FORBIDDEN, "runtime access revoked"));
                }
            }
        }
    }
    Ok(id)
}

async fn runtime_profiles(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "not found"))?;
    let id = workspace_access(&**db, &identity, &raw).await?;
    let rows = db.query(
        &format!("SELECT {} FROM runtime_profile WHERE workspace_id=$1 AND enabled=true ORDER BY created_at ASC", workspace_read::PROFILE_COLUMNS),
        &[&id],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list runtime profiles"))?;
    let profiles: Vec<_> = rows.iter().map(workspace_read::profile_row).collect();
    Ok(GoJson(
        json!({ "workspace_id": raw.trim(), "runtime_profiles": profiles }),
    ))
}

#[derive(Serialize)]
struct RepoData {
    url: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    description: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    r#ref: String,
}

fn repo_field(raw: &Value, key: &str) -> Option<String> {
    let field = raw.as_object().and_then(|fields| {
        fields.get(key).or_else(|| {
            fields
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(key))
                .map(|(_, value)| value)
        })
    });
    match field {
        None | Some(Value::Null) => Some(String::new()),
        Some(Value::String(value)) => Some(value.clone()),
        _ => None,
    }
}

fn normalized_repos(raw: &Value) -> Vec<RepoData> {
    let Some(rows) = raw.as_array() else {
        return Vec::new();
    };
    let mut repos = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for row in rows {
        if !row.is_object() && !row.is_null() {
            return Vec::new();
        }
        let Some((url, description, reference)) = repo_field(row, "url")
            .zip(repo_field(row, "description"))
            .zip(repo_field(row, "ref"))
            .map(|((url, description), reference)| (url, description, reference))
        else {
            return Vec::new();
        };
        let url = url.trim().to_owned();
        if !url.is_empty() && seen.insert(url.clone()) {
            repos.push(RepoData {
                url,
                description,
                r#ref: reference,
            });
        }
    }
    repos
}

async fn repos(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "not found"))?;
    let workspace = workspace_access(&**db, &identity, &raw).await?;
    let row = db
        .query_opt(
            "SELECT repos,settings FROM workspace WHERE id=$1",
            &[&workspace],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let repos = normalized_repos(&row.get::<_, Value>(0));
    let mut urls: Vec<&str> = repos.iter().map(|repo| repo.url.as_str()).collect();
    urls.sort_unstable();
    let version = hex::encode(Sha256::digest(urls.join("\n").as_bytes()));
    let mut response = json!({
        "workspace_id": raw.trim(),
        "repos": repos,
        "repos_version": version,
        "settings": row.get::<_, Value>(1),
    });
    if let Some(certs) = &state.git_extra_ca_certs {
        response["ca_certs"] = json!(certs);
    }
    Ok(GoJson(response))
}

// The Go GC wire contract uses a zero time, not null, for unfinished runs.
fn gc_completed_at(completed: Option<chrono::DateTime<Utc>>) -> Value {
    match completed {
        Some(time) => json!(time.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)),
        None => json!("0001-01-01T00:00:00Z"),
    }
}

// The daemon only needs the lifecycle category; never expose a custom status as
// a legacy terminal key unless its catalog category resolves in this workspace.
fn issue_gc_wire<'a>(status: &'a str, category: Option<&'a str>) -> (&'a str, &'a str) {
    let builtin = match status {
        "backlog" | "todo" => Some("unstarted"),
        "in_progress" | "in_review" | "blocked" => Some("started"),
        "done" => Some("done"),
        "cancelled" => Some("closed"),
        _ => None,
    };
    let category = builtin
        .or(category.filter(|value| matches!(*value, "unstarted" | "started" | "done" | "closed")));
    match category {
        Some(value) if builtin.is_some() => (status, value),
        Some("unstarted") => ("todo", "unstarted"),
        Some("started") => ("in_progress", "started"),
        Some("done") => ("done", "done"),
        Some("closed") => ("cancelled", "closed"),
        _ => (status, ""),
    }
}

fn gc_updated_at(value: chrono::DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

/// Go `BatchIssueGCCheck`: one answer per requested id, workspace-scoped in SQL
/// so a foreign or missing issue is indistinguishable from a deleted one.
/// Custom statuses resolve through the same fail-closed projection as the
/// single-issue route.
async fn batch_issue_gc_check(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let workspace = Uuid::parse_str(raw.trim())
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to check issues"))?;
    workspace_access(&**db, &identity, &workspace.to_string()).await?;
    #[derive(serde::Deserialize)]
    struct Body {
        #[serde(default)]
        issue_ids: Vec<String>,
    }
    let request: Body = serde_json::from_slice(&bytes)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if request.issue_ids.len() > 500 {
        return Err(error(StatusCode::BAD_REQUEST, "too many issue_ids"));
    }
    let mut parsed = Vec::with_capacity(request.issue_ids.len());
    for issue in &request.issue_ids {
        parsed.push(
            Uuid::parse_str(issue.trim())
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid issue_id"))?,
        );
    }
    let rows = if parsed.is_empty() {
        Vec::new()
    } else {
        db.query(
            "SELECT id,status,updated_at FROM issue WHERE workspace_id=$1 AND id=ANY($2::uuid[])",
            &[&workspace, &parsed],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to check issues"))?
    };
    let by_id: HashMap<Uuid, &Row> = rows.iter().map(|row| (row.get(0), row)).collect();
    let custom: Vec<String> = rows
        .iter()
        .map(|row| row.get::<_, String>(1))
        .filter(|status| {
            !matches!(
                status.as_str(),
                "backlog" | "todo" | "in_progress" | "in_review" | "blocked" | "done" | "cancelled"
            )
        })
        .collect();
    // One catalog read for the whole batch; an all-built-in batch performs none.
    let mut categories = HashMap::new();
    if !custom.is_empty() {
        for row in db
            .query(
                "SELECT key,category FROM issue_status WHERE workspace_id=$1 AND key=ANY($2::text[])",
                &[&workspace, &custom],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to check issues"))?
        {
            categories.insert(row.get::<_, String>(0), row.get::<_, String>(1));
        }
    }
    let issues: Vec<Value> = request
        .issue_ids
        .iter()
        .zip(parsed)
        .map(|(original, id)| {
            let Some(row) = by_id.get(&id) else {
                return json!({"id": original, "found": false});
            };
            let status: String = row.get(1);
            let (wire_status, wire_category) =
                issue_gc_wire(&status, categories.get(&status).map(String::as_str));
            json!({
                "id": original,
                "found": true,
                "status": wire_status,
                "category": wire_category,
                "updated_at": gc_updated_at(row.get(2)),
            })
        })
        .collect();
    Ok(GoJson(json!({"issues": issues})))
}

async fn issue_gc_check(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid issue_id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let row = db
        .query_opt(
            "SELECT workspace_id,status,updated_at FROM issue WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let workspace: Uuid = row.get(0);
    workspace_access(&**db, &identity, &workspace.to_string()).await?;
    let status: String = row.get(1);
    let category: Option<String> = if matches!(
        status.as_str(),
        "backlog" | "todo" | "in_progress" | "in_review" | "blocked" | "done" | "cancelled"
    ) {
        None
    } else {
        // Go's read resolver fails closed on a missing/invalid catalog entry.
        db.query_opt(
            "SELECT category FROM issue_status WHERE workspace_id=$1 AND key=$2",
            &[&workspace, &status],
        )
        .await
        .ok()
        .flatten()
        .map(|entry| entry.get(0))
    };
    let (wire_status, wire_category) = issue_gc_wire(&status, category.as_deref());
    Ok(GoJson(json!({
        "status": wire_status,
        "category": wire_category,
        "updated_at": row.get::<_, chrono::DateTime<Utc>>(2).to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
    })))
}

async fn chat_session_gc_check(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid session_id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    let row = db
        .query_opt(
            "SELECT workspace_id,status,updated_at FROM chat_session WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    let workspace: Uuid = row.get(0);
    workspace_access(&**db, &identity, &workspace.to_string()).await?;
    Ok(GoJson(json!({
        "status": row.get::<_, String>(1),
        "updated_at": row.get::<_, chrono::DateTime<Utc>>(2).to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
    })))
}

async fn autopilot_run_gc_check(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let id = Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid run_id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "autopilot run not found"))?;
    // A deleted parent is indistinguishable from a missing run to the daemon.
    let row = db.query_opt(
        "SELECT a.workspace_id,r.status,r.completed_at FROM autopilot_run r JOIN autopilot a ON a.id=r.autopilot_id WHERE r.id=$1",
        &[&id],
    ).await.map_err(|_| error(StatusCode::NOT_FOUND, "autopilot run not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "autopilot run not found"))?;
    let workspace: Uuid = row.get(0);
    workspace_access(&**db, &identity, &workspace.to_string()).await?;
    Ok(GoJson(json!({
        "status": row.get::<_, String>(1),
        "completed_at": gc_completed_at(row.get(2)),
    })))
}

// Unlike the lightweight status poll, GC must resolve the task's source scope.
// Only a completed lookup proving absence may yield 404; DB faults must not
// tell the daemon to delete a healthy workdir.
async fn task_gc_check(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task_id"))?;
    let failed = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load task");
    let missing = || error(StatusCode::NOT_FOUND, "task not found");
    let db = state.pool.get().await.map_err(|_| failed())?;
    let task = db.query_opt(
        "SELECT issue_id,chat_session_id,autopilot_run_id,context,runtime_id,status,completed_at FROM agent_task_queue WHERE id=$1",
        &[&id],
    ).await.map_err(|_| failed())?.ok_or_else(missing)?;
    let issue: Option<Uuid> = task.get("issue_id");
    let chat: Option<Uuid> = task.get("chat_session_id");
    let run: Option<Uuid> = task.get("autopilot_run_id");
    let mut scope = None;
    let mut lookup_failed = false;
    if let Some(issue) = issue {
        match db
            .query_opt("SELECT workspace_id FROM issue WHERE id=$1", &[&issue])
            .await
        {
            Ok(Some(row)) => scope = Some(row.get::<_, Uuid>(0)),
            Err(_) => lookup_failed = true,
            _ => {}
        }
    }
    if scope.is_none()
        && let Some(chat) = chat
    {
        match db
            .query_opt(
                "SELECT workspace_id FROM chat_session WHERE id=$1",
                &[&chat],
            )
            .await
        {
            Ok(Some(row)) => scope = Some(row.get::<_, Uuid>(0)),
            Err(_) => lookup_failed = true,
            _ => {}
        }
    }
    if scope.is_none()
        && let Some(run) = run
    {
        match db.query_opt(
            "SELECT a.workspace_id FROM autopilot_run r JOIN autopilot a ON a.id=r.autopilot_id WHERE r.id=$1",
            &[&run],
        ).await {
            Ok(Some(row)) => scope = Some(row.get::<_, Uuid>(0)),
            Err(_) => lookup_failed = true,
            _ => {}
        }
    }
    if scope.is_none() && issue.is_none() && chat.is_none() && run.is_none() {
        let context: Option<Value> = task.get("context");
        if let Some(value) =
            context.filter(|v| v.get("type").and_then(Value::as_str) == Some("quick_create"))
        {
            // Preserve Go's malformed-workspace behavior: access check returns 404.
            let raw = value
                .get("workspace_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            scope =
                Some(Uuid::parse_str(raw).map_err(|_| error(StatusCode::NOT_FOUND, "not found"))?);
        }
    }
    let workspace = scope.ok_or_else(|| if lookup_failed { failed() } else { missing() })?;
    workspace_access(&**db, &identity, &workspace.to_string()).await?;
    let runtime: Option<Uuid> = task.get("runtime_id");
    let runtime = runtime.ok_or_else(missing)?;
    let rt = db
        .query_opt(
            "SELECT workspace_id,daemon_id,owner_id FROM agent_runtime WHERE id=$1",
            &[&runtime],
        )
        .await
        .map_err(|_| failed())?
        .ok_or_else(missing)?;
    let rt_workspace: Uuid = rt.get(0);
    let rt_daemon: Option<String> = rt.get(1);
    let rt_owner: Option<Uuid> = rt.get(2);
    let bound = match identity {
        DaemonIdentity::User(user) => rt_owner == Some(user),
        DaemonIdentity::Bound { daemon, owner, .. } => {
            rt_daemon.as_deref() == Some(&daemon)
                && !daemon.trim().is_empty()
                && (daemon.starts_with("qoder-managed-") || owner.is_some() && owner == rt_owner)
        }
    };
    if rt_workspace != workspace || !bound {
        return Err(missing());
    }
    Ok(GoJson(json!({
        "status": task.get::<_, String>("status"),
        "completed_at": gc_completed_at(task.get("completed_at")),
    })))
}

async fn task_status(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(task_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let id =
        Uuid::parse_str(&task_id).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task_id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load task"))?;
    let task = db
        .query_opt(
            "SELECT q.status,a.workspace_id,q.runtime_id,r.workspace_id,r.daemon_id,r.owner_id FROM agent_task_queue q JOIN agent a ON a.id=q.agent_id LEFT JOIN agent_runtime r ON r.id=q.runtime_id WHERE q.id=$1",
            &[&id],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load task"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "task not found"))?;
    let workspace: Uuid = task.get(1);
    match &identity {
        DaemonIdentity::User(user) => {
            let member = db
                .query_opt(
                    "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                    &[&workspace, user],
                )
                .await
                .map_err(|_| error(StatusCode::NOT_FOUND, "not found"))?;
            if member.is_none() {
                return Err(error(StatusCode::NOT_FOUND, "not found"));
            }
        }
        DaemonIdentity::Bound {
            workspace: bound,
            daemon,
            hash,
            ..
        } => {
            if *bound != workspace {
                return Err(error(StatusCode::NOT_FOUND, "not found"));
            }
            if daemon.starts_with("qoder-managed-") {
                let allowed: bool = db
                    .query_one(
                        "SELECT EXISTS(SELECT 1 FROM cloud_agent_connection c JOIN daemon_token d ON d.id=c.token_id JOIN member m ON m.workspace_id=c.workspace_id AND m.user_id=c.owner_id WHERE c.provider='qoder_cloud' AND c.workspace_id=$1 AND c.enabled AND d.daemon_id=$2 AND d.token_hash=$3 AND d.expires_at>now() AND m.role IN ('owner','admin'))",
                        &[bound, daemon, hash],
                    )
                    .await
                    .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "runtime authorization unavailable"))?
                    .get(0);
                if !allowed {
                    return Err(error(StatusCode::FORBIDDEN, "runtime access revoked"));
                }
            }
        }
    }
    if task.get::<_, Option<Uuid>>(2).is_some() {
        let runtime_workspace: Option<Uuid> = task.get(3);
        let runtime_daemon: Option<String> = task.get(4);
        let runtime_owner: Option<Uuid> = task.get(5);
        let bound = match &identity {
            DaemonIdentity::User(user) => runtime_owner == Some(*user),
            DaemonIdentity::Bound { daemon, owner, .. } => {
                runtime_daemon.as_deref() == Some(daemon.as_str())
                    && !daemon.trim().is_empty()
                    && (daemon.starts_with("qoder-managed-")
                        || owner.is_some() && owner == &runtime_owner)
            }
        };
        if runtime_workspace != Some(workspace) || !bound {
            return Err(error(StatusCode::NOT_FOUND, "task not found"));
        }
    }
    Ok(GoJson(json!({ "status": task.get::<_, String>(0) })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issue_gc_wire_fails_closed_on_unknown_categories() {
        assert_eq!(issue_gc_wire("todo", None), ("todo", "unstarted"));
        assert_eq!(
            issue_gc_wire("in_review", Some("closed")),
            ("in_review", "started")
        );
        assert_eq!(issue_gc_wire("custom", Some("done")), ("done", "done"));
        assert_eq!(
            issue_gc_wire("custom", Some("started")),
            ("in_progress", "started")
        );
        assert_eq!(
            issue_gc_wire("custom", Some("unstarted")),
            ("todo", "unstarted")
        );
        assert_eq!(
            issue_gc_wire("custom", Some("closed")),
            ("cancelled", "closed")
        );
        assert_eq!(issue_gc_wire("custom", None), ("custom", ""));
        assert_eq!(issue_gc_wire("custom", Some("invalid")), ("custom", ""));
    }

    #[test]
    fn daemon_bearer_never_falls_back_to_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert("cookie", "multica_auth=some-session".parse().unwrap());
        assert_eq!(
            bearer(&headers).unwrap_err().1,
            "missing authorization header"
        );
        for value in ["bearer token", "Basic token", "Bearer"] {
            headers.insert("authorization", value.parse().unwrap());
            assert_eq!(
                bearer(&headers).unwrap_err().1,
                "invalid authorization format"
            );
        }
        headers.insert("authorization", "Bearer token".parse().unwrap());
        assert_eq!(bearer(&headers).unwrap(), "token");
    }

    #[test]
    fn discovery_etag_uses_go_json_escape_rules() {
        let rows = [WorkspaceName {
            id: Uuid::nil(),
            name: "<&>\u{2028}\u{2029}".into(),
        }];
        let raw = r#"[{"id":"00000000-0000-0000-0000-000000000000","name":"\u003c\u0026\u003e\u2028\u2029"}]"#;
        assert_eq!(
            etag(&rows),
            format!("W/\"{}\"", hex::encode(Sha256::digest(raw.as_bytes())))
        );
        assert_ne!(etag(&rows), etag(&[]));
    }
}
