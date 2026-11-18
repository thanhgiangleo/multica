//! Public plugin context, scoped issue/comment reads and isolated KV storage.
//! No network/plugin execution or public issue/comment mutations.
use super::{
    Actor, ApiState, GoJson, Identity, catalog::parse_params, extensions_sprint_lifecycle,
    timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, RawQuery, State},
    http::{HeaderMap, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

#[derive(Clone, Copy)]
enum Mode {
    Token,
    Session,
}

fn action_routes(state: ApiState) -> Router<ApiState> {
    Router::new()
        .route("/context", get(context))
        .route("/issues/{issue_ref}", get(get_issue))
        .route("/issues/{issue_ref}/comments", get(list_comments))
        .route("/storage/{scope}", get(list_storage))
        .route(
            "/storage/{scope}/{key}",
            get(get_storage).put(put_storage).delete(delete_storage),
        )
        .route_layer(middleware::from_fn_with_state(state, guard))
}

pub(super) fn public_routes(state: ApiState) -> Router<ApiState> {
    Router::new().nest("/v1", action_routes(state).layer(Extension(Mode::Token)))
}

pub(super) fn bridge_routes(state: ApiState) -> Router<ApiState> {
    Router::new().nest(
        "/api/plugin-bridge/v1",
        action_routes(state).layer(Extension(Mode::Session)),
    )
}

async fn guard(
    State(state): State<ApiState>,
    Extension(mode): Extension<Mode>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response> {
    if matches!(mode, Mode::Token) {
        let token = request
            .headers()
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "))
            .map(str::trim);
        if !token.is_some_and(|t| t.starts_with("mpi_") || t.starts_with("mpc_")) {
            return Err(problem(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "a Plugin bearer token is required",
            ));
        }
    }
    if !state.extensions_sprint_enabled {
        return Err(problem(
            StatusCode::FORBIDDEN,
            "plugin_api_disabled",
            "Plugin management is not enabled",
        ));
    }
    Ok(next.run(request).await)
}

#[derive(Debug)]
struct Problem(StatusCode, &'static str, String);
type Result<T> = std::result::Result<T, Problem>;

impl IntoResponse for Problem {
    fn into_response(self) -> Response {
        let id = Uuid::new_v4().to_string();
        let mut response = GoJson(json!({
            "type":format!("urn:multica:problem:{}",self.1),
            "title":self.0.canonical_reason().unwrap_or("Request failed"),
            "status":self.0.as_u16(),"code":self.1,"detail":self.2,
            "request_id":id,"error":self.2,
        }))
        .into_response();
        *response.status_mut() = self.0;
        response.headers_mut().insert(
            "content-type",
            HeaderValue::from_static("application/problem+json"),
        );
        response.headers_mut().insert(
            "x-request-id",
            HeaderValue::from_str(&id).expect("UUID header"),
        );
        response
    }
}

fn problem(status: StatusCode, code: &'static str, detail: impl Into<String>) -> Problem {
    Problem(status, code, detail.into())
}
fn unavailable() -> Problem {
    problem(
        StatusCode::BAD_GATEWAY,
        "upstream_unavailable",
        "load plugin installation",
    )
}
fn not_found(detail: &'static str) -> Problem {
    problem(StatusCode::NOT_FOUND, "not_found", detail)
}
fn forbidden(detail: impl Into<String>) -> Problem {
    problem(StatusCode::FORBIDDEN, "forbidden", detail)
}
fn invalid(detail: &'static str) -> Problem {
    problem(StatusCode::BAD_REQUEST, "invalid_request", detail)
}
fn quota(detail: impl Into<String>) -> Problem {
    problem(StatusCode::INSUFFICIENT_STORAGE, "quota_exceeded", detail)
}

struct Caller {
    installation: Uuid,
    workspace: Uuid,
    user: Option<Uuid>,
    scopes: Vec<String>,
    config: Value,
}
impl Caller {
    fn require_scope(&self, scope: &str) -> Result<()> {
        if self.scopes.iter().any(|s| s == scope) {
            Ok(())
        } else {
            Err(problem(
                StatusCode::FORBIDDEN,
                "missing_scope",
                format!("this Plugin was not granted the {scope} scope"),
            ))
        }
    }

    fn storage_scope(&self, scope: &str) -> Result<Uuid> {
        let required = if scope == "user" {
            "storage:user"
        } else {
            "storage:workspace"
        };
        self.require_scope(required)?;
        match scope {
            "workspace" => Ok(self.workspace),
            "user" => self.user.ok_or_else(|| {
                problem(
                    StatusCode::FORBIDDEN,
                    "member_required",
                    "this endpoint requires a user; the presented token acts as the Plugin itself",
                )
            }),
            _ => Err(invalid("storage scope must be \"workspace\" or \"user\"")),
        }
    }
}

async fn caller(
    db: &impl GenericClient,
    enabled: bool,
    mode: Mode,
    identity: Option<Extension<Identity>>,
    actor: Option<Extension<Actor>>,
    headers: &HeaderMap,
) -> Result<Caller> {
    // The origin boundary comes before feature flags or database access.
    let token = match mode {
        Mode::Token => {
            let token = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(str::trim)
                .filter(|v| v.starts_with("mpi_") || v.starts_with("mpc_"))
                .ok_or_else(|| {
                    problem(
                        StatusCode::UNAUTHORIZED,
                        "unauthorized",
                        "a Plugin bearer token is required",
                    )
                })?;
            Some(token)
        }
        Mode::Session => None,
    };
    if !enabled {
        return Err(problem(
            StatusCode::FORBIDDEN,
            "plugin_api_disabled",
            "Plugin management is not enabled",
        ));
    }
    let user = match mode {
        Mode::Session => {
            let Some(Extension(Identity(user))) = identity else {
                return Err(problem(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    "missing authenticated user",
                ));
            };
            // Task and daemon identities are not browser-surface consent.
            if !matches!(actor, Some(Extension(Actor::Human))) {
                return Err(forbidden("this endpoint requires a user session"));
            }
            Some(user)
        }
        Mode::Token => None,
    };
    let row = if let Some(token) = token {
        if token.starts_with("mpc_") {
            // No callback issuer/resolver is wired in this slice. Never treat a
            // callback credential as an install token or a member identity.
            return Err(problem(
                StatusCode::FORBIDDEN,
                "callback_tokens_disabled",
                "callback tokens are not enabled",
            ));
        }
        let hash = hex::encode(Sha256::digest(token.as_bytes()));
        db.query_opt("SELECT id,workspace_id,enabled,granted_scopes,manifest,config FROM plugin_installation WHERE token_hash=$1 FOR SHARE", &[&hash]).await
            .map_err(|_|unavailable())?.ok_or_else(||forbidden("invalid plugin token"))?
    } else {
        let installation = headers
            .get("x-multica-plugin-installation")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        if installation.is_empty() {
            return Err(invalid("plugin installation is required"));
        }
        let installation = Uuid::parse_str(installation)
            .map_err(|_| not_found("plugin installation not found"))?;
        db.query_opt("SELECT id,workspace_id,enabled,granted_scopes,manifest,config FROM plugin_installation WHERE id=$1 FOR SHARE", &[&installation]).await
            .map_err(|_|unavailable())?.ok_or_else(||not_found("plugin installation not found"))?
    };
    if !row.get::<_, bool>("enabled") {
        return Err(forbidden("this Plugin is disabled"));
    }
    let workspace: Uuid = row.get("workspace_id");
    if let Some(user) = user
        && db
            .query_opt(
                "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
                &[&workspace, &user],
            )
            .await
            .map_err(|_| not_found("workspace not found"))?
            .is_none()
    {
        return Err(not_found("workspace not found"));
    }
    let scopes = row.get::<_, Value>("granted_scopes");
    Ok(Caller {
        installation: row.get("id"),
        workspace,
        user,
        scopes: scopes
            .as_array()
            .map(|s| {
                s.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        config: extensions_sprint_lifecycle::visible_config(
            &row.get::<_, Value>("manifest"),
            &row.get::<_, Value>("config"),
        )
        .map_err(|_| {
            problem(
                StatusCode::BAD_GATEWAY,
                "upstream_unavailable",
                "read plugin configuration",
            )
        })?,
    })
}

fn net_domains(scopes: &[String]) -> Vec<String> {
    scopes
        .iter()
        .filter_map(|s| s.strip_prefix("net:"))
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

async fn context(
    State(state): State<ApiState>,
    Extension(mode): Extension<Mode>,
    identity: Option<Extension<Identity>>,
    actor: Option<Extension<Actor>>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>> {
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    let tx = db.transaction().await.map_err(|_| unavailable())?;
    let caller = caller(
        &*tx,
        state.extensions_sprint_enabled,
        mode,
        identity,
        actor,
        &headers,
    )
    .await?;
    let workspace = tx
        .query_opt(
            "SELECT id,name,slug,issue_prefix FROM workspace WHERE id=$1",
            &[&caller.workspace],
        )
        .await
        .map_err(|_| {
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "failed to load the workspace",
            )
        })?
        .ok_or_else(|| not_found("workspace not found"))?;
    let mut out = json!({"workspace":{"id":caller.workspace,"name":workspace.get::<_,String>("name"),"slug":workspace.get::<_,String>("slug")},
        "actor":if caller.user.is_some(){"member"}else{"plugin"},
        "config":caller.config,"granted_net_domains":net_domains(&caller.scopes)});
    if let Some(user) = caller.user {
        let user = tx
            .query_one("SELECT id,name FROM \"user\" WHERE id=$1", &[&user])
            .await
            .map_err(|_| {
                problem(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "failed to load the user",
                )
            })?;
        out["user"] = json!({"id":user.get::<_,Uuid>("id"),"name":user.get::<_,String>("name")});
    }
    let params = parse_params(raw.as_deref());
    if let Some(raw) = params.get("issue_id").filter(|s| !s.is_empty()) {
        let issue = load_issue(&*tx, caller.workspace, raw, &workspace).await?;
        out["issue"] = json!({"id":issue.get::<_,Uuid>("id"),"title":issue.get::<_,String>("title"),
            "identifier":format!("{}-{}",effective_prefix(&workspace),issue.get::<_,i32>("number"))});
    }
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(GoJson(out))
}

async fn get_issue(
    State(state): State<ApiState>,
    Extension(mode): Extension<Mode>,
    identity: Option<Extension<Identity>>,
    actor: Option<Extension<Actor>>,
    headers: HeaderMap,
    Path(raw): Path<String>,
) -> Result<(HeaderMap, GoJson<Value>)> {
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    let tx = db.transaction().await.map_err(|_| unavailable())?;
    let caller = caller(
        &*tx,
        state.extensions_sprint_enabled,
        mode,
        identity,
        actor,
        &headers,
    )
    .await?;
    caller.require_scope("issues:read")?;
    let workspace = load_workspace(&*tx, caller.workspace).await?;
    let row = load_issue(&*tx, caller.workspace, &raw, &workspace).await?;
    let revision: i64 = row.get("revision");
    let payload = issue_payload(&row, &effective_prefix(&workspace));
    tx.commit().await.map_err(|_| unavailable())?;
    let mut headers = HeaderMap::new();
    headers.insert(
        "etag",
        HeaderValue::from_str(&format!("W/\"{revision}\"")).expect("revision header"),
    );
    Ok((headers, GoJson(payload)))
}

async fn list_comments(
    State(state): State<ApiState>,
    Extension(mode): Extension<Mode>,
    identity: Option<Extension<Identity>>,
    actor: Option<Extension<Actor>>,
    headers: HeaderMap,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>> {
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    let tx = db.transaction().await.map_err(|_| unavailable())?;
    let caller = caller(
        &*tx,
        state.extensions_sprint_enabled,
        mode,
        identity,
        actor,
        &headers,
    )
    .await?;
    caller.require_scope("comments:read")?;
    let workspace = load_workspace(&*tx, caller.workspace).await?;
    let issue = load_issue(&*tx, caller.workspace, &raw, &workspace).await?;
    let issue: Uuid = issue.get("id");
    // Bound the newest window, then restore chronological order. Public v1 does
    // not expand missing parent chains beyond this 200-comment window.
    let rows = tx.query("SELECT * FROM (SELECT id,author_type,author_id,content,type,parent_id,created_at,deleted_at FROM comment WHERE issue_id=$1 AND workspace_id=$2 ORDER BY created_at DESC,id DESC LIMIT 200) recent ORDER BY created_at ASC,id ASC", &[&issue,&caller.workspace]).await
        .map_err(|_| problem(StatusCode::INTERNAL_SERVER_ERROR,"internal_error","failed to list comments"))?;
    let comments: Vec<Value> = rows.iter().map(comment_payload).collect();
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(GoJson(json!({"comments":comments})))
}

async fn load_workspace(db: &impl GenericClient, ws: Uuid) -> Result<Row> {
    db.query_opt(
        "SELECT id,name,slug,issue_prefix FROM workspace WHERE id=$1",
        &[&ws],
    )
    .await
    .map_err(|_| {
        problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "failed to load the workspace",
        )
    })?
    .ok_or_else(|| not_found("workspace not found"))
}

fn object_or_empty(value: Value) -> Value {
    if value.is_object() { value } else { json!({}) }
}

fn issue_payload(row: &Row, prefix: &str) -> Value {
    let status: String = row.get("status");
    let mut out = json!({
        "id":row.get::<_,Uuid>("id"),"workspace_id":row.get::<_,Uuid>("workspace_id"),
        "number":row.get::<_,i32>("number"),"identifier":format!("{prefix}-{}",row.get::<_,i32>("number")),
        "title":row.get::<_,String>("title"),"description":row.get::<_,Option<String>>("description"),
        "status":status,"priority":row.get::<_,String>("priority"),
        "assignee_type":row.get::<_,Option<String>>("assignee_type"),"assignee_id":row.get::<_,Option<Uuid>>("assignee_id"),
        "creator_type":row.get::<_,String>("creator_type"),"creator_id":row.get::<_,Uuid>("creator_id"),
        "parent_issue_id":row.get::<_,Option<Uuid>>("parent_issue_id"),"project_id":row.get::<_,Option<Uuid>>("project_id"),
        "position":row.get::<_,f64>("position"),"stage":row.get::<_,Option<i32>>("stage"),
        "start_date":row.get::<_,Option<chrono::NaiveDate>>("start_date").map(|d|d.to_string()),
        "due_date":row.get::<_,Option<chrono::NaiveDate>>("due_date").map(|d|d.to_string()),
        "created_at":timestamp(row.get("created_at")),"updated_at":timestamp(row.get("updated_at")),
        "revision":row.get::<_,i64>("revision"),
        "last_activity_at":row.get::<_,Option<chrono::DateTime<chrono::Utc>>>("last_activity_at").map(|d|d.with_timezone(&chrono::Local).to_rfc3339_opts(chrono::SecondsFormat::AutoSi,true)),
        "metadata":object_or_empty(row.get("metadata")),"properties":object_or_empty(row.get("properties")),
    });
    if matches!(
        status.as_str(),
        "backlog" | "todo" | "in_progress" | "in_review" | "done" | "blocked" | "cancelled"
    ) {
        out["status_category"] = json!(status);
    }
    out
}

fn comment_payload(row: &Row) -> Value {
    let deleted: Option<chrono::DateTime<chrono::Utc>> = row.get("deleted_at");
    let mut out = json!({"id":row.get::<_,Uuid>("id"),"author_type":row.get::<_,String>("author_type"),
        "author_id":row.get::<_,Uuid>("author_id"),"type":row.get::<_,String>("type"),
        "content":if deleted.is_some(){String::new()}else{row.get::<_,String>("content")},
        "created_at":row.get::<_,chrono::DateTime<chrono::Utc>>("created_at").to_rfc3339_opts(chrono::SecondsFormat::Secs,true)});
    if let Some(parent) = row.get::<_, Option<Uuid>>("parent_id") {
        out["parent_id"] = json!(parent);
    }
    if let Some(deleted) = deleted {
        out["deleted_at"] = json!(deleted.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    }
    out
}

fn effective_prefix(workspace: &Row) -> String {
    let prefix: String = workspace.get("issue_prefix");
    if prefix.is_empty() {
        super::squad::legacy_prefix(workspace.get("name"))
    } else {
        prefix
    }
}

async fn load_issue(db: &impl GenericClient, ws: Uuid, raw: &str, workspace: &Row) -> Result<Row> {
    let row = if let Ok(id) = Uuid::parse_str(raw) {
        db.query_opt(
            "SELECT * FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
    } else if let Some((prefix, number)) = raw.rsplit_once('-') {
        if !prefix.eq_ignore_ascii_case(&effective_prefix(workspace)) {
            return Err(not_found("issue not found"));
        }
        let number = number
            .parse::<i32>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| not_found("issue not found"))?;
        db.query_opt(
            "SELECT * FROM issue WHERE number=$1 AND workspace_id=$2",
            &[&number, &ws],
        )
        .await
    } else {
        return Err(not_found("issue not found"));
    };
    row.map_err(|_| not_found("issue not found"))?
        .ok_or_else(|| not_found("issue not found"))
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(invalid("storage key is required"));
    }
    if key.len() > 1024 {
        return Err(quota("storage key exceeds 1024 bytes"));
    }
    if key.contains('\0') {
        return Err(invalid("storage key must not contain null bytes"));
    }
    Ok(())
}
fn check_quota(count: i64, bytes: i64, value: &str) -> Result<()> {
    if value.len() > 102400 {
        return Err(quota("storage value exceeds 102400 bytes"));
    }
    if count >= 1000 {
        return Err(quota(
            "storage scope already holds the maximum of 1000 keys",
        ));
    }
    if bytes + value.len() as i64 > 5 * 1024 * 1024 {
        return Err(quota("storage scope exceeds its 5242880 byte budget"));
    }
    if value.contains('\0') {
        return Err(invalid("storage value must not contain null bytes"));
    }
    Ok(())
}

async fn list_storage(
    State(state): State<ApiState>,
    Extension(mode): Extension<Mode>,
    identity: Option<Extension<Identity>>,
    actor: Option<Extension<Actor>>,
    headers: HeaderMap,
    Path(scope): Path<String>,
) -> Result<GoJson<Value>> {
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    let tx = db.transaction().await.map_err(|_| unavailable())?;
    let caller = caller(
        &*tx,
        state.extensions_sprint_enabled,
        mode,
        identity,
        actor,
        &headers,
    )
    .await?;
    let scope_id = caller.storage_scope(&scope)?;
    let rows=tx.query("SELECT key,octet_length(value)::bigint AS size_bytes,updated_at FROM plugin_storage WHERE installation_id=$1 AND scope_type=$2 AND scope_id=$3 ORDER BY key ASC", &[&caller.installation,&scope,&scope_id]).await
        .map_err(|_|problem(StatusCode::BAD_GATEWAY,"upstream_unavailable","list plugin storage"))?;
    let keys = rows.iter().map(|r|json!({"key":r.get::<_,String>("key"),"size_bytes":r.get::<_,i64>("size_bytes"),"updated_at":timestamp(r.get("updated_at"))})).collect::<Vec<_>>();
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(GoJson(json!({"keys":keys})))
}

async fn get_storage(
    State(state): State<ApiState>,
    Extension(mode): Extension<Mode>,
    identity: Option<Extension<Identity>>,
    actor: Option<Extension<Actor>>,
    headers: HeaderMap,
    Path((scope, key)): Path<(String, String)>,
) -> Result<GoJson<Value>> {
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    let tx = db.transaction().await.map_err(|_| unavailable())?;
    let caller = caller(
        &*tx,
        state.extensions_sprint_enabled,
        mode,
        identity,
        actor,
        &headers,
    )
    .await?;
    let scope_id = caller.storage_scope(&scope)?;
    validate_key(&key)?;
    let row=tx.query_opt("SELECT value FROM plugin_storage WHERE installation_id=$1 AND scope_type=$2 AND scope_id=$3 AND key=$4", &[&caller.installation,&scope,&scope_id,&key]).await
        .map_err(|_|problem(StatusCode::BAD_GATEWAY,"upstream_unavailable","read plugin storage"))?
        .ok_or_else(||not_found("storage key not found"))?;
    let value: String = row.get(0);
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(GoJson(json!({"value":value})))
}

async fn put_storage(
    State(state): State<ApiState>,
    Extension(mode): Extension<Mode>,
    identity: Option<Extension<Identity>>,
    actor: Option<Extension<Actor>>,
    headers: HeaderMap,
    Path((scope, key)): Path<(String, String)>,
    bytes: axum::body::Bytes,
) -> Result<StatusCode> {
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    let tx = db.transaction().await.map_err(|_| unavailable())?;
    let caller = caller(
        &*tx,
        state.extensions_sprint_enabled,
        mode,
        identity,
        actor,
        &headers,
    )
    .await?;
    let scope_id = caller.storage_scope(&scope)?;
    validate_key(&key)?;
    let body: Value =
        serde_json::from_slice(&bytes).map_err(|_| invalid("invalid request body"))?;
    let object = body
        .as_object()
        .ok_or_else(|| invalid("invalid request body"))?;
    let value = match object.get("value") {
        None | Some(Value::Null) => "",
        Some(Value::String(value)) => value,
        _ => return Err(invalid("invalid request body")),
    };
    check_quota(0, 0, value)?;
    // Serialize quota accounting for exactly this bucket, not all plugins.
    // Installation FOR SHARE also fences concurrent lifecycle deletion.
    let bucket = format!("plugin-storage:{}:{scope}:{scope_id}", caller.installation);
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&bucket],
    )
    .await
    .map_err(|_| unavailable())?;
    let usage=tx.query_one("SELECT COUNT(*)::bigint,COALESCE(SUM(octet_length(value)),0)::bigint FROM plugin_storage WHERE installation_id=$1 AND scope_type=$2 AND scope_id=$3 AND key<>$4", &[&caller.installation,&scope,&scope_id,&key]).await.map_err(|_|unavailable())?;
    check_quota(usage.get(0), usage.get(1), value)?;
    tx.execute("INSERT INTO plugin_storage(installation_id,scope_type,scope_id,key,value) VALUES($1,$2,$3,$4,$5) ON CONFLICT(installation_id,scope_type,scope_id,key) DO UPDATE SET value=EXCLUDED.value,updated_at=now()", &[&caller.installation,&scope,&scope_id,&key,&value]).await
        .map_err(|_|problem(StatusCode::BAD_GATEWAY,"upstream_unavailable","write plugin storage"))?;
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_storage(
    State(state): State<ApiState>,
    Extension(mode): Extension<Mode>,
    identity: Option<Extension<Identity>>,
    actor: Option<Extension<Actor>>,
    headers: HeaderMap,
    Path((scope, key)): Path<(String, String)>,
) -> Result<StatusCode> {
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    let tx = db.transaction().await.map_err(|_| unavailable())?;
    let caller = caller(
        &*tx,
        state.extensions_sprint_enabled,
        mode,
        identity,
        actor,
        &headers,
    )
    .await?;
    let scope_id = caller.storage_scope(&scope)?;
    validate_key(&key)?;
    let rows=tx.execute("DELETE FROM plugin_storage WHERE installation_id=$1 AND scope_type=$2 AND scope_id=$3 AND key=$4", &[&caller.installation,&scope,&scope_id,&key]).await
        .map_err(|_|problem(StatusCode::BAD_GATEWAY,"upstream_unavailable","delete plugin storage"))?;
    if rows == 0 {
        return Err(not_found("storage key not found"));
    }
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quotas_count_utf8_bytes_and_allow_exact_replacements() {
        assert!(validate_key(&"é".repeat(512)).is_ok());
        assert_eq!(
            validate_key(&"é".repeat(513)).unwrap_err().0,
            StatusCode::INSUFFICIENT_STORAGE
        );
        assert!(check_quota(999, 5242880 - 102400, &"é".repeat(51200)).is_ok());
        assert!(check_quota(1000, 0, "").is_err());
        assert!(check_quota(0, 0, &"é".repeat(51201)).is_err());
        assert!(check_quota(0, 5242880, "x").is_err());
        assert!(validate_key("\0").is_err());
        assert!(check_quota(0, 0, "\0").is_err());
        assert_eq!(
            net_domains(&["net:example.invalid".into(), "issues:read".into()]),
            vec!["example.invalid"]
        );
    }
}
