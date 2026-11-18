//! Workspace creation and settings mutations against the frozen Go contract.
use super::{Actor, ApiState, GoJson, Identity, timestamp};
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{post, put},
};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use std::collections::HashSet;
use tokio_postgres::error::SqlState;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/workspaces", post(create))
        .route("/api/workspaces/", post(create))
        .route("/api/workspaces/{id}", put(update).patch(update))
        .route("/api/workspaces/{id}/", put(update).patch(update))
}
#[derive(Debug)]
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
struct Create {
    #[serde(default, deserialize_with = "go_string")]
    name: String,
    #[serde(default, deserialize_with = "go_string")]
    slug: String,
    description: Option<String>,
    context: Option<String>,
    issue_prefix: Option<String>,
}
fn go_string<'de, D: Deserializer<'de>>(value: D) -> Result<String, D::Error> {
    Ok(Option::<String>::deserialize(value)?.unwrap_or_default())
}
fn email_allowed(raw: &str, email: &str) -> bool {
    let mut entries = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .peekable();
    entries.peek().is_none() || entries.any(|allowed| allowed.eq_ignore_ascii_case(email.trim()))
}
#[derive(Deserialize)]
struct Reserved {
    groups: Vec<ReservedGroup>,
}
#[derive(Deserialize)]
struct ReservedGroup {
    slugs: Vec<String>,
}
fn reserved(slug: &str) -> bool {
    let list: Reserved =
        serde_json::from_str(include_str!("account_catalog_write_reserved_slugs.json"))
            .expect("frozen Go reserved slug catalog must be valid JSON");
    list.groups
        .iter()
        .any(|group| group.slugs.iter().any(|s| s == slug))
}
fn go_slug_lower(raw: &str) -> String {
    // Go strings.ToLower applies one-rune mappings; Rust expands dotted I.
    raw.chars()
        .map(|ch| ch.to_lowercase().next().unwrap_or(ch))
        .collect()
}

fn go_prefix_upper(raw: &str) -> String {
    // Go strings.ToUpper never expands one rune into several (e.g. ß).
    raw.chars()
        .map(|ch| {
            let mut upper = ch.to_uppercase();
            let first = upper.next().unwrap_or(ch);
            if upper.next().is_some() { ch } else { first }
        })
        .collect()
}

fn normalize(mut req: Create) -> Result<(Create, String), Failure> {
    req.name = req.name.trim().to_owned();
    req.slug = go_slug_lower(req.slug.trim());
    if req.name.is_empty() || req.slug.is_empty() {
        return Err(fail(StatusCode::BAD_REQUEST, "name and slug are required"));
    }
    if req.slug.split('-').any(|part| part.is_empty())
        || !req
            .slug
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "slug must contain only lowercase letters, numbers, and hyphens",
        ));
    }
    if reserved(&req.slug) {
        return Err(fail(StatusCode::BAD_REQUEST, "slug is reserved"));
    }
    let prefix = match req.issue_prefix.as_deref().map(str::trim) {
        Some(s) if !s.is_empty() => {
            let p = go_prefix_upper(s);
            if p.len() > 10
                || !p
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "issue prefix must be 1-10 uppercase letters or digits",
                ));
            }
            p
        }
        _ => req
            .slug
            .bytes()
            .filter(|b| b.is_ascii_alphanumeric())
            .take(4)
            .map(|b| (b as char).to_ascii_uppercase())
            .collect(),
    };
    Ok((req, prefix))
}

pub(super) const SEED: &str = r#"INSERT INTO issue_status (workspace_id,key,name,description,category,color,is_system,position)
VALUES
 ($1,'backlog','Backlog','Parked. Assigning an issue here never starts an agent run.','unstarted','#6b7280',TRUE,0),
 ($1,'todo','Todo','Queued for work. Moving an issue here starts the assigned agent.','unstarted','#6b7280',TRUE,0),
 ($1,'in_progress','In Progress','Actively being worked on.','started','#f59e0b',TRUE,0),
 ($1,'in_review','In Review','Work delivered, waiting on human review. Finalizes the autopilot run.','started','#22c55e',TRUE,0),
 ($1,'done','Done','Completed.','done','#3b82f6',TRUE,0),
 ($1,'blocked','Blocked','Stalled on an external dependency.','started','#ef4444',TRUE,0),
 ($1,'cancelled','Cancelled','Decided not to do.','closed','#6b7280',TRUE,0)
ON CONFLICT DO NOTHING"#;

async fn create(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Json<Create>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, GoJson<Value>), Failure> {
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "this endpoint is only available to human actors",
        ));
    }
    if std::env::var("DISABLE_WORKSPACE_CREATION").as_deref() == Ok("true") {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "workspace creation is disabled for this instance",
        ));
    }
    let mut db = state.pool.get().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create workspace",
        )
    })?;
    if let Ok(raw) = std::env::var("WORKSPACE_CREATION_ALLOWED_EMAILS") {
        let allowlist: Vec<&str> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        if !allowlist.is_empty() {
            let account = db
                .query_opt("SELECT email FROM \"user\" WHERE id=$1", &[&user])
                .await
                .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?
                .ok_or_else(|| fail(StatusCode::UNAUTHORIZED, "user not found"))?;
            let email: String = account.get(0);
            if !email_allowed(&raw, &email) {
                return Err(fail(
                    StatusCode::FORBIDDEN,
                    "workspace creation is restricted to selected users on this instance",
                ));
            }
        }
    }
    let Json(req) = body.map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let (req, prefix) = normalize(req)?;
    let tx = db.transaction().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create workspace",
        )
    })?;
    let ws = tx.query_one("INSERT INTO workspace(name,slug,description,context,issue_prefix) VALUES($1,$2,$3,$4,$5) RETURNING id,name,slug,description,context,settings,repos,issue_prefix,avatar_url,created_at,updated_at",
            &[&req.name,&req.slug,&req.description,&req.context,&prefix]).await
        .map_err(|e| if e.code()==Some(&SqlState::UNIQUE_VIOLATION) {
            fail(StatusCode::CONFLICT,"workspace slug already exists")
        } else {fail(StatusCode::INTERNAL_SERVER_ERROR,"failed to create workspace")})?;
    let id: Uuid = ws.get("id");
    tx.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
        &[&id, &user],
    )
    .await
    .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to add owner"))?;
    tx.execute(SEED, &[&id]).await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to seed issue statuses",
        )
    })?;
    let response = json!({
        "id": id, "name": ws.get::<_, String>("name"), "slug": ws.get::<_, String>("slug"),
        "description": ws.get::<_, Option<String>>("description"), "context": ws.get::<_, Option<String>>("context"),
        "settings": ws.get::<_, Value>("settings"), "repos": ws.get::<_, Value>("repos"),
        "issue_prefix": ws.get::<_, String>("issue_prefix"), "avatar_url": ws.get::<_, Option<String>>("avatar_url"),
        "created_at": timestamp(ws.get("created_at")), "updated_at": timestamp(ws.get("updated_at"))
    });
    tx.commit().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create workspace",
        )
    })?;
    drop(db);
    if let Some(metrics) = &state.workspace_metrics {
        metrics
            .record(
                super::account_catalog_write_workspace_effects::WorkspaceCreated {
                    user_id: user,
                    workspace_id: id,
                },
            )
            .await;
    }
    if let Some(publisher) = &state.catalog_publisher {
        publisher.notify_daemon_workspaces_changed(vec![user]).await;
    }
    Ok((StatusCode::CREATED, GoJson(response)))
}

#[derive(Deserialize)]
struct Update {
    name: Option<String>,
    description: Option<String>,
    context: Option<String>,
    settings: Option<Value>,
    repos: Option<Value>,
    issue_prefix: Option<String>,
    avatar_url: Option<String>,
}

fn legacy_pr_on(settings: &serde_json::Map<String, Value>) -> bool {
    match settings.get("pr_auto_complete_enabled") {
        None | Some(Value::Null) => true,
        Some(Value::Bool(on)) => *on,
        _ => false,
    }
}

fn pr_merge_status(settings: &serde_json::Map<String, Value>) -> String {
    match settings.get("pr_merge_status") {
        None | Some(Value::Null) => {
            if legacy_pr_on(settings) {
                "done".to_owned()
            } else {
                "none".to_owned()
            }
        }
        Some(Value::String(value)) if !value.trim().is_empty() => value.trim().to_ascii_lowercase(),
        _ => "none".to_owned(),
    }
}

fn reconcile_settings(stored: &Value, incoming: &mut Value) {
    let Some(settings) = incoming.as_object_mut() else {
        return;
    };
    if settings.contains_key("pr_auto_complete_enabled") {
        let previous = stored.as_object().is_none_or(legacy_pr_on);
        if legacy_pr_on(settings) != previous {
            let status = if legacy_pr_on(settings) {
                "done"
            } else {
                "none"
            };
            settings.insert("pr_merge_status".into(), json!(status));
        }
    }
    if pr_merge_status(settings) == "none" {
        settings.insert("pr_auto_complete_enabled".into(), json!(false));
    } else {
        settings.remove("pr_auto_complete_enabled");
    }
}

fn valid_repo_url(raw: &str) -> bool {
    if let Ok(url) = reqwest::Url::parse(raw)
        && url.host_str().is_some()
        && matches!(url.scheme(), "http" | "https" | "ssh" | "git")
    {
        return true;
    }
    if raw.contains(' ') || raw.contains("://") {
        return false;
    }
    let Some(colon) = raw.find(':') else {
        return false;
    };
    let at = raw.find('@');
    colon > 0 && colon + 1 < raw.len() && at.is_none_or(|index| index < colon && index + 1 < colon)
}

fn normalize_repos(value: Value) -> Result<Value, Failure> {
    let Value::Array(repos) = value else {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "repos must be an array of repository objects",
        ));
    };
    let mut seen = HashSet::new();
    let mut normalized = Vec::new();
    for (index, repo) in repos.into_iter().enumerate() {
        let Some(repo) = repo.as_object() else {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "repos must be an array of repository objects",
            ));
        };
        let url = match repo.get("url") {
            None | Some(Value::Null) => "",
            Some(Value::String(url)) => url.as_str(),
            _ => {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "repos must be an array of repository objects",
                ));
            }
        };
        let description = match repo.get("description").unwrap_or(&Value::Null) {
            Value::Null => "",
            Value::String(description) => description.as_str(),
            _ => {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "repos must be an array of repository objects",
                ));
            }
        };
        let url = url.trim();
        if url.is_empty() {
            return Err(Failure(
                StatusCode::BAD_REQUEST,
                json!({"error":format!("repos[{index}]: url is required")}),
            ));
        }
        if !valid_repo_url(url) {
            return Err(Failure(
                StatusCode::BAD_REQUEST,
                json!({"error":format!("repos[{index}]: url must be a valid http(s) or ssh git URL")}),
            ));
        }
        if seen.insert(url.to_owned()) {
            let mut entry = json!({"url":url});
            let description = description.trim();
            if !description.is_empty() {
                entry["description"] = json!(description);
            }
            normalized.push(entry);
        }
    }
    Ok(json!(normalized))
}

async fn update(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_ws): Path<String>,
    body: Result<Json<Update>, axum::extract::rejection::JsonRejection>,
) -> Result<GoJson<Value>, Failure> {
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
    let mut db = state.pool.get().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update workspace",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update workspace",
        )
    })?;
    let current = tx
        .query_opt(
            "SELECT settings,avatar_url FROM workspace WHERE id=$1 FOR UPDATE",
            &[&ws],
        )
        .await
        .map_err(|_| {
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to update workspace",
            )
        })?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    let member = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    let role: String = member.get(0);
    if role != "owner" && role != "admin" {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "insufficient workspace permissions",
        ));
    }
    let Json(req) = body.map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let notify_daemons = req.name.is_some() || req.settings.is_some();
    let name = req.name.map(|name| name.trim().to_owned());
    if name.as_deref().is_some_and(str::is_empty) {
        return Err(fail(StatusCode::BAD_REQUEST, "name is required"));
    }
    let prefix = req
        .issue_prefix
        .map(|prefix| go_prefix_upper(prefix.trim()));
    if prefix.as_deref().is_some_and(|prefix| {
        !prefix.is_empty()
            && (prefix.len() > 10
                || !prefix
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()))
    }) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "issue prefix must be 1-10 uppercase letters or digits",
        ));
    }
    let prefix = prefix.filter(|prefix| !prefix.is_empty());
    let mut settings = req.settings;
    if let Some(settings) = settings.as_mut() {
        reconcile_settings(&current.get::<_, Value>("settings"), settings);
    }
    let repos = req.repos.map(normalize_repos).transpose()?;
    let current_avatar: Option<String> = current.get("avatar_url");
    let avatar = match req.avatar_url {
        Some(raw) => Some(
            super::account_catalog_write_avatar::accept(
                &state,
                &*tx,
                &raw,
                current_avatar.as_deref().unwrap_or(""),
            )
            .await
            .map_err(|message| fail(StatusCode::FORBIDDEN, message))?,
        ),
        None => None,
    };
    let row = tx.query_one("UPDATE workspace SET name=COALESCE($2,name), description=COALESCE($3,description), context=COALESCE($4,context), settings=COALESCE($5,settings), repos=COALESCE($6,repos), issue_prefix=COALESCE($7,issue_prefix), avatar_url=COALESCE($8,avatar_url), updated_at=now() WHERE id=$1 RETURNING id,name,slug,description,context,settings,repos,issue_prefix,avatar_url,created_at,updated_at",
        &[&ws,&name,&req.description,&req.context,&settings,&repos,&prefix,&avatar]).await
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR,"failed to update workspace"))?;
    let response = json!({
        "id":row.get::<_,Uuid>("id"), "name":row.get::<_,String>("name"),
        "slug":row.get::<_,String>("slug"), "description":row.get::<_,Option<String>>("description"),
        "context":row.get::<_,Option<String>>("context"), "settings":row.get::<_,Value>("settings"),
        "repos":row.get::<_,Value>("repos"), "issue_prefix":row.get::<_,String>("issue_prefix"),
        "avatar_url":row.get::<_,Option<String>>("avatar_url"),
        "created_at":timestamp(row.get("created_at")), "updated_at":timestamp(row.get("updated_at"))
    });
    tx.commit().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update workspace",
        )
    })?;
    let daemon_users: Vec<Uuid> = if notify_daemons {
        db.query("SELECT user_id FROM member WHERE workspace_id=$1", &[&ws])
            .await
            .unwrap_or_default()
            .iter()
            .map(|row| row.get("user_id"))
            .collect()
    } else {
        Vec::new()
    };
    drop(db);
    super::account_catalog_write_catalog_publish::publish(
        &state,
        ws,
        user,
        super::account_catalog_write_catalog_publish::CatalogChange::WorkspaceUpdated {
            workspace: response.clone(),
        },
    )
    .await;
    if notify_daemons && let Some(publisher) = &state.catalog_publisher {
        publisher
            .notify_daemon_workspaces_changed(daemon_users)
            .await;
    }
    Ok(GoJson(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creation_validation_and_legacy_json() {
        let req: Create =
            serde_json::from_value(json!({"name":" 中文 ","slug":" Front-End "})).unwrap();
        let (req, prefix) = normalize(req).unwrap_or_else(|_| panic!("valid create rejected"));
        assert_eq!(req.name, "中文");
        assert_eq!(req.slug, "front-end");
        assert_eq!(prefix, "FRON");
        let dotted: Create = serde_json::from_value(
            json!({"name":"Unicode","slug":"İdea-Lab","issue_prefix":" ıſ9 "}),
        )
        .unwrap();
        let (dotted, prefix) = normalize(dotted).unwrap();
        assert_eq!(dotted.slug, "idea-lab");
        assert_eq!(prefix, "IS9");
        let sharp_s: Create =
            serde_json::from_value(json!({"name":"Unicode","slug":"test","issue_prefix":"ß"}))
                .unwrap();
        assert!(normalize(sharp_s).is_err());
        for value in [
            json!({}),
            json!({"name":null,"slug":"a"}),
            json!({"name":"yes","slug":"login"}),
            json!({"name":"yes","slug":"a--b"}),
            json!({"name":"yes","slug":"a","issue_prefix":"abcdefghijk"}),
        ] {
            let req: Create = serde_json::from_value(value).unwrap();
            assert!(normalize(req).is_err());
        }
        assert!(!reserved("research-lab"));
        assert!(email_allowed("", "one@example.com"));
        assert!(email_allowed(" , ", "one@example.com"));
        assert!(email_allowed(
            "other@example.com, ONE@EXAMPLE.COM",
            " one@example.com "
        ));
        assert!(!email_allowed("other@example.com", "one@example.com"));
    }

    #[test]
    fn workspace_settings_and_repos_match_legacy_update_contract() {
        let mut old_client = json!({"pr_auto_complete_enabled":false,"pr_merge_status":"done"});
        reconcile_settings(&json!({}), &mut old_client);
        assert_eq!(old_client["pr_merge_status"], "none");
        assert_eq!(old_client["pr_auto_complete_enabled"], false);
        let mut new_client = json!({"pr_merge_status":"In_Review"});
        reconcile_settings(&json!({}), &mut new_client);
        assert_eq!(new_client["pr_merge_status"], "In_Review");
        assert!(new_client.get("pr_auto_complete_enabled").is_none());
        let mut old_enabled = json!({"pr_auto_complete_enabled":true});
        reconcile_settings(&old_client, &mut old_enabled);
        assert_eq!(old_enabled["pr_merge_status"], "done");
        assert!(old_enabled.get("pr_auto_complete_enabled").is_none());
        let mut malformed = json!({"pr_merge_status":42});
        reconcile_settings(&json!({}), &mut malformed);
        assert_eq!(malformed["pr_auto_complete_enabled"], false);

        let repos = normalize_repos(json!([
            {"url":" https://github.com/example/repo.git ","description":" primary ","ignored":42},
            {"url":"https://github.com/example/repo.git"},
            {"url":"git@example.com:owner/repo.git"}
        ]))
        .unwrap();
        assert_eq!(repos.as_array().unwrap().len(), 2);
        assert_eq!(repos[0]["description"], "primary");
        assert!(repos[0].get("ignored").is_none());
        for invalid in ["not-a-url", "git@host:/path://", "user@host:", "http://"] {
            assert!(!valid_repo_url(invalid), "{invalid}");
        }
    }
}
