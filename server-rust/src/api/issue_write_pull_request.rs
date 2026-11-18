//! Issue-side PR links and per-issue merge automation. Provider sync belongs elsewhere.
use super::{
    ApiError, ApiState, GoJson, Identity, actor::Actor, catalog::WorkspaceId, error,
    issue_read::load_issue, issue_write::write_actor,
};
use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{post, put},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct IssuePullRequestActivity {
    pub workspace_id: Uuid,
    pub issue_id: Uuid,
    pub id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    pub disabled: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Integrator connects the authoritative PR read model, workspace event bus and
/// PR merge decision service. Neither a synthetic list nor a no-op merger is safe.
#[async_trait]
pub trait IssuePullRequestRuntime: Send + Sync {
    async fn list(&self, workspace_id: Uuid, issue_id: Uuid) -> Result<Value, ()>;
    async fn maybe_auto_complete(&self, workspace_id: Uuid, issue_id: Uuid);
    fn publish_changed(&self, workspace_id: Uuid, issue_id: Uuid);
    fn publish_activity(&self, event: IssuePullRequestActivity);
}
pub type SharedIssuePullRequestRuntime = Arc<dyn IssuePullRequestRuntime>;

#[derive(Debug, Default)]
struct Request<const TOGGLE: bool> {
    url: String,
    pull_request_id: String,
    disabled: Option<bool>,
}
impl<'de, const TOGGLE: bool> Deserialize<'de> for Request<TOGGLE> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor<const TOGGLE: bool>;
        impl<'de, const TOGGLE: bool> serde::de::Visitor<'de> for Visitor<TOGGLE> {
            type Value = Request<TOGGLE>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(Request::default())
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut request = Request::default();
                while let Some(key) = map.next_key::<String>()? {
                    let key: String = key
                        .chars()
                        .map(|c| match c {
                            '\u{017f}' => 's',
                            '\u{212a}' => 'k',
                            c => c.to_ascii_lowercase(),
                        })
                        .collect();
                    match key.as_str() {
                        "disabled" if TOGGLE => {
                            request.disabled = map.next_value::<Option<bool>>()?;
                        }
                        "url" | "pull_request_id" if !TOGGLE => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                if key == "url" {
                                    request.url = value;
                                } else {
                                    request.pull_request_id = value;
                                }
                            }
                        }
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(request)
            }
        }
        d.deserialize_any(Visitor::<TOGGLE>)
    }
}
fn decode<const TOGGLE: bool>(body: &[u8]) -> Result<Request<TOGGLE>, serde_json::Error> {
    Request::deserialize(&mut serde_json::Deserializer::from_slice(body))
}
#[derive(Clone, Copy)]
enum Provider {
    Github,
    Vcs,
}
impl Provider {
    fn table(self) -> &'static str {
        match self {
            Self::Github => "issue_pull_request",
            Self::Vcs => "issue_vcs_pull_request",
        }
    }
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}/pull-requests", post(link))
        .route(
            "/api/issues/{id}/pull-requests/{pr_id}",
            axum::routing::delete(unlink),
        )
        .route("/api/issues/{id}/pr-auto-complete", put(toggle))
}

fn fail(action: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, action)
}
fn runtime(state: &ApiState) -> Result<&SharedIssuePullRequestRuntime, ApiError> {
    state.issue_pull_request_runtime.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue pull request runtime unavailable",
        )
    })
}
async fn list(
    runtime: &SharedIssuePullRequestRuntime,
    ws: Uuid,
    issue: Uuid,
) -> Result<GoJson<Value>, ApiError> {
    let value = runtime
        .list(ws, issue)
        .await
        .map_err(|_| fail("failed to list pull requests"))?;
    if !value.get("pull_requests").is_some_and(Value::is_array)
        || !value.get("auto_complete").is_some_and(Value::is_object)
    {
        return Err(fail("failed to list pull requests"));
    }
    Ok(GoJson(value))
}

// Go net/url keeps RawPath only when its bytes pass validEncoded. If a raw
// space or Unicode byte makes RawPath invalid, EscapedPath re-escapes decoded
// Path instead; an encoded slash then becomes an actual path separator.
fn escaped_path(path: &str) -> Option<String> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let bytes = path.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut valid_raw = true;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'%' {
            let digits = bytes.get(index + 1..index + 3)?;
            let upper = (digits[0] as char).to_digit(16)?;
            let lower = (digits[1] as char).to_digit(16)?;
            decoded.push(((upper << 4) | lower) as u8);
            index += 3;
            continue;
        }
        decoded.push(byte);
        valid_raw &= byte.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@/[]".contains(&byte);
        index += 1;
    }
    if valid_raw {
        return Some(path.to_owned());
    }
    let mut escaped = String::with_capacity(decoded.len());
    for byte in decoded {
        if byte.is_ascii_alphanumeric() || b"-._~$&+,/:;=@".contains(&byte) {
            escaped.push(byte as char);
        } else {
            escaped.push('%');
            escaped.push(HEX[(byte >> 4) as usize] as char);
            escaped.push(HEX[(byte & 15) as usize] as char);
        }
    }
    Some(escaped)
}

// Go's URL matcher accepts a mirrored PR URL with a numeric last segment,
// then strips a suffix such as /files. Keep lookup inside this workspace.
fn normalized_url(raw: &str) -> Option<String> {
    let url = raw.trim();
    if url.is_empty() {
        return None;
    }
    let url = if url.contains("://") {
        url.to_owned()
    } else {
        format!("https://{url}")
    };
    let parsed = reqwest::Url::parse(&url).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return None;
    }
    if url.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return None;
    }
    if let Some((_, fragment)) = url.split_once('#') {
        // Go parses fragment escapes, but intentionally does not parse query escapes.
        escaped_path(fragment)?;
    }
    let remainder = url.split_once("://")?.1;
    let raw_authority = remainder.split(&['/', '?', '#'][..]).next()?;
    // Go URL.Host excludes userinfo but preserves host spelling and port.
    let authority = raw_authority.rsplit('@').next()?;
    // Work from the original escaped path: URL parsers may collapse dot
    // segments or rewrite escapes, unlike Go's url.URL.EscapedPath.
    let path = remainder[raw_authority.len()..]
        .split(&['?', '#'][..])
        .next()?;
    let path = escaped_path(path)?;
    let segments: Vec<_> = path.split('/').collect();
    let index = (2..segments.len().saturating_sub(1)).find(|&i| {
        !segments[1..i].join("/").is_empty()
            && matches!(segments[i], "pull" | "pulls" | "merge_requests")
            && !segments[i + 1].is_empty()
            && segments[i + 1].bytes().all(|b| b.is_ascii_digit())
    })?;
    let prefix = format!(
        "{}://{}{}",
        parsed.scheme(),
        authority,
        segments[..=index + 1].join("/")
    );
    Some(prefix.to_lowercase())
}

async fn resolve(
    db: &Client,
    ws: Uuid,
    id: Option<Uuid>,
    url: Option<&str>,
    action: &'static str,
) -> Result<Option<(Uuid, Provider)>, ApiError> {
    let lookup = if id.is_some() {
        "id=$2"
    } else {
        "lower(rtrim(html_url, '/'))=lower($2)"
    };
    for (table, provider) in [
        ("github_pull_request", Provider::Github),
        ("vcs_pull_request", Provider::Vcs),
    ] {
        let sql = format!(
            "SELECT id FROM {table} WHERE workspace_id=$1 AND {lookup} ORDER BY pr_updated_at DESC LIMIT 1"
        );
        let row = if let Some(id) = id {
            db.query_opt(&sql, &[&ws, &id]).await
        } else {
            db.query_opt(&sql, &[&ws, &url.unwrap_or_default()]).await
        }
        .map_err(|_| fail(action))?;
        if let Some(row) = row {
            return Ok(Some((row.get(0), provider)));
        }
    }
    Ok(None)
}

async fn link(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let runtime = runtime(&state)?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to link pull request"))?;
    let issue: Uuid = load_issue(&db, ws, &raw).await?.get("id");
    let request = decode::<false>(&body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if request.url.trim().is_empty() && request.pull_request_id.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "url is required"));
    }
    let id = if !request.pull_request_id.is_empty() {
        Uuid::parse_str(&request.pull_request_id).ok()
    } else {
        None
    };
    let url = if request.pull_request_id.is_empty() {
        normalized_url(&request.url)
    } else {
        None
    };
    let (pr, provider) = if id.is_some() || url.is_some() {
        resolve(&db, ws, id, url.as_deref(), "failed to link pull request").await?
    } else {None}.ok_or_else(|| error(StatusCode::NOT_FOUND, "pull request not found; it appears here once its repository is connected and the PR has been synced"))?;
    let (_, actor_id, _) = write_actor(actor, user)?;
    let sql = format!(
        "INSERT INTO {}(issue_id,pull_request_id,linked_by_type,linked_by_id) VALUES ($1,$2,'member',$3) ON CONFLICT(issue_id,pull_request_id) DO UPDATE SET linked_by_type='member',linked_by_id=EXCLUDED.linked_by_id WHERE {}.linked_by_type IS DISTINCT FROM 'member'",
        provider.table(),
        provider.table()
    );
    let changed = db
        .execute(&sql, &[&issue, &pr, &actor_id])
        .await
        .map_err(|_| fail("failed to link pull request"))?
        > 0;
    db.execute(
        "DELETE FROM issue_pull_request_exclusion WHERE issue_id=$1 AND pull_request_id=$2",
        &[&issue, &pr],
    )
    .await
    .map_err(|_| fail("failed to link pull request"))?;
    drop(db);
    if changed {
        runtime.maybe_auto_complete(ws, issue).await;
    }
    runtime.publish_changed(ws, issue);
    list(runtime, ws, issue).await
}

async fn unlink(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, pr_raw)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    let runtime = runtime(&state)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to unlink pull request"))?;
    let issue: Uuid = load_issue(&db, ws, &raw).await?.get("id");
    let pr_id = Uuid::parse_str(&pr_raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid pull request id"))?;
    let (pr, provider) = resolve(&db, ws, Some(pr_id), None, "failed to unlink pull request")
        .await?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "pull request not found"))?;
    let (actor_type, actor_id, _) = write_actor(actor, user)?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to unlink pull request"))?;
    let sql = format!(
        "DELETE FROM {} WHERE issue_id=$1 AND pull_request_id=$2",
        provider.table()
    );
    let changed = tx
        .execute(&sql, &[&issue, &pr])
        .await
        .map_err(|_| fail("failed to unlink pull request"))?
        > 0;
    tx.execute("INSERT INTO issue_pull_request_exclusion(issue_id,pull_request_id,workspace_id,excluded_by_type,excluded_by_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT(issue_id,pull_request_id) DO UPDATE SET excluded_by_type=EXCLUDED.excluded_by_type,excluded_by_id=EXCLUDED.excluded_by_id,created_at=now()", &[&issue,&pr,&ws,&actor_type,&actor_id]).await.map_err(|_| fail("failed to unlink pull request"))?;
    tx.commit()
        .await
        .map_err(|_| fail("failed to unlink pull request"))?;
    drop(db);
    if changed {
        runtime.maybe_auto_complete(ws, issue).await;
    }
    runtime.publish_changed(ws, issue);
    list(runtime, ws, issue).await
}

async fn toggle(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let runtime = runtime(&state)?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to update pull request automation"))?;
    let issue: Uuid = load_issue(&db, ws, &raw).await?.get("id");
    let disabled = decode::<true>(&body)
        .ok()
        .and_then(|v| v.disabled)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "disabled is required"))?;
    let (actor_type, actor_id, _) = write_actor(actor, user)?;
    let before: bool = db.query_one("SELECT EXISTS(SELECT 1 FROM issue_pr_automation WHERE issue_id=$1 AND auto_complete_disabled)", &[&issue]).await.map_err(|_| fail("failed to update pull request automation"))?.get(0);
    db.execute("INSERT INTO issue_pr_automation(issue_id,workspace_id,auto_complete_disabled,updated_by_type,updated_by_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT(issue_id) DO UPDATE SET auto_complete_disabled=EXCLUDED.auto_complete_disabled,updated_by_type=EXCLUDED.updated_by_type,updated_by_id=EXCLUDED.updated_by_id,updated_at=now()", &[&issue,&ws,&disabled,&actor_type,&actor_id]).await.map_err(|_| fail("failed to update pull request automation"))?;
    if before != disabled {
        // Go treats activity as best effort; the setting was already committed.
        let activity_id = Uuid::now_v7();
        if let Ok(row) = db.query_one("INSERT INTO activity_log(id,workspace_id,issue_id,actor_type,actor_id,action,details) VALUES($1,$2,$3,$4,$5,'pr_auto_complete_changed',$6) RETURNING created_at", &[&activity_id,&ws,&issue,&actor_type,&actor_id,&json!({"disabled":disabled})]).await {
            runtime.publish_activity(IssuePullRequestActivity {workspace_id:ws,issue_id:issue,id:activity_id,actor_type,actor_id,disabled,created_at:row.get("created_at")});
        }
    }
    drop(db);
    runtime.publish_changed(ws, issue);
    list(runtime, ws, issue).await
}

#[cfg(test)]
mod tests {
    use super::*;

    // Common URL regression cases; the generated Go oracle covers edge cases.
    #[test]
    fn normalizes_go_pr_urls() {
        for (raw, expected) in [
            (
                "https://github.com/acme/widgets/pull/42",
                Some("https://github.com/acme/widgets/pull/42"),
            ),
            (
                "  HTTPS://GitHub.com/Acme/Widgets/pull/42/files?diff=1#x  ",
                Some("https://github.com/acme/widgets/pull/42"),
            ),
            (
                "github.com/acme/widgets/pull/42",
                Some("https://github.com/acme/widgets/pull/42"),
            ),
            (
                "https://gitlab.com/grp/proj/-/merge_requests/7",
                Some("https://gitlab.com/grp/proj/-/merge_requests/7"),
            ),
            (
                "https://example.com/owner/repo/pulls/9/",
                Some("https://example.com/owner/repo/pulls/9"),
            ),
            (
                "https://example.com/owner%2Frepo/pull/9",
                Some("https://example.com/owner%2frepo/pull/9"),
            ),
            ("https://github.com/acme/widgets/issues/42", None),
            ("https://github.com/acme/widgets/pull/42abc", None),
            ("https://github.com/pull/42", None),
            ("https://github.com/", None),
            ("ftp://github.com/acme/widgets/pull/42", None),
            ("https://github.com/acme/widgets/pull/", None),
            ("", None),
        ] {
            assert_eq!(normalized_url(raw).as_deref(), expected, "raw={raw:?}");
        }
    }

    #[test]
    fn matches_frozen_go_url_oracle() {
        let rows: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/issue_write_pull_request_oracle.json"
        ))
        .unwrap();
        for row in rows {
            let raw = row["raw"].as_str().unwrap();
            let expected = row["normalized"].as_str();
            assert_eq!(normalized_url(raw).as_deref(), expected, "raw={raw:?}");
        }
    }

    // Go decodes the first JSON value and ignores the rest of the body.
    #[test]
    fn decodes_go_request_semantics() {
        let link = decode::<false>(br#"{"URL":"u","pull_request_id":"p"}"#).unwrap();
        assert_eq!(link.url, "u");
        assert_eq!(link.pull_request_id, "p");

        let precedence =
            decode::<false>(br#"{"url":"first","pull_request_id":"p","url":"second"}"#).unwrap();
        assert_eq!(precedence.url, "second");

        let nulled = decode::<false>(br#"{"url":"a","url":null}"#).unwrap();
        assert_eq!(nulled.url, "a");

        let trailing = decode::<false>(br#"{"url":"a"} trailing"#).unwrap();
        assert_eq!(trailing.url, "a");

        let toggle = decode::<true>(br#"{"disabled":true,"url":"ignored"}"#).unwrap();
        assert_eq!(toggle.disabled, Some(true));
        assert_eq!(toggle.url, "");

        assert_eq!(decode::<true>(b"null").unwrap().disabled, None);
        assert_eq!(
            decode::<true>(br#"{"disabled":null}"#).unwrap().disabled,
            None
        );
    }
}
