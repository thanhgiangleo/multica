//! Quick-create admission and durable enqueue. No model/provider calls.
use super::{ApiError, ApiState, GoJson, Identity, actor::Actor, catalog::WorkspaceId, error};
use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use serde::{
    Deserialize, de,
    de::{MapAccess, Visitor},
};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct IssueQuickCreateAdmission {
    pub workspace_id: Uuid,
    pub requester_id: Uuid,
    pub agent_id: Uuid,
    pub runtime_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub struct IssueQuickCreatePrepared {
    /// Snapshot from the real runtime lookup after AgentReadiness succeeds.
    pub runtime_id: Uuid,
    pub provider: String,
    pub cli_version: String,
    pub runtime_mcp_overlay: Option<Value>,
    pub runtime_connected_apps: Option<Value>,
}

#[derive(Clone, Debug)]
pub enum IssueQuickCreateReadinessError {
    Unavailable { reason: String, reason_code: String },
    LookupFailed,
}

#[derive(Clone, Debug)]
pub struct IssueQuickCreateQueued {
    pub task_id: Uuid,
    pub workspace_id: Uuid,
    pub requester_id: Uuid,
    pub agent_id: Uuid,
    pub runtime_id: Uuid,
}

/// Integrator connects the shared runtime readiness/overlay service and the
/// post-commit RecordHumanDelegation + NotifyTaskEnqueued paths. No default
/// adapter: a missing dependency must not accept work that cannot be dispatched.
#[async_trait]
pub trait IssueQuickCreateRuntime: Send + Sync {
    async fn prepare(
        &self,
        admission: IssueQuickCreateAdmission,
    ) -> Result<IssueQuickCreatePrepared, IssueQuickCreateReadinessError>;
    fn enqueued(&self, task: IssueQuickCreateQueued);
}

#[derive(Debug, Default)]
struct Request {
    agent_id: String,
    squad_id: String,
    prompt: String,
    priority: String,
    due_date: String,
    project_id: String,
    parent_issue_id: String,
    attachment_ids: Vec<String>,
}
impl<'de> Deserialize<'de> for Request {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct RequestVisitor;
        impl<'de> Visitor<'de> for RequestVisitor {
            type Value = Request;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object or null")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Request, E> {
                Ok(Request::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Request, M::Error> {
                let mut req = Request::default();
                while let Some(key) = map.next_key::<String>()? {
                    // encoding/json folds field names, processes duplicates in
                    // order, and ignores null for strings (but clears slices).
                    let key: String = key
                        .chars()
                        .map(|c| match c {
                            '\u{017f}' => 's',
                            '\u{212a}' => 'k',
                            c => c.to_ascii_lowercase(),
                        })
                        .collect();
                    let field = match key.as_str() {
                        "agent_id" => &mut req.agent_id,
                        "squad_id" => &mut req.squad_id,
                        "prompt" => &mut req.prompt,
                        "priority" => &mut req.priority,
                        "due_date" => &mut req.due_date,
                        "project_id" => &mut req.project_id,
                        "parent_issue_id" => &mut req.parent_issue_id,
                        "attachment_ids" => {
                            req.attachment_ids = map
                                .next_value::<Option<Vec<Option<String>>>>()?
                                .unwrap_or_default()
                                .into_iter()
                                .map(Option::unwrap_or_default)
                                .collect();
                            continue;
                        }
                        _ => {
                            map.next_value::<de::IgnoredAny>()?;
                            continue;
                        }
                    };
                    if let Some(value) = map.next_value::<Option<String>>()? {
                        *field = value;
                    }
                }
                Ok(req)
            }
        }
        d.deserialize_any(RequestVisitor)
    }
}

fn parse(body: &[u8]) -> Result<Request, ApiError> {
    // Frozen Go decodes one value, ignores unknown fields and accepts duplicate
    // keys (last wins). Do not use regular CreateIssue's unique-key validator.
    let mut decoder = serde_json::Deserializer::from_slice(body);
    let mut req = Request::deserialize(&mut decoder)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    req.prompt = req.prompt.trim().to_owned();
    if req.prompt.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "prompt is required"));
    }
    req.priority = req.priority.trim().to_lowercase();
    if !["", "urgent", "high", "medium", "low"].contains(&req.priority.as_str()) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "priority must be one of: urgent, high, medium, low",
        ));
    }
    req.due_date = req.due_date.trim().to_owned();
    if !req.due_date.is_empty() {
        req.due_date = super::issue_write::calendar::parse(&req.due_date)
            .map(|date| date.format("%Y-%m-%d").to_string())
            .ok_or_else(|| {
                error(
                    StatusCode::BAD_REQUEST,
                    "invalid due_date format, expected YYYY-MM-DD",
                )
            })?;
    }
    if req.agent_id.trim().is_empty() == req.squad_id.trim().is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "exactly one of agent_id or squad_id is required",
        ));
    }
    Ok(req)
}

fn is_terminal(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled")
}

fn id(raw: &str, message: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, message))
}

// Go versionRe is deliberately unanchored and ignores trailing prerelease text.
fn semver(raw: &str) -> Option<([u64; 3], usize, usize)> {
    let b = raw.as_bytes();
    for start in 0..b.len() {
        if !b[start].is_ascii_digit() {
            continue;
        }
        let mut end = start;
        let mut v = [0; 3];
        let mut valid = true;
        for (index, part) in v.iter_mut().enumerate() {
            let begin = end;
            while end < b.len() && b[end].is_ascii_digit() {
                end += 1;
            }
            if begin == end {
                valid = false;
                break;
            }
            // Frozen Go ignores strconv.Atoi errors, leaving an overflowing
            // component at zero. Treating it as MAX would bypass the daemon
            // minimum-version gate for malformed version strings.
            *part = raw[begin..end].parse::<i64>().unwrap_or(0) as u64;
            if index != 2 {
                if b.get(end) != Some(&b'.') {
                    valid = false;
                    break;
                }
                end += 1;
            }
        }
        if valid {
            return Some((v, start, end));
        }
    }
    None
}
fn version_ok(raw: &str, fields: bool) -> bool {
    let raw = raw.trim();
    let Some((version, start, end)) = semver(raw) else {
        return false;
    };
    // Frozen devDescribeRe accepts a prefix, including a trailing -dirty suffix.
    if start == 0 || (start == 1 && raw.starts_with('v')) {
        let tail = &raw[end..];
        if let Some(tail) = tail.strip_prefix('-') {
            let digits = tail.bytes().take_while(u8::is_ascii_digit).count();
            if digits > 0
                && tail[digits..]
                    .strip_prefix("-g")
                    .is_some_and(|s| s.as_bytes().first().is_some_and(u8::is_ascii_hexdigit))
            {
                return true;
            }
        }
    }
    version >= if fields { [0, 4, 3] } else { [0, 2, 21] }
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/issues/quick-create", post(create))
}
async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let req = parse(&body)?;
    let create_runtime = state.issue_create_runtime.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue create runtime unavailable",
        )
    })?;
    let runtime = create_runtime.quick_create_runtime().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "quick-create runtime unavailable",
        )
    })?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to enqueue quick-create task",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    // The token owner can be the runtime's owner, not the human who invoked
    // this task. Never spend that owner's private-agent rights on an A2A hop.
    let (requester, parent_task) = match &actor {
        Actor::Human | Actor::Cloud => (user, None),
        Actor::Task(task) => {
            let parent = db.query_opt(
                "SELECT q.originator_user_id,q.status FROM agent_task_queue q JOIN agent a ON a.id=q.agent_id WHERE q.id=$1 AND q.agent_id=$2 AND a.workspace_id=$3",
                &[&task.task,&task.agent,&ws],
            ).await.map_err(|_| fail())?.ok_or_else(|| error(StatusCode::FORBIDDEN,"task cannot invoke an agent"))?;
            if is_terminal(&parent.get::<_, String>("status")) {
                return Err(error(StatusCode::FORBIDDEN, "task cannot invoke an agent"));
            }
            let originator: Uuid = parent
                .get::<_, Option<Uuid>>("originator_user_id")
                .ok_or_else(|| {
                    error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "quick-create delegated attribution unavailable",
                    )
                })?;
            (originator, Some(task.task))
        }
        Actor::Daemon => {
            return Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "quick-create delegated attribution unavailable",
            ));
        }
    };
    let member: bool = db
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2)",
            &[&ws, &requester],
        )
        .await
        .map_err(|_| fail())?
        .get(0);
    if !member {
        return Err(error(StatusCode::FORBIDDEN, "task cannot invoke an agent"));
    }
    let squad = if req.squad_id.trim().is_empty() {
        None
    } else {
        Some(id(&req.squad_id, "invalid squad_id")?)
    };
    let agent = if let Some(squad) = squad {
        let row = db
            .query_opt(
                "SELECT leader_id,archived_at FROM squad WHERE id=$1 AND workspace_id=$2",
                &[&squad, &ws],
            )
            .await
            .map_err(|_| fail())?
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "squad not found"))?;
        if row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
            .is_some()
        {
            return Err(error(StatusCode::BAD_REQUEST, "squad is archived"));
        }
        row.get("leader_id")
    } else {
        id(&req.agent_id, "invalid agent_id")?
    };
    let row = db.query_opt("SELECT runtime_id,archived_at,owner_id,permission_mode FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user'", &[&agent,&ws])
        .await.map_err(|_| fail())?.ok_or_else(|| error(StatusCode::BAD_REQUEST, "assignee_id does not refer to an agent of this workspace"))?;
    if row
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
        .is_some()
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "cannot assign to archived agent",
        ));
    }
    let allowed = row.get::<_,Option<Uuid>>("owner_id") == Some(requester) || (row.get::<_,String>("permission_mode") == "public_to"
        && db.query_one("SELECT EXISTS(SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND ((target_type='workspace' AND target_id=$2) OR (target_type='member' AND target_id=$3)))", &[&agent,&ws,&requester]).await.map_err(|_| fail())?.get::<_,bool>(0));
    if !allowed {
        return Err(error(
            StatusCode::FORBIDDEN,
            "you do not have permission to assign work to this agent",
        ));
    }
    let runtime_id: Option<Uuid> = row.get("runtime_id");
    let prepared =
        match runtime
            .prepare(IssueQuickCreateAdmission {
                workspace_id: ws,
                requester_id: requester,
                agent_id: agent,
                runtime_id,
            })
            .await
        {
            Ok(p) => p,
            Err(IssueQuickCreateReadinessError::Unavailable {
                reason,
                reason_code,
            }) => return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                GoJson(
                    json!({"code":"agent_unavailable","reason":reason,"reason_code":reason_code}),
                ),
            )
                .into_response()),
            Err(IssueQuickCreateReadinessError::LookupFailed) => {
                return Err(error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to check agent runtime",
                ));
            }
        };
    if runtime_id != Some(prepared.runtime_id) {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "quick-create runtime changed",
        ));
    }
    if prepared.provider == "qoder" {
        return Ok((StatusCode::UNPROCESSABLE_ENTITY,GoJson(json!({"code":"agent_unavailable","reason":"Qoder Cloud Agents supports existing issue runs only; create the issue manually and assign it to this agent"}))).into_response());
    }
    let fields = !req.priority.is_empty() || !req.due_date.is_empty();
    if !version_ok(&prepared.cli_version, fields) {
        // Check the base gate first, even when the request also carries fields.
        let min = if version_ok(&prepared.cli_version, false) && fields {
            "0.4.3"
        } else {
            "0.2.21"
        };
        return Ok((StatusCode::UNPROCESSABLE_ENTITY,GoJson(json!({"code":"daemon_version_unsupported","current_version":prepared.cli_version,"min_version":min,"runtime_id":prepared.runtime_id}))).into_response());
    }
    let attachments: Vec<Uuid> = req
        .attachment_ids
        .iter()
        .map(|s| id(s, "invalid attachment_ids"))
        .collect::<Result<_, _>>()?;
    let project = if req.project_id.trim().is_empty() {
        None
    } else {
        let p = id(&req.project_id, "invalid project_id")?;
        if db
            .query_opt(
                "SELECT id FROM project WHERE id=$1 AND workspace_id=$2",
                &[&p, &ws],
            )
            .await
            .map_err(|_| fail())?
            .is_none()
        {
            return Err(error(StatusCode::BAD_REQUEST, "project not found"));
        }
        Some(p)
    };
    let parent = if req.parent_issue_id.trim().is_empty() {
        None
    } else {
        let p = id(&req.parent_issue_id, "invalid parent_issue_id")?;
        if db
            .query_opt(
                "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2",
                &[&p, &ws],
            )
            .await
            .map_err(|_| fail())?
            .is_none()
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "parent issue not found in this workspace",
            ));
        }
        Some(p)
    };
    let policy = create_runtime.count_policy(ws).await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue count policy unavailable",
        )
    })?;
    if let Some(limit) = policy.limit {
        if limit <= 0 {
            return Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "invalid issue count policy",
            ));
        }
        let used: i64 = db
            .query_one(
                "SELECT count(*) FROM (SELECT 1 FROM issue WHERE workspace_id=$1 LIMIT $2) limited",
                &[&ws, &limit],
            )
            .await
            .map_err(|_| fail())?
            .get(0);
        if used >= limit {
            return Ok((StatusCode::PAYMENT_REQUIRED,GoJson(json!({"code":"issue_limit_reached","limit":limit,"policy_revision":policy.revision,"error":"workspace has reached its issue limit"}))).into_response());
        }
    }
    let mut context = json!({"type":"quick_create","prompt":req.prompt,"requester_id":requester,"workspace_id":ws});
    for (key, value) in [("priority", req.priority), ("due_date", req.due_date)] {
        if !value.is_empty() {
            context[key] = json!(value);
        }
    }
    if let Some(p) = project {
        context["project_id"] = json!(p);
    }
    if let Some(p) = parent {
        context["parent_issue_id"] = json!(p);
    }
    if let Some(s) = squad {
        context["squad_id"] = json!(s);
    }
    if !attachments.is_empty() {
        context["attachment_ids"] = json!(attachments);
    }
    let task = Uuid::now_v7();
    let tx = db.transaction().await.map_err(|_| fail())?;
    // The INSERT predicate acquires workspace, agent, and runtime owner locks
    // in that order; revalidate archived/runtime/permission only after it.
    let inserted = tx
        .query_opt(
            include_str!("issue_write_quick_create.sql"),
            &[
                &agent,
                &prepared.runtime_id,
                &context,
                &requester,
                &prepared.runtime_mcp_overlay,
                &prepared.runtime_connected_apps,
                &task,
                &parent_task,
            ],
        )
        .await
        .map_err(|_| fail())?;
    if inserted.is_none() {
        return Err(fail());
    }
    // Recheck the chain at the write boundary. A terminal transition revokes
    // invocation rights even when the token itself has not yet been revoked.
    if let Some(parent) = parent_task {
        let current = tx.query_opt(
            "SELECT q.originator_user_id,q.status FROM agent_task_queue q JOIN agent a ON a.id=q.agent_id WHERE q.id=$1 AND a.workspace_id=$2 FOR SHARE OF q",
            &[&parent,&ws],
        ).await.map_err(|_| fail())?;
        if !current.is_some_and(|row| {
            row.get::<_, Option<Uuid>>("originator_user_id") == Some(requester)
                && !is_terminal(&row.get::<_, String>("status"))
        }) {
            return Err(error(StatusCode::FORBIDDEN, "task cannot invoke an agent"));
        }
    }
    let live = tx.query_opt("SELECT id FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user' AND archived_at IS NULL AND runtime_id=$3 AND EXISTS(SELECT 1 FROM member WHERE workspace_id=$2 AND user_id=$4) AND (owner_id=$4 OR (permission_mode='public_to' AND EXISTS(SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=agent.id AND ((t.target_type='workspace' AND t.target_id=$2) OR (t.target_type='member' AND t.target_id=$4))))) FOR SHARE", &[&agent,&ws,&prepared.runtime_id,&requester]).await.map_err(|_| fail())?;
    if live.is_none() {
        return Err(fail());
    }
    tx.commit().await.map_err(|_| fail())?;
    runtime.enqueued(IssueQuickCreateQueued {
        task_id: task,
        workspace_id: ws,
        requester_id: requester,
        agent_id: agent,
        runtime_id: prepared.runtime_id,
    });
    state.wake_daemon(Some(prepared.runtime_id), Some(task));
    Ok((StatusCode::ACCEPTED, GoJson(json!({"task_id":task}))).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quick_create_wire_validation_and_version_gates() {
        let a = Uuid::nil();
        let req = parse(format!(r#"{{"agent_id":"{a}","prompt":"old","prompt":"  new  ","priority":" HIGH ","due_date":" 2026-11-02 ","attachment_ids":null}} trailing"#).as_bytes()).unwrap();
        assert_eq!(req.prompt, "new");
        assert_eq!(req.priority, "high");
        assert_eq!(req.due_date, "2026-11-02");
        for (input, expected) in [
            ("2026-11-30T00:00:00Z", "2026-11-30"),
            ("2026-11-30T07:00:00+07:00", "2026-11-30"),
            ("2026-11-29T19:00:00-05:00", "2026-11-30"),
            ("2026-11-30T00:00:00.000Z", "2026-11-30"),
        ] {
            let req = parse(
                format!(r#"{{"agent_id":"{a}","prompt":"x","due_date":"{input}"}}"#).as_bytes(),
            )
            .unwrap();
            assert_eq!(req.due_date, expected, "{input}");
        }
        for input in [
            "2026-11-30T00:00:01Z",
            "2026-11-30T00:00:00.001Z",
            "2026-11-30T00:00:00+07:00",
            "2026-11-30T00:00:00",
            "+2026-11-30",
            "2026-11-3",
            "02026-11-30",
        ] {
            assert!(
                parse(
                    format!(r#"{{"agent_id":"{a}","prompt":"x","due_date":"{input}"}}"#).as_bytes()
                )
                .is_err(),
                "{input}"
            );
        }
        let req = parse(format!(r#"{{"AGENT_ID":"{a}","prompt":"retained","prompt":null,"attachment_ids":[null,"{a}"],"attachment_ids":null}}"#).as_bytes()).unwrap();
        assert_eq!(req.prompt, "retained");
        assert!(req.attachment_ids.is_empty());
        assert_eq!(parse(b"null").unwrap_err().0, StatusCode::BAD_REQUEST);
        for body in [
            json!({}),
            json!({"agent_id":a,"prompt":" "}),
            json!({"agent_id":a,"squad_id":a,"prompt":"x"}),
            json!({"agent_id":a,"prompt":"x","priority":"none"}),
            json!({"agent_id":a,"prompt":"x","due_date":"2026-02-30"}),
            json!({"agent_id":a,"prompt":"x","due_date":"2026-1-02"}),
            json!({"agent_id":a,"prompt":3}),
        ] {
            assert!(parse(body.to_string().as_bytes()).is_err(), "{body}");
        }
        for (raw, base, fields) in [
            ("", false, false),
            ("garbage", false, false),
            ("0.2.20", false, false),
            ("v0.2.21", true, false),
            ("version 0.4.3 extra", true, true),
            ("0.4.3-rc1", true, true),
            ("v0.2.1-2-gAb12-dirty", true, true),
            ("v0.2.1-2-gZ", false, false),
            ("0.4.2", true, false),
            ("1.0.0", true, true),
            ("9223372036854775808.4.3", true, true),
            ("0.9223372036854775808.3", false, false),
            ("0.4.9223372036854775808", true, false),
            ("0.4.3-9223372036854775808-gabc", true, true),
        ] {
            assert_eq!(version_ok(raw, false), base, "base {raw}");
            assert_eq!(version_ok(raw, true), fields, "fields {raw}");
        }
    }
}
