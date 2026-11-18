//! Static squad reads. Runtime/task-derived member status is a separate route.
use super::{ApiError, ApiState, GoJson, catalog::WorkspaceId, error, timestamp};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/squads", get(list))
        .route("/api/squads/", get(list))
        .route("/api/squads/{id}", get(detail))
        .route("/api/squads/{id}/", get(detail))
        .route("/api/squads/{id}/members", get(members))
        .route("/api/squads/{id}/members/status", get(member_status))
}

#[derive(Serialize)]
struct Squad {
    id: Uuid,
    workspace_id: Uuid,
    name: String,
    description: String,
    instructions: String,
    avatar_url: Option<String>,
    leader_id: Uuid,
    creator_id: Uuid,
    created_at: String,
    updated_at: String,
    archived_at: Option<String>,
    archived_by: Option<Uuid>,
    member_count: usize,
    member_preview: Option<Vec<MemberPreview>>,
}

#[derive(Serialize)]
struct MemberPreview {
    member_type: String,
    member_id: Uuid,
    role: String,
}

#[derive(Default)]
struct Summary {
    count: usize,
    preview: Vec<MemberPreview>,
}

impl Summary {
    fn add(&mut self, row: &Row) {
        self.count += 1;
        if self.preview.len() < 3 {
            self.preview.push(MemberPreview {
                member_type: row.get("member_type"),
                member_id: row.get("member_id"),
                role: row.get("role"),
            });
        }
    }
}

fn response(row: &Row, summary: Option<Summary>, detail: bool) -> Squad {
    let (member_count, member_preview) = match summary {
        Some(summary) => (summary.count, Some(summary.preview)),
        // Go detail applies a non-nil summary with a nil preview slice, while
        // list leaves the converter's empty slice untouched when no rows exist.
        None => (0, if detail { None } else { Some(Vec::new()) }),
    };
    Squad {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        name: row.get("name"),
        description: row.get("description"),
        instructions: row.get("instructions"),
        // Like the frozen handler without Storage, preserve stored values.
        // Private-object signing/CDN resolution is not part of this read slice.
        avatar_url: row.get("avatar_url"),
        leader_id: row.get("leader_id"),
        creator_id: row.get("creator_id"),
        created_at: timestamp(row.get("created_at")),
        updated_at: timestamp(row.get("updated_at")),
        archived_at: row
            .get::<_, Option<DateTime<Utc>>>("archived_at")
            .map(timestamp),
        archived_by: row.get("archived_by"),
        member_count,
        member_preview,
    }
}

async fn load(db: &impl GenericClient, ws: Uuid, raw: &str) -> Result<Row, ApiError> {
    let id =
        Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid squad id"))?;
    // Archived squads remain addressable, as in GetSquadInWorkspace.
    db.query_opt(
        "SELECT * FROM squad WHERE id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .map_err(|_| error(StatusCode::NOT_FOUND, "squad not found"))?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "squad not found"))
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
) -> Result<GoJson<Vec<Squad>>, ApiError> {
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list squads");
    let db = state.pool.get().await.map_err(|_| fail())?;
    let rows = db
        .query("SELECT * FROM squad WHERE workspace_id=$1 AND archived_at IS NULL ORDER BY created_at ASC", &[&ws])
        .await.map_err(|_| fail())?;
    let preview = db.query(
        "SELECT sm.squad_id,sm.member_type,sm.member_id,sm.role FROM squad_member sm JOIN squad s ON s.id=sm.squad_id WHERE s.workspace_id=$1 AND s.archived_at IS NULL ORDER BY sm.squad_id ASC,(sm.member_type='agent' AND sm.member_id=s.leader_id) DESC,sm.created_at ASC",
        &[&ws],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list squad member preview"))?;
    let mut summaries: HashMap<Uuid, Summary> = HashMap::new();
    for row in preview {
        summaries.entry(row.get("squad_id")).or_default().add(&row);
    }
    Ok(GoJson(
        rows.iter()
            .map(|r| response(r, summaries.remove(&r.get("id")), false))
            .collect(),
    ))
}

async fn detail(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Squad>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "squad not found"))?;
    let row = load(&**db, ws, &raw).await?;
    let id: Uuid = row.get("id");
    let preview = db.query(
        "SELECT sm.member_type,sm.member_id,sm.role FROM squad_member sm JOIN squad s ON s.id=sm.squad_id WHERE sm.squad_id=$1 ORDER BY (sm.member_type='agent' AND sm.member_id=s.leader_id) DESC,sm.created_at ASC",
        &[&id],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load squad member preview"))?;
    let mut summary = Summary::default();
    for row in &preview {
        summary.add(row);
    }
    Ok(GoJson(response(
        &row,
        (!preview.is_empty()).then_some(summary),
        true,
    )))
}

async fn members(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Vec<serde_json::Value>>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "squad not found"))?;
    let squad = load(&**db, ws, &raw).await?;
    let id: Uuid = squad.get("id");
    let rows = db
        .query(
            "SELECT * FROM squad_member WHERE squad_id=$1 ORDER BY created_at ASC",
            &[&id],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list squad members",
            )
        })?;
    Ok(GoJson(
        rows.iter()
            .map(|row| {
                serde_json::json!({
                    "id": row.get::<_, Uuid>("id"),
                    "squad_id": row.get::<_, Uuid>("squad_id"),
                    "member_type": row.get::<_, String>("member_type"),
                    "member_id": row.get::<_, Uuid>("member_id"),
                    "role": row.get::<_, String>("role"),
                    "created_at": timestamp(row.get("created_at")),
                })
            })
            .collect(),
    ))
}

#[derive(Serialize)]
struct ActiveIssue {
    issue_id: Uuid,
    identifier: String,
    title: String,
    issue_status: String,
}

#[derive(Serialize)]
struct MemberStatus {
    member_type: String,
    member_id: Uuid,
    status: Option<&'static str>,
    active_issues: Vec<ActiveIssue>,
    last_active_at: Option<String>,
}

struct StatusEntry {
    response: MemberStatus,
    archived: bool,
    working: bool,
    runtime_status: Option<String>,
    runtime_seen: Option<DateTime<Utc>>,
    latest_dispatch: Option<DateTime<Utc>>,
}

fn member_availability(entry: &StatusEntry, now: DateTime<Utc>) -> &'static str {
    if entry.archived {
        return "archived";
    }
    if entry.working {
        return "working";
    }
    if entry.runtime_status.is_none() {
        return "offline";
    }
    if entry.runtime_status.as_deref() == Some("online") {
        return "idle";
    }
    if entry
        .runtime_seen
        .is_some_and(|seen| (now - seen).num_seconds() < 300)
    {
        return "unstable";
    }
    "offline"
}

#[path = "squad_briefing.rs"]
pub(super) mod briefing;

pub(super) fn legacy_prefix(name: &str) -> String {
    let letters: String = name
        .chars()
        .filter(char::is_ascii_alphabetic)
        .take(3)
        .collect();
    if letters.is_empty() {
        "WS".into()
    } else {
        letters.to_ascii_uppercase()
    }
}

async fn member_status(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<serde_json::Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "squad not found"))?;
    let squad = load(&**db, ws, &raw).await?;
    let id: Uuid = squad.get("id");
    // Unlike the frozen SQL, every joined entity must stay inside the selected
    // workspace: historical malformed cross-workspace links must not leak data.
    let rows = db.query(
        "SELECT sm.member_type, sm.member_id, a.archived_at AS agent_archived_at, \
         ar.status AS runtime_status, ar.last_seen_at AS runtime_last_seen_at, \
         atq.id AS task_id, atq.status AS task_status, atq.issue_id AS task_issue_id, \
         atq.dispatched_at AS task_dispatched_at, i.number AS issue_number, \
         i.title AS issue_title, i.status AS issue_status \
         FROM squad_member sm \
         LEFT JOIN agent a ON sm.member_type='agent' AND a.id=sm.member_id AND a.workspace_id=$2 \
         LEFT JOIN agent_runtime ar ON ar.id=a.runtime_id AND ar.workspace_id=$2 \
         LEFT JOIN agent_task_queue atq ON a.id IS NOT NULL AND atq.agent_id=a.id \
           AND atq.status IN ('dispatched','running','waiting_local_directory') \
           AND EXISTS (SELECT 1 FROM agent_runtime tr WHERE tr.id=atq.runtime_id AND tr.workspace_id=$2) \
           AND (atq.issue_id IS NULL OR EXISTS (SELECT 1 FROM issue ti WHERE ti.id=atq.issue_id AND ti.workspace_id=$2)) \
         LEFT JOIN issue i ON i.id=atq.issue_id AND i.workspace_id=$2 \
         WHERE sm.squad_id=$1 ORDER BY sm.created_at ASC, atq.dispatched_at DESC NULLS LAST",
        &[&id, &ws],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list squad member status"))?;
    // The Go helper silently falls back to an empty prefix on DB errors.
    let prefix: String = db
        .query_opt(
            "SELECT name, issue_prefix FROM workspace WHERE id=$1",
            &[&ws],
        )
        .await
        .ok()
        .flatten()
        .map(|row| {
            let stored: String = row.get("issue_prefix");
            if stored.is_empty() {
                legacy_prefix(row.get("name"))
            } else {
                stored
            }
        })
        .unwrap_or_default();
    let mut entries: Vec<StatusEntry> = Vec::new();
    let mut by_member = HashMap::<Uuid, usize>::new();
    for row in rows {
        let member_id: Uuid = row.get("member_id");
        let index = *by_member.entry(member_id).or_insert_with(|| {
            let index = entries.len();
            entries.push(StatusEntry {
                response: MemberStatus {
                    member_type: row.get("member_type"),
                    member_id,
                    status: None,
                    active_issues: Vec::new(),
                    last_active_at: None,
                },
                archived: row
                    .get::<_, Option<DateTime<Utc>>>("agent_archived_at")
                    .is_some(),
                working: false,
                runtime_status: row.get("runtime_status"),
                runtime_seen: row.get("runtime_last_seen_at"),
                latest_dispatch: None,
            });
            index
        });
        let entry = &mut entries[index];
        if entry.response.member_type != "agent" || row.get::<_, Option<Uuid>>("task_id").is_none()
        {
            continue;
        }
        let task_status: Option<String> = row.get("task_status");
        entry.working |= matches!(task_status.as_deref(), Some("dispatched" | "running"));
        if let Some(issue_id) = row.get::<_, Option<Uuid>>("task_issue_id") {
            entry.response.active_issues.push(ActiveIssue {
                issue_id,
                identifier: format!(
                    "{}-{}",
                    prefix,
                    row.get::<_, Option<i32>>("issue_number")
                        .unwrap_or_default()
                ),
                title: row
                    .get::<_, Option<String>>("issue_title")
                    .unwrap_or_default(),
                issue_status: row
                    .get::<_, Option<String>>("issue_status")
                    .unwrap_or_default(),
            });
        }
        if let Some(dispatched) = row.get::<_, Option<DateTime<Utc>>>("task_dispatched_at")
            && entry
                .latest_dispatch
                .is_none_or(|latest| dispatched > latest)
        {
            entry.latest_dispatch = Some(dispatched);
        }
    }
    let now = Utc::now();
    let members: Vec<MemberStatus> = entries
        .into_iter()
        .map(|mut entry| {
            if entry.response.member_type == "agent" {
                entry.response.status = Some(member_availability(&entry, now));
                entry.response.last_active_at =
                    entry.latest_dispatch.or(entry.runtime_seen).map(timestamp);
            }
            entry.response
        })
        .collect();
    Ok(GoJson(serde_json::json!({"members":members})))
}

#[cfg(test)]
mod status_tests {
    use super::*;
    #[test]
    fn legacy_issue_prefix_does_not_use_new_slug_rule() {
        assert_eq!(legacy_prefix("My Team"), "MYT");
        assert_eq!(legacy_prefix("前端团队"), "WS");
    }
    #[test]
    fn workload_overrides_offline_but_archive_wins() {
        let mut entry = StatusEntry {
            response: MemberStatus {
                member_type: "agent".into(),
                member_id: Uuid::nil(),
                status: None,
                active_issues: vec![],
                last_active_at: None,
            },
            archived: false,
            working: true,
            runtime_status: Some("offline".into()),
            runtime_seen: None,
            latest_dispatch: None,
        };
        assert_eq!(member_availability(&entry, Utc::now()), "working");
        entry.archived = true;
        assert_eq!(member_availability(&entry, Utc::now()), "archived");
    }
}
