//! Workflow handoff edits and project-specific copies. Event publication belongs to integration.
use super::*;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/issue-workflows/{id}/handoffs/{status}",
            axum::routing::put(save),
        )
        .route(
            "/api/issue-workflows/{id}/handoffs/{status}/",
            axum::routing::put(save),
        )
        .layer(DefaultBodyLimit::disable())
}

pub(super) use super::request_decode::HandoffRequest;

fn invalid_handoff(message: impl Into<String>) -> Failure {
    Failure(
        StatusCode::BAD_REQUEST,
        json!({"error":message.into(), "code":"invalid_handoff"}),
    )
}
fn handoff_failure() -> Failure {
    fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to save handoff")
}
pub(super) fn step_value(key: &str, request: &HandoffRequest) -> Value {
    let kind = request.handler.r#type.trim();
    let kind = if kind.is_empty() { "none" } else { kind };
    let mut step = json!({
        "status_key":key.trim(),
        "handler":{"type":kind},
        "instructions":if kind == "none" { "" } else { request.instructions.trim() },
    });
    if matches!(kind, "member" | "agent" | "squad") && !request.handler.id.trim().is_empty() {
        step["handler"]["id"] = json!(request.handler.id.trim());
    }
    for (field, value) in [
        ("next_status_key", request.next_status_key.trim()),
        ("back_status_key", request.back_status_key.trim()),
    ] {
        if !value.is_empty() {
            step[field] = json!(value);
        }
    }
    step
}
pub(super) fn normalized_steps(raw: Value) -> Result<Vec<Value>, Failure> {
    super::super::workflow::decode_step_values(raw)
        .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to read workflow"))
}
fn validate_step(step: &Value, steps: &[Value]) -> Result<(), Failure> {
    let key = step["status_key"].as_str().unwrap_or_default();
    let keys: HashSet<&str> = steps
        .iter()
        .filter_map(|s| s["status_key"].as_str())
        .collect();
    if !keys.contains(key) {
        return Err(invalid_handoff(format!(
            "status {key:?} is not part of this workflow"
        )));
    }
    let kind = step["handler"]["type"].as_str().unwrap_or_default();
    match kind {
        "none" | "project_lead" => {}
        "agent" | "member"
            if step["handler"]["id"]
                .as_str()
                .unwrap_or_default()
                .is_empty() =>
        {
            return Err(invalid_handoff(format!("choose a {kind} to hand off to")));
        }
        "agent" | "member" => {}
        "squad" | "creator" => {
            return Err(invalid_handoff(
                "hand off to an agent, a member or the project lead",
            ));
        }
        _ => return Err(invalid_handoff(format!("unknown handler type {kind:?}"))),
    }
    if kind != "none" && matches!(key, "backlog" | "done" | "cancelled") {
        return Err(invalid_handoff(format!(
            "issues entering {key:?} cannot be handed off"
        )));
    }
    if step["instructions"]
        .as_str()
        .unwrap_or_default()
        .chars()
        .count()
        > 8000
    {
        return Err(invalid_handoff(
            "instructions must be at most 8000 characters",
        ));
    }
    for field in ["next_status_key", "back_status_key"] {
        if let Some(reference) = step[field].as_str().filter(|v| !v.is_empty())
            && (reference == key || !keys.contains(reference))
        {
            return Err(invalid_handoff(format!(
                "{reference:?} is not another status of this workflow"
            )));
        }
    }
    Ok(())
}
fn copy_name(workflow: &str, project: &str, taken: &HashSet<String>) -> String {
    for n in 1.. {
        let mut suffix = if n == 1 {
            format!(" ({project})")
        } else {
            format!(" ({project} {n})")
        };
        let mut room = 64usize.saturating_sub(suffix.chars().count());
        if room < 1 {
            suffix = format!(" {n}");
            room = 64usize.saturating_sub(suffix.chars().count());
        }
        let base = workflow.chars().take(room).collect::<String>();
        let name = format!("{}{suffix}", base.trim());
        if !taken.contains(&name.to_lowercase()) {
            return name;
        }
    }
    unreachable!()
}

async fn save(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, key)): Path<(String, String)>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<(StatusCode, super::super::GoJson<Value>), Failure> {
    let id =
        parse_go_uuid(&raw).ok_or_else(|| fail(StatusCode::BAD_REQUEST, "invalid workflow id"))?;
    let body = body.map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let body: HandoffRequest = super::super::account_catalog_write_profile::account_request(&body)
        .map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let copy_for = if body.copy_for_project_id.is_empty() {
        None
    } else {
        Some(
            parse_go_uuid(&body.copy_for_project_id)
                .ok_or_else(|| fail(StatusCode::BAD_REQUEST, "invalid copy_for_project_id"))?,
        )
    };
    let mut db = state.pool.get().await.map_err(|_| handoff_failure())?;
    let tx = db.transaction().await.map_err(|_| handoff_failure())?;
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&format!("{ws}:issue_status")],
    )
    .await
    .map_err(|_| handoff_failure())?;
    let role: String = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| handoff_failure())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .get(0);
    let current = tx
        .query_opt(
            "SELECT * FROM issue_workflow WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| handoff_failure())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workflow not found"))?;
    if !matches!(actor, Actor::Human)
        || (!matches!(role.as_str(), "owner" | "admin")
            && current.get::<_, Option<Uuid>>("owner_id") != Some(user))
    {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "only the workflow owner and workspace admins can change this workflow",
        ));
    }
    let mut steps = normalized_steps(current.get("steps"))?;
    let next = step_value(&key, &body);
    validate_step(&next, &steps)?;
    let kind = next["handler"]["type"].as_str().unwrap_or_default();
    let target = next["handler"]["id"].as_str().unwrap_or_default();
    if matches!(kind, "member" | "agent")
        && !steps
            .iter()
            .any(|s| s["handler"]["type"] == kind && s["handler"]["id"] == target)
    {
        let target_id =
            parse_go_uuid(target).ok_or_else(|| invalid_handoff("invalid handler id"))?;
        if kind == "agent" {
            // Shared agent assignment authorization is owned by the integration worker.
            return Err(Failure(
                StatusCode::FORBIDDEN,
                json!({"error":"you do not have permission to assign work to this agent","code":"invalid_handoff"}),
            ));
        }
        if tx
            .query_opt(
                "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &target_id],
            )
            .await
            .map_err(|_| handoff_failure())?
            .is_none()
        {
            return Err(invalid_handoff(
                "assignee_id does not refer to a member of this workspace",
            ));
        }
    }
    for step in &mut steps {
        if step["status_key"] == key.trim() {
            *step = next.clone();
        }
    }
    let stored: Vec<Value> = steps
        .iter()
        .map(|s| {
            let mut value = json!({"status_key":s["status_key"]});
            for field in ["next_status_key", "back_status_key"] {
                if let Some(text) = s[field].as_str().filter(|v| !v.is_empty()) {
                    value[field] = json!(text);
                }
            }
            if s["handler"]["type"] != "none" {
                value["handler"] = s["handler"].clone();
                if let Some(text) = s["instructions"].as_str().filter(|v| !v.is_empty()) {
                    value["instructions"] = json!(text);
                }
            }
            value
        })
        .collect();
    let (row, projects, status, project_event) = if let Some(project_id) = copy_for {
        if current.get::<_, bool>("is_default") {
            return Err(invalid_handoff(
                "the Default workflow cannot be copied for one project",
            ));
        }
        let project = tx.query_opt("SELECT id,title,workflow_id,lead_type,lead_id FROM project WHERE id=$1 AND workspace_id=$2 FOR UPDATE", &[&project_id,&ws])
            .await.map_err(|_| handoff_failure())?
            .ok_or_else(|| fail(StatusCode::NOT_FOUND, "project not found"))?;
        if project.get::<_, Option<Uuid>>("workflow_id") != Some(id) {
            return Err(Failure(
                StatusCode::CONFLICT,
                json!({"error":"the project does not use this workflow","code":"invalid_handoff"}),
            ));
        }
        if !(matches!(role.as_str(), "owner" | "admin")
            || project.get::<_, Option<String>>("lead_type").as_deref() == Some("member")
                && project.get::<_, Option<Uuid>>("lead_id") == Some(user))
        {
            return Err(fail(
                StatusCode::FORBIDDEN,
                "only workspace admins and the project lead can change this project's workflow",
            ));
        }
        let names: HashSet<String> = tx
            .query(
                "SELECT lower(name) FROM issue_workflow WHERE workspace_id=$1",
                &[&ws],
            )
            .await
            .map_err(|_| handoff_failure())?
            .iter()
            .map(|r| r.get(0))
            .collect();
        let name = copy_name(
            &current.get::<_, String>("name"),
            &project.get::<_, String>("title"),
            &names,
        );
        let row=tx.query_one("INSERT INTO issue_workflow(workspace_id,name,description,initial_status_key,steps,owner_id) VALUES($1,$2,$3,'todo',$4,$5) RETURNING *",
            &[&ws,&name,&current.get::<_, String>("description"),&json!(stored),&user])
            .await.map_err(|_| handoff_failure())?;
        let updated = tx.query_one(
            "UPDATE project SET workflow_id=$3,updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *",
            &[&project_id, &ws, &row.get::<_, Uuid>("id")],
        )
        .await
        .map_err(|_| handoff_failure())?;
        let project_event =
            super::super::account_catalog_write_projects::response(&tx, ws, &updated, None).await;
        (
            row,
            vec![project_id],
            StatusCode::CREATED,
            Some(project_event),
        )
    } else {
        let row=tx.query_one("UPDATE issue_workflow SET name=name,description=description,initial_status_key='todo',steps=$3,updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *",
            &[&id,&ws,&json!(stored)]).await.map_err(|_| handoff_failure())?;
        let projects: Vec<Uuid> = if row.get::<_, bool>("is_default") {
            tx.query("SELECT id FROM project WHERE workspace_id=$1 AND (workflow_id IS NULL OR workflow_id=$2 OR NOT EXISTS (SELECT 1 FROM issue_workflow WHERE id=project.workflow_id AND workspace_id=$1)) ORDER BY created_at,id", &[&ws,&id]).await
        } else {
            tx.query("SELECT id FROM project WHERE workspace_id=$1 AND workflow_id=$2 ORDER BY created_at,id", &[&ws,&id]).await
        }.map_err(|_| handoff_failure())?.iter().map(|r| r.get(0)).collect();
        (row, projects, StatusCode::OK, None)
    };
    tx.commit().await.map_err(|_| handoff_failure())?;
    drop(db);
    use super::super::account_catalog_write_catalog_publish::{
        CatalogChange, WorkflowAction, publish,
    };
    let action = if copy_for.is_some() {
        WorkflowAction::Created
    } else {
        WorkflowAction::Updated
    };
    publish(&state, ws, user, CatalogChange::WorkflowChanged { action }).await;
    if let Some(project) = project_event {
        publish(&state, ws, user, CatalogChange::ProjectUpdated { project }).await;
    }
    let mut result = response(&row, &steps);
    result["is_default"] = json!(row.get::<_, bool>("is_default"));
    result["project_ids"] = json!(projects);
    Ok((status, super::super::GoJson(result)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_names_are_case_insensitive_and_rune_bounded() {
        let taken = HashSet::from(["flow (campaign)".to_owned(), "flow (campaign 2)".to_owned()]);
        assert_eq!(copy_name("Flow", "Campaign", &taken), "Flow (Campaign 3)");
        let title = "界".repeat(80);
        let workflow = "流".repeat(64);
        let name = copy_name(&workflow, &title, &HashSet::new());
        assert_eq!(name.chars().count(), 64);
        assert!(name.ends_with(" 1"));
        let name = copy_name(&workflow, "短", &HashSet::new());
        assert_eq!(name.chars().count(), 64);
        assert!(name.ends_with(" (短)"));
    }

    #[test]
    fn handoff_normalizes_and_validates_without_rearranging_columns() {
        let steps = normalized_steps(
            json!([{"status_key":"todo"},{"status_key":"in_progress"},{"status_key":"done"}]),
        )
        .unwrap_or_default();
        let request = HandoffRequest {
            handler: HandlerInput {
                r#type: " none ".into(),
                id: Uuid::new_v4().to_string(),
            },
            instructions: " discard ".into(),
            next_status_key: " done ".into(),
            ..Default::default()
        };
        let value = step_value(" in_progress ", &request);
        assert_eq!(value["handler"], json!({"type":"none"}));
        assert_eq!(value["instructions"], "");
        assert_eq!(value["next_status_key"], "done");
        assert!(validate_step(&value, &steps).is_ok());
        for key in ["missing", "done"] {
            let value = step_value(
                key,
                &HandoffRequest {
                    handler: HandlerInput {
                        r#type: "project_lead".into(),
                        id: String::new(),
                    },
                    ..Default::default()
                },
            );
            assert!(validate_step(&value, &steps).is_err());
        }
        let value = step_value(
            "in_progress",
            &HandoffRequest {
                back_status_key: "in_progress".into(),
                ..Default::default()
            },
        );
        assert!(validate_step(&value, &steps).is_err());
    }
}
