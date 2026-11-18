//! Claim-only response assembly and the final, locked delivery authorization.
use super::{
    ApiState,
    daemon_claim_build::{
        self as build, ClaimBuildFailure, ClaimRuntime, FinalizeClaimError, TaskToken,
    },
    daemon_claim_check::{self, CheckClaimError},
    daemon_claim_compose as compose, daemon_claim_join, daemon_claim_load as load,
    daemon_claim_pure as pure, daemon_claim_service as service,
    issue_workflow::{self, ClaimWorkflowIssue},
    p0_daemon_claim::capability,
    runtime_chat_task, runtime_skills_sprint_bundles as skills,
    squad::briefing::{self as squad_briefing, LeaderWorkflow},
};
use axum::http::{HeaderMap, StatusCode};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) struct BuiltClaim {
    pub response: Value,
    pub delivered_comment_ids: Vec<Uuid>,
    pub issue_snapshot: Option<Vec<u8>>,
}

fn optional(out: &mut Value, field: &str, text: impl AsRef<str>) {
    let text = text.as_ref();
    if !text.is_empty() {
        out[field] = json!(text);
    }
}

fn project(out: &mut Value, context: load::ClaimProjectContext) {
    optional(out, "project_id", context.project_id);
    optional(out, "project_title", context.title);
    optional(out, "project_description", context.description);
    if !context.resources.is_empty() {
        out["project_resources"] = json!(context.resources);
    }
    if !context.repos.is_empty() {
        out["repos"] = json!(context.repos);
    }
}

fn project_failure() -> ClaimBuildFailure {
    ClaimBuildFailure::unsettled(
        "error_project_context",
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to load project context",
    )
}

fn append_briefing(agent: &mut Value, briefing: &str) {
    let existing = agent["instructions"].as_str().unwrap_or("");
    agent["instructions"] = if existing.trim().is_empty() {
        json!(briefing)
    } else {
        json!(format!("{existing}\n\n{briefing}"))
    };
}

fn private_runtime_denial(owner: Option<Uuid>) -> &'static str {
    if owner.is_none() {
        "This private runtime cannot run the assigned agent because the agent has no owner."
    } else {
        "This private runtime cannot run the assigned agent because the agent and runtime have different owners."
    }
}

fn runtime_allows_agent(runtime: &ClaimRuntime, owner: Option<Uuid>) -> bool {
    runtime.visibility != "private"
        || runtime.owner_id.is_none()
        || owner.is_none()
        || owner == runtime.owner_id
        || owner.is_some_and(|owner| runtime.shared_with.contains(&owner))
}

fn rerun_source_matches(task: &Row, source: &load::PriorTaskRow) -> bool {
    source.agent_id == task.get::<_, Uuid>("agent_id")
        && source.context.as_ref().and_then(|v| v["type"].as_str()) != Some("triage")
        && source.issue_id == task.get::<_, Option<Uuid>>("issue_id")
        && source.chat_session_id == task.get::<_, Option<Uuid>>("chat_session_id")
}

fn status_wire_category<'a>(key: &'a str, category: &str) -> &'a str {
    match key {
        "done" => "done",
        "cancelled" => "cancelled",
        "backlog" | "todo" | "in_progress" | "in_review" | "blocked" => key,
        _ => match category {
            "done" => "done",
            "closed" => "cancelled",
            _ => key,
        },
    }
}

pub(super) async fn build_claimed_task_response(
    state: &ApiState,
    headers: &HeaderMap,
    task: &Row,
    runtime: &ClaimRuntime,
) -> Result<BuiltClaim, ClaimBuildFailure> {
    let db = state.pool.get().await.map_err(|_| {
        ClaimBuildFailure::unsettled(
            "error_source_load",
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to load task wakeup",
        )
    })?;
    let task_id: Uuid = task.get("id");
    let agent_id: Uuid = task.get("agent_id");
    let mut out = runtime_chat_task::response(task, runtime.workspace_id);
    let composio_enabled = super::p0_auth::enabled_env_flag("COMPOSIO_MCP_APPS");
    match daemon_claim_check::check_claim(&db, task).await {
        Err(CheckClaimError::Database) => {
            return Err(build::reject_claim_source_load(state, task, false, "wakeup").await);
        }
        Err(CheckClaimError::Forbidden) => {
            return Err(build::fail_claimed_task_before_launch(
                state,
                task,
                "Wakeup is disabled or its authorization is no longer available.",
                "invalid_task_identity",
                "wakeup_unavailable",
                StatusCode::CONFLICT,
                "wakeup unavailable",
            )
            .await);
        }
        Ok(()) => {}
    }
    if task
        .get::<_, Option<DateTime<Utc>>>("dispatched_at")
        .is_some()
    {
        out["start_claim_supported"] = json!(true);
    }
    out["leader_role_resolved"] = json!(true);
    let coalesced = capability(headers, "coalesced-comments-v1");
    let mut delivered_comment_ids = Vec::new();
    let mut issue_snapshot = None;
    if composio_enabled
        && let Some(apps) = build::connected_apps_for_claim(
            task.get::<_, Option<Value>>("runtime_connected_apps")
                .as_ref(),
            task_id,
        )
    {
        out["connected_apps"] = apps;
    }
    let row = match db.query_opt(
        "SELECT id,name,instructions,custom_env,custom_args,mcp_config,model,thinking_level,service_tier,runtime_config,runtime_id,owner_id,system_key,disabled_runtime_skills FROM agent WHERE id=$1",
        &[&agent_id]
    ).await {
        Ok(Some(row)) => row,
        Ok(None) => return Err(build::fail_claimed_task_before_launch(state, task,
            "Task identity is invalid: the assigned agent no longer exists.", "invalid_task_identity",
            "error_invalid_task_identity", StatusCode::CONFLICT, "task agent no longer exists").await),
        Err(_) => {
            let _ = service::requeue_task_after_claim_failure(state, task).await;
            return Err(ClaimBuildFailure::unsettled("error_load_task_agent", StatusCode::INTERNAL_SERVER_ERROR, "failed to load task agent"));
        }
    };
    if row.get::<_, Option<Uuid>>("runtime_id") != Some(runtime.id) {
        return Err(build::fail_claimed_task_before_launch(state, task,
            "The agent moved to another runtime before this task could start. Retry the task to run it on the agent's current runtime.",
            "invalid_task_identity", "error_agent_runtime_changed", StatusCode::CONFLICT,
            "agent runtime changed before task delivery").await);
    }
    let agent_owner: Option<Uuid> = row.get("owner_id");
    if !runtime_allows_agent(runtime, agent_owner) {
        return Err(build::fail_claimed_task_before_launch(
            state,
            task,
            private_runtime_denial(agent_owner),
            "runtime_access_denied",
            "error_runtime_access_denied",
            StatusCode::FORBIDDEN,
            "private runtime does not permit task agent",
        )
        .await);
    }
    let system_key: String = row
        .get::<_, Option<String>>("system_key")
        .unwrap_or_default();
    let legacy_redirects = !capability(headers, "platform-skill-v1");
    let config: Option<Value> = row.get("mcp_config");
    let mut mcp_config = config.clone();
    if let Ok(bindings) = load::enabled_agent_mcp_servers(&db, agent_id).await {
        if !bindings.is_empty() {
            let encoded = config.as_ref().and_then(|v| serde_json::to_vec(v).ok());
            mcp_config = compose::resolve_agent_mcp_config(&bindings, encoded.as_deref());
        }
    }
    let overlay: Option<Value> = task.get("runtime_mcp_overlay");
    if composio_enabled && let Some(overlay) = overlay.as_ref() {
        let encoded = mcp_config.as_ref().and_then(|v| serde_json::to_vec(v).ok());
        let overlay = serde_json::to_vec(overlay).ok();
        mcp_config = compose::merge_mcp_overlay(encoded.as_deref(), overlay.as_deref());
    }
    let runtime_config: Option<Value> = row.get("runtime_config");
    let mut agent = json!({
        "id":agent_id,
        "name":row.get::<_,String>("name"),
        "instructions":row.get::<_,String>("instructions"),
    });
    for (field, value) in [
        ("custom_env", row.get::<_, Option<Value>>("custom_env")),
        ("custom_args", row.get::<_, Option<Value>>("custom_args")),
        ("mcp_config", mcp_config),
    ] {
        if let Some(value) = value.filter(|v| !v.is_null() && v != &json!({}) && v != &json!([])) {
            agent[field] = value;
        }
    }
    for field in ["model", "thinking_level", "service_tier"] {
        optional(
            &mut agent,
            field,
            row.get::<_, Option<String>>(field).unwrap_or_default(),
        );
    }
    if let Some(config) = runtime_config
        .as_ref()
        .filter(|v| !v.is_null() && *v != &json!({}))
    {
        agent["runtime_config"] = config.clone();
    }
    let disabled: Option<Value> = row.get("disabled_runtime_skills");
    let disabled =
        pure::disabled_runtime_skills_for(disabled.as_ref(), runtime.id, &runtime.provider);
    if disabled.as_array().is_some_and(|values| !values.is_empty()) {
        agent["disabled_runtime_skills"] = disabled;
    }
    if runtime.provider == "openclaw" {
        optional(
            &mut out,
            "model_override",
            pure::openclaw_model_override(runtime_config.as_ref()),
        );
    }
    if system_key == "mika" {
        if let Some(instructions) =
            super::agents_squads_sprint::bindings::settings::compose_mika_instructions(
                agent["name"].as_str().unwrap_or(""),
                agent["instructions"].as_str().unwrap_or(""),
            )
        {
            agent["instructions"] = json!(instructions);
        }
    }
    if capability(headers, "skill-bundles-v1") {
        let (_, refs) =
            skills::load_agent_skill_bundles(&db, agent_id, &system_key, legacy_redirects)
                .await
                .map_err(|_| build::reject_claim_skill_load())?;
        if !refs.is_empty() {
            agent["skill_refs"] = json!(refs);
        }
    } else {
        let mut loaded = skills::load_agent_skills(&db, agent_id)
            .await
            .map_err(|_| build::reject_claim_skill_load())?;
        loaded.extend(skills::builtin_skill_data(&system_key, legacy_redirects));
        if !loaded.is_empty() {
            agent["skills"] = json!(loaded);
        }
    }
    if agent["id"] != out["agent_id"] {
        return Err(build::fail_claimed_task_before_launch(
            state,
            task,
            "Task identity is invalid: the task and response agent disagree.",
            "invalid_task_identity",
            "error_invalid_task_identity",
            StatusCode::CONFLICT,
            "task response agent identity mismatch",
        )
        .await);
    }
    out["agent"] = agent;
    if let Some(owner) = runtime.owner_id {
        if let Ok(Some((name, description))) = load::user_profile(&db, owner).await {
            optional(&mut out, "requesting_user_name", name);
            optional(&mut out, "requesting_user_profile_description", description);
        }
    }

    let mut issue_number = 0;
    if let Some(id) = task.get::<_, Option<Uuid>>("issue_id") {
        let issue = match load::issue(&db, id).await {
            Ok(Some(issue)) => issue,
            other => {
                return Err(build::reject_claim_source_load(
                    state,
                    task,
                    matches!(other, Ok(None)),
                    "issue",
                )
                .await);
            }
        };
        if let Some(failure) = build::reject_claim_on_workspace_mismatch(
            state,
            task,
            issue.workspace_id,
            runtime.workspace_id,
        )
        .await
        {
            return Err(failure);
        }
        out["workspace_id"] = json!(issue.workspace_id);
        optional(&mut out, "thread_name", &issue.title);
        issue_number = issue.number;
        optional(&mut out, "issue_status", &issue.status);
        if let Some(kind) = &issue.assignee_type {
            optional(&mut out, "issue_assignee_type", kind);
        }
        if let Some(assignee) = issue.assignee_id {
            out["issue_assignee_id"] = json!(assignee);
        }
        let current = pure::build_issue_state_snapshot(&issue.title, &issue.description);
        issue_snapshot = Some(build::issue_snapshot_bytes(&current));
        let legacy = !capability(headers, "workflow-status-rules-v1");
        let workflow = issue_workflow::claim_project_workflow(
            &db,
            &ClaimWorkflowIssue {
                workspace_id: issue.workspace_id,
                project_id: issue.project_id,
                status: &issue.status,
                number: issue.number,
                assignee_type: issue.assignee_type.as_deref(),
                assignee_id: issue.assignee_id,
                creator_type: issue.creator_type.as_deref(),
                creator_id: issue.creator_id,
            },
            agent_id,
            task.get("squad_id"),
            legacy,
        )
        .await;
        if task.get::<_, bool>("is_leader_task") {
            let mut injected = false;
            if let Some(squad_id) = task.get::<_, Option<Uuid>>("squad_id") {
                if let Ok(Some(squad)) = load::squad(&db, squad_id, issue.workspace_id).await {
                    if squad.leader_id == Some(agent_id) {
                        let owns = issue.assignee_type.as_deref() == Some("squad")
                            && issue.assignee_id == Some(squad.id);
                        let role = match (&workflow, legacy) {
                            (Some(_), true) => LeaderWorkflow::LegacyDaemon,
                            (Some(_), false) => LeaderWorkflow::CurrentDaemon,
                            _ => LeaderWorkflow::None,
                        };
                        let briefing = squad_briefing::build_leader_briefing(
                            &**db,
                            &squad_briefing::Squad {
                                id: squad.id,
                                name: squad.name,
                                leader_id: agent_id,
                                instructions: squad.instructions,
                            },
                            owns,
                            role,
                            capability(headers, "cli-comment-steer-v1"),
                        )
                        .await;
                        append_briefing(&mut out["agent"], &briefing);
                        injected = true;
                    }
                }
            }
            if !injected {
                out.as_object_mut().unwrap().remove("is_leader_task");
            }
        }
        let context =
            load::resolve_claim_project_context(&db, issue.project_id, issue.workspace_id)
                .await
                .map_err(|_| project_failure())?;
        project(&mut out, context);
        if let Some(brief) = workflow {
            out["project_workflow"] = json!({"brief":brief});
        }
        let trigger: Option<Uuid> = task.get("trigger_comment_id");
        let mut planned: Vec<Uuid> = task
            .get::<_, Option<Vec<Uuid>>>("coalesced_comment_ids")
            .unwrap_or_default();
        if let Some(trigger) = trigger {
            planned.push(trigger);
        }
        let loaded = load::build_coalesced_comment_data(&db, runtime.workspace_id, &planned).await;
        let selected: Vec<pure::ClaimComment> =
            if trigger.is_some_and(|id| loaded.iter().any(|comment| comment.id == id)) {
                pure::select_comment_delivery(
                    &loaded,
                    trigger,
                    !coalesced,
                    load::MAX_CLAIM_COMMENT_PAYLOAD_BYTES,
                )
                .into_iter()
                .cloned()
                .collect()
            } else {
                Vec::new()
            };
        delivered_comment_ids = load::comment_data_ids(&selected);
        out.as_object_mut().unwrap().remove("coalesced_comment_ids");
        let mut folded = Vec::new();
        for comment in &selected {
            if Some(comment.id) == trigger {
                optional(&mut out, "trigger_comment_content", &comment.content);
                out["trigger_thread_id"] = json!(comment.thread_id);
                optional(&mut out, "trigger_author_type", &comment.author_type);
                optional(&mut out, "trigger_author_name", &comment.author_name);
            } else {
                folded.push(comment.clone());
            }
        }
        if !folded.is_empty() {
            out["coalesced_comment_ids"] = json!(folded.iter().map(|c| c.id).collect::<Vec<_>>());
            out["coalesced_comments"] = json!(
                folded
                    .iter()
                    .map(pure::ClaimComment::wire)
                    .collect::<Vec<_>>()
            );
        }
        let mut loaded_trigger = None;
        let mut scope = None;
        if let Some(trigger) = trigger {
            if let Ok(Some(row)) = load::comment(&db, trigger, runtime.workspace_id).await {
                if !row.deleted {
                    optional(&mut out, "trigger_comment_content", &row.comment.content);
                    out["trigger_thread_id"] = json!(row.comment.thread_id);
                    optional(&mut out, "trigger_author_type", &row.comment.author_type);
                    optional(&mut out, "trigger_author_name", &row.comment.author_name);
                    scope = Some(trigger);
                    loaded_trigger = Some(row);
                }
            }
        }
        if !coalesced {
            if !folded.is_empty()
                || (out["trigger_comment_content"]
                    .as_str()
                    .unwrap_or("")
                    .is_empty()
                    && !selected.is_empty())
            {
                out["trigger_comment_content"] =
                    json!(pure::format_legacy_comment_bundle(&selected));
            }
            out.as_object_mut().unwrap().remove("coalesced_comment_ids");
            out.as_object_mut().unwrap().remove("coalesced_comments");
        } else if out["trigger_comment_content"]
            .as_str()
            .unwrap_or("")
            .is_empty()
            && !selected.is_empty()
        {
            out["trigger_comment_content"] = json!(
                "The newest triggering comment is no longer available. Address every earlier comment included below."
            );
        }
        let mut resumed_at = None;
        let mut anchor = None;
        if let Some(source_id) = task
            .get::<_, Option<Uuid>>("rerun_of_task_id")
            .filter(|_| !pure::is_fresh_rerun(task.get::<_, Option<Value>>("context").as_ref()))
        {
            if let Ok(Some(source)) = load::prior_task(&db, source_id).await {
                if rerun_source_matches(task, &source) {
                    if let Some(dir) = source.work_dir.as_deref() {
                        optional(&mut out, "prior_work_dir", dir);
                    }
                    let retired = if let Some(session) = source.session_id.as_deref() {
                        load::session_retired(&db, agent_id, session)
                            .await
                            .unwrap_or(true)
                    } else {
                        false
                    };
                    if retired {
                        out["prior_session_resume_unavailable"] = json!(true);
                    }
                    if !pure::resume_unsafe_failure(
                        source.failure_reason.as_deref().unwrap_or(""),
                        source.error.as_deref().unwrap_or(""),
                    ) && source.runtime_id == Some(runtime.id)
                        && !retired
                    {
                        if let Some(session) = source.session_id.as_deref() {
                            optional(&mut out, "prior_session_id", session);
                            resumed_at = source.started_at;
                            anchor = pure::new_resumed_run_anchor(
                                &source.status,
                                source.started_at,
                                pure::decode_issue_state_snapshot(source.issue_snapshot.as_deref()),
                            );
                        }
                    }
                    if source.session_rollout_missing {
                        out["prior_session_resume_unavailable"] = json!(true);
                    }
                } else {
                    out["prior_session_resume_unavailable"] = json!(true);
                }
            }
        } else if !task.get::<_, bool>("force_fresh_session") {
            if let Ok(Some(prior)) = load::last_task_session(&db, agent_id, id).await {
                if prior.runtime_id == Some(runtime.id) {
                    optional(&mut out, "prior_session_id", &prior.session_id);
                    resumed_at = prior.started_at;
                    anchor = pure::new_resumed_run_anchor(
                        &prior.status,
                        prior.started_at,
                        prior.snapshot,
                    );
                }
                if let Some(dir) = prior.work_dir {
                    optional(&mut out, "prior_work_dir", dir);
                }
            }
            if load::latest_task_rollout_missing(&db, agent_id, id)
                .await
                .unwrap_or(false)
            {
                out["prior_session_resume_unavailable"] = json!(true);
            }
        } else if task.get::<_, Option<Uuid>>("retry_of_task_id").is_some() {
            fresh_retry_workdir(&mut out, task, headers);
        }
        if out["prior_session_id"]
            .as_str()
            .is_some_and(|s| !s.is_empty())
        {
            if let Some(trigger) = loaded_trigger
                .as_ref()
                .filter(|c| pure::trigger_comment_edited_since(&c.edit, resumed_at))
            {
                if let Some(at) = trigger.edit.updated_at {
                    out["trigger_comment_edited_at"] = json!(pure::rfc3339_secs(at));
                }
                if trigger.edit.revision != 0 {
                    out["trigger_comment_revision"] = json!(trigger.edit.revision);
                }
            }
            if coalesced && !folded.is_empty() {
                let mut edited = Vec::new();
                for comment in &folded {
                    let facts = pure::CommentEditFacts {
                        created_at: comment.created_at_value,
                        updated_at: comment.updated_at,
                        revision: comment.revision,
                        resolved_at: comment.resolved_at,
                    };
                    if pure::trigger_comment_edited_since(&facts, resumed_at) {
                        edited.push(
                            comment.wire_edited(&pure::rfc3339_secs(comment.updated_at.unwrap())),
                        );
                    } else {
                        edited.push(comment.wire());
                    }
                }
                out["coalesced_comments"] = json!(edited);
            }
        }
        if let Some(anchor) = anchor {
            if let Some(prev) = anchor.snapshot {
                out["issue_state_delta_known"] = json!(true);
                let changed = pure::changed_fields_since(&current, &prev);
                if !changed.is_empty() {
                    out["issue_changed_fields"] = json!(changed);
                }
            }
            if let Some(trigger) = scope {
                if let Ok(count) = load::count_new_comments_since(
                    &db,
                    id,
                    runtime.workspace_id,
                    anchor.started_at,
                    trigger,
                    agent_id,
                )
                .await
                {
                    out["new_comments_delta_known"] = json!(true);
                    if count > 0 {
                        out["new_comment_count"] = json!(count);
                        out["new_comments_since"] = json!(pure::rfc3339_secs(anchor.started_at));
                    }
                }
            }
        }
    }
    if let Some(chat_id) = task.get::<_, Option<Uuid>>("chat_session_id") {
        let chat = match load::chat_session(&db, chat_id).await {
            Ok(Some(chat)) => chat,
            other => {
                return Err(build::reject_claim_source_load(
                    state,
                    task,
                    matches!(other, Ok(None)),
                    "chat session",
                )
                .await);
            }
        };
        if let Some(failure) = build::reject_claim_on_workspace_mismatch(
            state,
            task,
            chat.workspace_id,
            runtime.workspace_id,
        )
        .await
        {
            return Err(failure);
        }
        out["workspace_id"] = json!(chat.workspace_id);
        out["chat_session_id"] = json!(chat.id);
        optional(&mut out, "thread_name", &chat.title);
        if chat.is_agent_intro
            && let Ok(false) = load::chat_session_has_user_message(&db, chat.id).await
        {
            out["chat_intro"] = json!(true);
        }
        match load::channel_task_delivery(&db, task_id, &|_| false).await {
            Ok(Some(channel)) => {
                optional(&mut out, "chat_channel_type", &channel.channel_type);
                optional(&mut out, "chat_type", &channel.chat_type);
                if channel.delivers_files {
                    out["chat_channel_delivers_files"] = json!(true);
                }
                if channel.channel_type == "slack"
                    && channel.channel_thread_id.as_deref().is_some_and(|id| {
                        !id.is_empty() && Some(id) != channel.channel_message_id.as_deref()
                    })
                {
                    out["chat_in_thread"] = json!(true);
                }
            }
            Ok(None) => {}
            Err(_) => {
                return Err(build::reject_claim_source_load(
                    state,
                    task,
                    false,
                    "channel task delivery",
                )
                .await);
            }
        }
        project(
            &mut out,
            load::resolve_claim_project_context(&db, chat.project_id, chat.workspace_id)
                .await
                .map_err(|_| project_failure())?,
        );
        let fresh: bool = task.get("force_fresh_session");
        let revision: Option<i64> = task.get("channel_context_revision");
        if !fresh && revision.is_none() {
            if let (Some(session), Some(bound)) = (chat.session_id.as_deref(), chat.runtime_id) {
                if bound == runtime.id {
                    optional(&mut out, "prior_session_id", session);
                }
            }
            if let Some(dir) = chat.work_dir.as_deref() {
                optional(&mut out, "prior_work_dir", dir);
            }
        }
        let messages = if let Some(owner) = task.get::<_, Option<Uuid>>("chat_input_task_id") {
            load::chat_input_messages(&db, owner).await
        } else {
            load::legacy_chat_messages(&db, chat.id)
                .await
                .map(|messages| load::trailing_input_messages(messages, Utc::now()))
        }
        .map_err(|_| {
            ClaimBuildFailure::unsettled(
                "error_chat_input_load",
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load chat input",
            )
        })?;
        if !fresh {
            if revision.is_some()
                || out["prior_session_id"].as_str().unwrap_or("").is_empty()
                || out["prior_work_dir"].as_str().unwrap_or("").is_empty()
            {
                if let Ok(Some(prior)) = load::last_chat_task_session(&db, chat.id, revision).await
                {
                    if out["prior_session_id"].as_str().unwrap_or("").is_empty()
                        && prior.runtime_id == Some(runtime.id)
                    {
                        optional(&mut out, "prior_session_id", prior.session_id);
                    }
                    if out["prior_work_dir"].as_str().unwrap_or("").is_empty() {
                        if let Some(dir) = prior.work_dir {
                            optional(&mut out, "prior_work_dir", dir);
                        }
                    }
                }
            }
            if load::latest_chat_rollout_missing(&db, chat.id, revision)
                .await
                .unwrap_or(false)
            {
                out["prior_session_resume_unavailable"] = json!(true);
            }
        } else if task.get::<_, Option<Uuid>>("retry_of_task_id").is_some() {
            fresh_retry_workdir(&mut out, task, headers);
        }
        let ids = messages
            .iter()
            .map(|message| message.id)
            .collect::<Vec<_>>();
        let attachments = load::chat_attachments(&db, &ids, chat.workspace_id)
            .await
            .map_err(|_| {
                ClaimBuildFailure::unsettled(
                    "error_chat_attachment_load",
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to load chat attachments",
                )
            })?;
        let mut by_message = std::collections::HashMap::<Uuid, Vec<Value>>::new();
        for (message_id, attachment) in attachments {
            by_message.entry(message_id).or_default().push(attachment);
        }
        let attachments = messages
            .iter()
            .flat_map(|m| by_message.remove(&m.id).unwrap_or_default())
            .collect::<Vec<_>>();
        if !attachments.is_empty() {
            out["chat_message_attachments"] = json!(attachments);
        }
        let content = messages
            .iter()
            .map(|m| m.content.as_str())
            .filter(|s| !s.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n\n");
        optional(&mut out, "chat_message", &content);
        if task.get::<_, Option<Uuid>>("chat_input_task_id").is_some()
            && out["chat_intro"] != true
            && content.trim().is_empty()
        {
            let _ = service::cancel_task(state, task_id, None).await;
            return Err(ClaimBuildFailure::unsettled(
                "error_empty_chat_input",
                StatusCode::INTERNAL_SERVER_ERROR,
                "chat task has no user input",
            ));
        }
        if out["thread_name"].as_str().unwrap_or("").trim().is_empty() {
            optional(&mut out, "thread_name", content);
        }
    }
    if let Some(run_id) = task.get::<_, Option<Uuid>>("autopilot_run_id") {
        let run = match load::autopilot_run(&db, run_id).await {
            Ok(Some(run)) => run,
            other => {
                return Err(build::reject_claim_source_load(
                    state,
                    task,
                    matches!(other, Ok(None)),
                    "autopilot run",
                )
                .await);
            }
        };
        let ap = match load::autopilot(&db, run.autopilot_id).await {
            Ok(Some(ap)) => ap,
            other => {
                return Err(build::reject_claim_source_load(
                    state,
                    task,
                    matches!(other, Ok(None)),
                    "autopilot",
                )
                .await);
            }
        };
        if let Some(failure) = build::reject_claim_on_workspace_mismatch(
            state,
            task,
            ap.workspace_id,
            runtime.workspace_id,
        )
        .await
        {
            return Err(failure);
        }
        out["workspace_id"] = json!(ap.workspace_id);
        out["autopilot_id"] = json!(run.autopilot_id);
        optional(&mut out, "autopilot_source", &ap.source);
        if let Some(payload) = run.trigger_payload {
            out["autopilot_trigger_payload"] = payload;
        }
        optional(&mut out, "autopilot_title", &ap.title);
        optional(&mut out, "thread_name", &ap.title);
        optional(&mut out, "autopilot_description", &ap.description);
        project(
            &mut out,
            load::resolve_claim_project_context(&db, ap.project_id, ap.workspace_id)
                .await
                .map_err(|_| project_failure())?,
        );
    }
    if task.get::<_, Option<Uuid>>("issue_id").is_none()
        && task.get::<_, Option<Uuid>>("chat_session_id").is_none()
        && task.get::<_, Option<Uuid>>("autopilot_run_id").is_none()
    {
        if let Some(context) = task
            .get::<_, Option<Value>>("context")
            .filter(|v| v["type"] == "quick_create")
        {
            for (wire, key) in [
                ("quick_create_prompt", "prompt"),
                ("quick_create_priority", "priority"),
                ("quick_create_due_date", "due_date"),
            ] {
                optional(&mut out, wire, context[key].as_str().unwrap_or(""));
            }
            optional(
                &mut out,
                "thread_name",
                context["prompt"].as_str().unwrap_or(""),
            );
            if let Some(ids) = context["attachment_ids"]
                .as_array()
                .filter(|ids| !ids.is_empty())
            {
                out["quick_create_attachment_ids"] = json!(ids);
            }
            let ws_raw = context["workspace_id"].as_str().unwrap_or("");
            let ws = Uuid::parse_str(ws_raw).ok();
            optional(&mut out, "workspace_id", ws_raw);
            if ws != Some(runtime.workspace_id) {
                let _ = service::cancel_task(state, task_id, None).await;
                return Err(ClaimBuildFailure::unsettled(
                    "error_workspace",
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "task workspace isolation check failed",
                ));
            }
            let ws = ws.unwrap();
            if let Some(source) = context["source_context_id"]
                .as_str()
                .filter(|s| !s.is_empty())
            {
                let source = Uuid::parse_str(source);
                let Ok(source) = source else {
                    return Err(build::fail_claimed_task_before_launch(
                        state,
                        task,
                        "Captured source context is invalid. Start again from the branch point.",
                        "agent_unknown",
                        "error_source_context_invalid",
                        StatusCode::CONFLICT,
                        "captured source context is invalid",
                    )
                    .await);
                };
                match load::issue_source_context(&db, ws, source).await {
                    Err(_) => {
                        let _ = service::requeue_task_after_claim_failure(state, task).await;
                        return Err(ClaimBuildFailure::unsettled("error_source_context_load", StatusCode::INTERNAL_SERVER_ERROR, "failed to load captured source context"));
                    }
                    Ok(Some(capture)) if capture.state == "pending" && capture.origin_task_id == task_id => {
                        out["quick_create_source_context"] = capture.snapshot;
                    }
                    _ => return Err(build::fail_claimed_task_before_launch(state, task,
                        "Captured source context is no longer available. Start again from the branch point.", "agent_unknown",
                        "error_source_context_unavailable", StatusCode::CONFLICT, "captured source context is unavailable").await),
                }
            }
            let project_id = context["project_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok());
            project(
                &mut out,
                load::resolve_claim_project_context(&db, project_id, ws)
                    .await
                    .map_err(|_| project_failure())?,
            );
            if let Some(parent) = context["parent_issue_id"]
                .as_str()
                .filter(|s| !s.is_empty())
            {
                let mut exists = true;
                if let Ok(parent_id) = Uuid::parse_str(parent) {
                    match load::issue(&db, parent_id).await {
                        Ok(Some(parent_issue)) if parent_issue.workspace_id == ws => {
                            if let Ok(Some(workspace)) = load::workspace(&db, ws).await {
                                optional(
                                    &mut out,
                                    "parent_issue_identifier",
                                    format!("{}-{}", workspace.issue_prefix, parent_issue.number),
                                );
                            }
                        }
                        Ok(None)
                            if !context["source_context_id"]
                                .as_str()
                                .unwrap_or("")
                                .is_empty() =>
                        {
                            exists = false
                        }
                        _ => {}
                    }
                }
                if exists {
                    optional(&mut out, "parent_issue_id", parent);
                }
            }
            if let Some(id) = context["squad_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok())
            {
                if let Ok(Some(squad)) = load::squad(&db, id, ws).await {
                    if squad.leader_id == Some(agent_id) {
                        let briefing = squad_briefing::build_leader_briefing(
                            &**db,
                            &squad_briefing::Squad {
                                id: squad.id,
                                name: squad.name.clone(),
                                leader_id: agent_id,
                                instructions: squad.instructions,
                            },
                            false,
                            LeaderWorkflow::None,
                            capability(headers, "cli-comment-steer-v1"),
                        )
                        .await;
                        append_briefing(&mut out["agent"], &briefing);
                        out["squad_id"] = json!(squad.id);
                        optional(&mut out, "squad_name", squad.name);
                    }
                }
            }
        }
    }
    if out["workspace_id"] != json!(runtime.workspace_id) {
        let _ = service::cancel_task(state, task_id, None).await;
        return Err(ClaimBuildFailure::unsettled(
            "error_workspace",
            StatusCode::INTERNAL_SERVER_ERROR,
            "task workspace isolation check failed",
        ));
    }
    if let Ok(Some(ws)) = load::workspace(&db, runtime.workspace_id).await {
        optional(&mut out, "workspace_slug", ws.slug);
        if issue_number > 0 {
            optional(
                &mut out,
                "issue_identifier",
                compose::issue_identifier(&ws.issue_prefix, issue_number),
            );
        }
        optional(&mut out, "workspace_context", ws.context);
    }
    if let Ok(entries) = load::issue_statuses(&db, runtime.workspace_id).await {
        let mut statuses = Vec::new();
        let mut omitted = 0;
        for (key, name, category, description, system) in entries {
            if system {
                continue;
            }
            if statuses.len() == 30 {
                omitted += 1;
                continue;
            }
            let mut entry =
                json!({"key":key,"name":name,"category":status_wire_category(&key, &category)});
            optional(&mut entry, "description", description);
            statuses.push(entry);
        }
        if !statuses.is_empty() {
            out["issue_statuses"] = json!(statuses);
        }
        if omitted > 0 {
            out["issue_statuses_omitted"] = json!(omitted);
        }
    }
    if let Some(reason) = pure::worktree_claim_block_reason(
        out["project_resources"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(&[]),
        runtime.daemon_id.as_deref(),
        build::worktree_capability(headers),
    ) {
        if service::cancel_task(state, task_id, Some((&reason, "local_directory_error")))
            .await
            .is_err()
        {
            let _ = service::requeue_task_after_claim_failure(state, task).await;
            return Err(ClaimBuildFailure::unsettled(
                "error_worktree_gate_cancel",
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to cancel a worktree task blocked by daemon version; task requeued",
            ));
        }
        return Err(ClaimBuildFailure::unsettled_owned(
            "error_worktree_daemon_version",
            StatusCode::UNPROCESSABLE_ENTITY,
            reason,
        ));
    }
    if capability(headers, "joined-wakeups-v1") {
        if let Some(joined) = daemon_claim_join::join_waiting_wakeups(state, task).await {
            optional(
                &mut out,
                "wakeup_joined",
                daemon_claim_join::joined_wakeup_notes(Some(&joined)),
            );
        }
    }
    super::p0_tasks::hydrate_attribution(&db, std::slice::from_mut(&mut out)).await;
    if let Some(originator) = out["attribution"]["originator"].as_object() {
        let originator = Value::Object(originator.clone());
        if let Some(id) = originator["id"].as_str() {
            out["initiator_type"] = json!("member");
            optional(&mut out, "initiator_id", id);
            optional(
                &mut out,
                "initiator_name",
                originator["name"].as_str().unwrap_or(""),
            );
            optional(
                &mut out,
                "initiator_email",
                originator["email"].as_str().unwrap_or(""),
            );
        }
    }
    Ok(BuiltClaim {
        response: out,
        delivered_comment_ids,
        issue_snapshot,
    })
}

fn fresh_retry_workdir(out: &mut Value, task: &Row, headers: &HeaderMap) {
    if capability(headers, "checkout-keeps-work-v1") {
        if let Some(dir) = task.get::<_, Option<String>>("work_dir") {
            optional(out, "prior_work_dir", dir);
        }
    }
    out["prior_session_resume_unavailable"] = json!(true);
}

pub(super) async fn finalize_claim_delivery(
    state: &ApiState,
    task: &Row,
    runtime: &ClaimRuntime,
    response: &mut Value,
    token: TaskToken<'_>,
    delivered: &[Uuid],
    snapshot: Option<&[u8]>,
    daemon_token: Option<&build::RemoteMcpDaemonToken>,
) -> Result<Vec<Uuid>, ClaimBuildFailure> {
    let task_id: Uuid = task.get("id");
    let comment_backed = task.get::<_, Option<Uuid>>("trigger_comment_id").is_some()
        || !task
            .get::<_, Option<Vec<Uuid>>>("coalesced_comment_ids")
            .unwrap_or_default()
            .is_empty();
    let result = service::finalize_task_claim(
        state,
        task_id,
        runtime,
        token,
        delivered,
        comment_backed,
        snapshot,
        daemon_token,
    )
    .await;
    match result {
        Ok((receipt, profile)) => {
            response
                .as_object_mut()
                .unwrap()
                .remove("requesting_user_name");
            response
                .as_object_mut()
                .unwrap()
                .remove("requesting_user_profile_description");
            if let Some((name, description)) = profile {
                optional(response, "requesting_user_name", name);
                optional(response, "requesting_user_profile_description", description);
            }
            Ok(receipt)
        }
        Err(FinalizeClaimError::Authz(authz)) => {
            let (message, failure_reason, outcome, status, claim_message) = match authz.reason {
                "error_agent_runtime_changed" => (
                    "The agent moved to another runtime before this task could start. Retry the task to run it on the agent's current runtime.",
                    "invalid_task_identity",
                    "error_agent_runtime_changed",
                    StatusCode::CONFLICT,
                    "agent runtime changed before task delivery",
                ),
                "error_runtime_owner_missing" => (
                    "This runtime cannot run the assigned agent because the runtime has no owner.",
                    "runtime_access_denied",
                    "error_runtime_access_denied",
                    StatusCode::FORBIDDEN,
                    "private runtime does not permit task agent",
                ),
                _ => (
                    private_runtime_denial(authz.agent_owner_id),
                    "runtime_access_denied",
                    "error_runtime_access_denied",
                    StatusCode::FORBIDDEN,
                    "private runtime does not permit task agent",
                ),
            };
            Err(build::fail_claimed_task_before_launch(
                state,
                task,
                message,
                failure_reason,
                outcome,
                status,
                claim_message,
            )
            .await)
        }
        Err(FinalizeClaimError::Api(_)) => Err(ClaimBuildFailure::unsettled(
            "error_claim_finalize",
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to finalize task claim",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn category_and_private_owner_wire_rules() {
        assert_eq!(status_wire_category("custom", "closed"), "cancelled");
        assert_eq!(status_wire_category("custom", "done"), "done");
        assert_eq!(status_wire_category("custom", "started"), "custom");
        let owner = Uuid::now_v7();
        let runtime = ClaimRuntime {
            id: Uuid::nil(),
            workspace_id: Uuid::nil(),
            daemon_id: None,
            owner_id: Some(owner),
            visibility: "private".into(),
            shared_with: vec![],
            provider: String::new(),
        };
        assert!(runtime_allows_agent(&runtime, None));
        assert!(runtime_allows_agent(&runtime, Some(owner)));
        assert!(!runtime_allows_agent(&runtime, Some(Uuid::now_v7())));
    }
}
