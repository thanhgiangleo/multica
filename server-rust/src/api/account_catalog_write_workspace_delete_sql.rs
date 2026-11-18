//! Frozen Go workspace teardown SQL. Regenerate with tools/build_account_catalog_write_workspace_delete_sql.py.
//! workspace_delete.sql SHA-256: 665ec85a553408853641b4369e7419f5edaead7d717b7cab52a28733c7631a70

pub(super) const PREPARE_WORKSPACE_DELETION_LINKS: &str = r#"WITH
detached_comments AS (
    UPDATE comment
    SET parent_id = NULL
    WHERE comment.workspace_id = $1
      AND parent_id IS NOT NULL
),
detached_issues AS (
    UPDATE issue
    SET parent_issue_id = NULL
    WHERE issue.workspace_id = $1
      AND parent_issue_id IS NOT NULL
)
UPDATE webhook_delivery
SET replayed_from_delivery_id = NULL
WHERE webhook_delivery.workspace_id = $1
  AND replayed_from_delivery_id IS NOT NULL;"#;

pub(super) const DETACH_TASK_BATCH_REFERENCES: &str = r#"WITH detached_runs AS (
    UPDATE autopilot_run
    SET task_id = NULL
    WHERE task_id = ANY($1::uuid[])
)
UPDATE agent_task_queue
SET parent_task_id = NULL
WHERE parent_task_id = ANY($1::uuid[]);"#;

pub(super) const DELETE_TASK_BATCH: &str = r#"WITH
batch AS MATERIALIZED (
    SELECT id FROM unnest($1::uuid[]) AS t(id)
),
deleted_task_usage AS (
    DELETE FROM task_usage WHERE task_id IN (SELECT id FROM batch)
),
deleted_task_messages AS (
    DELETE FROM task_message WHERE task_id IN (SELECT id FROM batch)
),
deleted_task_tokens AS (
    DELETE FROM task_token WHERE task_id IN (SELECT id FROM batch)
),
deleted_task_supplements AS (
    DELETE FROM task_supplement WHERE task_id IN (SELECT id FROM batch)
),
deleted_task_supplement_capabilities AS (
    DELETE FROM task_supplement_capability WHERE task_id IN (SELECT id FROM batch)
),
deleted_channel_outbound_cards AS (
    DELETE FROM channel_outbound_card_message WHERE task_id IN (SELECT id FROM batch)
),
deleted_lark_outbound_cards AS (
    DELETE FROM lark_outbound_card_message WHERE task_id IN (SELECT id FROM batch)
),
deleted_channel_task_deliveries AS (
    DELETE FROM channel_task_delivery WHERE task_id IN (SELECT id FROM batch)
),
deleted_draft_restores AS (
    DELETE FROM chat_draft_restore WHERE task_id IN (SELECT id FROM batch)
)
DELETE FROM agent_task_queue WHERE id IN (SELECT id FROM batch);"#;

pub(super) const DELETE_TASK_TOKENS_BY_AGENT: &str =
    r#"DELETE FROM task_token WHERE agent_id = $1;"#;

pub(super) const DELETE_CHAT_PINNED_AGENTS_BY_WORKSPACE: &str = r#"DELETE FROM chat_pinned_agent
WHERE workspace_id = $1;"#;

pub(super) const STEPS: &[(&str, &str)] = &[
    (
        "DeleteWorkspaceLeafData",
        r#"WITH
ws_agents AS MATERIALIZED (
    SELECT id FROM agent WHERE workspace_id = $1
),
ws_issues AS MATERIALIZED (
    SELECT id FROM issue WHERE workspace_id = $1
),
ws_labels AS MATERIALIZED (
    SELECT id FROM issue_label WHERE workspace_id = $1
),
ws_skills AS MATERIALIZED (
    SELECT id FROM skill WHERE workspace_id = $1
),
ws_squads AS MATERIALIZED (
    SELECT id FROM squad WHERE workspace_id = $1
),
ws_sessions AS MATERIALIZED (
    SELECT id FROM chat_session WHERE workspace_id = $1
),
ws_autopilots AS MATERIALIZED (
    SELECT id FROM autopilot WHERE workspace_id = $1
),
ws_github_prs AS MATERIALIZED (
    SELECT id FROM github_pull_request WHERE workspace_id = $1
),
ws_vcs_prs AS MATERIALIZED (
    SELECT id FROM vcs_pull_request WHERE workspace_id = $1
),
ws_vcs_connections AS MATERIALIZED (
    SELECT id FROM vcs_connection WHERE workspace_id = $1
),
ws_channel_installations AS MATERIALIZED (
    SELECT id FROM channel_installation WHERE workspace_id = $1
),
ws_lark_installations AS MATERIALIZED (
    SELECT id FROM lark_installation WHERE workspace_id = $1
),
ws_telegram_issue_alert_rules AS MATERIALIZED (
    SELECT id FROM zbs_telegram_issue_alert_rule WHERE workspace_id = $1
),
deleted_task_tokens AS (
    DELETE FROM task_token
    WHERE workspace_id = $1
),
deleted_orphan_task_supplements AS (
    DELETE FROM task_supplement WHERE workspace_id = $1
),
deleted_orphan_task_supplement_capabilities AS (
    DELETE FROM task_supplement_capability WHERE workspace_id = $1
),
deleted_hourly_dirty AS (
    DELETE FROM task_usage_hourly_dirty WHERE workspace_id = $1
),
deleted_hourly AS (
    DELETE FROM task_usage_hourly WHERE workspace_id = $1
),
deleted_attachments AS (
    DELETE FROM attachment WHERE workspace_id = $1
),
deleted_channel_outbound_cards AS (
    DELETE FROM channel_outbound_card_message
    WHERE chat_session_id IN (SELECT id FROM ws_sessions)
),
deleted_lark_outbound_cards AS (
    DELETE FROM lark_outbound_card_message
    WHERE chat_session_id IN (SELECT id FROM ws_sessions)
),
deleted_draft_restores AS (
    DELETE FROM chat_draft_restore
    WHERE chat_session_id IN (SELECT id FROM ws_sessions)
),
deleted_agent_builder_drafts AS (
    DELETE FROM agent_builder_draft WHERE workspace_id = $1
),
deleted_comment_reactions AS (
    DELETE FROM comment_reaction WHERE workspace_id = $1
),
deleted_issue_reactions AS (
    DELETE FROM issue_reaction WHERE workspace_id = $1
),
deleted_activity AS (
    DELETE FROM activity_log WHERE workspace_id = $1
),
deleted_telegram_issue_alert_deliveries AS (
    DELETE FROM zbs_telegram_issue_alert_delivery
    WHERE rule_id IN (SELECT id FROM ws_telegram_issue_alert_rules)
),
deleted_gateway_daily_telegram_deliveries AS (
    DELETE FROM zbs_gateway_daily_telegram_delivery WHERE workspace_id = $1
),
deleted_inbox AS (
    DELETE FROM inbox_item WHERE workspace_id = $1
),
deleted_issue_dependencies AS (
    DELETE FROM issue_dependency
    WHERE issue_id IN (SELECT id FROM ws_issues)
       OR depends_on_issue_id IN (SELECT id FROM ws_issues)
),
deleted_issue_subscribers AS (
    DELETE FROM issue_subscriber
    WHERE issue_id IN (SELECT id FROM ws_issues)
),
deleted_issue_labels AS (
    DELETE FROM issue_to_label
    WHERE issue_id IN (SELECT id FROM ws_issues)
       OR label_id IN (SELECT id FROM ws_labels)
),
deleted_agent_labels AS (
    DELETE FROM agent_to_label
    WHERE agent_id IN (SELECT id FROM ws_agents)
       OR label_id IN (SELECT id FROM ws_labels)
),
deleted_skill_labels AS (
    DELETE FROM skill_to_label
    WHERE skill_id IN (SELECT id FROM ws_skills)
       OR label_id IN (SELECT id FROM ws_labels)
),
deleted_issue_github_links AS (
    DELETE FROM issue_pull_request
    WHERE issue_id IN (SELECT id FROM ws_issues)
       OR pull_request_id IN (SELECT id FROM ws_github_prs)
),
deleted_issue_vcs_links AS (
    DELETE FROM issue_vcs_pull_request
    WHERE issue_id IN (SELECT id FROM ws_issues)
       OR pull_request_id IN (SELECT id FROM ws_vcs_prs)
),
deleted_issue_pr_automation AS (
    DELETE FROM issue_pr_automation WHERE workspace_id = $1
),
deleted_issue_pr_exclusions AS (
    DELETE FROM issue_pull_request_exclusion WHERE workspace_id = $1
),
deleted_agent_invocation_targets AS (
    DELETE FROM agent_invocation_target
    WHERE agent_id IN (SELECT id FROM ws_agents)
),
deleted_agent_skills AS (
    DELETE FROM agent_skill
    WHERE agent_id IN (SELECT id FROM ws_agents)
       OR skill_id IN (SELECT id FROM ws_skills)
),
deleted_skill_files AS (
    DELETE FROM skill_file
    WHERE skill_id IN (SELECT id FROM ws_skills)
),
deleted_daemon_connections AS (
    DELETE FROM daemon_connection
    WHERE agent_id IN (SELECT id FROM ws_agents)
),
deleted_squad_members AS (
    DELETE FROM squad_member
    WHERE squad_id IN (SELECT id FROM ws_squads)
),
deleted_project_resources AS (
    DELETE FROM project_resource WHERE workspace_id = $1
),
deleted_autopilot_collaborators AS (
    DELETE FROM autopilot_collaborator
    WHERE autopilot_id IN (SELECT id FROM ws_autopilots)
),
deleted_autopilot_subscribers AS (
    DELETE FROM autopilot_subscriber
    WHERE autopilot_id IN (SELECT id FROM ws_autopilots)
),
deleted_webhook_deliveries AS (
    DELETE FROM webhook_delivery WHERE workspace_id = $1
),
deleted_github_check_runs AS (
    DELETE FROM github_pull_request_check_run
    WHERE pr_id IN (SELECT id FROM ws_github_prs)
),
deleted_github_check_suites AS (
    DELETE FROM github_pull_request_check_suite
    WHERE pr_id IN (SELECT id FROM ws_github_prs)
),
deleted_pending_github_suites AS (
    DELETE FROM github_pending_check_suite WHERE workspace_id = $1
),
deleted_channel_task_deliveries AS (
    DELETE FROM channel_task_delivery
    WHERE installation_id IN (SELECT id FROM ws_channel_installations)
),
deleted_channel_outbound_messages AS (
    DELETE FROM channel_outbound_message
    WHERE installation_id IN (SELECT id FROM ws_channel_installations)
),
deleted_channel_reply_deliveries AS (
    DELETE FROM channel_reply_delivery
    WHERE installation_id IN (SELECT id FROM ws_channel_installations)
),
deleted_channel_chat_contexts AS (
    DELETE FROM channel_chat_context_generation
    WHERE chat_session_id IN (SELECT id FROM ws_sessions)
),
deleted_vcs_commit_statuses AS (
    DELETE FROM vcs_commit_status
    WHERE connection_id IN (SELECT id FROM ws_vcs_connections)
),
deleted_channel_chat_bindings AS (
    DELETE FROM channel_chat_session_binding
    WHERE installation_id IN (SELECT id FROM ws_channel_installations)
       OR chat_session_id IN (SELECT id FROM ws_sessions)
),
deleted_dingtalk_group_routes AS (
    DELETE FROM dingtalk_group_route WHERE workspace_id = $1
),
deleted_channel_inbound_dedup AS (
    DELETE FROM channel_inbound_message_dedup
    WHERE installation_id IN (SELECT id FROM ws_channel_installations)
),
deleted_channel_inbound_audit AS (
    DELETE FROM channel_inbound_audit
    WHERE installation_id IN (SELECT id FROM ws_channel_installations)
),
deleted_channel_user_bindings AS (
    DELETE FROM channel_user_binding WHERE workspace_id = $1
),
deleted_channel_binding_tokens AS (
    DELETE FROM channel_binding_token WHERE workspace_id = $1
),
deleted_lark_chat_bindings AS (
    DELETE FROM lark_chat_session_binding
    WHERE installation_id IN (SELECT id FROM ws_lark_installations)
       OR chat_session_id IN (SELECT id FROM ws_sessions)
),
deleted_lark_inbound_dedup AS (
    DELETE FROM lark_inbound_message_dedup
    WHERE installation_id IN (SELECT id FROM ws_lark_installations)
),
deleted_lark_inbound_audit AS (
    DELETE FROM lark_inbound_audit
    WHERE installation_id IN (SELECT id FROM ws_lark_installations)
),
deleted_lark_user_bindings AS (
    DELETE FROM lark_user_binding WHERE workspace_id = $1
),
deleted_lark_binding_tokens AS (
    DELETE FROM lark_binding_token WHERE workspace_id = $1
)
UPDATE channel_media_pending_object
SET state = CASE
        WHEN state = 'tombstoned' THEN 'tombstoned'
        ELSE 'deleting'
    END,
    lease_token = NULL,
    lease_expires_at = NULL,
    next_attempt_at = now(),
    last_error = NULL
WHERE channel_media_pending_object.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspaceAutopilotRuns",
        r#"DELETE FROM autopilot_run
WHERE autopilot_id IN (
    SELECT id FROM autopilot WHERE autopilot.workspace_id = $1
);"#,
    ),
    (
        "DeleteWorkspaceAutopilotQuotaReservations",
        r#"DELETE FROM autopilot_quota_reservation
WHERE autopilot_quota_reservation.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspaceAutopilotQuotaPeriods",
        r#"DELETE FROM autopilot_quota_period
WHERE autopilot_quota_period.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspaceChatMessages",
        r#"DELETE FROM chat_message
WHERE chat_session_id IN (
    SELECT id FROM chat_session WHERE chat_session.workspace_id = $1
);"#,
    ),
    (
        "DeleteWorkspaceForkData",
        "DELETE FROM zbs_zalo_update_inbox WHERE workspace_id=$1",
    ),
    (
        "DeleteWorkspaceCommunicationRoots",
        r#"WITH
deleted_sessions AS (
    DELETE FROM chat_session WHERE chat_session.workspace_id = $1
),
deleted_dingtalk_group_presence AS (
    DELETE FROM dingtalk_group_presence WHERE workspace_id = $1
),
deleted_dingtalk_bot_identity AS (
    DELETE FROM dingtalk_bot_identity WHERE workspace_id = $1
),
deleted_telegram_issue_alert_rules AS (
    DELETE FROM zbs_telegram_issue_alert_rule WHERE workspace_id = $1
),
deleted_gateway_daily_telegram_rules AS (
    DELETE FROM zbs_gateway_daily_telegram_rule WHERE workspace_id = $1
),
deleted_telegram_guest_invites AS (
    DELETE FROM zbs_telegram_guest_invite WHERE workspace_id = $1
),
deleted_telegram_guest_grants AS (
    DELETE FROM zbs_telegram_guest_grant WHERE workspace_id = $1
),
deleted_zalo_guest_invites AS (
    DELETE FROM zbs_zalo_guest_invite WHERE workspace_id = $1
),
deleted_zalo_guest_grants AS (
    DELETE FROM zbs_zalo_guest_grant WHERE workspace_id = $1
),
deleted_channel_installations AS (
    DELETE FROM channel_installation
    WHERE channel_installation.workspace_id = $1
)
DELETE FROM lark_installation WHERE lark_installation.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspaceComments",
        r#"DELETE FROM comment WHERE comment.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspaceSearchIndexChanges",
        r#"DELETE FROM search_index_change WHERE workspace_id = $1;"#,
    ),
    (
        "DeleteSourceContextAttachmentsByWorkspace",
        r#"DELETE FROM attachment
WHERE workspace_id = $1
  AND source_context_id IS NOT NULL;"#,
    ),
    (
        "DeleteIssueSourceContextsForWorkspace",
        r#"DELETE FROM issue_source_context
WHERE workspace_id = $1
RETURNING *;"#,
    ),
    (
        "DeleteWorkspaceIssueRoots",
        r#"WITH deleted_wakeup_receipts AS (
 DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE workspace_id=$1)
), deleted_wakeups AS (
 DELETE FROM issue_wakeup WHERE workspace_id=$1
),
deleted_child_events AS (
 DELETE FROM issue_child_event WHERE workspace_id=$1
),
deleted_issues AS (
    DELETE FROM issue WHERE issue.workspace_id = $1
),
deleted_labels AS (
    DELETE FROM issue_label WHERE issue_label.workspace_id = $1
),
deleted_properties AS (
    DELETE FROM issue_property WHERE issue_property.workspace_id = $1
),
deleted_issue_views AS (
    DELETE FROM issue_view WHERE issue_view.workspace_id = $1
),
deleted_issue_view_preferences AS (
    DELETE FROM issue_view_preference
    WHERE issue_view_preference.workspace_id = $1
)
DELETE FROM quick_action WHERE quick_action.workspace_id = $1;"#,
    ),
    (
        "DeleteIssueStatusEntriesForWorkspace",
        r#"DELETE FROM issue_status WHERE workspace_id = $1::uuid;"#,
    ),
    (
        "DeleteIssueWorkflowsForWorkspace",
        r#"DELETE FROM issue_workflow WHERE workspace_id = $1::uuid;"#,
    ),
    (
        "DeleteWorkspaceAutopilotChildren",
        r#"WITH
deleted_triggers AS (
    DELETE FROM autopilot_trigger
    WHERE autopilot_id IN (
        SELECT id FROM autopilot WHERE autopilot.workspace_id = $1
    )
)
DELETE FROM autopilot_rule_version
WHERE autopilot_rule_version.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspaceAutopilots",
        r#"DELETE FROM autopilot WHERE autopilot.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspacePullRequests",
        r#"WITH deleted_github_prs AS (
    DELETE FROM github_pull_request
    WHERE github_pull_request.workspace_id = $1
)
DELETE FROM vcs_pull_request WHERE vcs_pull_request.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspaceConnections",
        r#"WITH deleted_github_installations AS (
    DELETE FROM github_installation
    WHERE github_installation.workspace_id = $1
)
DELETE FROM vcs_connection WHERE vcs_connection.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspaceSquadsAndSkills",
        r#"WITH deleted_squads AS (
    DELETE FROM squad WHERE squad.workspace_id = $1
)
DELETE FROM skill WHERE skill.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspacePluginData",
        r#"WITH installations AS MATERIALIZED (
    SELECT plugin_installation.id
    FROM plugin_installation
    WHERE plugin_installation.workspace_id = $1
),
deleted_storage AS (
    DELETE FROM plugin_storage
    WHERE installation_id IN (SELECT id FROM installations)
),
deleted_secrets AS (
    DELETE FROM plugin_secret
    WHERE installation_id IN (SELECT id FROM installations)
),
deleted_hook_schedules AS (
    DELETE FROM plugin_hook_schedule
    WHERE installation_id IN (SELECT id FROM installations)
),
deleted_invocations AS (
    DELETE FROM plugin_invocation
    WHERE workspace_id = $1
),
versions AS MATERIALIZED (
    SELECT plugin_package_version.id
    FROM plugin_package_version
    WHERE plugin_package_version.workspace_id = $1
),
deleted_package_files AS (
    DELETE FROM plugin_package_file
    WHERE version_id IN (SELECT id FROM versions)
),
deleted_package_versions AS (
    DELETE FROM plugin_package_version
    WHERE workspace_id = $1
),
deleted_packages AS (
    DELETE FROM plugin_package
    WHERE workspace_id = $1
)
DELETE FROM plugin_installation WHERE id IN (SELECT id FROM installations);"#,
    ),
    (
        "DeleteWorkspaceMcpBindings",
        "DELETE FROM agent_mcp_server WHERE agent_id IN (SELECT id FROM agent WHERE workspace_id=$1) OR server_id IN (SELECT id FROM workspace_mcp_server WHERE workspace_id=$1)",
    ),
    (
        "DeleteWorkspaceAgents",
        r#"DELETE FROM agent WHERE agent.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspaceRuntimesAndProjects",
        r#"WITH
deleted_runtimes AS (
    DELETE FROM agent_runtime WHERE agent_runtime.workspace_id = $1
),
deleted_profiles AS (
    DELETE FROM runtime_profile WHERE runtime_profile.workspace_id = $1
)
DELETE FROM project WHERE project.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspaceAdministration",
        r#"WITH
deleted_members AS (
    DELETE FROM member WHERE member.workspace_id = $1
),
deleted_notification_preferences AS (
    DELETE FROM notification_preference
    WHERE notification_preference.workspace_id = $1
),
deleted_pins AS (
    DELETE FROM pinned_item WHERE pinned_item.workspace_id = $1
),
deleted_daemon_tokens AS (
    DELETE FROM daemon_token WHERE daemon_token.workspace_id = $1
),
detached_feedback AS (
    UPDATE feedback
    SET workspace_id = NULL
    WHERE feedback.workspace_id = $1
),
detached_client_usage AS (
    UPDATE client_usage_daily
    SET workspace_id = NULL
    WHERE client_usage_daily.workspace_id = $1
),
deleted_share_links AS (
    DELETE FROM workspace_share_link
    WHERE workspace_share_link.workspace_id = $1
)
DELETE FROM workspace_invitation
WHERE workspace_invitation.workspace_id = $1;"#,
    ),
    (
        "DeleteWorkspace",
        r#"WITH ws_installations AS (
    SELECT id FROM channel_installation WHERE workspace_id = $1
),
ws_sessions AS (
    SELECT id FROM chat_session WHERE workspace_id = $1
),
ws_agents AS (
    SELECT id FROM agent WHERE workspace_id = $1
),
ws_skills AS (
    SELECT id FROM skill WHERE workspace_id = $1
),
cleared_agent_label_assignments AS (
    DELETE FROM agent_to_label WHERE agent_id IN (SELECT id FROM ws_agents)
),
cleared_skill_label_assignments AS (
    DELETE FROM skill_to_label WHERE skill_id IN (SELECT id FROM ws_skills)
),
cleared_channel_task_deliveries AS (
    DELETE FROM channel_task_delivery WHERE installation_id IN (SELECT id FROM ws_installations)
),
cleared_channel_outbound_messages AS (
    DELETE FROM channel_outbound_message WHERE installation_id IN (SELECT id FROM ws_installations)
),
cleared_channel_reply_deliveries AS (
    DELETE FROM channel_reply_delivery WHERE installation_id IN (SELECT id FROM ws_installations)
),
cleared_chat_sessions AS (
    DELETE FROM channel_chat_session_binding WHERE installation_id IN (SELECT id FROM ws_installations)
    RETURNING chat_session_id
),
cleared_chat_contexts AS (
    DELETE FROM channel_chat_context_generation
    WHERE chat_session_id IN (SELECT chat_session_id FROM cleared_chat_sessions)
       OR chat_session_id IN (SELECT id FROM ws_sessions)
),
cleared_outbound_cards AS (
    DELETE FROM channel_outbound_card_message
    WHERE chat_session_id IN (SELECT chat_session_id FROM cleared_chat_sessions)
),
cleared_draft_restores AS (
    DELETE FROM chat_draft_restore
    WHERE chat_session_id IN (SELECT id FROM chat_session WHERE workspace_id = $1)
),
cleared_inbound_dedup AS (
    DELETE FROM channel_inbound_message_dedup WHERE installation_id IN (SELECT id FROM ws_installations)
),
cleared_dingtalk_group_presence AS (
    DELETE FROM dingtalk_group_presence WHERE installation_id IN (SELECT id FROM ws_installations)
),
cleared_dingtalk_bot_identity AS (
    DELETE FROM dingtalk_bot_identity WHERE installation_id IN (SELECT id FROM ws_installations)
),
cleared_dingtalk_group_routes AS (
    DELETE FROM dingtalk_group_route WHERE workspace_id = $1
),
cleared_audit AS (
    DELETE FROM channel_inbound_audit WHERE installation_id IN (SELECT id FROM ws_installations)
),
cleared_user_bindings AS (
    DELETE FROM channel_user_binding WHERE workspace_id = $1
),
cleared_binding_tokens AS (
    DELETE FROM channel_binding_token WHERE workspace_id = $1
),
cleared_installations AS (
    DELETE FROM channel_installation WHERE workspace_id = $1
),
cleared_issue_properties AS (
    DELETE FROM issue_property WHERE workspace_id = $1
),
cleared_quick_actions AS (
    DELETE FROM quick_action WHERE workspace_id = $1
),
ws_mcp_servers AS (
    SELECT id FROM workspace_mcp_server WHERE workspace_id = $1
),
cleared_agent_mcp_bindings AS (
    DELETE FROM agent_mcp_server
    WHERE server_id IN (SELECT id FROM ws_mcp_servers)
       OR agent_id IN (SELECT id FROM ws_agents)
),
cleared_workspace_mcp_servers AS (
    DELETE FROM workspace_mcp_server WHERE workspace_id = $1
),
deleted_pending_check_suites AS (
    DELETE FROM github_pending_check_suite WHERE workspace_id = $1
),
ws_github_prs AS (
    SELECT id FROM github_pull_request WHERE workspace_id = $1
),
cleared_github_pr_check_runs AS (
    DELETE FROM github_pull_request_check_run
    WHERE pr_id IN (SELECT id FROM ws_github_prs)
),
ws_vcs_prs AS (
    SELECT id FROM vcs_pull_request WHERE workspace_id = $1
),
ws_vcs_connections AS (
    SELECT id FROM vcs_connection WHERE workspace_id = $1
),
cleared_vcs_pr_links AS (
    DELETE FROM issue_vcs_pull_request
    WHERE pull_request_id IN (SELECT id FROM ws_vcs_prs)
),
cleared_vcs_commit_statuses AS (
    DELETE FROM vcs_commit_status
    WHERE connection_id IN (SELECT id FROM ws_vcs_connections)
),
cleared_vcs_prs AS (
    DELETE FROM vcs_pull_request WHERE workspace_id = $1
),
cleared_vcs_connections AS (
    DELETE FROM vcs_connection WHERE workspace_id = $1
),
cleared_client_usage_workspace AS (
    UPDATE client_usage_daily SET workspace_id = NULL WHERE workspace_id = $1
)
DELETE FROM workspace WHERE workspace.id = $1;"#,
    ),
];
