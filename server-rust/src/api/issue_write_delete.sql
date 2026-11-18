-- Frozen Go `DeleteIssue`: the workspace predicate is a SQL-layer tenant
-- guarantee, and issue_vcs_pull_request has no FK so its links are swept here.
WITH target AS (
    SELECT issue.id FROM issue WHERE issue.id = $1 AND issue.workspace_id = $2
),
cleared_wakeup_receipts AS (
 DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE issue_id IN (SELECT target.id FROM target))
),
cleared_wakeups AS (
 DELETE FROM issue_wakeup WHERE issue_id IN (SELECT target.id FROM target)
),
cleared_child_events AS (
 DELETE FROM issue_child_event WHERE parent_id IN (SELECT target.id FROM target) OR child_id IN (SELECT target.id FROM target)
),
cleared_task_supplements AS (
 DELETE FROM task_supplement WHERE issue_id IN (SELECT target.id FROM target)
),
cleared_task_supplement_capabilities AS (
 DELETE FROM task_supplement_capability WHERE issue_id IN (SELECT target.id FROM target)
),
cleared_vcs_pr_links AS (
    DELETE FROM issue_vcs_pull_request WHERE issue_id IN (SELECT target.id FROM target)
),
cleared_pr_automation AS (
    DELETE FROM issue_pr_automation WHERE issue_id IN (SELECT target.id FROM target)
),
cleared_pr_exclusions AS (
    DELETE FROM issue_pull_request_exclusion WHERE issue_id IN (SELECT target.id FROM target)
)
DELETE FROM issue WHERE issue.id IN (SELECT target.id FROM target)
