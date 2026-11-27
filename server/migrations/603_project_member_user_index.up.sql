-- Resolves a restricted member's project scope per request.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_project_member_user_workspace
    ON project_member (user_id, workspace_id);
