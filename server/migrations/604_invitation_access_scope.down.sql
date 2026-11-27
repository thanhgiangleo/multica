ALTER TABLE workspace_invitation
    DROP COLUMN IF EXISTS project_ids,
    DROP COLUMN IF EXISTS access_scope;
