-- A restricted invitee is confined to project_ids from the moment they accept.
-- Both columns are applied to member / project_member in the accepting
-- transaction; project ids are validated in application code (no foreign keys).
ALTER TABLE workspace_invitation
    ADD COLUMN IF NOT EXISTS access_scope TEXT NOT NULL DEFAULT 'workspace'
        CHECK (access_scope IN ('workspace', 'project')),
    ADD COLUMN IF NOT EXISTS project_ids UUID[] NOT NULL DEFAULT '{}';
