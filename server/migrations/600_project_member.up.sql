-- Projects a restricted member (member.access_scope = 'project') may work in.
-- No foreign keys by repo rule: project and user ids are validated in
-- application code, and rows are removed there when a project or member goes.
-- The primary key is attached in 602 from the index built CONCURRENTLY in 601.
CREATE TABLE IF NOT EXISTS project_member (
    project_id UUID NOT NULL,
    user_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    created_by UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
