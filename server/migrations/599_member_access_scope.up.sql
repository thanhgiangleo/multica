-- Restricted members. access_scope 'workspace' keeps today's behavior (the
-- member sees the whole workspace); 'project' confines the member to the
-- projects listed in project_member. Owners and admins are never restricted;
-- that rule is enforced in application code, not here.
ALTER TABLE member
    ADD COLUMN IF NOT EXISTS access_scope TEXT NOT NULL DEFAULT 'workspace'
    CHECK (access_scope IN ('workspace', 'project'));
