-- Backing index for project_member's primary key, attached in 602.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS project_member_pkey_uidx
    ON project_member (project_id, user_id);
