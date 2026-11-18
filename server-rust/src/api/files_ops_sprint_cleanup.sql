-- Integrator assigns the numbered migration. No foreign keys: a cleanup intent
-- must survive deletion of its attachment/workspace and service restarts.
CREATE TABLE IF NOT EXISTS attachment_object_cleanup (
    attachment_id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL,
    object_url TEXT NOT NULL,
    ready BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    cleanup_checked_at TIMESTAMPTZ
);
ALTER TABLE attachment_object_cleanup ADD COLUMN IF NOT EXISTS cleanup_checked_at TIMESTAMPTZ;

-- Preserve storage ownership before existing issue/comment/workspace cascades
-- remove the last attachment row. Enqueue only; object IO runs after commit in
-- the reconciler. A rolled-back parent deletion also rolls back this intent.
CREATE OR REPLACE FUNCTION files_ops_sprint_enqueue_attachment_cleanup()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO attachment_object_cleanup
        (attachment_id, workspace_id, object_url, ready)
    VALUES (OLD.id, OLD.workspace_id, OLD.url, true)
    ON CONFLICT (attachment_id) DO UPDATE
        SET workspace_id = EXCLUDED.workspace_id,
            object_url = EXCLUDED.object_url,
            ready = true,
            cleanup_checked_at = NULL;
    RETURN OLD;
END;
$$;
CREATE OR REPLACE TRIGGER files_ops_sprint_attachment_cleanup
AFTER DELETE ON attachment
FOR EACH ROW EXECUTE FUNCTION files_ops_sprint_enqueue_attachment_cleanup();
