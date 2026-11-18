WITH RECURSIVE subtree(node_id) AS (
    SELECT id FROM issue WHERE id=$1 AND workspace_id=$4
    UNION
    SELECT i.id FROM issue i JOIN subtree s ON i.parent_issue_id=s.node_id
    WHERE i.workspace_id=$4
), retire_descendants AS (
    UPDATE issue_subscriber sub SET unsubscribed_at=now(),opt_out_scope='subtree'
    WHERE sub.issue_id IN (SELECT node_id FROM subtree WHERE node_id<>$1)
      AND sub.user_type=$2 AND sub.user_id=$3 AND sub.unsubscribed_at IS NULL
    RETURNING sub.issue_id
), retire_root AS (
    INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason,unsubscribed_at,opt_out_scope)
    SELECT $1,$2,$3,'manual',now(),'subtree' FROM subtree WHERE node_id=$1
    ON CONFLICT(issue_id,user_type,user_id) DO UPDATE SET unsubscribed_at=now(),opt_out_scope='subtree'
    RETURNING issue_id
)
SELECT issue_id FROM retire_descendants UNION ALL SELECT issue_id FROM retire_root
