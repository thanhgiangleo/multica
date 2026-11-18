-- Owner row is locked; nullable structural values use explicit presence flags.
-- Status, position and duplicate rules mirror frozen Go UpdateIssue.
WITH candidate AS MATERIALIZED (
 SELECT i.*,
   COALESCE($3::text,i.title) AS next_title,
   COALESCE($4::text,i.description) AS next_description,
   COALESCE($5::text,i.priority) AS next_priority,
   COALESCE($20::text,i.status) AS next_status,
   CASE WHEN $6::double precision IS NOT NULL THEN $6::double precision
     WHEN i.status IS DISTINCT FROM COALESCE($20::text,i.status)
       THEN (SELECT COALESCE(MIN(target.position),0)-1 FROM issue target WHERE target.workspace_id=i.workspace_id AND target.status=$20::text)
     ELSE i.position END AS next_position,
   CASE WHEN $7::boolean THEN $8::date ELSE i.start_date END AS next_start_date,
   CASE WHEN $9::boolean THEN $10::date ELSE i.due_date END AS next_due_date,
   CASE WHEN $11::boolean THEN $12::uuid ELSE i.parent_issue_id END AS next_parent,
   CASE WHEN $13::boolean THEN $14::uuid ELSE i.project_id END AS next_project,
   CASE WHEN $15::boolean THEN $16::integer ELSE i.stage END AS next_stage,
   CASE WHEN $17::boolean THEN $18::text ELSE i.assignee_type END AS next_assignee_type,
   CASE WHEN $17::boolean THEN $19::uuid ELSE i.assignee_id END AS next_assignee_id,
   CASE WHEN $21::uuid IS NOT NULL THEN $21::uuid
     WHEN i.status='cancelled' AND COALESCE($20::text,i.status)='cancelled' THEN i.duplicate_of_issue_id
     ELSE NULL END AS next_duplicate
 FROM issue i WHERE i.id=$1 AND i.workspace_id=$2
), changed AS (
 SELECT candidate.*,
   ROW(title,description,status,priority,position,start_date,due_date,parent_issue_id,project_id,stage,assignee_type,assignee_id,duplicate_of_issue_id)
     IS DISTINCT FROM ROW(next_title,next_description,next_status,next_priority,next_position,next_start_date,next_due_date,next_parent,next_project,next_stage,next_assignee_type,next_assignee_id,next_duplicate) AS did_change,
   ROW(title,description,status,priority,start_date,due_date,parent_issue_id,project_id,stage,assignee_type,assignee_id,duplicate_of_issue_id)
     IS DISTINCT FROM ROW(next_title,next_description,next_status,next_priority,next_start_date,next_due_date,next_parent,next_project,next_stage,next_assignee_type,next_assignee_id,next_duplicate) AS did_activity
 FROM candidate
)
UPDATE issue i SET title=c.next_title,description=c.next_description,status=c.next_status,
 priority=c.next_priority,position=c.next_position,start_date=c.next_start_date,due_date=c.next_due_date,
 parent_issue_id=c.next_parent,project_id=c.next_project,stage=c.next_stage,
 assignee_type=c.next_assignee_type,assignee_id=c.next_assignee_id,
 duplicate_of_issue_id=c.next_duplicate,
 revision=i.revision+c.did_change::integer,
 last_activity_at=CASE WHEN c.did_activity THEN GREATEST(COALESCE(i.last_activity_at,i.updated_at),now()) ELSE i.last_activity_at END,
 updated_at=CASE WHEN c.did_change THEN now() ELSE i.updated_at END
FROM changed c WHERE i.id=c.id AND i.workspace_id=$2 RETURNING i.*
