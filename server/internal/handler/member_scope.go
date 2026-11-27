package handler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/middleware"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// memberScope is what a request's human member may reach. A zero value means
// unrestricted, so every caller that does not opt in keeps today's behavior.
type memberScope struct {
	restricted bool
	member     db.Member
	projectIDs []pgtype.UUID
}

func (s memberScope) allowsProject(projectID pgtype.UUID) bool {
	if !s.restricted {
		return true
	}
	if !projectID.Valid {
		return false
	}
	for _, id := range s.projectIDs {
		if id == projectID {
			return true
		}
	}
	return false
}

// requestMemberScope resolves the scope of the member behind r. Agent actors
// and daemon traffic are never restricted: they act through their own task
// token, not as the human who happens to own them.
//
// ok=false means the scope could not be determined; callers must fail closed.
func (h *Handler) requestMemberScope(r *http.Request, workspaceID string) (memberScope, bool) {
	if r.Header.Get("X-Actor-Source") == "task_token" {
		return memberScope{}, true
	}
	member, found := ctxMember(r.Context())
	if !found {
		var err error
		member, err = h.getWorkspaceMember(r.Context(), requestUserID(r), workspaceID)
		if err != nil {
			// Not a member at all: the route's own membership gate decides.
			return memberScope{}, true
		}
	}
	return h.memberScopeFor(r.Context(), member)
}

func (h *Handler) memberScopeFor(ctx context.Context, member db.Member) (memberScope, bool) {
	if !middleware.IsRestrictedMember(member) {
		return memberScope{}, true
	}
	ids, err := h.Queries.ListProjectMemberProjectIDs(ctx, db.ListProjectMemberProjectIDsParams{
		UserID:      member.UserID,
		WorkspaceID: member.WorkspaceID,
	})
	if err != nil {
		return memberScope{}, false
	}
	return memberScope{restricted: true, member: member, projectIDs: ids}, true
}

// scopedMemberOrDeny is the common entry for handlers: it writes the failure
// itself when the scope cannot be read.
func (h *Handler) scopedMemberOrDeny(w http.ResponseWriter, r *http.Request, workspaceID string) (memberScope, bool) {
	scope, ok := h.requestMemberScope(r, workspaceID)
	if !ok {
		writeError(w, http.StatusInternalServerError, "failed to resolve member scope")
		return memberScope{}, false
	}
	return scope, true
}

// projectScopeArg is the `project_id = ANY($n)` operand for dynamic issue
// queries; the empty list matches nothing, which is the right answer for a
// restricted member with no grants.
func (s memberScope) projectScopeArg() []pgtype.UUID {
	if s.projectIDs == nil {
		return []pgtype.UUID{}
	}
	return s.projectIDs
}

// targetsVisibleToMember drops workspace-wide invocation targets for a
// restricted member: they see an agent only when it names them personally.
func targetsVisibleToMember(targets []db.AgentInvocationTarget, member db.Member) []db.AgentInvocationTarget {
	if !middleware.IsRestrictedMember(member) {
		return targets
	}
	out := make([]db.AgentInvocationTarget, 0, len(targets))
	for _, t := range targets {
		if t.TargetType == "member" {
			out = append(out, t)
		}
	}
	return out
}

// denyProjectOutOfScope writes a 404 and returns true when a restricted member
// names a project outside its grants. Unrestricted members always pass. A null
// project is out of scope for a restricted member: they cannot file work that
// no project of theirs owns.
func (h *Handler) denyProjectOutOfScope(w http.ResponseWriter, r *http.Request, workspaceID string, projectID pgtype.UUID) bool {
	scope, ok := h.scopedMemberOrDeny(w, r, workspaceID)
	if !ok {
		return true
	}
	if scope.allowsProject(projectID) {
		return false
	}
	writeError(w, http.StatusNotFound, "project not found")
	return true
}

// denyIssueOutOfScope is the same gate for an issue named by id in a body or a
// batch, where loadIssueForUser is not on the path.
func (h *Handler) denyIssueOutOfScope(w http.ResponseWriter, r *http.Request, workspaceID string, issue db.Issue) bool {
	scope, ok := h.scopedMemberOrDeny(w, r, workspaceID)
	if !ok {
		return true
	}
	if scope.allowsProject(issue.ProjectID) {
		return false
	}
	writeError(w, http.StatusNotFound, "issue not found")
	return true
}

// accessGrant is a validated access scope plus the projects behind it.
type accessGrant struct {
	scope      string
	projectIDs []pgtype.UUID
}

// resolveAccessGrant validates a requested access scope for a member of the
// given role. scope "" means workspace-wide. Only a plain member can be
// confined, and every project must belong to the workspace; both are checked
// here because the schema carries no foreign keys.
func (h *Handler) resolveAccessGrant(ctx context.Context, workspaceID pgtype.UUID, role, scope string, rawProjectIDs []string) (accessGrant, int, string) {
	if scope == "" {
		scope = "workspace"
	}
	switch scope {
	case "workspace":
		if len(rawProjectIDs) > 0 {
			return accessGrant{}, http.StatusBadRequest, "project_ids requires access_scope \"project\""
		}
		return accessGrant{scope: "workspace"}, 0, ""
	case middleware.AccessScopeProject:
		if role != "member" {
			return accessGrant{}, http.StatusBadRequest, "only a member can be limited to projects"
		}
	default:
		return accessGrant{}, http.StatusBadRequest, "invalid access_scope"
	}

	seen := make(map[pgtype.UUID]struct{}, len(rawProjectIDs))
	ids := make([]pgtype.UUID, 0, len(rawProjectIDs))
	for _, raw := range rawProjectIDs {
		id, err := util.ParseUUID(raw)
		if err != nil {
			return accessGrant{}, http.StatusBadRequest, fmt.Sprintf("invalid project id %q", raw)
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) > 0 {
		found, err := h.Queries.ListProjectIDsInWorkspace(ctx, db.ListProjectIDsInWorkspaceParams{
			WorkspaceID: workspaceID,
			ProjectIds:  ids,
		})
		if err != nil {
			return accessGrant{}, http.StatusInternalServerError, "failed to validate projects"
		}
		if len(found) != len(ids) {
			return accessGrant{}, http.StatusBadRequest, "project not found in this workspace"
		}
	}
	return accessGrant{scope: middleware.AccessScopeProject, projectIDs: ids}, 0, ""
}

// memberProjectIDStrings lists a member's grants as strings, never nil.
func (h *Handler) memberProjectIDStrings(ctx context.Context, member db.Member) []string {
	out := []string{}
	if member.AccessScope != middleware.AccessScopeProject {
		return out
	}
	ids, err := h.Queries.ListProjectMemberProjectIDs(ctx, db.ListProjectMemberProjectIDsParams{
		UserID:      member.UserID,
		WorkspaceID: member.WorkspaceID,
	})
	if err != nil {
		return out
	}
	for _, id := range ids {
		out = append(out, uuidToString(id))
	}
	return out
}

// publishMemberAccessChanged makes every realtime node drop the member's
// sockets. Rooms are chosen at connect time, so the client reconnects into the
// ones its new scope allows.
func (h *Handler) publishMemberAccessChanged(workspaceID, userID, actorID string) {
	h.publish(protocol.EventMemberAccessChanged, workspaceID, "member", actorID, map[string]any{
		"workspace_id": workspaceID,
		"user_id":      userID,
	})
}

// replaceProjectGrants rewrites a member's grants to exactly projectIDs. qtx
// must belong to the caller's transaction so the member never has a moment
// with the old grants gone and the new ones missing.
func replaceProjectGrants(ctx context.Context, qtx *db.Queries, workspaceID, userID, grantedBy pgtype.UUID, projectIDs []pgtype.UUID) error {
	if err := qtx.DeleteProjectMembersByUser(ctx, db.DeleteProjectMembersByUserParams{
		UserID:      userID,
		WorkspaceID: workspaceID,
	}); err != nil {
		return err
	}
	if len(projectIDs) == 0 {
		return nil
	}
	return qtx.AddProjectMembers(ctx, db.AddProjectMembersParams{
		UserID:      userID,
		WorkspaceID: workspaceID,
		ProjectIds:  projectIDs,
		CreatedBy:   grantedBy,
	})
}

// visibleMemberFilter says which workspace members a viewer may list. An
// unrestricted viewer sees everyone. A confined one sees themselves, the
// members sharing a project with them, and owners and admins (whose names show
// up as issue authors and assignees). It returns nil when the grants cannot be
// read, so the caller fails closed.
func (h *Handler) visibleMemberFilter(ctx context.Context, viewer db.Member) func(userID pgtype.UUID, role string) bool {
	if !middleware.IsRestrictedMember(viewer) {
		return func(pgtype.UUID, string) bool { return true }
	}
	mine, err := h.Queries.ListProjectMemberProjectIDs(ctx, db.ListProjectMemberProjectIDsParams{
		UserID:      viewer.UserID,
		WorkspaceID: viewer.WorkspaceID,
	})
	if err != nil {
		return nil
	}
	mates := map[pgtype.UUID]bool{viewer.UserID: true}
	if len(mine) > 0 {
		ids, err := h.Queries.ListProjectMateUserIDs(ctx, db.ListProjectMateUserIDsParams{
			WorkspaceID: viewer.WorkspaceID,
			Column2:     mine,
		})
		if err != nil {
			return nil
		}
		for _, id := range ids {
			mates[id] = true
		}
	}
	return func(userID pgtype.UUID, role string) bool {
		return mates[userID] || role == "owner" || role == "admin"
	}
}
