package handler

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/middleware"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
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
