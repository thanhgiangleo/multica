package middleware

import (
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// AccessScopeProject confines a plain member to the projects listed in
// project_member. Owners and admins are never restricted.
const AccessScopeProject = "project"

// IsRestrictedMember reports whether member is confined to its granted
// projects. The role check keeps an owner/admin that carries a stale
// access_scope from being locked out.
func IsRestrictedMember(member db.Member) bool {
	return member.AccessScope == AccessScopeProject && member.Role == "member"
}

// restrictedRouteAllowed is the deny-by-default allow-list for restricted
// members. Anything not named here answers 403, so a route added later is
// closed to them until someone opens it on purpose.
//
// Handlers behind an allowed route still filter by project: this list decides
// which surfaces a restricted member may reach, not which rows they see.
func restrictedRouteAllowed(method, path string) bool {
	segs := splitPath(path)
	if len(segs) < 2 || segs[0] != "api" {
		return false
	}
	read := method == http.MethodGet || method == http.MethodHead
	switch segs[1] {
	case "issues", "chat":
		return true
	case "comments":
		return len(segs) >= 3
	case "attachments":
		return len(segs) >= 3
	case "tasks":
		// Only the transcript of a run; cancel/rerun by task id are not scoped.
		return read && len(segs) == 4 && segs[3] == "messages"
	case "projects":
		return read && (len(segs) == 2 || len(segs) == 3 || (len(segs) == 4 && segs[3] == "resources"))
	case "issue-statuses", "issue-workflows", "properties", "labels":
		return read
	case "agents":
		return read && (len(segs) == 2 || len(segs) == 3)
	case "runtimes":
		return read && len(segs) == 2
	case "workspaces":
		// Only the workspace itself. The member list carries every member's
		// name and email, so it stays closed until it can be limited to people
		// who share a project.
		return read && len(segs) == 3
	}
	return false
}

func splitPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

// authorizeRestrictedMember applies the allow-list and the per-resource scope
// check. It writes the response itself and returns false when the request must
// stop.
func authorizeRestrictedMember(w http.ResponseWriter, r *http.Request, queries *db.Queries, member db.Member) bool {
	if !restrictedRouteAllowed(r.Method, r.URL.Path) {
		writeError(w, http.StatusForbidden, "not available for project-scoped members")
		return false
	}
	segs := splitPath(r.URL.Path)
	switch segs[1] {
	case "comments":
		return requireScopedComment(w, r, queries, member, segs[2])
	case "attachments":
		return requireScopedAttachment(w, r, queries, member, segs[2])
	case "tasks":
		return requireScopedTask(w, r, queries, member, segs[2])
	}
	return true
}

// memberProjectIDs returns the projects a restricted member was granted.
func memberProjectIDs(r *http.Request, queries *db.Queries, member db.Member) ([]pgtype.UUID, error) {
	return queries.ListProjectMemberProjectIDs(r.Context(), db.ListProjectMemberProjectIDsParams{
		UserID:      member.UserID,
		WorkspaceID: member.WorkspaceID,
	})
}

func projectIDInScope(scope []pgtype.UUID, projectID pgtype.UUID) bool {
	if !projectID.Valid {
		return false
	}
	for _, id := range scope {
		if id == projectID {
			return true
		}
	}
	return false
}

// issueInScope answers false for an unknown issue as well, so a missing row and
// a forbidden one look the same.
func issueInScope(r *http.Request, queries *db.Queries, member db.Member, issueID pgtype.UUID) bool {
	if !issueID.Valid {
		return false
	}
	projectID, err := queries.GetIssueProjectID(r.Context(), db.GetIssueProjectIDParams{
		ID:          issueID,
		WorkspaceID: member.WorkspaceID,
	})
	if err != nil {
		return false
	}
	scope, err := memberProjectIDs(r, queries, member)
	if err != nil {
		return false
	}
	return projectIDInScope(scope, projectID)
}

func notFound(w http.ResponseWriter, what string) bool {
	writeError(w, http.StatusNotFound, what+" not found")
	return false
}

func requireScopedComment(w http.ResponseWriter, r *http.Request, queries *db.Queries, member db.Member, rawID string) bool {
	id, err := util.ParseUUID(rawID)
	if err != nil {
		return notFound(w, "comment")
	}
	comment, err := queries.GetCommentInWorkspace(r.Context(), db.GetCommentInWorkspaceParams{
		ID:          id,
		WorkspaceID: member.WorkspaceID,
	})
	if err != nil || !issueInScope(r, queries, member, comment.IssueID) {
		return notFound(w, "comment")
	}
	return true
}

func requireScopedAttachment(w http.ResponseWriter, r *http.Request, queries *db.Queries, member db.Member, rawID string) bool {
	id, err := util.ParseUUID(rawID)
	if err != nil {
		return notFound(w, "attachment")
	}
	att, err := queries.GetAttachment(r.Context(), db.GetAttachmentParams{
		ID:          id,
		WorkspaceID: member.WorkspaceID,
	})
	if err != nil {
		return notFound(w, "attachment")
	}
	issueID := att.IssueID
	if !issueID.Valid && att.CommentID.Valid {
		if c, err := queries.GetCommentInWorkspace(r.Context(), db.GetCommentInWorkspaceParams{
			ID:          att.CommentID,
			WorkspaceID: member.WorkspaceID,
		}); err == nil {
			issueID = c.IssueID
		}
	}
	if issueID.Valid {
		if !issueInScope(r, queries, member, issueID) {
			return notFound(w, "attachment")
		}
		return true
	}
	// Chat, task and source-context uploads belong to whoever uploaded them.
	if att.UploaderType == "member" && att.UploaderID == member.UserID {
		return true
	}
	return notFound(w, "attachment")
}

func requireScopedTask(w http.ResponseWriter, r *http.Request, queries *db.Queries, member db.Member, rawID string) bool {
	id, err := util.ParseUUID(rawID)
	if err != nil {
		return notFound(w, "task")
	}
	task, err := queries.GetAgentTask(r.Context(), id)
	if err != nil {
		return notFound(w, "task")
	}
	if task.IssueID.Valid {
		if !issueInScope(r, queries, member, task.IssueID) {
			return notFound(w, "task")
		}
		return true
	}
	// A chat task's transcript is gated to the session creator by the handler.
	if task.ChatSessionID.Valid {
		return true
	}
	return notFound(w, "task")
}
