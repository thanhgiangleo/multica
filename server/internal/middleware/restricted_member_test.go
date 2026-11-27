package middleware

import (
	"net/http"
	"testing"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestRestrictedRouteAllowed(t *testing.T) {
	const (
		get  = http.MethodGet
		post = http.MethodPost
		put  = http.MethodPut
		del  = http.MethodDelete
	)
	for _, tc := range []struct {
		method, path string
		want         bool
	}{
		// Work surface.
		{get, "/api/issues", true},
		{post, "/api/issues", true},
		{put, "/api/issues/abc", true},
		{post, "/api/issues/abc/comments", true},
		{get, "/api/chat/sessions", true},
		{post, "/api/chat/sessions", true},
		{get, "/api/projects", true},
		{get, "/api/projects/abc", true},
		{get, "/api/projects/abc/resources", true},
		{get, "/api/agents", true},
		{get, "/api/agents/abc", true},
		{get, "/api/issue-workflows", true},
		{get, "/api/issue-statuses", true},
		{get, "/api/tasks/abc/messages", true},
		{put, "/api/comments/abc", true},
		{get, "/api/attachments/abc", true},
		{get, "/api/workspaces/ws1", true},

		// Writes outside the work surface.
		{post, "/api/projects", false},
		{put, "/api/projects/abc", false},
		{del, "/api/projects/abc", false},
		{post, "/api/projects/abc/workflow", false},
		{post, "/api/projects/abc/resources", false},
		{post, "/api/agents", false},
		{put, "/api/agents/abc", false},
		{post, "/api/issue-workflows", false},
		{post, "/api/labels", false},

		// Whole-workspace surfaces stay closed until opened on purpose.
		{get, "/api/workspaces/ws1/members", true},
		{get, "/api/workspaces/ws1/invitations", false},
		// A confined member cannot lift their own confinement.
		{http.MethodPatch, "/api/workspaces/ws1/members/m1", false},
		{post, "/api/workspaces/ws1/members", false},
		{del, "/api/workspaces/ws1/members/m1", false},
		{get, "/api/inbox", false},
		{get, "/api/dashboard/usage", false},
		{get, "/api/squads", false},
		{get, "/api/autopilots", false},
		{get, "/api/skills", false},
		{get, "/api/runtimes/abc/usage", false},
		{post, "/api/runtimes/abc/update", false},
		{get, "/api/agent-task-snapshot", false},
		{get, "/api/working-agents", false},
		{get, "/api/issue-wakeups", false},
		{get, "/api/search-index/snapshot", false},
		{get, "/api/assignee-frequency", false},
		{get, "/api/pins", false},
		{post, "/api/tasks/abc/cancel", false},
		{post, "/api/tasks/abc/rerun", false},
		{get, "/api/some-future-endpoint", false},
		{get, "/", false},
		{get, "/api", false},
	} {
		if got := restrictedRouteAllowed(tc.method, tc.path); got != tc.want {
			t.Errorf("%s %s: got %v, want %v", tc.method, tc.path, got, tc.want)
		}
	}
}

func TestIsRestrictedMember(t *testing.T) {
	for _, tc := range []struct {
		name   string
		member db.Member
		want   bool
	}{
		{"plain member, workspace scope", db.Member{Role: "member", AccessScope: "workspace"}, false},
		{"plain member, project scope", db.Member{Role: "member", AccessScope: "project"}, true},
		{"admin is never restricted", db.Member{Role: "admin", AccessScope: "project"}, false},
		{"owner is never restricted", db.Member{Role: "owner", AccessScope: "project"}, false},
		{"zero value is unrestricted", db.Member{}, false},
	} {
		if got := IsRestrictedMember(tc.member); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
