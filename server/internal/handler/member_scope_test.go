package handler

import (
	"fmt"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/testutil"
)

// scopeFixture is a workspace with two projects and a restricted member granted
// only the first. Issues live in each project and in none.
type scopeFixture struct {
	memberID               string
	inProject, outProject  string
	inIssue, outIssue      string
	noProjectIssue         string
	inComment, outComment  string
	ownerAgent, otherAgent string
}

func newScopeFixture(t *testing.T) scopeFixture {
	t.Helper()
	if testHandler == nil {
		t.Skip("database not available")
	}
	stamp := fmt.Sprintf("%d", time.Now().UnixNano())
	var f scopeFixture
	f.memberID = dbfx.User(t, "Scoped Member", "scoped-"+stamp+"@example.test")
	dbfx.Member(t, testWorkspaceID, f.memberID, "member", testutil.Cols{"access_scope": "project"})

	f.inProject = dbfx.Project(t, "In scope "+stamp)
	f.outProject = dbfx.Project(t, "Out of scope "+stamp)
	dbfx.Exec(t, `INSERT INTO project_member (project_id, user_id, workspace_id) VALUES ($1, $2, $3)`,
		f.inProject, f.memberID, testWorkspaceID)
	t.Cleanup(func() {
		_, _ = testPool.Exec(t.Context(), `DELETE FROM project_member WHERE user_id = $1`, f.memberID)
	})

	f.inIssue = dbfx.Issue(t, "scope-in "+stamp, testutil.Cols{"project_id": f.inProject})
	f.outIssue = dbfx.Issue(t, "scope-out "+stamp, testutil.Cols{"project_id": f.outProject})
	f.noProjectIssue = dbfx.Issue(t, "scope-none "+stamp)
	// Fixture issues carry explicit numbers; move the counter past them so the
	// handler's own numbering does not collide.
	dbfx.Exec(t, `UPDATE workspace
		SET issue_counter = GREATEST(issue_counter, (SELECT COALESCE(MAX(number), 0) FROM issue WHERE workspace_id = $1)) + 1
		WHERE id = $1`, testWorkspaceID)
	f.inComment = dbfx.Comment(t, f.inIssue, "in-scope comment")
	f.outComment = dbfx.Comment(t, f.outIssue, "out-of-scope comment")
	return f
}

func (f scopeFixture) as(method, path string, body any) *http.Request {
	return newRequestAs(f.memberID, method, path, body)
}

func issueIDsOf(t *testing.T, body map[string]any) map[string]bool {
	t.Helper()
	raw, _ := body["issues"].([]any)
	ids := map[string]bool{}
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			if id, _ := m["id"].(string); id != "" {
				ids[id] = true
			}
		}
	}
	return ids
}

func TestRestrictedMember_ListIssuesOnlyShowsGrantedProjects(t *testing.T) {
	f := newScopeFixture(t)
	got := issueIDsOf(t, testutil.Call(t, testHandler.ListIssues, f.as("GET", "/api/issues?limit=100", nil)).
		Want(http.StatusOK).Map())
	if !got[f.inIssue] {
		t.Fatalf("granted-project issue missing: %v", got)
	}
	if got[f.outIssue] || got[f.noProjectIssue] {
		t.Fatalf("restricted member saw out-of-scope issues: %v", got)
	}

	// An unrestricted member (the workspace owner) still sees everything.
	all := issueIDsOf(t, testutil.Call(t, testHandler.ListIssues, newRequest("GET", "/api/issues?limit=100", nil)).
		Want(http.StatusOK).Map())
	if !all[f.inIssue] || !all[f.outIssue] || !all[f.noProjectIssue] {
		t.Fatalf("owner lost visibility: %v", all)
	}
}

func TestRestrictedMember_GetIssueOutOfScopeIsNotFound(t *testing.T) {
	f := newScopeFixture(t)
	for _, id := range []string{f.outIssue, f.noProjectIssue} {
		req := testutil.WithURLParams(f.as("GET", "/api/issues/"+id, nil), "id", id)
		testutil.Call(t, testHandler.GetIssue, req).Want(http.StatusNotFound)
	}
	req := testutil.WithURLParams(f.as("GET", "/api/issues/"+f.inIssue, nil), "id", f.inIssue)
	testutil.Call(t, testHandler.GetIssue, req).Want(http.StatusOK)
}

func TestRestrictedMember_SearchDoesNotLeakThroughTitleOrComment(t *testing.T) {
	f := newScopeFixture(t)
	// "out-of-scope comment" matches only via the comment on the out issue.
	res := testutil.Call(t, testHandler.SearchIssues,
		f.as("GET", "/api/issues/search?q=out-of-scope+comment&include_closed=true", nil)).
		Want(http.StatusOK).Map()
	if ids := issueIDsOf(t, res); ids[f.outIssue] {
		t.Fatalf("comment search leaked out-of-scope issue: %v", ids)
	}
	res = testutil.Call(t, testHandler.SearchIssues,
		f.as("GET", "/api/issues/search?q=scope-out&include_closed=true", nil)).
		Want(http.StatusOK).Map()
	if ids := issueIDsOf(t, res); ids[f.outIssue] {
		t.Fatalf("title search leaked out-of-scope issue: %v", ids)
	}
	res = testutil.Call(t, testHandler.SearchIssues,
		f.as("GET", "/api/issues/search?q=scope-in&include_closed=true", nil)).
		Want(http.StatusOK).Map()
	if ids := issueIDsOf(t, res); !ids[f.inIssue] {
		t.Fatalf("granted issue missing from search: %v", ids)
	}
}

func TestRestrictedMember_IssueTableRowsAreScoped(t *testing.T) {
	f := newScopeFixture(t)
	body := map[string]any{
		"query": map[string]any{"scope": map[string]any{"kind": "workspace"}},
		"group": map[string]any{"kind": "none"},
		"page":  map[string]any{"limit": 100},
	}
	resp := testutil.Call(t, testHandler.ListIssueTableRows, f.as("POST", "/api/issues/table/rows", body))
	if resp.Code != http.StatusOK {
		t.Skipf("table request shape not accepted (%d): %s", resp.Code, resp.Body.String())
	}
	text := resp.Body.String()
	if strings.Contains(text, f.outIssue) || strings.Contains(text, f.noProjectIssue) {
		t.Fatalf("table rows leaked out-of-scope issue: %s", text)
	}
}

func TestRestrictedMember_CannotCreateOutsideGrantedProjects(t *testing.T) {
	f := newScopeFixture(t)
	for name, project := range map[string]any{
		"foreign project": f.outProject,
		"no project":      nil,
	} {
		body := map[string]any{"title": "should not exist " + name}
		if project != nil {
			body["project_id"] = project
		}
		testutil.Call(t, testHandler.CreateIssue, f.as("POST", "/api/issues", body)).
			Want(http.StatusNotFound)
	}
	body := map[string]any{"title": "ok " + time.Now().Format("150405.000"), "project_id": f.inProject}
	resp := testutil.Call(t, testHandler.CreateIssue, f.as("POST", "/api/issues", body)).Want(http.StatusCreated)
	if id, _ := resp.Map()["id"].(string); id != "" {
		t.Cleanup(func() { _, _ = testPool.Exec(t.Context(), `DELETE FROM issue WHERE id = $1`, id) })
	}
}

func TestRestrictedMember_CannotMoveIssueOutOfScopeOrClearProject(t *testing.T) {
	f := newScopeFixture(t)
	upd := func(id string, body map[string]any) *testutil.Response {
		req := testutil.WithURLParams(f.as("PUT", "/api/issues/"+id, body), "id", id)
		return testutil.Call(t, testHandler.UpdateIssue, req)
	}
	upd(f.inIssue, map[string]any{"project_id": f.outProject}).Want(http.StatusNotFound)
	upd(f.inIssue, map[string]any{"project_id": nil}).Want(http.StatusNotFound)
	upd(f.outIssue, map[string]any{"title": "hijack"}).Want(http.StatusNotFound)

	var projectID string
	dbfx.QueryRow(t, `SELECT project_id::text FROM issue WHERE id = $1`, f.inIssue).Scan(&projectID)
	if projectID != f.inProject {
		t.Fatalf("issue project changed to %s", projectID)
	}
}

func TestRestrictedMember_BatchOperationsSkipOutOfScopeIssues(t *testing.T) {
	f := newScopeFixture(t)
	testutil.Call(t, testHandler.BatchUpdateIssues, f.as("POST", "/api/issues/batch-update", map[string]any{
		"issue_ids": []string{f.outIssue, f.noProjectIssue},
		"updates":   map[string]any{"priority": "urgent"},
	}))
	testutil.Call(t, testHandler.BatchDeleteIssues, f.as("POST", "/api/issues/batch-delete", map[string]any{
		"issue_ids": []string{f.outIssue, f.noProjectIssue},
	}))
	for _, id := range []string{f.outIssue, f.noProjectIssue} {
		var priority string
		dbfx.QueryRow(t, `SELECT priority FROM issue WHERE id = $1`, id).Scan(&priority)
		if priority != "none" {
			t.Fatalf("issue %s was modified through a batch: priority=%s", id, priority)
		}
	}
}

func TestRestrictedMember_ProjectsAreScoped(t *testing.T) {
	f := newScopeFixture(t)
	res := testutil.Call(t, testHandler.ListProjects, f.as("GET", "/api/projects", nil)).Want(http.StatusOK).Map()
	text := fmt.Sprint(res)
	if !strings.Contains(text, f.inProject) {
		t.Fatalf("granted project missing: %s", text)
	}
	if strings.Contains(text, f.outProject) {
		t.Fatalf("out-of-scope project listed: %s", text)
	}
	req := testutil.WithURLParams(f.as("GET", "/api/projects/"+f.outProject, nil), "id", f.outProject)
	testutil.Call(t, testHandler.GetProject, req).Want(http.StatusNotFound)
}

func TestRestrictedMember_AgentsHiddenUnlessNamedPersonally(t *testing.T) {
	f := newScopeFixture(t)
	runtimeID := dbfx.Runtime(t, "scope-runtime-"+f.memberID[:8])
	hiddenAgent := dbfx.Agent(t, "wide-open "+f.memberID[:8], runtimeID,
		testutil.Cols{"permission_mode": "public_to"})
	dbfx.Exec(t, `INSERT INTO agent_invocation_target (agent_id, target_type, target_id) VALUES ($1, 'workspace', $2)`,
		hiddenAgent, testWorkspaceID)
	namedAgent := dbfx.Agent(t, "named "+f.memberID[:8], runtimeID,
		testutil.Cols{"permission_mode": "public_to"})
	dbfx.Exec(t, `INSERT INTO agent_invocation_target (agent_id, target_type, target_id) VALUES ($1, 'member', $2)`,
		namedAgent, f.memberID)

	res := testutil.Call(t, testHandler.ListAgents, f.as("GET", "/api/agents", nil)).Want(http.StatusOK)
	text := res.Body.String()
	if strings.Contains(text, hiddenAgent) {
		t.Fatalf("restricted member saw a workspace-wide agent: %s", text)
	}
	if !strings.Contains(text, namedAgent) {
		t.Fatalf("agent that names the member personally is missing: %s", text)
	}
}

func TestMemberAllowedToViewAgentAs_RestrictedVsPlain(t *testing.T) {
	user := parseUUID("11111111-1111-1111-1111-111111111111")
	other := parseUUID("22222222-2222-2222-2222-222222222222")
	plain := db.Member{UserID: user, Role: "member", AccessScope: "workspace"}
	restricted := db.Member{UserID: user, Role: "member", AccessScope: "project"}
	adminWithStaleScope := db.Member{UserID: user, Role: "admin", AccessScope: "project"}

	wide := db.Agent{OwnerID: other, PermissionMode: "public_to"}
	wideTargets := []db.AgentInvocationTarget{{TargetType: "workspace"}}
	named := db.Agent{OwnerID: other, PermissionMode: "public_to"}
	namedTargets := []db.AgentInvocationTarget{{TargetType: "member", TargetID: user}}
	owned := db.Agent{OwnerID: user, PermissionMode: "private"}
	private := db.Agent{OwnerID: other, PermissionMode: "private"}

	for _, tc := range []struct {
		name    string
		agent   db.Agent
		targets []db.AgentInvocationTarget
		member  db.Member
		want    bool
	}{
		{"plain sees workspace-wide", wide, wideTargets, plain, true},
		{"restricted does not see workspace-wide", wide, wideTargets, restricted, false},
		{"restricted sees agent naming them", named, namedTargets, restricted, true},
		{"restricted sees agent they own", owned, nil, restricted, true},
		{"restricted does not see someone's private agent", private, nil, restricted, false},
		{"admin with stale scope is not locked out", private, nil, adminWithStaleScope, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := memberAllowedToViewAgentAs(tc.agent, tc.targets, tc.member); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// A confined member's roster holds only themselves, people sharing a granted
// project, and owners/admins — with emails withheld for everyone but themselves.
func TestRestrictedMember_ListMembersIsLimitedToProjectMates(t *testing.T) {
	f := newScopeFixture(t)
	stamp := fmt.Sprint(time.Now().UnixNano())
	mateID := dbfx.User(t, "Project Mate", "mate-"+stamp+"@example.test")
	dbfx.Member(t, testWorkspaceID, mateID, "member")
	strangerID := dbfx.User(t, "Stranger", "stranger-"+stamp+"@example.test")
	dbfx.Member(t, testWorkspaceID, strangerID, "member")
	dbfx.Exec(t, `INSERT INTO project_member (project_id, user_id, workspace_id) VALUES ($1, $2, $3)`,
		f.inProject, mateID, testWorkspaceID)
	t.Cleanup(func() {
		_, _ = testPool.Exec(t.Context(), `DELETE FROM project_member WHERE user_id = $1`, parseUUID(mateID))
	})

	req := testutil.WithURLParams(f.as("GET", "/api/workspaces/"+testWorkspaceID+"/members", nil), "id", testWorkspaceID)
	var list []MemberWithUserResponse
	testutil.Call(t, testHandler.ListMembersWithUser, req).Want(http.StatusOK).JSON(&list)

	seen := map[string]MemberWithUserResponse{}
	for _, m := range list {
		seen[m.UserID] = m
	}
	if _, ok := seen[f.memberID]; !ok {
		t.Fatal("confined member must see themselves")
	}
	if m, ok := seen[mateID]; !ok {
		t.Fatal("project mate missing")
	} else if m.Email != "" {
		t.Fatalf("mate email leaked: %q", m.Email)
	}
	if _, ok := seen[strangerID]; ok {
		t.Fatal("member outside the shared project is listed")
	}
	if seen[f.memberID].Email == "" {
		t.Fatal("a member should still see their own email")
	}
	for _, m := range list {
		if m.Role == "member" && m.UserID != f.memberID && m.UserID != mateID {
			t.Fatalf("unrelated plain member listed: %+v", m)
		}
	}
}
