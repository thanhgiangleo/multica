package handler

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/testutil"
)

func grantsOf(t *testing.T, userID string) map[string]bool {
	t.Helper()
	rows, err := testPool.Query(t.Context(), `SELECT project_id::text FROM project_member WHERE user_id = $1 AND workspace_id = $2`,
		parseUUID(userID), parseUUID(testWorkspaceID))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out[id] = true
	}
	return out
}

func scopeOf(t *testing.T, userID string) string {
	t.Helper()
	var scope string
	if err := testPool.QueryRow(t.Context(), `SELECT access_scope FROM member WHERE user_id = $1 AND workspace_id = $2`,
		parseUUID(userID), parseUUID(testWorkspaceID)).Scan(&scope); err != nil {
		t.Fatal(err)
	}
	return scope
}

func adminFixture(t *testing.T, role string) (userID, memberID string) {
	t.Helper()
	if testHandler == nil {
		t.Skip("database not available")
	}
	userID = dbfx.User(t, "Access "+role, fmt.Sprintf("access-%s-%d@example.test", role, time.Now().UnixNano()))
	memberID = dbfx.Member(t, testWorkspaceID, userID, role)
	t.Cleanup(func() {
		_, _ = testPool.Exec(t.Context(), `DELETE FROM project_member WHERE user_id = $1`, parseUUID(userID))
	})
	return userID, memberID
}

func updateMember(t *testing.T, memberID string, body map[string]any) *testutil.Response {
	t.Helper()
	req := newRequest("PATCH", "/api/workspaces/"+testWorkspaceID+"/members/"+memberID, body)
	req = withURLParams(req, "id", testWorkspaceID, "memberId", memberID)
	return testutil.Call(t, testHandler.UpdateMember, req)
}

func TestUpdateMember_ConfinesMemberToProjects(t *testing.T) {
	userID, memberID := adminFixture(t, "member")
	p1 := dbfx.Project(t, "Grant one "+memberID)
	p2 := dbfx.Project(t, "Grant two "+memberID)

	var resp MemberWithUserResponse
	updateMember(t, memberID, map[string]any{"access_scope": "project", "project_ids": []string{p1, p2}}).
		Want(http.StatusOK).JSON(&resp)
	if resp.AccessScope != "project" || len(resp.ProjectIDs) != 2 {
		t.Fatalf("response = %+v", resp)
	}
	if scopeOf(t, userID) != "project" || len(grantsOf(t, userID)) != 2 {
		t.Fatal("scope or grants not stored")
	}

	// Sending only project_ids replaces the grants and keeps the scope.
	updateMember(t, memberID, map[string]any{"project_ids": []string{p2}}).Want(http.StatusOK)
	if g := grantsOf(t, userID); len(g) != 1 || !g[p2] {
		t.Fatalf("grants after replace = %v, want only %s", g, p2)
	}

	// Back to workspace-wide drops the grants.
	updateMember(t, memberID, map[string]any{"access_scope": "workspace"}).Want(http.StatusOK)
	if scopeOf(t, userID) != "workspace" || len(grantsOf(t, userID)) != 0 {
		t.Fatal("workspace scope should clear grants")
	}
}

func TestUpdateMember_RejectsInvalidGrants(t *testing.T) {
	_, memberID := adminFixture(t, "member")
	_, adminMemberID := adminFixture(t, "admin")
	foreignProject := uuid.NewString()
	p1 := dbfx.Project(t, "Valid "+memberID)

	cases := []struct {
		name     string
		memberID string
		body     map[string]any
	}{
		{"unknown project", memberID, map[string]any{"access_scope": "project", "project_ids": []string{foreignProject}}},
		{"malformed project id", memberID, map[string]any{"access_scope": "project", "project_ids": []string{"nope"}}},
		{"unknown scope", memberID, map[string]any{"access_scope": "team"}},
		{"project ids without project scope", memberID, map[string]any{"access_scope": "workspace", "project_ids": []string{p1}}},
		{"admin cannot be confined", adminMemberID, map[string]any{"access_scope": "project", "project_ids": []string{p1}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			updateMember(t, tc.memberID, tc.body).Want(http.StatusBadRequest)
		})
	}
}

// Promoting a confined member must not leave a stale confinement behind.
func TestUpdateMember_PromotionDropsConfinement(t *testing.T) {
	userID, memberID := adminFixture(t, "member")
	p1 := dbfx.Project(t, "Promote "+memberID)
	updateMember(t, memberID, map[string]any{"access_scope": "project", "project_ids": []string{p1}}).Want(http.StatusOK)

	updateMember(t, memberID, map[string]any{"role": "admin"}).Want(http.StatusOK)
	if scopeOf(t, userID) != "workspace" || len(grantsOf(t, userID)) != 0 {
		t.Fatalf("promotion kept scope=%s grants=%v", scopeOf(t, userID), grantsOf(t, userID))
	}
}

func TestListMembers_ReportsAccessScope(t *testing.T) {
	userID, memberID := adminFixture(t, "member")
	p1 := dbfx.Project(t, "List "+memberID)
	updateMember(t, memberID, map[string]any{"access_scope": "project", "project_ids": []string{p1}}).Want(http.StatusOK)

	var list []MemberWithUserResponse
	req := withURLParam(newRequest("GET", "/api/workspaces/"+testWorkspaceID+"/members", nil), "id", testWorkspaceID)
	testutil.Call(t, testHandler.ListMembersWithUser, req).Want(http.StatusOK).JSON(&list)
	for _, m := range list {
		if m.UserID == userID {
			if m.AccessScope != "project" || len(m.ProjectIDs) != 1 || m.ProjectIDs[0] != p1 {
				t.Fatalf("member row = %+v", m)
			}
			return
		}
	}
	t.Fatal("member missing from list")
}

func inviteScoped(t *testing.T, email string, body CreateMemberRequest) *testutil.Response {
	t.Helper()
	clearInvitationsForTestWorkspace(t)
	useInvitationRateLimiters(t, InvitationRateLimiters{
		Actor: &stubInvitationRateLimiter{allowed: true}, Workspace: &stubInvitationRateLimiter{allowed: true}, Recipient: &stubInvitationRateLimiter{allowed: true},
	})
	body.Email = email
	req := withURLParam(newRequest("POST", "/api/workspaces/"+testWorkspaceID+"/members", body), "id", testWorkspaceID)
	return testutil.Call(t, testHandler.CreateInvitation, req)
}

func TestInvitation_ScopeAppliesAtomicallyOnAccept(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	p1 := dbfx.Project(t, "Invite one "+uuid.NewString())
	email := "scoped-invite-" + uuid.NewString() + "@multica.ai"
	userID := dbfx.User(t, "Scoped Invitee", email)
	t.Cleanup(func() {
		_, _ = testPool.Exec(t.Context(), `DELETE FROM project_member WHERE user_id = $1`, parseUUID(userID))
		_, _ = testPool.Exec(t.Context(), `DELETE FROM member WHERE user_id = $1 AND workspace_id = $2`, parseUUID(userID), parseUUID(testWorkspaceID))
	})

	var inv InvitationResponse
	inviteScoped(t, email, CreateMemberRequest{Role: "member", AccessScope: "project", ProjectIDs: []string{p1}}).
		Want(http.StatusCreated).JSON(&inv)
	if inv.AccessScope != "project" || len(inv.ProjectIDs) != 1 {
		t.Fatalf("invitation = %+v", inv)
	}

	req := newRequest("POST", "/api/invitations/"+inv.ID+"/accept", nil)
	req.Header.Set("X-User-ID", userID)
	req = withURLParam(req, "id", inv.ID)
	var member MemberWithUserResponse
	testutil.Call(t, testHandler.AcceptInvitation, req).Want(http.StatusOK).JSON(&member)

	if member.AccessScope != "project" || len(member.ProjectIDs) != 1 || member.ProjectIDs[0] != p1 {
		t.Fatalf("accepted member = %+v", member)
	}
	if scopeOf(t, userID) != "project" || !grantsOf(t, userID)[p1] {
		t.Fatal("confinement not stored with the membership")
	}
}

func TestInvitation_ScopeRejectedForAdminAndUnknownProject(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	p1 := dbfx.Project(t, "Reject "+uuid.NewString())
	inviteScoped(t, "reject-admin-"+uuid.NewString()+"@multica.ai",
		CreateMemberRequest{Role: "admin", AccessScope: "project", ProjectIDs: []string{p1}}).Want(http.StatusBadRequest)
	inviteScoped(t, "reject-proj-"+uuid.NewString()+"@multica.ai",
		CreateMemberRequest{Role: "member", AccessScope: "project", ProjectIDs: []string{uuid.NewString()}}).Want(http.StatusBadRequest)
}

// A project deleted between invite and accept can only shrink the grant.
func TestInvitation_DeletedProjectNeverWidensAccess(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	keep := dbfx.Project(t, "Keep "+uuid.NewString())
	gone := dbfx.Project(t, "Gone "+uuid.NewString())
	email := "shrink-" + uuid.NewString() + "@multica.ai"
	userID := dbfx.User(t, "Shrink Invitee", email)
	t.Cleanup(func() {
		_, _ = testPool.Exec(t.Context(), `DELETE FROM project_member WHERE user_id = $1`, parseUUID(userID))
		_, _ = testPool.Exec(t.Context(), `DELETE FROM member WHERE user_id = $1 AND workspace_id = $2`, parseUUID(userID), parseUUID(testWorkspaceID))
	})
	var inv InvitationResponse
	inviteScoped(t, email, CreateMemberRequest{Role: "member", AccessScope: "project", ProjectIDs: []string{keep, gone}}).
		Want(http.StatusCreated).JSON(&inv)
	dbfx.Exec(t, `DELETE FROM project WHERE id = $1`, gone)

	req := newRequest("POST", "/api/invitations/"+inv.ID+"/accept", nil)
	req.Header.Set("X-User-ID", userID)
	req = withURLParam(req, "id", inv.ID)
	testutil.Call(t, testHandler.AcceptInvitation, req).Want(http.StatusOK)

	g := grantsOf(t, userID)
	if scopeOf(t, userID) != "project" || len(g) != 1 || !g[keep] {
		t.Fatalf("scope=%s grants=%v, want project/[%s]", scopeOf(t, userID), g, keep)
	}
}

func TestDeleteMember_RemovesProjectGrants(t *testing.T) {
	userID, memberID := adminFixture(t, "member")
	p1 := dbfx.Project(t, "Del member "+memberID)
	updateMember(t, memberID, map[string]any{"access_scope": "project", "project_ids": []string{p1}}).Want(http.StatusOK)

	req := newRequest("DELETE", "/api/workspaces/"+testWorkspaceID+"/members/"+memberID, nil)
	req = withURLParams(req, "id", testWorkspaceID, "memberId", memberID)
	testutil.Call(t, testHandler.DeleteMember, req).Want(http.StatusNoContent)
	if g := grantsOf(t, userID); len(g) != 0 {
		t.Fatalf("grants survived removal: %v", g)
	}
}

func TestDeleteProject_RemovesGrants(t *testing.T) {
	userID, memberID := adminFixture(t, "member")
	p1 := dbfx.Project(t, "Del project "+memberID)
	p2 := dbfx.Project(t, "Keep project "+memberID)
	updateMember(t, memberID, map[string]any{"access_scope": "project", "project_ids": []string{p1, p2}}).Want(http.StatusOK)

	req := newRequest("DELETE", "/api/projects/"+p1, nil)
	req = withURLParam(req, "id", p1)
	testutil.Call(t, testHandler.DeleteProject, req).Want(http.StatusNoContent)
	if g := grantsOf(t, userID); len(g) != 1 || !g[p2] {
		t.Fatalf("grants after project delete = %v, want only %s", g, p2)
	}
}
