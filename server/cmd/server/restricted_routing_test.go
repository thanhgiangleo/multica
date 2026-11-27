package main

import (
	"context"
	"encoding/json"
	"sort"
	"testing"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/realtime"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func resolverFrom(m map[string]string) issueProjectResolver {
	return func(_ context.Context, _, issueID string) (string, bool) {
		id, ok := m[issueID]
		return id, ok
	}
}

func routedScopes(fb *fakeBroadcaster) []string {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	var out []string
	for _, c := range fb.scopeCalls {
		out = append(out, c.scopeType+"/"+c.scopeID)
	}
	sort.Strings(out)
	return out
}

func publishRouted(t *testing.T, e events.Event, issueProjects map[string]string) *fakeBroadcaster {
	t.Helper()
	bus := events.New()
	fb := &fakeBroadcaster{}
	registerListeners(bus, fb, withIssueProjectResolver(resolverFrom(issueProjects)))
	bus.Publish(e)
	return fb
}

func TestRestrictedRouting(t *testing.T) {
	cases := []struct {
		name  string
		event events.Event
		want  []string
	}{
		{
			name: "issue snapshot goes to its project",
			event: events.Event{Type: protocol.EventIssueUpdated, WorkspaceID: "ws", ActorType: "member", ActorID: "u",
				Payload: map[string]any{"issue": map[string]any{"id": "i1", "project_id": "p1"}}},
			want: []string{"project/p1"},
		},
		{
			name: "issue without a project reaches no project room",
			event: events.Event{Type: protocol.EventIssueCreated, WorkspaceID: "ws",
				Payload: map[string]any{"issue": map[string]any{"id": "i1", "project_id": nil}}},
			want: nil,
		},
		{
			name: "comment resolves its issue's project",
			event: events.Event{Type: protocol.EventCommentCreated, WorkspaceID: "ws",
				Payload: map[string]any{"comment": map[string]any{"id": "c", "issue_id": "i2"}}},
			want: []string{"project/p2"},
		},
		{
			name: "activity names the issue",
			event: events.Event{Type: protocol.EventActivityCreated, WorkspaceID: "ws",
				Payload: map[string]any{"issue_id": "i2"}},
			want: []string{"project/p2"},
		},
		{
			name: "issue task lifecycle goes to the issue's project",
			event: events.Event{Type: protocol.EventTaskQueued, WorkspaceID: "ws", TaskID: "t",
				Payload: map[string]any{"task_id": "t", "issue_id": "i2"}},
			want: []string{"project/p2"},
		},
		{
			name: "chat task transcript never reaches project rooms",
			event: events.Event{Type: protocol.EventTaskMessage, WorkspaceID: "ws", TaskID: "t", ChatSessionID: "c", ChatSessionOwnerID: "u",
				Payload: map[string]any{"task_id": "t", "issue_id": "i2"}},
			want: []string{"workspace_member/ws:u"},
		},
		{
			name: "project event carries its id",
			event: events.Event{Type: protocol.EventProjectUpdated, WorkspaceID: "ws",
				Payload: map[string]any{"project": map[string]any{"id": "p9"}}},
			want: []string{"project/p9"},
		},
		{
			name: "catalog change goes to the shared room",
			event: events.Event{Type: protocol.EventIssueStatusChanged, WorkspaceID: "ws",
				Payload: map[string]any{"action": "created"}},
			want: []string{"workspace_shared/ws"},
		},
		{
			name: "agent, squad, pin and inbox-like events reach restricted members through nothing",
			event: events.Event{Type: protocol.EventAgentStatus, WorkspaceID: "ws",
				Payload: map[string]any{"agent": map[string]any{"id": "a"}}},
			want: nil,
		},
		{
			name:  "squad event reaches no restricted room",
			event: events.Event{Type: protocol.EventSquadUpdated, WorkspaceID: "ws", Payload: map[string]any{}},
			want:  nil,
		},
	}
	issueProjects := map[string]string{"i2": "p2"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fb := publishRouted(t, tc.event, issueProjects)
			got := routedScopes(fb)
			if len(got) != len(tc.want) {
				t.Fatalf("scopes = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("scopes = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// Moving an issue out of a project tells that project's members it is gone,
// without leaking where it went or the issue's new fields.
func TestRestrictedRoutingIssueLeavesProject(t *testing.T) {
	p1 := "p1"
	fb := publishRouted(t, events.Event{
		Type: protocol.EventIssueUpdated, WorkspaceID: "ws", ActorType: "member", ActorID: "u",
		Payload: map[string]any{
			"issue":           map[string]any{"id": "i1", "project_id": "p2", "title": "secret"},
			"prev_project_id": &p1,
		},
	}, nil)

	fb.mu.Lock()
	defer fb.mu.Unlock()
	var toOld []byte
	for _, c := range fb.scopeCalls {
		if c.scopeType == realtime.ScopeProject && c.scopeID == "p1" {
			toOld = c.msg
		}
	}
	if toOld == nil {
		t.Fatalf("old project not told: %+v", fb.scopeCalls)
	}
	var frame struct {
		Type    string         `json:"type"`
		Payload map[string]any `json:"payload"`
	}
	if err := json.Unmarshal(toOld, &frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type != protocol.EventIssueDeleted || frame.Payload["issue_id"] != "i1" {
		t.Fatalf("old project got %s, want issue:deleted for i1", toOld)
	}
	if string(toOld) != "" && (contains(toOld, "secret") || contains(toOld, "p2")) {
		t.Fatalf("leave frame leaks the destination: %s", toOld)
	}
}

// prev_project_id exists for the router only.
func TestPrevProjectIDNeverReachesClients(t *testing.T) {
	p := "p1"
	out := projectOutbound(protocol.EventIssueUpdated, map[string]any{"issue": 1, "prev_project_id": &p, "prev_title": "t"})
	m := out.(map[string]any)
	if _, ok := m["prev_project_id"]; ok {
		t.Fatal("prev_project_id leaked to the outbound frame")
	}
}

func TestAccessChangedIsControlOnly(t *testing.T) {
	bus := events.New()
	fb := &fakeBroadcaster{}
	registerListeners(bus, fb)
	bus.Publish(events.Event{Type: protocol.EventMemberAccessChanged, WorkspaceID: "ws",
		Payload: map[string]any{"workspace_id": "ws", "user_id": "u"}})

	got := routedScopes(fb)
	want := []string{"workspace/ws", "workspace_shared/ws"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("scopes = %v, want %v", got, want)
	}
	if len(fb.workspaceCalls) != 0 {
		t.Fatal("control frame took the generic workspace fanout")
	}
}

func contains(b []byte, s string) bool { return len(s) > 0 && indexOf(string(b), s) >= 0 }
func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}
