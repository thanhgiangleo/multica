package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func restrictedClient(hub *Hub, userID, workspaceID string, projectIDs ...string) *Client {
	c := &Client{hub: hub, send: make(chan []byte, 8), userID: userID, workspaceID: workspaceID, restricted: true, projectIDs: projectIDs}
	hub.registerClient(c)
	return c
}

func plainClient(hub *Hub, userID, workspaceID string) *Client {
	c := &Client{hub: hub, send: make(chan []byte, 8), userID: userID, workspaceID: workspaceID}
	hub.registerClient(c)
	return c
}

func received(c *Client) bool {
	select {
	case <-c.send:
		return true
	default:
		return false
	}
}

// A project-scoped member must never sit in the workspace room, which carries
// every project's events.
func TestRestrictedClientSkipsWorkspaceRoom(t *testing.T) {
	hub := NewHub()
	r := restrictedClient(hub, "r", "ws", "p1")
	p := plainClient(hub, "p", "ws")

	hub.BroadcastToWorkspace("ws", []byte(`{"type":"issue:updated"}`))
	if received(r) {
		t.Fatal("restricted client received a workspace broadcast")
	}
	if !received(p) {
		t.Fatal("plain client missed a workspace broadcast")
	}
}

func TestRestrictedClientJoinsOnlyGrantedProjectsAndShared(t *testing.T) {
	hub := NewHub()
	r := restrictedClient(hub, "r", "ws", "p1")

	hub.BroadcastToScope(ScopeProject, "p1", []byte(`{"type":"a"}`))
	if !received(r) {
		t.Fatal("granted project not delivered")
	}
	hub.BroadcastToScope(ScopeProject, "p2", []byte(`{"type":"b"}`))
	if received(r) {
		t.Fatal("ungranted project delivered")
	}
	hub.BroadcastToScope(ScopeWorkspaceShared, "ws", []byte(`{"type":"c"}`))
	if !received(r) {
		t.Fatal("shared room not delivered")
	}
	hub.BroadcastToScope(ScopeWorkspaceShared, "other-ws", []byte(`{"type":"d"}`))
	if received(r) {
		t.Fatal("another workspace's shared room delivered")
	}
}

// subscribe frames cannot widen a restricted connection.
func TestRestrictedClientCannotSubscribeToWorkspaceOrProject(t *testing.T) {
	hub := NewHub()
	r := restrictedClient(hub, "r", "ws", "p1")

	r.handleSubscribe(ScopeWorkspace, "ws")
	assertSubscribeError(t, r, "forbidden")
	r.handleSubscribe(ScopeProject, "p2")
	assertSubscribeError(t, r, "unknown_scope")
	r.handleSubscribe(ScopeWorkspaceShared, "ws")
	assertSubscribeError(t, r, "unknown_scope")

	hub.BroadcastToWorkspace("ws", []byte(`{"type":"x"}`))
	hub.BroadcastToScope(ScopeProject, "p2", []byte(`{"type":"x"}`))
	if received(r) {
		t.Fatal("subscribe frame widened a restricted connection")
	}
}

func assertSubscribeError(t *testing.T, c *Client, want string) {
	t.Helper()
	select {
	case raw := <-c.send:
		var frame struct {
			Type    string            `json:"type"`
			Payload map[string]string `json:"payload"`
		}
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatal(err)
		}
		if frame.Type != "subscribe_error" || frame.Payload["error"] != want {
			t.Fatalf("frame = %s, want subscribe_error/%s", raw, want)
		}
	case <-time.After(time.Second):
		t.Fatal("no subscribe_error")
	}
}

func TestAccessChangedResetsOnlyThatMemberAndIsNotForwarded(t *testing.T) {
	for _, scope := range []string{ScopeWorkspace, ScopeWorkspaceShared} {
		hub := NewHub()
		target := restrictedClient(hub, "r", "ws", "p1")
		targetPlain := plainClient(hub, "r", "ws")
		bystander := restrictedClient(hub, "other", "ws", "p1")
		elsewhere := plainClient(hub, "r", "ws-2")
		peer := plainClient(hub, "p", "ws")

		frame := []byte(`{"type":"member:access_changed","payload":{"workspace_id":"ws","user_id":"r"}}`)
		hub.BroadcastToScopeDedup(scope, "ws", frame, "")

		if !target.stopped.Load() || !targetPlain.stopped.Load() {
			t.Fatalf("%s: member's sockets not reset", scope)
		}
		if bystander.stopped.Load() || elsewhere.stopped.Load() || peer.stopped.Load() {
			t.Fatalf("%s: unrelated sockets were reset", scope)
		}
		for name, c := range map[string]*Client{"bystander": bystander, "peer": peer, "elsewhere": elsewhere} {
			if received(c) {
				t.Fatalf("%s: control frame forwarded to %s", scope, name)
			}
		}
	}
}

func TestRestrictedMemberRemovalNoticeReachesTargetOnly(t *testing.T) {
	hub := NewHub()
	target := restrictedClient(hub, "r", "ws", "p1")
	bystander := restrictedClient(hub, "other", "ws", "p1")
	hub.membership = fakeChecker{}

	frame := []byte(`{"type":"member:removed","payload":{"workspace_id":"ws","user_id":"r","member_id":"m"}}`)
	hub.BroadcastToScopeDedup(ScopeWorkspaceShared, "ws", frame, "")

	deadline := time.Now().Add(2 * time.Second)
	for !target.stopped.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !target.stopped.Load() {
		t.Fatal("removed restricted member kept their socket")
	}
	if bystander.stopped.Load() {
		t.Fatal("bystander socket closed")
	}
}

type fakeChecker struct{}

func (fakeChecker) IsMember(_ context.Context, _, _ string) (bool, error) { return false, nil }
func (fakeChecker) Members(_ context.Context, _ []MembershipIdentity) (map[MembershipIdentity]bool, error) {
	return map[MembershipIdentity]bool{}, nil
}

type fakeAccessChecker struct {
	fakeChecker
	restricted bool
	projects   []string
	err        error
}

func (f fakeAccessChecker) Access(context.Context, string, string) (bool, []string, error) {
	return f.restricted, f.projects, f.err
}

func TestResolveAccessFailsClosed(t *testing.T) {
	if r, ids, err := resolveAccess(fakeChecker{}, "u", "ws"); r || ids != nil || err != nil {
		t.Fatalf("checker without AccessResolver = %v %v %v, want unrestricted", r, ids, err)
	}
	if _, _, err := resolveAccess(fakeAccessChecker{err: errors.New("db down")}, "u", "ws"); err == nil {
		t.Fatal("lookup failure must surface so the handshake rejects the socket")
	}
	if r, ids, err := resolveAccess(fakeAccessChecker{restricted: true, projects: []string{"p"}}, "u", "ws"); !r || len(ids) != 1 || err != nil {
		t.Fatalf("restricted resolve = %v %v %v", r, ids, err)
	}
}
