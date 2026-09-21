package handler

import (
	"net/http/httptest"
	"testing"
)

func TestRequirePluginActorAcceptsAuthenticatedSidecarMember(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest("POST", "/v1/task-channel/messages", nil)
	req.Header.Set("X-Actor-Source", "api_sidecar")
	response := httptest.NewRecorder()

	if !requirePluginActor(response, req, pluginActor{Type: "member"}) {
		t.Fatal("authenticated sidecar member was rejected")
	}
	if response.Code != 200 {
		t.Fatalf("status = %d, want untouched recorder", response.Code)
	}
}

func TestRequirePluginActorRejectsOrdinaryMember(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest("POST", "/v1/task-channel/messages", nil)
	response := httptest.NewRecorder()

	if requirePluginActor(response, req, pluginActor{Type: "member"}) {
		t.Fatal("ordinary member reached plugin-only endpoint")
	}
	if response.Code != 403 {
		t.Fatalf("status = %d, want 403", response.Code)
	}
}

func TestRequirePluginActorAcceptsPluginCredential(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest("POST", "/v1/task-channel/messages", nil)
	response := httptest.NewRecorder()

	if !requirePluginActor(response, req, pluginActor{Type: "plugin"}) {
		t.Fatal("plugin credential was rejected")
	}
}
