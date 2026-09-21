package main

import (
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/analytics"
	"github.com/multica-ai/multica/server/internal/apicatalog"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/realtime"
)

func TestAPICatalogCoversAssembledAppRouter(t *testing.T) {
	router := NewRouter(nil, realtime.NewHub(), events.New(), analytics.NoopClient{}, nil)
	routes, err := apicatalog.List(router)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) < 450 {
		t.Fatalf("catalog contains only %d routes; expected the full backend surface", len(routes))
	}

	want := map[string]bool{
		http.MethodPost + " /auth/verify-code":             false,
		http.MethodGet + " /api/meta/routes":               false,
		http.MethodGet + " /api/issues":                    false,
		http.MethodPost + " /api/issues":                   false,
		http.MethodGet + " /api/workspaces/{id}/members":   false,
		http.MethodGet + " /api/chat/sessions/{sessionId}": false,
		http.MethodGet + " /api/agents/{id}/tasks":         false,
		http.MethodGet + " /healthz":                       false,
		http.MethodGet + " /uploads/*":                     false,
		http.MethodGet + " /v1/context":                    false,
		http.MethodGet + " /ws":                            false,
	}
	for _, route := range routes {
		key := route.Method + " " + route.Path
		if _, ok := want[key]; ok {
			want[key] = true
		}
	}
	for route, found := range want {
		if !found {
			t.Errorf("catalog missing %s", route)
		}
	}
	t.Logf("catalog routes: %d", len(routes))
}
