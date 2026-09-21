// Package apicatalog exposes the registered app API routes as a small,
// machine-readable catalog. It deliberately reports router patterns rather
// than handler internals so an approval proxy can preserve Multica's method
// and path contract without duplicating the route list.
package apicatalog

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"
)

// Route is one gateway-compatible Multica HTTP API operation.
// Paths are Chi route patterns, so callers request a capability such as
// GET /api/issues/{id}, then call the same concrete path through the gateway.
type Route struct {
	Method    string `json:"method"`
	Path      string `json:"path"`
	Transport string `json:"transport"`
}

// Handler serves the live catalog. Walking at request time makes the response
// include routes registered after the handler itself was constructed.
func Handler(root chi.Routes) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		routes, err := List(root)
		if err != nil {
			http.Error(w, "failed to enumerate API routes", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]any{"routes": routes})
	}
}

// List returns every HTTP and WebSocket route currently registered on root.
// The router is source of truth: new backend routes appear in the gateway
// catalog without a second manually maintained allowlist.
func List(root chi.Routes) ([]Route, error) {
	seen := make(map[string]Route)
	if err := chi.Walk(root, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		path := canonicalPath(route)
		if method == "" || method == "*" || path == "" {
			return nil
		}
		transport := "http"
		if method == http.MethodGet && strings.HasSuffix(path, "/ws") {
			transport = "websocket"
		}
		key := method + " " + path
		seen[key] = Route{Method: method, Path: path, Transport: transport}
		return nil
	}); err != nil {
		return nil, err
	}

	routes := make([]Route, 0, len(seen))
	for _, route := range seen {
		routes = append(routes, route)
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Path == routes[j].Path {
			return routes[i].Method < routes[j].Method
		}
		return routes[i].Path < routes[j].Path
	})
	return routes, nil
}

func canonicalPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || path == "/" {
		return path
	}
	return strings.TrimRight(path, "/")
}
