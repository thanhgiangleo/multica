package apicatalog

import (
	"net/http"
	"slices"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestListUsesEveryRegisteredBackendRoutePattern(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/healthz", func(http.ResponseWriter, *http.Request) {})
	r.Post("/auth/verify-code", func(http.ResponseWriter, *http.Request) {})
	r.Get("/api/issues/{id}", func(http.ResponseWriter, *http.Request) {})
	r.Post("/api/issues", func(http.ResponseWriter, *http.Request) {})
	r.Get("/api/daemon/ws", func(http.ResponseWriter, *http.Request) {})
	r.Get("/uploads/*", func(http.ResponseWriter, *http.Request) {})
	r.Get("/v1/context", func(http.ResponseWriter, *http.Request) {})
	r.Get("/ws", func(http.ResponseWriter, *http.Request) {})
	r.Route("/api/workspaces/{workspaceID}", func(r chi.Router) {
		r.Get("/members", func(http.ResponseWriter, *http.Request) {})
	})

	routes, err := List(r)
	if err != nil {
		t.Fatal(err)
	}
	want := []Route{
		{Method: http.MethodGet, Path: "/api/daemon/ws", Transport: "websocket"},
		{Method: http.MethodPost, Path: "/api/issues", Transport: "http"},
		{Method: http.MethodGet, Path: "/api/issues/{id}", Transport: "http"},
		{Method: http.MethodGet, Path: "/api/workspaces/{workspaceID}/members", Transport: "http"},
		{Method: http.MethodPost, Path: "/auth/verify-code", Transport: "http"},
		{Method: http.MethodGet, Path: "/healthz", Transport: "http"},
		{Method: http.MethodGet, Path: "/uploads/*", Transport: "http"},
		{Method: http.MethodGet, Path: "/v1/context", Transport: "http"},
		{Method: http.MethodGet, Path: "/ws", Transport: "websocket"},
	}
	if !slices.Equal(routes, want) {
		t.Fatalf("routes = %#v, want %#v", routes, want)
	}
}
