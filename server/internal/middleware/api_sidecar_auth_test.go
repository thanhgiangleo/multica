package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

const (
	testAPISidecarKey         = "mgs_0123456789abcdef0123456789abcdef"
	testAPISidecarUserID      = "dab7ff15-0909-407f-9b0d-d92076dfb1e9"
	testAPISidecarWorkspaceID = "e1db11a9-5cde-4e2d-91ab-5220c9b94740"
	testAPISidecarKeyID       = "0236fe64-b8c6-4101-88e0-454814f44084"
	testPluginInstallationID  = "b9520c77-df2b-45c5-ab88-68b108fc63a5"
)

func apiSidecarRequest(token string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/api/issues", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(apiSidecarUserHeader, testAPISidecarUserID)
	req.Header.Set(apiSidecarWorkspaceHeader, testAPISidecarWorkspaceID)
	req.Header.Set(apiSidecarKeyIDHeader, testAPISidecarKeyID)
	req.Header.Set(apiSidecarPluginInstallationHeader, testPluginInstallationID)
	return req
}

func TestAuthAcceptsAPISidecarIdentityAndBindsWorkspace(t *testing.T) {
	t.Setenv(apiSidecarKeyEnvironmentVariable, testAPISidecarKey)

	called := false
	handler := Auth(nil, nil, nil, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if got := r.Header.Get("X-User-ID"); got != testAPISidecarUserID {
			t.Fatalf("X-User-ID = %q", got)
		}
		if got := r.Header.Get("X-Workspace-ID"); got != testAPISidecarWorkspaceID {
			t.Fatalf("X-Workspace-ID = %q", got)
		}
		if got := r.Header.Get("X-Actor-Source"); got != apiSidecarActorSource {
			t.Fatalf("X-Actor-Source = %q", got)
		}
		if got := r.Header.Get(apiSidecarKeyIDHeader); got != testAPISidecarKeyID {
			t.Fatalf("%s = %q", apiSidecarKeyIDHeader, got)
		}
		if got := r.Header.Get(canonicalPluginInstallationHeader); got != testPluginInstallationID {
			t.Fatalf("%s = %q", canonicalPluginInstallationHeader, got)
		}
		if !isWorkspaceBoundActor(r) {
			t.Fatal("API sidecar identity must be workspace-bound")
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, apiSidecarRequest(testAPISidecarKey))
	if !called || recorder.Code != http.StatusNoContent {
		t.Fatalf("called=%v status=%d body=%s", called, recorder.Code, recorder.Body.String())
	}
}

func TestAuthRejectsInvalidAPISidecarCredential(t *testing.T) {
	t.Setenv(apiSidecarKeyEnvironmentVariable, testAPISidecarKey)
	called := false
	handler := Auth(nil, nil, nil, nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, apiSidecarRequest("mgs_wrongwrongwrongwrongwrongwrongwrong"))
	if called || recorder.Code != http.StatusUnauthorized {
		t.Fatalf("called=%v status=%d", called, recorder.Code)
	}
}

func TestPluginBearerOnlyAcceptsSidecarAndCanRequireIt(t *testing.T) {
	t.Setenv(apiSidecarKeyEnvironmentVariable, testAPISidecarKey)
	t.Setenv(apiSidecarRequiredEnvironmentVariable, "true")
	called := false
	handler := PluginBearerOnly(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, apiSidecarRequest(testAPISidecarKey))
	if !called {
		t.Fatalf("valid sidecar request rejected: status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	called = false
	recorder = httptest.NewRecorder()
	direct := httptest.NewRequest(http.MethodGet, "/v1/context", nil)
	direct.Header.Set("Authorization", "Bearer mpi_direct_plugin_token")
	handler.ServeHTTP(recorder, direct)
	if called || recorder.Code != http.StatusUnauthorized {
		t.Fatalf("direct plugin token called=%v status=%d", called, recorder.Code)
	}
}

func TestPluginBearerOnlyStripsForgedSidecarIdentity(t *testing.T) {
	t.Setenv(apiSidecarRequiredEnvironmentVariable, "false")
	handler := PluginBearerOnly(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, name := range []string{
			apiSidecarUserHeader,
			apiSidecarWorkspaceHeader,
			apiSidecarKeyIDHeader,
			apiSidecarPluginInstallationHeader,
			canonicalPluginInstallationHeader,
			"X-Actor-Source",
			"X-User-ID",
			"X-Workspace-ID",
		} {
			if got := r.Header.Get(name); got != "" {
				t.Fatalf("forged %s reached handler: %q", name, got)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req := apiSidecarRequest("mpi_direct_plugin_token")
	req.Header.Set("X-Actor-Source", apiSidecarActorSource)
	req.Header.Set("X-User-ID", testAPISidecarUserID)
	req.Header.Set("X-Workspace-ID", testAPISidecarWorkspaceID)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

// An account-level sidecar read carries no workspace. It must authenticate and
// must NOT be workspace-bound, or every workspace lookup would resolve to "".
func TestAuthAllowsAccountLevelSidecarReadWithoutWorkspace(t *testing.T) {
	t.Setenv(apiSidecarKeyEnvironmentVariable, testAPISidecarKey)
	called := false
	handler := Auth(nil, nil, nil, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if got := r.Header.Get("X-Workspace-ID"); got != "" {
			t.Fatalf("X-Workspace-ID = %q, want empty", got)
		}
		if isWorkspaceBoundActor(r) {
			t.Fatal("account-level sidecar read must not be workspace-bound")
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/meta/routes", nil)
	req.Header.Set("Authorization", "Bearer "+testAPISidecarKey)
	req.Header.Set(apiSidecarUserHeader, testAPISidecarUserID)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if !called || recorder.Code != http.StatusNoContent {
		t.Fatalf("called=%v status=%d body=%s", called, recorder.Code, recorder.Body.String())
	}
}

// The sidecar identity is only as good as the service credential that carries
// it: no credential at all must not authenticate, whatever the headers claim.
func TestAuthRejectsSidecarIdentityWithoutServiceCredential(t *testing.T) {
	t.Setenv(apiSidecarKeyEnvironmentVariable, testAPISidecarKey)
	called := false
	handler := Auth(nil, nil, nil, nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/issues", nil)
	req.Header.Set(apiSidecarUserHeader, testAPISidecarUserID)
	req.Header.Set(apiSidecarWorkspaceHeader, testAPISidecarWorkspaceID)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if called || recorder.Code != http.StatusUnauthorized {
		t.Fatalf("called=%v status=%d", called, recorder.Code)
	}
}
