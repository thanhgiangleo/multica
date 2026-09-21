package middleware

import (
	"crypto/subtle"
	"net/http"
	"os"
	"strings"

	"github.com/multica-ai/multica/server/internal/util"
)

const (
	apiSidecarTokenPrefix                 = "mgs_"
	apiSidecarUserHeader                  = "X-Multica-Gateway-User-ID"
	apiSidecarWorkspaceHeader             = "X-Multica-Gateway-Workspace-ID"
	apiSidecarKeyIDHeader                 = "X-Multica-Gateway-Key-ID"
	apiSidecarPluginInstallationHeader    = "X-Multica-Gateway-Plugin-Installation-ID"
	canonicalPluginInstallationHeader     = "X-Multica-Plugin-Installation"
	apiSidecarActorSource                 = "api_sidecar"
	apiSidecarKeyEnvironmentVariable      = "MULTICA_API_SIDECAR_KEY"
	apiSidecarRequiredEnvironmentVariable = "MULTICA_API_SIDECAR_REQUIRED"
)

type apiSidecarIdentity struct {
	userID               string
	workspaceID          string
	keyID                string
	pluginInstallationID string
}

// tryAPISidecarAuth handles the single service credential used by the API
// sidecar — the only credential that talks to this backend on behalf of
// external clients. A client's own workspace API key never reaches here: the
// sidecar resolves it and asserts the resulting identity in headers, and the
// ordinary workspace middleware still verifies that the named user is a
// current member of the named workspace. Revoking the membership therefore
// revokes the API key's reach immediately.
//
// The workspace assertion is optional so the sidecar can make its own
// account-level reads (the route catalog, the workspace directory). When it is
// present the request is pinned to that workspace and no slug, query parameter,
// or URL parameter can move it.
//
// Gateway identity headers are always stripped first. A request authenticated
// by a JWT, PAT, task token, or plugin token must never be able to smuggle one
// into a downstream handler.
func tryAPISidecarAuth(w http.ResponseWriter, r *http.Request, next http.Handler) bool {
	identity := apiSidecarIdentity{
		userID:               strings.TrimSpace(r.Header.Get(apiSidecarUserHeader)),
		workspaceID:          strings.TrimSpace(r.Header.Get(apiSidecarWorkspaceHeader)),
		keyID:                strings.TrimSpace(r.Header.Get(apiSidecarKeyIDHeader)),
		pluginInstallationID: strings.TrimSpace(r.Header.Get(apiSidecarPluginInstallationHeader)),
	}
	stripAPISidecarHeaders(r)

	token := BearerToken(r)
	if !strings.HasPrefix(token, apiSidecarTokenPrefix) {
		return false
	}

	expected := strings.TrimSpace(os.Getenv(apiSidecarKeyEnvironmentVariable))
	if len(expected) < 32 || len(expected) != len(token) || subtle.ConstantTimeCompare([]byte(expected), []byte(token)) != 1 {
		writeError(w, http.StatusUnauthorized, "invalid API sidecar credential")
		return true
	}
	if !validUUID(identity.userID) {
		writeError(w, http.StatusUnauthorized, "invalid API sidecar identity")
		return true
	}
	if identity.workspaceID != "" && !validUUID(identity.workspaceID) {
		writeError(w, http.StatusUnauthorized, "invalid API sidecar workspace identity")
		return true
	}
	if identity.pluginInstallationID != "" && !validUUID(identity.pluginInstallationID) {
		writeError(w, http.StatusUnauthorized, "invalid API sidecar plugin identity")
		return true
	}

	r.Header.Set("X-User-ID", identity.userID)
	r.Header.Set("X-Actor-Source", apiSidecarActorSource)
	if identity.workspaceID != "" {
		r.Header.Set("X-Workspace-ID", identity.workspaceID)
	}
	if identity.keyID != "" {
		r.Header.Set(apiSidecarKeyIDHeader, identity.keyID)
	}
	if identity.pluginInstallationID != "" {
		r.Header.Set(canonicalPluginInstallationHeader, identity.pluginInstallationID)
	}
	next.ServeHTTP(w, r)
	return true
}

func stripAPISidecarHeaders(r *http.Request) {
	r.Header.Del(apiSidecarUserHeader)
	r.Header.Del(apiSidecarWorkspaceHeader)
	r.Header.Del(apiSidecarKeyIDHeader)
	r.Header.Del(apiSidecarPluginInstallationHeader)
	r.Header.Del(canonicalPluginInstallationHeader)
	// PluginBearerOnly does not pass through the general Auth middleware, so
	// strip its ordinary identity headers here as well. A direct plugin token
	// must never be able to impersonate the sidecar actor source.
	r.Header.Del("X-Actor-Source")
	r.Header.Del("X-User-ID")
	r.Header.Del("X-Workspace-ID")
}

func validUUID(value string) bool {
	_, err := util.ParseUUID(value)
	return err == nil
}

func apiSidecarRequired() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(apiSidecarRequiredEnvironmentVariable))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// isWorkspaceBoundActor reports whether the authenticated credential carries a
// server-set workspace binding that the client cannot renegotiate. Task tokens
// carry it from the token row; sidecar requests carry it from the workspace API
// key the sidecar resolved. In both cases any other workspace identifier on the
// request is the caller trying to widen its reach.
//
// A sidecar request without a workspace assertion is an account-level read and
// is deliberately not bound: binding it would resolve every workspace to the
// empty string and fail closed on routes that have no workspace at all.
func isWorkspaceBoundActor(r *http.Request) bool {
	switch r.Header.Get("X-Actor-Source") {
	case "task_token":
		return true
	case apiSidecarActorSource:
		return r.Header.Get("X-Workspace-ID") != ""
	default:
		return false
	}
}
