package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/multica-ai/multica/server/internal/integrations/taskchannel"
	"github.com/multica-ai/multica/server/pkg/plugincontract"
	publicapiv1 "github.com/multica-ai/multica/server/pkg/publicapi/v1"
)

type registerTaskChannelRequest struct {
	AgentID     string `json:"agent_id"`
	Provider    string `json:"provider"`
	AccountID   string `json:"account_id"`
	DisplayName string `json:"display_name"`
}

// ListTaskChannelInstallations is the provider-neutral discovery endpoint for
// the workspace Integrations directory. It intentionally returns no provider
// credential and does not make Zalo (or any future transport) part of core.
func (h *Handler) ListTaskChannelInstallations(w http.ResponseWriter, r *http.Request) {
	if h.TaskChannel == nil {
		writeJSON(w, http.StatusOK, map[string]any{"installations": []taskchannel.Installation{}})
		return
	}
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace id")
	if !ok {
		return
	}
	installations, err := h.TaskChannel.ListByWorkspace(r.Context(), workspaceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list task-channel installations")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"installations": installations})
}

func requirePluginActor(w http.ResponseWriter, r *http.Request, actor pluginActor) bool {
	if !actor.isMember() {
		return true
	}
	publicapiv1.WriteProblem(w, r, http.StatusForbidden, "plugin_token_required", "this endpoint requires the Plugin install token")
	return false
}

func (h *Handler) RegisterTaskChannel(w http.ResponseWriter, r *http.Request) {
	caller, actor, ok := h.pluginCaller(w, r, plugincontract.ScopeTasksWrite)
	if !ok || !requirePluginActor(w, r, actor) {
		return
	}
	if h.TaskChannel == nil {
		publicapiv1.WriteProblem(w, r, http.StatusServiceUnavailable, "task_channel_unavailable", "task-channel extensions are unavailable")
		return
	}
	var request registerTaskChannelRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		publicapiv1.WriteProblem(w, r, http.StatusBadRequest, "invalid_request", "invalid request body")
		return
	}
	agentID, ok := parseUUIDOrBadRequest(w, strings.TrimSpace(request.AgentID), "agent_id")
	if !ok {
		return
	}
	installation, err := h.TaskChannel.Register(r.Context(), taskchannel.RegisterParams{
		PluginInstallationID: caller.Installation.ID, WorkspaceID: caller.WorkspaceID,
		AgentID: agentID, InstallerUserID: caller.Installation.InstalledBy,
		Provider: request.Provider, AccountID: request.AccountID, DisplayName: request.DisplayName,
	})
	if err != nil {
		switch {
		case errors.Is(err, taskchannel.ErrInstallationNotFound):
			publicapiv1.WriteProblem(w, r, http.StatusNotFound, "agent_not_found", "agent not found in this Plugin workspace")
		case errors.Is(err, taskchannel.ErrInstallationConflict):
			publicapiv1.WriteProblem(w, r, http.StatusConflict, "installation_conflict", err.Error())
		default:
			publicapiv1.WriteProblem(w, r, http.StatusBadRequest, "invalid_request", err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, installation)
}

func (h *Handler) IngestTaskChannelMessage(w http.ResponseWriter, r *http.Request) {
	caller, actor, ok := h.pluginCaller(w, r, plugincontract.ScopeTasksWrite)
	if !ok || !requirePluginActor(w, r, actor) {
		return
	}
	if h.TaskChannel == nil {
		publicapiv1.WriteProblem(w, r, http.StatusServiceUnavailable, "task_channel_unavailable", "task-channel extensions are unavailable")
		return
	}
	var request taskchannel.InboundRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		publicapiv1.WriteProblem(w, r, http.StatusBadRequest, "invalid_request", "invalid request body")
		return
	}
	if err := h.TaskChannel.Ingest(r.Context(), caller.Installation.ID, request); err != nil {
		publicapiv1.WriteProblem(w, r, http.StatusBadRequest, "message_rejected", err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) ClaimTaskChannelOutbox(w http.ResponseWriter, r *http.Request) {
	caller, actor, ok := h.pluginCaller(w, r, plugincontract.ScopeTasksRead)
	if !ok || !requirePluginActor(w, r, actor) {
		return
	}
	if h.TaskChannel == nil {
		publicapiv1.WriteProblem(w, r, http.StatusServiceUnavailable, "task_channel_unavailable", "task-channel extensions are unavailable")
		return
	}
	var request struct {
		Limit int32 `json:"limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		publicapiv1.WriteProblem(w, r, http.StatusBadRequest, "invalid_request", "invalid request body")
		return
	}
	messages, err := h.TaskChannel.ClaimOutbox(r.Context(), caller.Installation.ID, request.Limit)
	if err != nil {
		publicapiv1.WriteProblem(w, r, http.StatusInternalServerError, "outbox_unavailable", "failed to claim task-channel messages")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": messages})
}

type taskChannelClaimRequest struct {
	ClaimToken string `json:"claim_token"`
	Error      string `json:"error,omitempty"`
}

func (h *Handler) AckTaskChannelOutbox(w http.ResponseWriter, r *http.Request) {
	h.finishTaskChannelOutbox(w, r, true)
}

func (h *Handler) RetryTaskChannelOutbox(w http.ResponseWriter, r *http.Request) {
	h.finishTaskChannelOutbox(w, r, false)
}

func (h *Handler) finishTaskChannelOutbox(w http.ResponseWriter, r *http.Request, ack bool) {
	caller, actor, ok := h.pluginCaller(w, r, plugincontract.ScopeTasksRead)
	if !ok || !requirePluginActor(w, r, actor) {
		return
	}
	if h.TaskChannel == nil {
		publicapiv1.WriteProblem(w, r, http.StatusServiceUnavailable, "task_channel_unavailable", "task-channel extensions are unavailable")
		return
	}
	id, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "outbox_id"), "outbox_id")
	if !ok {
		return
	}
	var request taskChannelClaimRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		publicapiv1.WriteProblem(w, r, http.StatusBadRequest, "invalid_request", "invalid request body")
		return
	}
	claimToken, ok := parseUUIDOrBadRequest(w, strings.TrimSpace(request.ClaimToken), "claim_token")
	if !ok {
		return
	}
	var err error
	if ack {
		err = h.TaskChannel.AckOutbox(r.Context(), caller.Installation.ID, id, claimToken)
	} else {
		err = h.TaskChannel.RetryOutbox(r.Context(), caller.Installation.ID, id, claimToken, request.Error)
	}
	if errors.Is(err, taskchannel.ErrOutboxClaimLost) {
		publicapiv1.WriteProblem(w, r, http.StatusConflict, "claim_lost", "this outbox claim is no longer active")
		return
	}
	if err != nil {
		publicapiv1.WriteProblem(w, r, http.StatusInternalServerError, "outbox_unavailable", "failed to update the task-channel message")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) RedeemTaskChannelBindingToken(w http.ResponseWriter, r *http.Request) {
	if h.TaskChannel == nil {
		writeError(w, http.StatusServiceUnavailable, "task-channel extensions are unavailable")
		return
	}
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	var request struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil || strings.TrimSpace(request.Token) == "" {
		writeError(w, http.StatusBadRequest, "token is required")
		return
	}
	userUUID, ok := parseUUIDOrBadRequest(w, userID, "user id")
	if !ok {
		return
	}
	redeemed, err := h.TaskChannel.BindingTokens().RedeemAndBind(r.Context(), request.Token, userUUID)
	if err != nil {
		switch {
		case errors.Is(err, taskchannel.ErrBindingTokenInvalid):
			writeError(w, http.StatusGone, "binding token invalid or expired")
		case errors.Is(err, taskchannel.ErrBindingAlreadyAssigned):
			writeError(w, http.StatusConflict, "this external account is already bound to another Multica user")
		case errors.Is(err, taskchannel.ErrBindingNotWorkspaceMember):
			writeError(w, http.StatusForbidden, "binding refused because you are not a workspace member")
		default:
			writeError(w, http.StatusInternalServerError, "failed to redeem task-channel binding token")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"workspace_id": uuidToString(redeemed.WorkspaceID), "installation_id": uuidToString(redeemed.InstallationID),
		"channel_user_id": redeemed.ChannelUserID,
	})
}
