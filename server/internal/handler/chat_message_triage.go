package handler

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// Per-message triage for chat replies: FLAGGING a reply ("I read this, I still
// owe an answer") independently from HANDLING it ("done with this").
//
// Why the read cursor is not enough: `chat_session.last_read_at` answers "has
// this been shown", one bit for a whole conversation. A reader who opens a
// conversation, reads three replies that each ask for something and gets
// pulled away has no way back to "which of these did I not answer" — all of
// them look read. So the deliberate decision is stored per message and per
// user, and is readable later from any device.
//
// Two independent decisions on chat_message_user_state:
//
//	flagged  — this reply needs my attention
//	handled  — I am done with this reply
//
// Neither set means "not decided yet". Keeping that third state addressable is
// what stops every read reply from rendering as an alarm — the failure mode of
// a single handled_at column.

// ChatMessageTriageResponse is the caller's own decision about one reply.
//
// `revision` is the user-state revision, deliberately separate from
// chat_message.revision (content edits): a client receiving a new user-state
// revision patches the marker only — it must not re-fetch or re-render the
// reply body.
type ChatMessageTriageResponse struct {
	ChatMessageID string  `json:"chat_message_id"`
	Flagged       bool    `json:"flagged"`
	Handled       bool    `json:"handled"`
	FlaggedAt     *string `json:"flagged_at,omitempty"`
	HandledAt     *string `json:"handled_at,omitempty"`
	Revision      int64   `json:"revision"`
}

// ChatMessageTriageListResponse carries every decision the caller has recorded
// in one session. Absent message ids mean "nothing decided" — the state of
// nearly every message, which therefore costs no row.
type ChatMessageTriageListResponse struct {
	States []ChatMessageTriageResponse `json:"states"`
	// OpenCount is the number of outstanding (flagged, not handled) replies in
	// the session, so a client renders the count without re-walking states.
	OpenCount int `json:"open_count"`
}

func chatMessageUserStateToResponse(state db.ChatMessageUserState) ChatMessageTriageResponse {
	return ChatMessageTriageResponse{
		ChatMessageID: util.UUIDToString(state.ChatMessageID),
		Flagged:       state.FlaggedAt.Valid,
		Handled:       state.HandledAt.Valid,
		FlaggedAt:     util.TimestampToPtr(state.FlaggedAt),
		HandledAt:     util.TimestampToPtr(state.HandledAt),
		Revision:      state.Revision,
	}
}

// loadTriageTarget resolves the session plus the specific reply being acted
// on, applying the same ownership / private-agent / public-session gates every
// other per-session chat read uses.
//
// The reply must BELONG to the session. Without that check the session id in
// the path is decorative: a member who can see one of their sessions could
// stamp state onto a reply in any other session by pasting its message id.
func (h *Handler) loadTriageTarget(
	w http.ResponseWriter,
	r *http.Request,
	userID, workspaceID, sessionID, messageID string,
) (db.ChatSession, db.ChatMessage, bool) {
	session, ok := h.gatePublicChatSessionForUser(w, r, userID, workspaceID, sessionID)
	if !ok {
		return db.ChatSession{}, db.ChatMessage{}, false
	}

	messageUUID, err := util.ParseUUID(messageID)
	if err != nil {
		writeError(w, http.StatusNotFound, "chat message not found")
		return db.ChatSession{}, db.ChatMessage{}, false
	}
	message, err := h.Queries.GetChatMessage(r.Context(), messageUUID)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "chat message not found")
		} else {
			writeError(w, http.StatusInternalServerError, "failed to load chat message")
		}
		return db.ChatSession{}, db.ChatMessage{}, false
	}
	if message.ChatSessionID != session.ID {
		writeError(w, http.StatusNotFound, "chat message not found")
		return db.ChatSession{}, db.ChatMessage{}, false
	}
	return session, message, true
}

// triageableReply enforces the one rule the product states in the UI: only
// assistant replies carry an obligation for the reader.
//
// The reader's own turns are never something they owe themselves, and a
// channel-originated inbound row is likewise not a pending reply. Refusing
// both also keeps "N outstanding" meaning "N replies waiting on you" — a
// self-flag would inflate a count that is read as a work queue.
func triageableReply(w http.ResponseWriter, message db.ChatMessage) bool {
	if message.Role == "assistant" {
		return true
	}
	writeErrorCode(w, http.StatusBadRequest, "triage_assistant_only", "only assistant replies can be marked")
	return false
}

// FlagChatMessage marks one reply as needing the caller's attention.
// Idempotent, so a double-tap from a slow client cannot error.
func (h *Handler) FlagChatMessage(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	workspaceID := ctxWorkspaceID(r.Context())
	sessionID := chi.URLParam(r, "sessionId")
	messageID := chi.URLParam(r, "messageId")

	session, message, ok := h.loadTriageTarget(w, r, userID, workspaceID, sessionID, messageID)
	if !ok {
		return
	}
	if !triageableReply(w, message) {
		return
	}

	state, err := h.Queries.FlagChatMessage(r.Context(), db.FlagChatMessageParams{
		ChatMessageID: message.ID,
		UserID:        parseUUID(userID),
		WorkspaceID:   session.WorkspaceID,
		ChatSessionID: session.ID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to flag chat message")
		return
	}

	resp := chatMessageUserStateToResponse(state)
	h.publishTriageChange(session, userID, resp)
	writeJSON(w, http.StatusOK, resp)
}

// UnflagChatMessage removes the caller's flag without claiming the reply is
// done. The row survives with both timestamps NULL: "looked at it, decided
// nothing yet" stays distinguishable from "never touched".
func (h *Handler) UnflagChatMessage(w http.ResponseWriter, r *http.Request) {
	h.clearTriage(w, r, func(ctx context.Context, message db.ChatMessage, userID string) error {
		return h.Queries.UnflagChatMessage(ctx, db.UnflagChatMessageParams{
			ChatMessageID: message.ID,
			UserID:        parseUUID(userID),
		})
	})
}

// HandleChatMessage marks one reply as dealt with, clearing any flag.
func (h *Handler) HandleChatMessage(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	workspaceID := ctxWorkspaceID(r.Context())
	sessionID := chi.URLParam(r, "sessionId")
	messageID := chi.URLParam(r, "messageId")

	session, message, ok := h.loadTriageTarget(w, r, userID, workspaceID, sessionID, messageID)
	if !ok {
		return
	}
	if !triageableReply(w, message) {
		return
	}

	state, err := h.Queries.HandleChatMessage(r.Context(), db.HandleChatMessageParams{
		ChatMessageID: message.ID,
		UserID:        parseUUID(userID),
		WorkspaceID:   session.WorkspaceID,
		ChatSessionID: session.ID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to mark chat message handled")
		return
	}

	resp := chatMessageUserStateToResponse(state)
	h.publishTriageChange(session, userID, resp)
	writeJSON(w, http.StatusOK, resp)
}

// UnhandleChatMessage reverses a "dealt with" decision. The reply returns to
// the undecided pool, not to the outstanding one: undoing a completion is not
// the same statement as asking for attention.
func (h *Handler) UnhandleChatMessage(w http.ResponseWriter, r *http.Request) {
	h.clearTriage(w, r, func(ctx context.Context, message db.ChatMessage, userID string) error {
		return h.Queries.UnhandleChatMessage(ctx, db.UnhandleChatMessageParams{
			ChatMessageID: message.ID,
			UserID:        parseUUID(userID),
		})
	})
}

// clearTriage is the shared body of the two "remove a decision" routes. Both
// are idempotent and both answer with the resulting state (possibly the
// all-false zero value), so a client replaces its cache without a follow-up
// read.
func (h *Handler) clearTriage(
	w http.ResponseWriter,
	r *http.Request,
	clear func(ctx context.Context, message db.ChatMessage, userID string) error,
) {
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	workspaceID := ctxWorkspaceID(r.Context())
	sessionID := chi.URLParam(r, "sessionId")
	messageID := chi.URLParam(r, "messageId")

	session, message, ok := h.loadTriageTarget(w, r, userID, workspaceID, sessionID, messageID)
	if !ok {
		return
	}
	if !triageableReply(w, message) {
		return
	}
	if err := clear(r.Context(), message, userID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to clear chat message triage")
		return
	}

	resp := ChatMessageTriageResponse{ChatMessageID: util.UUIDToString(message.ID)}
	if state, err := h.findTriageState(r.Context(), userID, session.ID, message.ID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load chat message triage")
		return
	} else if state != nil {
		resp = chatMessageUserStateToResponse(*state)
	}

	// A cleared decision is still a change other devices must see — the reply
	// drops out of their outstanding list. Publishing the zero-value state
	// (flagged=false, handled=false) is what carries that.
	h.publishTriageChange(session, userID, resp)
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) findTriageState(
	ctx context.Context,
	userID string,
	sessionID, messageID pgtype.UUID,
) (*db.ChatMessageUserState, error) {
	rows, err := h.Queries.ListChatMessageStatesBySession(ctx, db.ListChatMessageStatesBySessionParams{
		UserID:        parseUUID(userID),
		ChatSessionID: sessionID,
	})
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].ChatMessageID == messageID {
			return &rows[i], nil
		}
	}
	return nil, nil
}

// ListChatMessageTriage returns every decision the caller recorded in this
// session plus the outstanding count.
//
// Deliberately session-scoped rather than keyed to the ids on screen: the
// transcript is paged, and the client needs "N outstanding" even while the
// flagged reply itself is on a page it has not loaded yet.
func (h *Handler) ListChatMessageTriage(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	workspaceID := ctxWorkspaceID(r.Context())
	sessionID := chi.URLParam(r, "sessionId")

	session, ok := h.gatePublicChatSessionForUser(w, r, userID, workspaceID, sessionID)
	if !ok {
		return
	}

	rows, err := h.Queries.ListChatMessageStatesBySession(r.Context(), db.ListChatMessageStatesBySessionParams{
		UserID:        parseUUID(userID),
		ChatSessionID: session.ID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list chat message triage")
		return
	}
	openCount, err := h.Queries.CountOpenChatMessageStatesBySession(r.Context(), db.CountOpenChatMessageStatesBySessionParams{
		UserID:        parseUUID(userID),
		ChatSessionID: session.ID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to count open chat messages")
		return
	}

	states := make([]ChatMessageTriageResponse, 0, len(rows))
	for _, row := range rows {
		states = append(states, chatMessageUserStateToResponse(row))
	}
	writeJSON(w, http.StatusOK, ChatMessageTriageListResponse{States: states, OpenCount: int(openCount)})
}

// HandleAllOpenChatMessages deals with every outstanding reply in the session
// in one write — the triage bar's "I have read all of these" action.
func (h *Handler) HandleAllOpenChatMessages(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	workspaceID := ctxWorkspaceID(r.Context())
	sessionID := chi.URLParam(r, "sessionId")

	session, ok := h.gatePublicChatSessionForUser(w, r, userID, workspaceID, sessionID)
	if !ok {
		return
	}

	if err := h.Queries.HandleOpenChatMessagesInSession(r.Context(), db.HandleOpenChatMessagesInSessionParams{
		UserID:        parseUUID(userID),
		ChatSessionID: session.ID,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to handle open chat messages")
		return
	}

	// Bulk state has no single message to name, so the frame carries the
	// session and `bulk_cleared`; every receiver takes the same path (refresh
	// the session's triage list) instead of matching per-message ids.
	h.publishChat(protocol.EventChatMessageTriageChanged, util.UUIDToString(session.WorkspaceID), "member", userID, util.UUIDToString(session.ID), ChatMessageTriageChangedPayload{
		ChatSessionID: util.UUIDToString(session.ID),
		BulkCleared:   true,
	})

	w.WriteHeader(http.StatusNoContent)
}

// ChatMessageTriageChangedPayload is the realtime shape for a triage change.
//
// A zero ChatMessageID with BulkCleared set means "every outstanding reply in
// this session changed at once". Clients that only track counts can ignore the
// message id entirely.
type ChatMessageTriageChangedPayload struct {
	ChatSessionID string  `json:"chat_session_id"`
	ChatMessageID string  `json:"chat_message_id,omitempty"`
	Flagged       bool    `json:"flagged"`
	Handled       bool    `json:"handled"`
	FlaggedAt     *string `json:"flagged_at,omitempty"`
	HandledAt     *string `json:"handled_at,omitempty"`
	Revision      int64   `json:"revision,omitempty"`
	BulkCleared   bool    `json:"bulk_cleared,omitempty"`
}

// publishTriageChange broadcasts a decision to the caller's other devices.
//
// Owner-scoped by construction: publishChat stamps ChatSessionOwnerID, and the
// realtime layer only delivers chat events to the session's creator, so one
// member's flags never enter another member's stream. The state is theirs.
func (h *Handler) publishTriageChange(session db.ChatSession, userID string, state ChatMessageTriageResponse) {
	h.publishChat(protocol.EventChatMessageTriageChanged, util.UUIDToString(session.WorkspaceID), "member", userID, util.UUIDToString(session.ID), ChatMessageTriageChangedPayload{
		ChatSessionID: util.UUIDToString(session.ID),
		ChatMessageID: state.ChatMessageID,
		Flagged:       state.Flagged,
		Handled:       state.Handled,
		FlaggedAt:     state.FlaggedAt,
		HandledAt:     state.HandledAt,
		Revision:      state.Revision,
	})
}
