package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// insertTriageTestMessage writes one chat_message into a session and returns
// its id. Role decides whether the triage routes will accept it.
func insertTriageTestMessage(t *testing.T, sessionID, role, content string) string {
	t.Helper()
	msg, err := testHandler.Queries.CreateChatMessage(context.Background(), db.CreateChatMessageParams{
		ChatSessionID: parseUUID(sessionID),
		Role:          role,
		Content:       content,
	})
	if err != nil {
		t.Fatalf("create chat message: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM chat_message WHERE id = $1`, msg.ID)
	})
	return uuidToString(msg.ID)
}

func triageRequest(method, sessionID, messageID, suffix string) *http.Request {
	target := "/api/chat/sessions/" + sessionID
	if messageID == "" {
		target += "/message-triage" + suffix
	} else {
		target += "/messages/" + messageID + "/triage" + suffix
	}
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("X-User-ID", testUserID)
	req.Header.Set("X-Workspace-ID", testWorkspaceID)
	req = withURLParam(req, "sessionId", sessionID)
	if messageID != "" {
		req = withURLParam(req, "messageId", messageID)
	}
	return req
}

// TestChatMessageTriage_FlagIsSeparateFromHandled is the core behavioural
// contract: flagging ("I read it, I still owe an answer") and handling ("done
// with it") are independent stored decisions, and clearing one must not
// fabricate the other.
func TestChatMessageTriage_FlagIsSeparateFromHandled(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	agentID := createHandlerTestAgent(t, "TriageAgent", []byte("[]"))
	sessionID := createHandlerTestChatSession(t, agentID)
	messageID := insertTriageTestMessage(t, sessionID, "assistant", "can you check the logs?")

	// Flag.
	w := httptest.NewRecorder()
	testHandler.FlagChatMessage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodPost, sessionID, messageID, "/flag")))
	if w.Code != http.StatusOK {
		t.Fatalf("flag: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var flagged ChatMessageTriageResponse
	if err := json.Unmarshal(w.Body.Bytes(), &flagged); err != nil {
		t.Fatalf("decode flag response: %v", err)
	}
	if !flagged.Flagged || flagged.Handled {
		t.Fatalf("after flag = %+v, want flagged and not handled", flagged)
	}

	// The list reports it as outstanding.
	w = httptest.NewRecorder()
	testHandler.ListChatMessageTriage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodGet, sessionID, "", "")))
	if w.Code != http.StatusOK {
		t.Fatalf("list: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var list ChatMessageTriageListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if list.OpenCount != 1 || len(list.States) != 1 || list.States[0].ChatMessageID != messageID {
		t.Fatalf("list after flag = %+v, want exactly this message open", list)
	}

	// Handling it discharges the obligation — it must not leave the flag set,
	// or the reply would count as both outstanding and done.
	w = httptest.NewRecorder()
	testHandler.HandleChatMessage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodPost, sessionID, messageID, "/handle")))
	if w.Code != http.StatusOK {
		t.Fatalf("handle: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var handled ChatMessageTriageResponse
	if err := json.Unmarshal(w.Body.Bytes(), &handled); err != nil {
		t.Fatalf("decode handle response: %v", err)
	}
	if handled.Flagged || !handled.Handled {
		t.Fatalf("after handle = %+v, want handled and not flagged", handled)
	}

	w = httptest.NewRecorder()
	testHandler.ListChatMessageTriage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodGet, sessionID, "", "")))
	var afterHandle ChatMessageTriageListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &afterHandle); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if afterHandle.OpenCount != 0 {
		t.Fatalf("open count after handle = %d, want 0", afterHandle.OpenCount)
	}
}

// TestChatMessageTriage_UnflagKeepsRowAsUndecided covers the state that the
// two-column design exists for: clearing a flag returns the reply to
// "undecided", and that has to stay distinguishable from "never touched" so a
// later handled_at does not read as if the reader had acted on it twice.
func TestChatMessageTriage_UnflagKeepsRowAsUndecided(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	agentID := createHandlerTestAgent(t, "TriageUnflagAgent", []byte("[]"))
	sessionID := createHandlerTestChatSession(t, agentID)
	messageID := insertTriageTestMessage(t, sessionID, "assistant", "one more thing")

	w := httptest.NewRecorder()
	testHandler.FlagChatMessage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodPost, sessionID, messageID, "/flag")))
	if w.Code != http.StatusOK {
		t.Fatalf("flag: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	testHandler.UnflagChatMessage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodDelete, sessionID, messageID, "/flag")))
	if w.Code != http.StatusOK {
		t.Fatalf("unflag: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var cleared ChatMessageTriageResponse
	if err := json.Unmarshal(w.Body.Bytes(), &cleared); err != nil {
		t.Fatalf("decode unflag response: %v", err)
	}
	if cleared.Flagged || cleared.Handled {
		t.Fatalf("after unflag = %+v, want neither decision", cleared)
	}

	w = httptest.NewRecorder()
	testHandler.ListChatMessageTriage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodGet, sessionID, "", "")))
	var list ChatMessageTriageListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if list.OpenCount != 0 {
		t.Fatalf("open count after unflag = %d, want 0", list.OpenCount)
	}
	if len(list.States) != 1 || list.States[0].ChatMessageID != messageID {
		t.Fatalf("states after unflag = %+v, want the single undecided row retained", list.States)
	}
}

// TestChatMessageTriage_RejectsOwnMessage pins the product rule the UI relies
// on: only assistant replies carry an obligation, so a reader cannot inflate
// their own outstanding count by flagging their own turn.
func TestChatMessageTriage_RejectsOwnMessage(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	agentID := createHandlerTestAgent(t, "TriageOwnMessageAgent", []byte("[]"))
	sessionID := createHandlerTestChatSession(t, agentID)
	messageID := insertTriageTestMessage(t, sessionID, "user", "my own prompt")

	w := httptest.NewRecorder()
	testHandler.FlagChatMessage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodPost, sessionID, messageID, "/flag")))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("flag own message: expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestChatMessageTriage_RejectsMessageFromAnotherSession blocks the id-pasting
// bypass: the path's session id must be the message's actual session, or the
// route would let a member stamp state onto any reply whose id they know.
func TestChatMessageTriage_RejectsMessageFromAnotherSession(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	agentID := createHandlerTestAgent(t, "TriageCrossSessionAgent", []byte("[]"))
	mineSessionID := createHandlerTestChatSession(t, agentID)
	otherSessionID := createHandlerTestChatSession(t, agentID)
	foreignMessageID := insertTriageTestMessage(t, otherSessionID, "assistant", "not in my session")

	w := httptest.NewRecorder()
	testHandler.FlagChatMessage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodPost, mineSessionID, foreignMessageID, "/flag")))
	if w.Code != http.StatusNotFound {
		t.Fatalf("cross-session flag: expected 404, got %d: %s", w.Code, w.Body.String())
	}

	var count int
	if err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM chat_message_user_state WHERE chat_message_id = $1`,
		parseUUID(foreignMessageID),
	).Scan(&count); err != nil {
		t.Fatalf("count states: %v", err)
	}
	if count != 0 {
		t.Fatalf("cross-session flag wrote %d state rows, want 0", count)
	}
}

// TestChatMessageTriage_HandleAllClearsOnlyOpenReply pins the bulk action's
// scope: it discharges outstanding replies and leaves an already-handled row
// alone, so "mark all read" cannot silently rewrite history.
func TestChatMessageTriage_HandleAllClearsOnlyOpenReply(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	agentID := createHandlerTestAgent(t, "TriageHandleAllAgent", []byte("[]"))
	sessionID := createHandlerTestChatSession(t, agentID)
	openMessageID := insertTriageTestMessage(t, sessionID, "assistant", "waiting on me")
	doneMessageID := insertTriageTestMessage(t, sessionID, "assistant", "already dealt with")

	w := httptest.NewRecorder()
	testHandler.FlagChatMessage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodPost, sessionID, openMessageID, "/flag")))
	if w.Code != http.StatusOK {
		t.Fatalf("flag: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	testHandler.HandleChatMessage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodPost, sessionID, doneMessageID, "/handle")))
	if w.Code != http.StatusOK {
		t.Fatalf("handle: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var handledBefore pgtype.Timestamptz
	if err := testPool.QueryRow(context.Background(),
		`SELECT handled_at FROM chat_message_user_state WHERE chat_message_id = $1`,
		parseUUID(doneMessageID),
	).Scan(&handledBefore); err != nil {
		t.Fatalf("read handled_at: %v", err)
	}

	w = httptest.NewRecorder()
	testHandler.HandleAllOpenChatMessages(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodPost, sessionID, "", "/handle-all")))
	if w.Code != http.StatusNoContent {
		t.Fatalf("handle all: expected 204, got %d: %s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	testHandler.ListChatMessageTriage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodGet, sessionID, "", "")))
	var list ChatMessageTriageListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if list.OpenCount != 0 {
		t.Fatalf("open count after handle-all = %d, want 0", list.OpenCount)
	}

	var handledAfter pgtype.Timestamptz
	if err := testPool.QueryRow(context.Background(),
		`SELECT handled_at FROM chat_message_user_state WHERE chat_message_id = $1`,
		parseUUID(doneMessageID),
	).Scan(&handledAfter); err != nil {
		t.Fatalf("read handled_at: %v", err)
	}
	if !handledAfter.Time.Equal(handledBefore.Time) {
		t.Fatalf("handle-all rewrote an already-handled timestamp: before %s after %s",
			handledBefore.Time, handledAfter.Time)
	}
}

// TestChatMessageTriage_RejectsMalformedMessageID keeps an unparseable id from
// reaching the query layer, where it would surface as a 500.
func TestChatMessageTriage_RejectsMalformedMessageID(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	agentID := createHandlerTestAgent(t, "TriageBadIDAgent", []byte("[]"))
	sessionID := createHandlerTestChatSession(t, agentID)

	w := httptest.NewRecorder()
	testHandler.FlagChatMessage(w, withChatTestWorkspaceCtx(t, triageRequest(
		http.MethodPost, sessionID, "not-a-uuid", "/flag")))
	if w.Code != http.StatusNotFound {
		t.Fatalf("malformed message id: expected 404, got %d: %s", w.Code, w.Body.String())
	}
}