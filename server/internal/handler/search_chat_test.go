package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBuildChatSearchQuery_SinglePhrasePlaceholders(t *testing.T) {
	sql, args := buildChatSearchQuery("Rollout 50%", []string{"Rollout 50%"})

	// $1..$4 are the caller-filled placeholders, $5 the phrase pattern, and the
	// last two are limit/offset. A single-term query allocates exactly 7 args.
	if len(args) != 7 {
		t.Fatalf("expected 7 args for single-term query, got %d: %#v", len(args), args)
	}
	for i := 0; i < 4; i++ {
		if args[i] != nil {
			t.Fatalf("arg %d should be a nil placeholder, got %#v", i, args[i])
		}
	}
	// escapeLike escapes % and _ so a literal query cannot become a wildcard.
	if args[4] != "%rollout 50\\%%" {
		t.Fatalf("phrase pattern = %#v", args[4])
	}
	if args[5] != nil || args[6] != nil {
		t.Fatalf("limit/offset should be nil placeholders: %#v", args[5:])
	}

	for _, want := range []string{
		"cs.workspace_id = $1",
		"cs.creator_id = $2",
		"cs.agent_id = ANY($3::uuid[])",
		"message_kind <> ALL($4::text[])",
		"LOWER(m.content) LIKE $5",
		"LIMIT $6 OFFSET $7",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("sql missing %q:\n%s", want, sql)
		}
	}
	// A single-term query must not emit a redundant per-term predicate.
	if strings.Count(sql, "LOWER(m.content) LIKE") != 1 {
		t.Fatalf("expected exactly one content predicate:\n%s", sql)
	}
}

func TestBuildChatSearchQuery_MultiTermAddsConjunctivePredicates(t *testing.T) {
	sql, args := buildChatSearchQuery("deploy rollback", []string{"deploy", "rollback"})

	// 4 placeholders + phrase + 2 terms + limit/offset.
	if len(args) != 9 {
		t.Fatalf("expected 9 args, got %d: %#v", len(args), args)
	}
	if args[4] != "%deploy rollback%" {
		t.Fatalf("phrase pattern = %#v", args[4])
	}
	if args[5] != "%deploy%" || args[6] != "%rollback%" {
		t.Fatalf("term patterns = %#v", args[5:7])
	}
	if strings.Count(sql, "LOWER(m.content) LIKE") != 3 {
		t.Fatalf("expected phrase + one predicate per term:\n%s", sql)
	}
	if !strings.Contains(sql, "LIMIT $8 OFFSET $9") {
		t.Fatalf("limit/offset placeholders wrong:\n%s", sql)
	}
}

func TestBuildChatSearchQuery_ExcludesHiddenMessageKinds(t *testing.T) {
	sql, _ := buildChatSearchQuery("anything", []string{"anything"})

	// The $4 hidden-kinds array is applied twice: once to decide whether an
	// implicit session is non-empty, and once to filter the messages returned.
	// Leaving either out leaks rows visibleChatMessages would have dropped.
	if got := strings.Count(sql, "message_kind <> ALL($4::text[])"); got != 2 {
		t.Fatalf("hidden-kind predicate applied %d times, want 2:\n%s", got, sql)
	}
	if !strings.Contains(strings.Join(chatSearchHiddenMessageKinds, ","), "channel_command") ||
		!strings.Contains(strings.Join(chatSearchHiddenMessageKinds, ","), "onboarding_kickoff") {
		t.Fatalf("hidden kinds = %#v", chatSearchHiddenMessageKinds)
	}
}

func TestSearchChatMessages_RequiresQuery(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/chat/search", nil)
	req.Header.Set("X-User-ID", testUserID)
	w := httptest.NewRecorder()
	testHandler.SearchChatMessages(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without q, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSearchChatMessages_FindsAcrossSessionsAndSkipsHiddenKinds(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("handler test harness not initialised (DATABASE_URL unset)")
	}
	agentID := createHandlerTestAgent(t, "ChatSearchAgent", []byte("[]"))
	olderSession := createHandlerTestChatSession(t, agentID)
	newerSession := createHandlerTestChatSession(t, agentID)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

	// A token unique to this test so unrelated rows in the shared test DB
	// cannot widen the result set.
	const token = "zqchatsearchprobe"

	insertChatMessageRole(t, olderSession, "user", "the "+token+" rollout plan", "message", false, base)
	insertChatMessageRole(t, olderSession, "user", "/issue "+token+" hidden command", "channel_command", false, base.Add(time.Second))
	insertChatMessageRole(t, newerSession, "assistant", token+" is ready", "message", false, base.Add(2*time.Second))
	insertChatMessageRole(t, newerSession, "assistant", "hidden "+token+" kickoff", "onboarding_kickoff", false, base.Add(3*time.Second))

	req := httptest.NewRequest(http.MethodGet, "/api/chat/search?q="+token, nil)
	req.Header.Set("X-User-ID", testUserID)
	req = withChatTestWorkspaceCtx(t, req)
	w := httptest.NewRecorder()
	testHandler.SearchChatMessages(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var body struct {
		Messages []SearchChatMessageResponse `json:"messages"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	// Two visible hits, newest first. The channel_command and
	// onboarding_kickoff rows must never appear.
	if len(body.Messages) != 2 {
		t.Fatalf("expected 2 visible matches, got %d: %#v", len(body.Messages), body.Messages)
	}
	if body.Messages[0].SessionID != newerSession || body.Messages[0].Role != "assistant" {
		t.Fatalf("first hit should be the newest visible message: %#v", body.Messages[0])
	}
	if body.Messages[1].SessionID != olderSession {
		t.Fatalf("second hit should come from the older session: %#v", body.Messages[1])
	}
	for _, m := range body.Messages {
		if !strings.Contains(strings.ToLower(m.Snippet), token) {
			t.Fatalf("snippet should contain the match: %#v", m)
		}
		if m.SessionTitle == "" || m.MatchSource != "message" {
			t.Fatalf("hit missing session context: %#v", m)
		}
	}
}
