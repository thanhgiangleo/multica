package handler

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
)

// chatSearchHiddenMessageKinds are message kinds that never render in a chat
// window, so matching them would surface transcript text the user cannot open
// in place. channel_command rows are channel slash-command inputs (filtered by
// ListChatMessages' SQL), and onboarding_kickoff rows are the hidden
// server-authored first turn that visibleChatMessages drops before returning a
// transcript. Chat search must exclude both in SQL — filtering after the scan
// would spend the LIMIT budget on rows the caller can never see.
var chatSearchHiddenMessageKinds = []string{
	"channel_command",
	// protocol.ChatMessageKindOnboardingKickoff — kept as a literal to avoid a
	// second import cycle-prone dependency in this file.
	"onboarding_kickoff",
}

// SearchChatMessageResponse is one matching transcript message plus the session
// context needed to jump straight to it from the palette: without session_id
// and session_title the caller cannot tell which of their chats the hit came
// from, which is the whole point of searching across all of them.
type SearchChatMessageResponse struct {
	MessageID     string `json:"message_id"`
	SessionID     string `json:"session_id"`
	SessionTitle  string `json:"session_title"`
	SessionStatus string `json:"session_status"`
	AgentID       string `json:"agent_id"`
	Role          string `json:"role"`
	MessageKind   string `json:"message_kind"`
	Snippet       string `json:"snippet"`
	MatchSource   string `json:"match_source"`
	CreatedAt     string `json:"created_at"`
}

// chatSearchResult is the scan target for one matching message.
type chatSearchResult struct {
	messageID     pgtype.UUID
	chatSessionID pgtype.UUID
	sessionTitle  string
	sessionStatus string
	agentID       pgtype.UUID
	role          string
	messageKind   string
	content       string
	createdAt     pgtype.Timestamptz
}

// buildChatSearchQuery assembles the cross-chat search statement. Like
// buildSearchQuery it lowercases the search text in Go so only the column side
// needs LOWER(), and returns the args slice so the caller can fill the
// workspace/creator placeholders it deliberately leaves nil.
//
// Parameter order matches the returned SQL:
//
//	$1 workspace_id, $2 creator_id, $3 agent_ids::uuid[], $4 hidden_kinds::text[],
//	$5 phrase pattern, $6.. extra term patterns (only when terms > 1),
//	$n-1 limit, $n offset.
//
// Visible sessions mirror ListChatSessionsByCreator: the caller's own sessions
// in this workspace whose agent they may still access, plus the same
// "explicitly created OR has a visible message" rule that hides empty implicit
// sessions. The agent allowlist is enforced here rather than dropped because a
// member whose role was downgraded must not be able to read transcripts of a
// private agent they previously had access to — the same hole ListChatSessions
// guards in its ACL comment.
func buildChatSearchQuery(phrase string, terms []string) (string, []any) {
	args := []any{}
	nextArg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}

	// Allocate the prepared placeholders first so the caller can fill them by
	// index (args[0..3]) regardless of where they appear in the statement.
	wsParam := nextArg(nil)          // $1, filled by caller
	creatorParam := nextArg(nil)     // $2, filled by caller
	agentIDsParam := nextArg(nil)    // $3, filled by caller
	hiddenKindsParam := nextArg(nil) // $4, filled by caller
	escapedPhrase := escapeLike(strings.ToLower(phrase))
	phraseParam := nextArg("%" + escapedPhrase + "%") // $5

	var contentFilters []string
	contentFilters = append(contentFilters, "LOWER(m.content) LIKE "+phraseParam)
	if len(terms) > 1 {
		for _, term := range terms {
			termParam := nextArg("%" + escapeLike(strings.ToLower(term)) + "%")
			contentFilters = append(contentFilters, "LOWER(m.content) LIKE "+termParam)
		}
	}

	limitParam := nextArg(nil)
	offsetParam := nextArg(nil)

	sql := `
WITH visible_sessions AS (
  SELECT cs.id, cs.title, cs.status, cs.agent_id
  FROM chat_session AS cs
  WHERE cs.workspace_id = ` + wsParam + `
    AND cs.creator_id = ` + creatorParam + `
    AND cs.agent_id = ANY(` + agentIDsParam + `::uuid[])
    AND (
      cs.explicitly_created_at IS NOT NULL
      OR EXISTS (
        SELECT 1 FROM chat_message AS seed
        WHERE seed.chat_session_id = cs.id
          AND seed.message_kind <> ALL(` + hiddenKindsParam + `::text[])
      )
    )
)
SELECT m.id, m.chat_session_id, s.title, s.status, s.agent_id,
       m.role, m.message_kind, m.content, m.created_at
FROM chat_message AS m
JOIN visible_sessions AS s ON s.id = m.chat_session_id
WHERE m.message_kind <> ALL(` + hiddenKindsParam + `::text[])
  AND ` + strings.Join(contentFilters, "\n  AND ") + `
ORDER BY m.created_at DESC, m.id DESC
LIMIT ` + limitParam + ` OFFSET ` + offsetParam

	return sql, args
}

// SearchChatMessages searches transcript content across every chat session the
// caller created in this workspace and may still access. GET /api/chat/search.
//
// Query params mirror /api/issues/search: q (required), limit (default 20, cap
// 50), offset (>= 0). Response is the wrapped {"messages": [...]} shape used by
// the other search endpoints; ListChatMessages returns a bare array because it
// reads one session, but a search result needs its session context per hit.
func (h *Handler) SearchChatMessages(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	workspaceID := ctxWorkspaceID(ctx)

	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		writeError(w, http.StatusBadRequest, "q parameter is required")
		return
	}

	limit := 20
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}
	if limit > 50 {
		limit = 50
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}

	wsUUID, ok := parseUUIDOrBadRequest(w, workspaceID, "workspace_id")
	if !ok {
		return
	}
	creatorUUID, ok := parseUUIDOrBadRequest(w, userID, "user_id")
	if !ok {
		return
	}

	// Same ACL shape as ListChatSessions: resolve the agent set the actor may
	// currently read before touching transcripts.
	member, ok := h.workspaceMember(w, r, workspaceID)
	if !ok {
		return
	}
	actorType, actorID := h.resolveActor(r, userID, workspaceID)
	allowed, ok := h.accessibleAgentIDs(ctx, workspaceID, actorType, actorID, member.Role)
	if !ok {
		writeError(w, http.StatusInternalServerError, "failed to resolve agent access")
		return
	}
	if len(allowed) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"messages": []SearchChatMessageResponse{}})
		return
	}
	agentIDs := make([]pgtype.UUID, 0, len(allowed))
	for id := range allowed {
		parsed, err := util.ParseUUID(id)
		if err != nil {
			slog.Warn("skip unparseable agent id in chat search allowlist", "agent_id", id, "error", err)
			continue
		}
		agentIDs = append(agentIDs, parsed)
	}
	if len(agentIDs) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"messages": []SearchChatMessageResponse{}})
		return
	}

	terms := splitSearchTerms(q)
	sqlQuery, args := buildChatSearchQuery(q, terms)
	// $1..$4 are the prepared placeholders; the last two are limit and offset.
	args[0] = wsUUID
	args[1] = creatorUUID
	args[2] = agentIDs
	args[3] = chatSearchHiddenMessageKinds
	args[len(args)-2] = limit
	args[len(args)-1] = offset

	var results []chatSearchResult
	err := runSearchQuery(ctx, h.TxStarter, sqlQuery, args, func(rows pgx.Rows) error {
		for rows.Next() {
			var sr chatSearchResult
			if err := rows.Scan(
				&sr.messageID,
				&sr.chatSessionID,
				&sr.sessionTitle,
				&sr.sessionStatus,
				&sr.agentID,
				&sr.role,
				&sr.messageKind,
				&sr.content,
				&sr.createdAt,
			); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			results = append(results, sr)
		}
		return rows.Err()
	})
	if err != nil {
		if writeSearchCancellation(w, r, err) {
			return
		}
		// Statement-timeout surfaces as SQLSTATE 57014; return 503 so the client
		// can ask the user to refine the query instead of showing a generic
		// failure. Same ladder as SearchIssues.
		if isSearchStatementTimeout(err) {
			slog.Warn("search chat messages timed out",
				"workspace_id", workspaceID,
				"query", q,
				"timeout", searchStatementTimeout)
			writeError(w, http.StatusServiceUnavailable, "search timed out; please refine your query or try again")
			return
		}
		slog.Warn("search chat messages failed", "error", err, "workspace_id", workspaceID, "query", q)
		writeError(w, http.StatusInternalServerError, "failed to search chat messages")
		return
	}
	if writeSearchCancellation(w, r, ctx.Err()) {
		return
	}

	resp := make([]SearchChatMessageResponse, len(results))
	for i, sr := range results {
		resp[i] = SearchChatMessageResponse{
			MessageID:     uuidToString(sr.messageID),
			SessionID:     uuidToString(sr.chatSessionID),
			SessionTitle:  sr.sessionTitle,
			SessionStatus: sr.sessionStatus,
			AgentID:       uuidToString(sr.agentID),
			Role:          sr.role,
			MessageKind:   normalizeMessageKind(sr.messageKind),
			Snippet:       extractSnippet(sr.content, q),
			MatchSource:   "message",
			CreatedAt:     timestampToString(sr.createdAt),
		}
	}

	if writeSearchCancellation(w, r, ctx.Err()) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"messages": resp,
	})
}
