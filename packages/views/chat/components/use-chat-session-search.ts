"use client";

import { useEffect, useState } from "react";
import { api } from "@multica/core/api";
import type { ChatSession } from "@multica/core/types";

// The endpoint caps `limit` at 50 and pages per matching MESSAGE, not per
// session — one chatty session can spend a whole page on its own. A filter has
// to be complete to be trustworthy, so we walk pages until a short one comes
// back, bounded by MAX_PAGES so a pathological query cannot stall the list.
const PAGE_SIZE = 50;
const MAX_PAGES = 5;
const DEBOUNCE_MS = 300;

interface SearchState {
  /** The trimmed query these ids belong to; guards against stale writes. */
  query: string;
  sessionIds: Set<string>;
  /** The page cap was hit with the last page still full — possibly more hits. */
  truncated: boolean;
  /** The request chain stopped early; `sessionIds` holds whatever was read. */
  failed: boolean;
}

const IDLE: SearchState = {
  query: "",
  sessionIds: new Set(),
  truncated: false,
  failed: false,
};

export interface ChatSessionSearch {
  /** Sessions whose transcript matched, or null when the current query has no
   *  settled result yet (in flight, failed, or no query at all). */
  sessionIds: ReadonlySet<string> | null;
  isLoading: boolean;
  failed: boolean;
  truncated: boolean;
}

/**
 * Transcript search behind the chat-list filter.
 *
 * A chat session carries only its `last_message`, so "this conversation
 * contains <query>" is not answerable from the sessions cache — it has to ask
 * the server, which scans every message the caller may read. That is exactly
 * what `/api/chat/search` does, so the filter reuses it and keeps the
 * `session_id` set rather than the message rows.
 *
 * Degradation is deliberate and visible: a rejected request leaves
 * `sessionIds` empty and sets `failed`, so the caller filters on what it can
 * see locally (title / last message) and says so — never a silent
 * "everything matches".
 */
export function useChatSessionSearch(query: string): ChatSessionSearch {
  const q = query.trim();
  const [state, setState] = useState<SearchState>(IDLE);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!q) {
      setState(IDLE);
      setIsLoading(false);
      return;
    }
    setIsLoading(true);
    const controller = new AbortController();
    const timer = setTimeout(async () => {
      const sessionIds = new Set<string>();
      let truncated = false;
      let failed = false;
      try {
        for (let page = 0; page < MAX_PAGES; page += 1) {
          const res = await api.searchChatMessages({
            q,
            limit: PAGE_SIZE,
            offset: page * PAGE_SIZE,
            signal: controller.signal,
          });
          if (controller.signal.aborted) return;
          for (const hit of res.messages) sessionIds.add(hit.session_id);
          if (res.messages.length < PAGE_SIZE) break;
          if (page === MAX_PAGES - 1) truncated = true;
        }
      } catch {
        // Aborted requests are superseded queries, not failures. Anything else
        // (offline, 503 timeout, lost access) keeps the pages already read.
        if (controller.signal.aborted) return;
        failed = true;
      }
      if (controller.signal.aborted) return;
      setState({ query: q, sessionIds, truncated, failed });
      setIsLoading(false);
    }, DEBOUNCE_MS);

    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [q]);

  // Results only describe the query they were fetched for. While a new query is
  // in flight the list falls back to local matching instead of filtering by the
  // previous query's sessions.
  const settled = state.query === q;
  return {
    sessionIds: q !== "" && settled ? state.sessionIds : null,
    isLoading: isLoading || (q !== "" && !settled),
    failed: settled && state.failed,
    truncated: settled && state.truncated,
  };
}

/**
 * Fold a string to its unaccented lowercase form so a filter can ignore
 * Vietnamese tone marks and case in BOTH directions: "khong" finds "không" and
 * "Không" finds "khong". NFD splits precomposed letters into base + combining
 * marks, which are then stripped; `đ` has no decomposition and is mapped by
 * hand.
 */
export function foldForSearch(input: string): string {
  return input
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/đ/g, "d");
}

/**
 * Apply the filter to the sessions cache. A session matches when the server
 * found the query somewhere in its transcript, or — for instant feedback
 * before the request settles, and for sessions the search cannot see — when
 * its title or last message contains it.
 */
export function filterSessionsByQuery(
  sessions: ChatSession[],
  query: string,
  remoteSessionIds: ReadonlySet<string> | null,
): ChatSession[] {
  const needle = foldForSearch(query.trim());
  if (!needle) return sessions;
  return sessions.filter((session) => {
    if (remoteSessionIds?.has(session.id)) return true;
    if (foldForSearch(session.title?.trim() ?? "").includes(needle)) return true;
    const last = session.last_message?.content;
    return !!last && foldForSearch(last).includes(needle);
  });
}