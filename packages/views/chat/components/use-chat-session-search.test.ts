import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@multica/core/api";
import type { ChatSession, SearchChatMessageResult } from "@multica/core/types";
import {
  filterSessionsByQuery,
  foldForSearch,
  useChatSessionSearch,
} from "./use-chat-session-search";

vi.mock("@multica/core/api", () => ({ api: { searchChatMessages: vi.fn() } }));

const DEBOUNCE_MS = 300;
const PAGE_SIZE = 50;
const MAX_PAGES = 5;

function hit(sessionId: string): SearchChatMessageResult {
  return {
    message_id: `message-${sessionId}`,
    session_id: sessionId,
    session_title: sessionId,
    session_status: "active",
    agent_id: "agent-1",
    role: "user",
    message_kind: "message",
    snippet: "…",
    match_source: "message",
    created_at: "2026-01-01T00:00:00Z",
  };
}

function fullPage(prefix: string): SearchChatMessageResult[] {
  return Array.from({ length: PAGE_SIZE }, (_, i) => hit(`${prefix}-${i}`));
}

function session(overrides: Partial<ChatSession> & { id: string }): ChatSession {
  return {
    workspace_id: "ws-1",
    agent_id: "agent-1",
    creator_id: "user-1",
    title: "",
    status: "active",
    has_unread: false,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

/** Let the debounce fire and the fetch chain settle inside act(). */
async function settle() {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(DEBOUNCE_MS);
  });
  await act(async () => {});
}

function renderSearch(initialQuery: string) {
  return renderHook(({ query }: { query: string }) => useChatSessionSearch(query), {
    initialProps: { query: initialQuery },
  });
}

describe("foldForSearch", () => {
  it("strips Vietnamese tone marks in both directions", () => {
    expect(foldForSearch("Không chạy được")).toBe("khong chay duoc");
    expect(foldForSearch("khong chay duoc")).toBe("khong chay duoc");
  });

  it("maps đ to d, which NFD does not decompose", () => {
    expect(foldForSearch("Đã xong")).toBe("da xong");
    expect(foldForSearch("đ")).toBe("d");
  });

  it("lowercases plain ASCII and leaves unknown characters alone", () => {
    expect(foldForSearch("ABC 123")).toBe("abc 123");
    expect(foldForSearch("日本語")).toBe("日本語");
  });
});

describe("filterSessionsByQuery", () => {
  const sessions = [
    session({ id: "s-remote", title: "Deploy notes" }),
    session({ id: "s-title", title: "Không chạy được trên zbs2" }),
    session({
      id: "s-last",
      title: "Untitled",
      last_message: { content: "da fix xong loi", role: "user", created_at: "2026-01-01T00:00:00Z" },
    }),
    session({ id: "s-other", title: "Weekly sync" }),
  ];

  it("keeps a session the transcript search matched, even when its title does not", () => {
    expect(
      filterSessionsByQuery(sessions, "abc", new Set(["s-remote"])).map((s) => s.id),
    ).toEqual(["s-remote"]);
  });

  it("matches titles without diacritics, and titles with them", () => {
    expect(
      filterSessionsByQuery(sessions, "khong chay", null).map((s) => s.id),
    ).toEqual(["s-title"]);
    expect(
      filterSessionsByQuery(sessions, "Không chạy", null).map((s) => s.id),
    ).toEqual(["s-title"]);
  });

  it("matches the last message so an untyped chat still shows up", () => {
    expect(filterSessionsByQuery(sessions, "đã fix", null).map((s) => s.id)).toEqual([
      "s-last",
    ]);
  });

  it("uses only local fields when the remote result is still unknown", () => {
    // null means "query has not settled" — the local fallback keeps the list
    // responsive instead of showing the previous query's matches.
    expect(filterSessionsByQuery(sessions, "weekly", null).map((s) => s.id)).toEqual([
      "s-other",
    ]);
  });

  it("returns the same array for a blank query so the list is untouched", () => {
    expect(filterSessionsByQuery(sessions, "   ", new Set(["s-remote"]))).toBe(sessions);
  });

  it("returns nothing when neither side matches", () => {
    expect(filterSessionsByQuery(sessions, "zzz", new Set(["missing"]))).toEqual([]);
  });
});

describe("useChatSessionSearch", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.mocked(api.searchChatMessages).mockReset();
    vi.mocked(api.searchChatMessages).mockResolvedValue({ messages: [] });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("does nothing until the query is typed", async () => {
    const { result } = renderSearch("");

    await settle();

    expect(api.searchChatMessages).not.toHaveBeenCalled();
    expect(result.current.sessionIds).toBeNull();
    expect(result.current.isLoading).toBe(false);
  });

  it("debounces and reports the sessions whose transcript matched", async () => {
    vi.mocked(api.searchChatMessages).mockResolvedValueOnce({
      messages: [hit("s-1"), hit("s-1"), hit("s-2")],
    });
    const { result } = renderSearch("abc");

    // Loading starts with the query, before the debounce elapses.
    expect(result.current.isLoading).toBe(true);
    expect(api.searchChatMessages).not.toHaveBeenCalled();

    await settle();

    expect(api.searchChatMessages).toHaveBeenCalledTimes(1);
    expect(vi.mocked(api.searchChatMessages).mock.calls[0]![0]).toMatchObject({
      q: "abc",
      limit: PAGE_SIZE,
      offset: 0,
    });
    expect(result.current.sessionIds).toEqual(new Set(["s-1", "s-2"]));
    expect(result.current.isLoading).toBe(false);
    expect(result.current.failed).toBe(false);
  });

  it("stops at the first short page", async () => {
    vi.mocked(api.searchChatMessages)
      .mockResolvedValueOnce({ messages: fullPage("a") })
      .mockResolvedValueOnce({ messages: [hit("b-0")] });
    const { result } = renderSearch("abc");

    await settle();

    expect(api.searchChatMessages).toHaveBeenCalledTimes(2);
    expect(vi.mocked(api.searchChatMessages).mock.calls[1]![0]).toMatchObject({ offset: PAGE_SIZE });
    expect(result.current.sessionIds?.size).toBe(PAGE_SIZE + 1);
    expect(result.current.truncated).toBe(false);
  });

  it("walks up to the page cap and says the result may be incomplete", async () => {
    vi.mocked(api.searchChatMessages).mockImplementation(async () => ({
      messages: fullPage("p"),
    }));
    const { result } = renderSearch("abc");

    await settle();

    expect(api.searchChatMessages).toHaveBeenCalledTimes(MAX_PAGES);
    expect(result.current.truncated).toBe(true);
    expect(result.current.failed).toBe(false);
  });

  it("falls back to local matching and reports the failure", async () => {
    vi.mocked(api.searchChatMessages).mockRejectedValue(new Error("503"));
    const { result } = renderSearch("abc");

    await settle();

    expect(result.current.failed).toBe(true);
    // An empty set, not null: the search ran and found nothing it could use, so
    // the list must narrow locally rather than pretend the result is pending.
    expect(result.current.sessionIds).toEqual(new Set());
    expect(result.current.isLoading).toBe(false);
  });

  it("keeps the pages read before a mid-chain failure", async () => {
    vi.mocked(api.searchChatMessages)
      .mockResolvedValueOnce({ messages: fullPage("a") })
      .mockRejectedValueOnce(new Error("offline"));
    const { result } = renderSearch("abc");

    await settle();

    expect(result.current.failed).toBe(true);
    expect(result.current.sessionIds?.size).toBe(PAGE_SIZE);
  });

  it("coalesces keystrokes into one request for the final query", async () => {
    const { rerender } = renderSearch("a");
    rerender({ query: "ab" });
    rerender({ query: "abc" });

    await settle();

    expect(api.searchChatMessages).toHaveBeenCalledTimes(1);
    expect(vi.mocked(api.searchChatMessages).mock.calls[0]![0]).toMatchObject({ q: "abc" });
  });

  it("drops the previous result while a new query is in flight", async () => {
    vi.mocked(api.searchChatMessages).mockResolvedValueOnce({ messages: [hit("old")] });
    const { result, rerender } = renderSearch("old");
    await settle();
    expect(result.current.sessionIds).toEqual(new Set(["old"]));

    vi.mocked(api.searchChatMessages).mockResolvedValueOnce({ messages: [hit("new")] });
    rerender({ query: "new" });

    // Not the previous query's sessions — those would be a wrong filter.
    expect(result.current.sessionIds).toBeNull();
    expect(result.current.isLoading).toBe(true);

    await settle();
    expect(result.current.sessionIds).toEqual(new Set(["new"]));
  });

  it("ignores a response that arrives after the query changed", async () => {
    let resolveStale: ((value: { messages: SearchChatMessageResult[] }) => void) | undefined;
    vi.mocked(api.searchChatMessages).mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveStale = resolve;
        }),
    );
    const { result, rerender } = renderSearch("slow");

    await act(async () => {
      await vi.advanceTimersByTimeAsync(DEBOUNCE_MS);
    });

    rerender({ query: "fast" });
    vi.mocked(api.searchChatMessages).mockResolvedValueOnce({ messages: [hit("fast")] });

    // The superseded request resolves late; un-aborted, it would overwrite the
    // newer query's result.
    await act(async () => {
      resolveStale?.({ messages: [hit("slow")] });
    });
    await settle();

    expect(result.current.sessionIds).toEqual(new Set(["fast"]));
    expect(result.current.isLoading).toBe(false);
  });
});