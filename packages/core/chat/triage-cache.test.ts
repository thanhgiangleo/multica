// @vitest-environment node
import { describe, expect, it } from "vitest";
import { QueryClient } from "@tanstack/react-query";
import {
  applyTriagePatch,
  countOpenTriageStates,
  syncSessionOpenCount,
  writeTriageStateFromPayload,
} from "./triage-cache";
import { chatKeys } from "./queries";
import type {
  ChatMessageTriageList,
  ChatMessageTriageState,
  ChatSession,
} from "../types";

function state(
  messageId: string,
  overrides: Partial<ChatMessageTriageState> = {},
): ChatMessageTriageState {
  return {
    chat_message_id: messageId,
    flagged: true,
    handled: false,
    flagged_at: "2026-11-26T10:00:00Z",
    revision: 1,
    ...overrides,
  };
}

function session(id: string, openCount?: number): ChatSession {
  return {
    id,
    workspace_id: "ws-1",
    agent_id: "agent-1",
    creator_id: "user-1",
    title: id,
    status: "active",
    has_unread: false,
    unread_count: 0,
    open_count: openCount,
    created_at: "2026-11-26T00:00:00Z",
    updated_at: "2026-11-26T00:00:00Z",
  };
}

describe("countOpenTriageStates", () => {
  it("counts only flagged rows that are not handled", () => {
    expect(
      countOpenTriageStates([
        state("a"),
        state("b", { flagged: false, handled: true, flagged_at: undefined }),
        state("c"),
      ]),
    ).toBe(2);
  });

  it("returns 0 for an empty list", () => {
    expect(countOpenTriageStates([])).toBe(0);
  });
});

describe("applyTriagePatch", () => {
  it("adds a row for a first flag and counts it", () => {
    const next = applyTriagePatch(undefined, {
      sessionId: "s1",
      messageId: "m1",
      flagged: true,
    });
    expect(next.states).toHaveLength(1);
    expect(next.states[0]?.flagged).toBe(true);
    expect(next.open_count).toBe(1);
  });

  it("keeps the original flag time when re-flagging", () => {
    const old: ChatMessageTriageList = { states: [state("m1")], open_count: 1 };
    const next = applyTriagePatch(old, {
      sessionId: "s1",
      messageId: "m1",
      flagged: true,
    });
    expect(next.states[0]?.flagged_at).toBe("2026-11-26T10:00:00Z");
    expect(next.open_count).toBe(1);
  });

  it("drops the row entirely when the reply becomes undecided", () => {
    const old: ChatMessageTriageList = { states: [state("m1")], open_count: 1 };
    const next = applyTriagePatch(old, {
      sessionId: "s1",
      messageId: "m1",
      flagged: false,
    });
    // "Neither flagged nor handled" is the absence of a row, not a false/false
    // row: otherwise the next read cannot tell it from a decision.
    expect(next.states).toHaveLength(0);
    expect(next.open_count).toBe(0);
  });

  it("keeps a handled row and clears an outstanding flag when handling", () => {
    const old: ChatMessageTriageList = {
      states: [state("m1"), state("m2")],
      open_count: 2,
    };
    const next = applyTriagePatch(old, {
      sessionId: "s1",
      messageId: "m1",
      flagged: false,
      handled: true,
    });
    expect(next.states.find((s) => s.chat_message_id === "m1")?.handled).toBe(true);
    expect(next.open_count).toBe(1);
  });

  it("leaves other rows untouched", () => {
    const old: ChatMessageTriageList = { states: [state("m1"), state("m2")], open_count: 2 };
    const next = applyTriagePatch(old, {
      sessionId: "s1",
      messageId: "m1",
      handled: true,
    });
    expect(next.states.find((s) => s.chat_message_id === "m2")).toEqual(state("m2"));
  });
});

describe("writeTriageStateFromPayload", () => {
  it("overwrites the optimistic row with the server's values and revision", () => {
    const qc = new QueryClient();
    qc.setQueryData<ChatMessageTriageList>(chatKeys.triage("s1"), {
      states: [{ ...state("m1", { revision: 2 }), flagged_at: undefined }],
      open_count: 1,
    });
    const authoritative = state("m1", { revision: 7 });

    const next = writeTriageStateFromPayload(qc, "s1", authoritative);

    expect(next.states[0]).toEqual(authoritative);
    expect(next.open_count).toBe(1);
    expect(qc.getQueryData(chatKeys.triage("s1"))).toEqual(next);
  });

  it("recomputes the count from the rows", () => {
    const qc = new QueryClient();
    qc.setQueryData<ChatMessageTriageList>(chatKeys.triage("s1"), {
      states: [state("m1"), state("m2")],
      open_count: 2,
    });

    const next = writeTriageStateFromPayload(
      qc,
      "s1",
      state("m1", { flagged: false, handled: true, flagged_at: undefined, handled_at: "x" }),
    );

    expect(next.open_count).toBe(1);
  });
});

describe("syncSessionOpenCount", () => {
  it("projects the count into the matching session row only", () => {
    const qc = new QueryClient();
    qc.setQueryData<ChatSession[]>(chatKeys.sessions("ws-1"), [
      session("s1", 3),
      session("s2", 1),
    ]);

    syncSessionOpenCount(qc, "ws-1", "s1", 0);

    const cached = qc.getQueryData<ChatSession[]>(chatKeys.sessions("ws-1"));
    expect(cached?.find((s) => s.id === "s1")?.open_count).toBe(0);
    expect(cached?.find((s) => s.id === "s2")?.open_count).toBe(1);
  });

  it("sets the value rather than incrementing, so a duplicate frame cannot drift", () => {
    const qc = new QueryClient();
    qc.setQueryData<ChatSession[]>(chatKeys.sessions("ws-1"), [session("s1", 4)]);

    syncSessionOpenCount(qc, "ws-1", "s1", 1);
    syncSessionOpenCount(qc, "ws-1", "s1", 1);

    expect(qc.getQueryData<ChatSession[]>(chatKeys.sessions("ws-1"))?.[0]?.open_count).toBe(1);
  });

  it("does nothing without a workspace id or a cached list", () => {
    const qc = new QueryClient();
    syncSessionOpenCount(qc, null, "s1", 0);
    syncSessionOpenCount(qc, "ws-1", "s1", 5);
    expect(qc.getQueryData(chatKeys.sessions("ws-1"))).toBeUndefined();
  });
});