import type { QueryClient } from "@tanstack/react-query";
import type {
  ChatMessageTriageList,
  ChatMessageTriageState,
  ChatSession,
} from "../types";
import { chatKeys } from "./queries";

/**
 * One place where a triage decision is written into the query caches.
 *
 * Two writers produce these decisions — the local mutations (optimistic patch,
 * then the server's authoritative row) and the realtime `chat:message_triage`
 * frame from another device of the same member — and both must leave the caches
 * in exactly the same shape:
 *
 *   chatKeys.triage(sessionId)  → the per-reply decisions the transcript reads
 *   chatKeys.sessions(wsId)     → the thread list's per-session badge
 *
 * Keeping the projection in one module is what stops the two paths drifting:
 * a badge that only the local path updates goes stale the moment the same
 * member flags a reply from their phone.
 */

export interface TriagePatch {
  sessionId: string;
  messageId: string;
  flagged?: boolean;
  handled?: boolean;
}

/** Recompute the outstanding count from the decision rows themselves. */
export function countOpenTriageStates(states: ChatMessageTriageState[]): number {
  return states.filter((state) => state.flagged && !state.handled).length;
}

export function applyTriagePatch(
  old: ChatMessageTriageList | undefined,
  patch: TriagePatch,
): ChatMessageTriageList {
  const base = old ?? { states: [], open_count: 0 };
  const existing = base.states.find((s) => s.chat_message_id === patch.messageId);
  const next: ChatMessageTriageState = {
    chat_message_id: patch.messageId,
    flagged: patch.flagged ?? existing?.flagged ?? false,
    handled: patch.handled ?? existing?.handled ?? false,
    flagged_at: (patch.flagged ?? existing?.flagged) ? existing?.flagged_at : undefined,
    handled_at: (patch.handled ?? existing?.handled) ? existing?.handled_at : undefined,
    // Local bumps are placeholders; the server response overwrites this. The
    // value only has to change so anything keyed on it re-renders.
    revision: (existing?.revision ?? 0) + 1,
  };
  // A reply with neither decision is "undecided", which is the absence of a
  // row — keeping a false/false row would make "never touched" and "decided
  // nothing" indistinguishable on the next read.
  const states =
    next.flagged || next.handled
      ? existing
        ? base.states.map((s) => (s.chat_message_id === patch.messageId ? next : s))
        : [...base.states, next]
      : base.states.filter((s) => s.chat_message_id !== patch.messageId);
  return { states, open_count: countOpenTriageStates(states) };
}

/**
 * Write one decision from a realtime frame.
 *
 * Returns the resulting list so the caller can carry its count into the
 * sessions cache without a second read of the same entry.
 */
export function writeTriageStateFromPayload(
  qc: QueryClient,
  sessionId: string,
  state: ChatMessageTriageState,
): ChatMessageTriageList {
  let result: ChatMessageTriageList = { states: [], open_count: 0 };
  qc.setQueryData<ChatMessageTriageList>(chatKeys.triage(sessionId), (old) => {
    result = applyTriagePatch(old, {
      sessionId,
      messageId: state.chat_message_id,
      flagged: state.flagged,
      handled: state.handled,
    });
    // The patch above invents timestamps/revision; the frame carries the
    // persisted ones, so overwrite the row with the authoritative values
    // instead of keeping the optimistic placeholders.
    const states = result.states.map((s) =>
      s.chat_message_id === state.chat_message_id ? state : s,
    );
    result = { states, open_count: countOpenTriageStates(states) };
    return result;
  });
  return result;
}

/**
 * Project one session's outstanding count into the thread-list cache.
 *
 * Recomputed from the decision rows (not incremented), so a duplicated or
 * out-of-order frame cannot drift the badge away from the truth. Skipped when
 * the sessions list is not cached — an absent entry means the list has not been
 * loaded on this client, and inventing a row would be worse than the refetch
 * that will happen when it is.
 */
export function syncSessionOpenCount(
  qc: QueryClient,
  wsId: string | null | undefined,
  sessionId: string,
  openCount: number,
): void {
  if (!wsId) return;
  qc.setQueryData<ChatSession[]>(chatKeys.sessions(wsId), (old) =>
    old?.map((session) =>
      session.id === sessionId ? { ...session, open_count: openCount } : session,
    ),
  );
}