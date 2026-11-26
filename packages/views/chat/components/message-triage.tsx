"use client";

import { createContext, useContext, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { cn } from "@multica/ui/lib/utils";
import { Button } from "@multica/ui/components/ui/button";
import { Tooltip, TooltipTrigger, TooltipContent } from "@multica/ui/components/ui/tooltip";
import { Check, Flag } from "@multica/ui/icons";
import type { ChatMessageTriageState } from "@multica/core/types";
import { chatMessageTriageOptions } from "@multica/core/chat/queries";
import {
  useFlagChatMessage,
  useHandleAllOpenChatMessages,
  useHandleChatMessage,
  useUnflagChatMessage,
} from "@multica/core/chat/mutations";
import { useLocale, useT } from "../../i18n";

/**
 * Read-but-unhandled triage for chat replies.
 *
 * Why this exists next to the read cursor: `chat_session.last_read_at` answers
 * "has this been shown", one bit for a whole conversation. A reader who opens a
 * conversation, sees three replies that each ask for something, and gets pulled
 * away comes back to a chat where all three look read and none look pending.
 * The decision is therefore stored per reply and per reader on
 * `chat_message_user_state`, and these components are the only surface for it.
 *
 * Three states, two stored timestamps:
 *   flagged ∧ ¬handled → needs action (the open queue)
 *   handled            → done
 *   neither            → undecided, which is also the absence of a row
 *
 * Keeping "neither" addressable is the reason a row survives clearing a flag:
 * otherwise a reader who flagged a reply, looked again and let it go would be
 * indistinguishable from one who never opened the chat — and every read reply
 * would have to be treated as potential work.
 *
 * The stored states are the caller's own (the API is creator-scoped), so two
 * members in the same workspace never see each other's markers.
 */

/** Per-session decisions, shared with every reply row under one provider. */
interface ChatTriageContextValue {
  sessionId: string | null;
  byMessageId: Map<string, ChatMessageTriageState>;
}

const ChatTriageContext = createContext<ChatTriageContextValue | null>(null);

/**
 * Shares one session's decisions with every reply row below it.
 *
 * Mounted at the conversation pane rather than the app root: the list is
 * remounted per session, and a provider higher up would keep the previous
 * session id for a frame and paint its markers on the new conversation.
 */
export function ChatTriageProvider({
  sessionId,
  children,
}: {
  sessionId: string | null | undefined;
  children: React.ReactNode;
}) {
  const { data } = useQuery({
    ...chatMessageTriageOptions(sessionId ?? ""),
    enabled: !!sessionId,
  });
  const value = useMemo<ChatTriageContextValue>(
    () => ({
      sessionId: sessionId ?? null,
      byMessageId: new Map(
        (data?.states ?? []).map((state) => [state.chat_message_id, state]),
      ),
    }),
    [data, sessionId],
  );
  return <ChatTriageContext.Provider value={value}>{children}</ChatTriageContext.Provider>;
}

/** The row's decision, or null when nothing is stored (never touched / not loaded). */
export function useMessageTriageState(messageId: string): ChatMessageTriageState | null {
  const context = useContext(ChatTriageContext);
  return context?.byMessageId.get(messageId) ?? null;
}

function useTriageSessionId(): string | null {
  return useContext(ChatTriageContext)?.sessionId ?? null;
}

/**
 * Whether this subtree is inside a chat session that can carry triage.
 *
 * False in the agent-builder conversation, which reuses the same message list
 * over a chat session that has no triage surface. Callers use it to skip
 * rendering an otherwise-empty action strip rather than to change behaviour.
 */
export function useHasTriageSession(): boolean {
  return useTriageSessionId() !== null;
}

/** "Done 14:19" caption, or just "Done" when the server sent no timestamp. */
function formatHandledAt(value: string | undefined, locale: string): string | null {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toLocaleTimeString(locale, { hour: "2-digit", minute: "2-digit" });
}

/**
 * "Needs action" chip in the reply's footer.
 *
 * Amber rather than red: this is the reader's own reminder, not a failure. The
 * icon repeats the meaning so the chip still reads in grayscale.
 */
export function TriageFlagChip({ className }: { className?: string }) {
  const { t } = useT("chat");
  return (
    <span
      className={cn(
        "inline-flex shrink-0 items-center gap-1 rounded-sm border border-warning/50 bg-warning/20 px-1.5 py-px text-micro font-semibold text-foreground",
        className,
      )}
    >
      <Flag className="size-2.5 text-priority-high" aria-hidden="true" />
      {t(($) => $.message_list.triage_needs_action)}
    </span>
  );
}

/**
 * "Done 14:19" caption.
 *
 * Muted on purpose: a handled reply is history, and one that announces its
 * completeness competes with the replies that still need the reader.
 */
export function TriageHandledCaption({ value }: { value?: string }) {
  const { t } = useT("chat");
  const locale = useLocale();
  const time = formatHandledAt(value, locale);
  return (
    <span className="inline-flex shrink-0 items-center gap-1 text-micro text-faint-foreground">
      <Check className="size-2.5 text-success" aria-hidden="true" />
      {time
        ? t(($) => $.message_list.triage_handled_at, { time })
        : t(($) => $.message_list.triage_handled)}
    </span>
  );
}

/**
 * The two per-reply actions: flag/unflag and mark-done.
 *
 * Hidden until the row is hovered or focused on a hover-capable pointer, and
 * always visible on touch — the same split the thread list uses, because a
 * phone can never reach a hover strip.
 *
 * Both buttons stay mounted while their mutation is in flight (disabled rather
 * than removed), so the footer does not jump under the cursor mid-tap.
 */
export function TriageActions({
  messageId,
  state,
  className,
}: {
  messageId: string;
  state: ChatMessageTriageState | null;
  className?: string;
}) {
  const { t } = useT("chat");
  const sessionId = useTriageSessionId();
  const flag = useFlagChatMessage();
  const unflag = useUnflagChatMessage();
  const handle = useHandleChatMessage();

  // No session means no provider above this row (the agent-builder conversation
  // renders the same list without chat sessions). Triage is a chat-session
  // concept, so the actions simply do not exist there.
  if (!sessionId) return null;

  const flagged = !!state?.flagged;
  const handled = !!state?.handled;
  const pending = flag.isPending || unflag.isPending || handle.isPending;
  const vars = { sessionId, messageId };
  const flagLabel = flagged
    ? t(($) => $.message_list.triage_unflag)
    : t(($) => $.message_list.triage_flag);
  const handleLabel = t(($) => $.message_list.triage_handle);

  return (
    <span
      className={cn(
        "inline-flex shrink-0 items-center gap-0.5 transition-opacity [@media(hover:hover)]:opacity-0 [@media(hover:hover)]:group-hover/msg:opacity-100 [@media(hover:hover)]:group-focus-within/msg:opacity-100",
        className,
      )}
    >
      <Tooltip>
        <TooltipTrigger
          render={
            <Button
              variant="ghost"
              size="icon-xs"
              disabled={pending}
              aria-pressed={flagged}
              aria-label={flagLabel}
              className={cn(
                "hover:text-foreground",
                flagged ? "text-priority-high" : "text-faint-foreground",
              )}
              onClick={() => (flagged ? unflag : flag).mutate(vars)}
            />
          }
        >
          <Flag className={cn("size-3", flagged && "fill-current")} />
        </TooltipTrigger>
        <TooltipContent side="top">{flagLabel}</TooltipContent>
      </Tooltip>
      <Tooltip>
        <TooltipTrigger
          render={
            <Button
              variant="ghost"
              size="icon-xs"
              disabled={pending}
              aria-pressed={handled}
              aria-label={handleLabel}
              className={cn(
                "hover:text-foreground",
                handled ? "text-success" : "text-faint-foreground",
              )}
              onClick={() => handle.mutate(vars)}
            />
          }
        >
          <Check className="size-3" />
        </TooltipTrigger>
        <TooltipContent side="top">{handleLabel}</TooltipContent>
      </Tooltip>
    </span>
  );
}

/**
 * Session-level bar under the header: how many replies are waiting, and one
 * action that discharges them all.
 *
 * Rendered only while something is outstanding, so a conversation with nothing
 * pending carries no extra chrome. The bulk action is the escape hatch for a
 * reader who saw the batch and owes nothing on any of it — which is also what
 * keeps a stale flag from becoming permanent noise.
 */
export function ChatTriageBar({ sessionId }: { sessionId: string | null | undefined }) {
  const { t } = useT("chat");
  const { data } = useQuery({
    ...chatMessageTriageOptions(sessionId ?? ""),
    enabled: !!sessionId,
  });
  const handleAll = useHandleAllOpenChatMessages();
  const openCount = data?.open_count ?? 0;

  if (!sessionId || openCount === 0) return null;

  return (
    <div className="flex shrink-0 items-center gap-2 border-b border-warning/30 bg-warning/15 px-4 py-1.5">
      <Flag className="size-3 shrink-0 text-priority-high" aria-hidden="true" />
      <span className="min-w-0 truncate text-label text-foreground">
        {t(($) => $.window.triage_bar, { count: openCount })}
      </span>
      <span className="flex-1" />
      <Button
        variant="ghost"
        size="xs"
        disabled={handleAll.isPending}
        className="shrink-0 text-muted-foreground hover:text-foreground"
        onClick={() => handleAll.mutate(sessionId)}
      >
        {t(($) => $.window.triage_handle_all)}
      </Button>
    </div>
  );
}

/**
 * Header count chip — the only triage affordance visible while the reader is
 * scrolled away from the flagged reply itself.
 */
export function ChatTriageHeaderChip({ sessionId }: { sessionId: string | null | undefined }) {
  const { t } = useT("chat");
  const { data } = useQuery({
    ...chatMessageTriageOptions(sessionId ?? ""),
    enabled: !!sessionId,
  });
  const openCount = data?.open_count ?? 0;
  if (!sessionId || openCount === 0) return null;
  return (
    <span className="inline-flex shrink-0 items-center gap-1 rounded-sm border border-warning/50 px-1.5 py-px text-micro font-medium text-foreground">
      <Flag className="size-2.5 text-priority-high" aria-hidden="true" />
      {t(($) => $.window.triage_chip, { count: openCount })}
    </span>
  );
}

/**
 * Left rail on a flagged reply: a 2px amber edge on the row, so the reply is
 * findable while scrolling past it without reading a word.
 */
export function TriageRail() {
  return (
    <span
      aria-hidden="true"
      className="absolute inset-y-1.5 left-0 w-0.5 rounded-full bg-warning"
    />
  );
}