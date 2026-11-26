// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, screen } from "@testing-library/react";
import { renderWithI18n } from "../../test/i18n";
import enChat from "../../locales/en/chat.json";
import type { ChatMessageTriageState } from "@multica/core/types";

// The triage surface is three pieces of UI reading one query and writing three
// mutations. These tests pin the wiring the transcript relies on: the chip and
// the rail follow the stored decision, the actions fire the right mutation for
// the row's current state, and the bar only exists while replies are waiting.
// Cache projection is covered in core/chat/triage-cache.test.ts.

const mutate = vi.hoisted(() => ({
  flag: vi.fn(),
  unflag: vi.fn(),
  handle: vi.fn(),
  unhandle: vi.fn(),
  handleAll: vi.fn(),
}));
const queryData = vi.hoisted<{ current: { states: ChatMessageTriageState[]; open_count: number } | undefined }>(
  () => ({ current: undefined }),
);

vi.mock("@tanstack/react-query", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@tanstack/react-query")>();
  return {
    ...actual,
    useQuery: () => ({ data: queryData.current }),
  };
});

vi.mock("@multica/core/chat/mutations", () => ({
  useFlagChatMessage: () => ({ mutate: mutate.flag, isPending: false }),
  useUnflagChatMessage: () => ({ mutate: mutate.unflag, isPending: false }),
  useHandleChatMessage: () => ({ mutate: mutate.handle, isPending: false }),
  useUnhandleChatMessage: () => ({ mutate: mutate.unhandle, isPending: false }),
  useHandleAllOpenChatMessages: () => ({ mutate: mutate.handleAll, isPending: false }),
}));

import {
  ChatTriageBar,
  ChatTriageHeaderChip,
  ChatTriageProvider,
  TriageActions,
  TriageFlagChip,
  TriageRail,
  useMessageTriageState,
} from "./message-triage";

function flaggedRow(): ChatMessageTriageState {
  return {
    chat_message_id: "m1",
    flagged: true,
    handled: false,
    flagged_at: "2026-11-26T10:00:00Z",
    revision: 1,
  };
}

beforeEach(() => {
  queryData.current = undefined;
  vi.clearAllMocks();
});

describe("TriageActions", () => {
  it("flags an untouched reply", () => {
    renderWithI18n(
      <ChatTriageProvider sessionId="s1">
        <TriageActions messageId="m1" state={null} />
      </ChatTriageProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: enChat.message_list.triage_flag }));
    expect(mutate.flag).toHaveBeenCalledWith({ sessionId: "s1", messageId: "m1" });
  });

  it("unflags a flagged reply from the same button", () => {
    renderWithI18n(
      <ChatTriageProvider sessionId="s1">
        <TriageActions messageId="m1" state={flaggedRow()} />
      </ChatTriageProvider>,
    );
    const button = screen.getByRole("button", { name: enChat.message_list.triage_unflag });
    expect(button).toHaveAttribute("aria-pressed", "true");
    fireEvent.click(button);
    expect(mutate.unflag).toHaveBeenCalledWith({ sessionId: "s1", messageId: "m1" });
  });

  it("marks a reply done", () => {
    renderWithI18n(
      <ChatTriageProvider sessionId="s1">
        <TriageActions messageId="m1" state={flaggedRow()} />
      </ChatTriageProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: enChat.message_list.triage_handle }));
    expect(mutate.handle).toHaveBeenCalledWith({ sessionId: "s1", messageId: "m1" });
  });

  it("renders nothing without a chat session above it", () => {
    // The agent-builder conversation reuses the message list with no provider.
    renderWithI18n(<TriageActions messageId="m1" state={null} />);
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });
});

describe("decision markers", () => {
  it("labels the needs-action chip in words, not only colour", () => {
    renderWithI18n(<TriageFlagChip />);
    expect(screen.getByText(enChat.message_list.triage_needs_action)).toBeInTheDocument();
  });

  it("exposes the row decision through the provider", () => {
    queryData.current = { states: [flaggedRow()], open_count: 1 };
    function Probe() {
      const state = useMessageTriageState("m1");
      return <span>{state ? `${state.flagged}:${state.handled}` : "none"}</span>;
    }
    renderWithI18n(
      <ChatTriageProvider sessionId="s1">
        <Probe />
      </ChatTriageProvider>,
    );
    expect(screen.getByText("true:false")).toBeInTheDocument();
  });

  it("renders no rail element of its own text", () => {
    const { container } = renderWithI18n(
      <div className="relative">
        <TriageRail />
      </div>,
    );
    expect(container.querySelector(".bg-warning")).not.toBeNull();
  });
});

describe("session-level triage", () => {
  it("hides the bar when nothing is outstanding", () => {
    queryData.current = { states: [], open_count: 0 };
    renderWithI18n(<ChatTriageBar sessionId="s1" />);
    expect(screen.queryByText(enChat.window.triage_handle_all)).not.toBeInTheDocument();
  });

  it("shows the outstanding count and discharges it all", () => {
    queryData.current = { states: [flaggedRow()], open_count: 1 };
    renderWithI18n(<ChatTriageBar sessionId="s1" />);
    fireEvent.click(screen.getByRole("button", { name: enChat.window.triage_handle_all }));
    expect(mutate.handleAll).toHaveBeenCalledWith("s1");
  });

  it("hides the header chip at zero and shows the count otherwise", () => {
    queryData.current = { states: [], open_count: 0 };
    const { unmount, container } = renderWithI18n(<ChatTriageHeaderChip sessionId="s1" />);
    expect(container).toBeEmptyDOMElement();
    unmount();

    queryData.current = { states: [flaggedRow()], open_count: 3 };
    renderWithI18n(<ChatTriageHeaderChip sessionId="s1" />);
    expect(screen.getByText("3 needs action")).toBeInTheDocument();
  });
});