// @vitest-environment jsdom

import { beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, screen } from "@testing-library/react";
import type { DashboardUsageDaily } from "@multica/core/types";
import { renderWithI18n } from "../../test/i18n";
import { addDaysIso, todayIso } from "../../runtimes/utils";

// The widget reads the same rollup as the dashboard: three daily queries in
// RANGES order (1d, 3d, 7d), a client-side tz trim, and the shared cost math.
// These tests pin the two behaviours the strip exists for — the selected
// window's figure is the trimmed one, and refresh re-reads only those keys.

const invalidateQueries = vi.hoisted(() => vi.fn());
const useQueriesMock = vi.hoisted(() => vi.fn());
const queryArgs = vi.hoisted(() => ({ current: [] as { queryKey: readonly unknown[] }[] }));
// 0 = loaded, 1 = still pending. Flips between tests.
const pendingRef = vi.hoisted(() => ({ current: false }));

vi.mock("@multica/core/hooks", () => ({ useWorkspaceId: () => "ws-1" }));
vi.mock("../../common/use-viewing-timezone", () => ({
  useViewingTimezone: () => "UTC",
}));
vi.mock("@tanstack/react-query", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@tanstack/react-query")>();
  return {
    ...actual,
    useQueryClient: () => ({ invalidateQueries }),
    useQueries: (args: { queries: { queryKey: readonly unknown[] }[] }) => {
      queryArgs.current = args.queries;
      return useQueriesMock(args);
    },
  };
});

import { ChatUsageWidget } from "./chat-usage-widget";

const TODAY = todayIso("UTC");
const YESTERDAY = addDaysIso(TODAY, -1);

// No pricing, no cost ticks: the widget reports tokens only.
function row(date: string, inputTokens: number): DashboardUsageDaily {
  return {
    date,
    provider: "anthropic",
    model: "claude-sonnet-4-6",
    input_tokens: inputTokens,
    output_tokens: 0,
    cache_read_tokens: 0,
    cache_write_tokens: 0,
    task_count: 1,
  };
}

// The server returns the extra day of headroom, so yesterday's row must be
// dropped at 1d and kept at 3d.
const ROWS: DashboardUsageDaily[] = [row(TODAY, 1_000), row(YESTERDAY, 2_000)];

function renderedFigures(): string[] {
  return Array.from(document.querySelectorAll("number-flow-react")).map(
    (el) => el.getAttribute("aria-label") ?? "",
  );
}

beforeEach(() => {
  cleanup();
  invalidateQueries.mockReset();
  queryArgs.current = [];
  pendingRef.current = false;
  useQueriesMock.mockReset();
  useQueriesMock.mockImplementation(() =>
    queryArgs.current.map(() =>
      pendingRef.current
        ? { data: undefined, isFetching: true }
        : { data: ROWS, isFetching: false },
    ),
  );
});

describe("ChatUsageWidget", () => {
  it("shows and fetches only 1d in compact mode", () => {
    renderWithI18n(<ChatUsageWidget compact />);

    expect(screen.getByText("1d")).toBeInTheDocument();
    expect(renderedFigures()).toEqual(["1K"]);
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    const dailyKeys = queryArgs.current.map(({ queryKey }) =>
      queryKey[queryKey.indexOf("daily") + 1],
    );
    expect(dailyKeys).toEqual([1]);
  });

  it("offers 1d / 3d / 7d and starts on 1d", () => {
    renderWithI18n(<ChatUsageWidget />);

    expect(screen.getByRole("button", { name: "1d" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("button", { name: "3d" })).toHaveAttribute("aria-pressed", "false");
    expect(screen.getByRole("button", { name: "7d" })).toHaveAttribute("aria-pressed", "false");
  });

  it("fetches all three windows up front so switching is instant", () => {
    renderWithI18n(<ChatUsageWidget />);

    expect(queryArgs.current.map((q) => q.queryKey)).toHaveLength(3);
    expect(useQueriesMock).toHaveBeenCalled();
  });

  it("shows only today's tokens at 1d, dropping the server's headroom day", () => {
    renderWithI18n(<ChatUsageWidget />);

    expect(renderedFigures()).toEqual(["1K"]);
  });

  it("adds the previous day's tokens once the 3d window is selected", () => {
    renderWithI18n(<ChatUsageWidget />);

    fireEvent.click(screen.getByRole("button", { name: "3d" }));

    expect(screen.getByRole("button", { name: "3d" })).toHaveAttribute("aria-pressed", "true");
    expect(renderedFigures()).toEqual(["3K"]);
  });

  it("reports no cost, only tokens", () => {
    renderWithI18n(<ChatUsageWidget />);

    const labels = renderedFigures();
    expect(labels.every((label) => !label.startsWith("$"))).toBe(true);
    expect(screen.queryByText(/\$/)).toBeNull();
  });

  it("shows a dash instead of a zero while the rollup is still loading", () => {
    pendingRef.current = true;
    renderWithI18n(<ChatUsageWidget />);

    // A zero would claim "nothing was spent"; the dash says "not loaded yet".
    expect(renderedFigures()).toEqual([]);
  });

  it("refreshes only the three windows it reads", () => {
    renderWithI18n(<ChatUsageWidget />);

    fireEvent.click(screen.getByRole("button", { name: "Refresh usage" }));

    const dailyKeys = invalidateQueries.mock.calls.map(([arg]) => {
      const key = (arg as { queryKey: unknown[] }).queryKey;
      return key[key.indexOf("daily") + 1];
    });
    expect(dailyKeys).toEqual([1, 3, 7]);
  });
});