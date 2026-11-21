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
vi.mock("@multica/core/runtimes/custom-pricing-store", () => ({
  useCustomPricingStore: (selector: (s: { pricings: Record<string, never> }) => unknown) =>
    selector({ pricings: {} }),
  getCustomPricing: () => undefined,
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

function row(date: string, ticks: number, inputTokens: number): DashboardUsageDaily {
  return {
    date,
    provider: "anthropic",
    model: "claude-sonnet-4-6",
    input_tokens: inputTokens,
    output_tokens: 0,
    cache_read_tokens: 0,
    cache_write_tokens: 0,
    cost_usd_ticks: ticks,
    task_count: 1,
  };
}

// 300_000_000 ticks = $0.03; 700_000_000 = $0.07. The server returns the extra
// day of headroom, so yesterday's row must be dropped at 1d and kept at 3d.
const ROWS: DashboardUsageDaily[] = [
  row(TODAY, 300_000_000, 1_000),
  row(YESTERDAY, 700_000_000, 2_000),
];

// The token counter renders its own number-flow inside the same strip; the
// cost assertions want only the currency figure.
function renderedCosts(): string[] {
  return Array.from(document.querySelectorAll("number-flow-react"))
    .map((el) => el.getAttribute("aria-label") ?? "")
    .filter((label) => label.startsWith("$"));
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

  it("shows only today's spend at 1d, dropping the server's headroom day", () => {
    renderWithI18n(<ChatUsageWidget />);

    expect(renderedCosts()).toEqual(["$0.03"]);
  });

  it("adds the previous day once the 3d window is selected", () => {
    renderWithI18n(<ChatUsageWidget />);

    fireEvent.click(screen.getByRole("button", { name: "3d" }));

    expect(screen.getByRole("button", { name: "3d" })).toHaveAttribute("aria-pressed", "true");
    expect(renderedCosts()).toEqual(["$0.10"]);
  });

  it("shows a dash instead of a zero while the rollup is still loading", () => {
    pendingRef.current = true;
    renderWithI18n(<ChatUsageWidget />);

    // A zero would claim "nothing was spent"; the dash says "not loaded yet".
    expect(renderedCosts()).toEqual([]);
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