"use client";

import { useMemo, useState } from "react";
import { RefreshCw } from "@multica/ui/icons";
import { useQueries, useQueryClient } from "@tanstack/react-query";
import { IconButton } from "@multica/ui/components/common/icon-button";
import { TooltipHint } from "@multica/ui/components/common/tooltip-hint";
import {
  CompactNumberFlow,
  CurrencyNumberFlow,
} from "@multica/ui/components/ui/number-flow";
import { useWorkspaceId } from "@multica/core/hooks";
import {
  dashboardKeys,
  dashboardUsageDailyOptions,
} from "@multica/core/dashboard";
import { useCustomPricingStore } from "@multica/core/runtimes/custom-pricing-store";
import { cn } from "@multica/ui/lib/utils";
import { useViewingTimezone } from "../../common/use-viewing-timezone";
import { computeDailyTotals } from "../../dashboard/utils";
import { addDaysIso, formatTokens, todayIso } from "../../runtimes/utils";
import { useT } from "../../i18n";

// Only the three short windows the chat header has room for. The full
// dashboard keeps 1d–180d; this is a glanceable strip, not a replacement.
const RANGES = [1, 3, 7] as const;
type UsageRange = (typeof RANGES)[number];


/**
 * Workspace usage at a glance, inside the conversation header.
 *
 * Reads the same `/usage` daily rollup the dashboard page does — same query
 * options, same tz, same client-side cost math — so the number here can never
 * disagree with the dashboard. One row of range buttons with the selected
 * window's cost beside them, plus a refresh that invalidates only these three
 * keys rather than the whole dashboard scope.
 *
 * Every range is fetched up front so switching windows is instant; the queries
 * share the dashboard's five-minute refetch and are deduped per range, so this
 * costs at most three requests a session and usually zero extra ones (the
 * dashboard's own 1d/7d views reuse the same keys when the window matches).
 */
export function ChatUsageWidget() {
  const { t, i18n } = useT("chat");
  const wsId = useWorkspaceId();
  const viewTZ = useViewingTimezone();
  const queryClient = useQueryClient();
  const locales = i18n.resolvedLanguage ?? i18n.language;
  const [days, setDays] = useState<UsageRange>(1);

  // Custom model rates are saved elsewhere in the app; re-render so the
  // estimate below picks them up without a reload.
  useCustomPricingStore((s) => s.pricings);

  const queries = useQueries({
    queries: RANGES.map((range) =>
      dashboardUsageDailyOptions(wsId, range, null, viewTZ),
    ),
  });
  const active = queries[RANGES.indexOf(days)];
  const data = active?.data;

  // The API returns headroom days beyond the requested window (the dashboard
  // does the same trim); anchor the cutoff on the viewer's tz so 1d really is
  // "today" in the same calendar the backend slices on.
  const totals = useMemo(() => {
    if (!data) return null;
    const cutoff = addDaysIso(todayIso(viewTZ), -(days - 1));
    return computeDailyTotals(data.filter((row) => row.date >= cutoff));
  }, [data, days, viewTZ]);

  const tokenCount = totals
    ? totals.input + totals.output + totals.cacheRead + totals.cacheWrite
    : 0;
  const isFetching = queries.some((q) => q.isFetching);

  const refresh = () => {
    for (const range of RANGES) {
      void queryClient.invalidateQueries({
        queryKey: dashboardKeys.daily(wsId, range, null, viewTZ),
      });
    }
  };

  return (
    <div className="hidden shrink-0 items-center gap-1.5 @[26rem]:flex">
      <div
        role="group"
        aria-label={t(($) => $.usage.range_label)}
        className="inline-flex items-center gap-0.5 rounded-full border border-surface-border bg-muted/50 p-0.5"
      >
        {RANGES.map((range) => {
          const label =
            range === 1
              ? t(($) => $.usage.range_1d)
              : range === 3
                ? t(($) => $.usage.range_3d)
                : t(($) => $.usage.range_7d);
          const selected = range === days;
          return (
            <button
              key={range}
              type="button"
              aria-pressed={selected}
              onClick={() => setDays(range)}
              className={cn(
                "rounded-full px-2 py-0.5 text-caption font-medium transition-colors",
                selected
                  ? "bg-background text-foreground shadow-sm"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              {label}
            </button>
          );
        })}
      </div>

      {/* The figure the selected range resolves to. `null` totals means the
          rollup has not arrived yet — a dash reads as "not loaded", where a
          zero would read as "nothing was spent". */}
      <TooltipHint
        label={
          totals
            ? t(($) => $.usage.tokens_hint, { tokens: formatTokens(tokenCount) })
            : null
        }
      >
        <span className="inline-flex min-w-[3.5rem] items-center justify-end gap-1 text-caption font-medium tabular-nums">
          {totals ? (
            <>
              <CurrencyNumberFlow value={totals.cost} locales={locales} />
              <span className="hidden text-muted-foreground @[34rem]:inline">
                <CompactNumberFlow value={tokenCount} locales={locales} />
              </span>
            </>
          ) : (
            <span aria-hidden className="text-muted-foreground">
              {"—"}
            </span>
          )}
        </span>
      </TooltipHint>

      <IconButton
        label={t(($) => $.usage.refresh)}
        tooltipSide="bottom"
        onClick={refresh}
        className="text-muted-foreground"
      >
        <RefreshCw className={cn(isFetching && "animate-spin")} />
      </IconButton>
    </div>
  );
}