import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { useSearchHighlight } from "./use-search-highlight";

class FakeHighlight {
  ranges: Range[];
  constructor(...ranges: Range[]) {
    this.ranges = ranges;
  }
}

let highlights: Map<string, FakeHighlight>;

beforeEach(() => {
  highlights = new Map();
  vi.stubGlobal("CSS", { highlights });
  vi.stubGlobal("Highlight", FakeHighlight);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
  document.body.innerHTML = "";
});

function mountContainer(html: string): HTMLElement {
  const el = document.createElement("div");
  el.innerHTML = html;
  document.body.append(el);
  return el;
}

/** Let the hook's rAF-coalesced recompute run. */
async function settle() {
  await act(async () => {
    await new Promise((resolve) => requestAnimationFrame(() => resolve(null)));
  });
}

describe("useSearchHighlight", () => {
  it("paints matches in the container for the current query", async () => {
    const container = mountContainer("<p>FRIDAY — báo cáo OpenRouter</p>");
    renderHook(() => useSearchHighlight({ container, query: "bao cao" }));

    await settle();

    const ranges = highlights.get("multica-search-match")?.ranges ?? [];
    expect(ranges.map((r) => r.toString())).toEqual(["báo cáo"]);
  });

  it("clears the paint when the query is emptied", async () => {
    const container = mountContainer("<p>báo cáo</p>");
    const { rerender } = renderHook(
      ({ query }: { query: string }) => useSearchHighlight({ container, query }),
      { initialProps: { query: "báo cáo" } },
    );
    await settle();
    expect(highlights.has("multica-search-match")).toBe(true);

    rerender({ query: "" });
    await settle();

    expect(highlights.has("multica-search-match")).toBe(false);
  });

  it("repaints when streamed text or newly mounted rows appear", async () => {
    const container = mountContainer("<p>báo cáo</p>");
    renderHook(() => useSearchHighlight({ container, query: "báo cáo" }));
    await settle();
    expect(highlights.get("multica-search-match")?.ranges).toHaveLength(1);

    await act(async () => {
      container.insertAdjacentHTML("beforeend", "<p>báo cáo lần nữa</p>");
    });
    await settle();

    expect(highlights.get("multica-search-match")?.ranges).toHaveLength(2);
  });

  it("drops its paint on unmount so a stale tint cannot linger", async () => {
    const container = mountContainer("<p>báo cáo</p>");
    const { unmount } = renderHook(() =>
      useSearchHighlight({ container, query: "báo cáo" }),
    );
    await settle();
    expect(highlights.has("multica-search-match")).toBe(true);

    unmount();

    expect(highlights.has("multica-search-match")).toBe(false);
  });
});