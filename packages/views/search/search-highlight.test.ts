import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { collectSearchRanges, clearSearchHighlights, setSearchHighlights } from "./search-highlight";

// jsdom has no CSS Custom Highlight API; stand one up per test so the painter
// can be observed. `new Highlight(...ranges)` is only stored, never applied.
class FakeHighlight {
  ranges: Range[];
  constructor(...ranges: Range[]) {
    this.ranges = ranges;
  }
}
let highlights: Map<string, FakeHighlight>;

beforeEach(() => {
  highlights = new Map<string, FakeHighlight>();
  vi.stubGlobal("CSS", { highlights });
  vi.stubGlobal("Highlight", FakeHighlight);
});

afterEach(() => {
  vi.unstubAllGlobals();
  document.body.innerHTML = "";
});

function mountReport(html: string): HTMLElement {
  const root = document.createElement("div");
  root.innerHTML = html;
  document.body.append(root);
  return root;
}

describe("collectSearchRanges", () => {
  it("finds matches regardless of accents and case", () => {
    const root = mountReport("<p>Báo cáo OpenRouter</p>");

    expect(collectSearchRanges(root, "bao cao")).toHaveLength(1);
    expect(collectSearchRanges(root, "BÁO CÁO")).toHaveLength(1);
    expect(collectSearchRanges(root, "openrouter")).toHaveLength(1);
  });

  it("marks the matched substring only, not the whole node", () => {
    const root = mountReport("<p>FRIDAY — báo cáo OpenRouter</p>");
    const [range] = collectSearchRanges(root, "báo cáo");

    expect(range?.toString()).toBe("báo cáo");
  });

  it("collects every occurrence, each confined to its own text node", () => {
    const root = mountReport("<p>báo cáo</p><p>another <strong>báo</strong> cáo</p>");

    // Two nodes match the whole phrase in the first paragraph and one in the
    // second ("báo" is split by <strong>, so only the first paragraph plus the
    // trailing "cáo" node do not form a phrase — the phrase test above covers
    // the contiguous case).
    const single = collectSearchRanges(root, "báo cáo");
    expect(single).toHaveLength(1);
    expect(single[0]?.toString()).toBe("báo cáo");

    const perWord = collectSearchRanges(root, "báo");
    expect(perWord.map((r) => r.toString())).toEqual(["báo", "báo"]);
  });

  it("walks rows and message bodies but skips marked-ignored chrome", () => {
    const root = mountReport(
      '<div data-search-ignore>37 báo cáo found</div><p>báo cáo</p>',
    );

    const ranges = collectSearchRanges(root, "báo cáo");
    expect(ranges).toHaveLength(1);
    expect(ranges[0]?.startContainer.textContent).toBe("báo cáo");
  });

  it("returns nothing for a blank query", () => {
    const root = mountReport("<p>báo cáo</p>");
    expect(collectSearchRanges(root, "   ")).toEqual([]);
  });
});

describe("search highlight ownership", () => {
  it("merges ranges from every surface into one paint", () => {
    const list = mountReport("<p>list hit</p>");
    const conversation = mountReport("<p>conversation hit</p>");

    setSearchHighlights("list", collectSearchRanges(list, "hit"));
    setSearchHighlights("conversation", collectSearchRanges(conversation, "hit"));

    expect(highlights.get("multica-search-match")?.ranges).toHaveLength(2);
  });

  it("clearing one surface leaves the other's paint in place", () => {
    const list = mountReport("<p>list hit</p>");
    const conversation = mountReport("<p>conversation hit</p>");
    setSearchHighlights("list", collectSearchRanges(list, "hit"));
    setSearchHighlights("conversation", collectSearchRanges(conversation, "hit"));

    clearSearchHighlights("conversation");

    expect(highlights.get("multica-search-match")?.ranges).toHaveLength(1);
  });

  it("drops the paint when the last surface clears", () => {
    const list = mountReport("<p>list hit</p>");
    setSearchHighlights("list", collectSearchRanges(list, "hit"));
    clearSearchHighlights("list");

    expect(highlights.has("multica-search-match")).toBe(false);
  });
});