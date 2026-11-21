"use client";

import { findFoldedRanges, foldForSearch } from "./fold-search";

/**
 * Chat search highlighting.
 *
 * Typing in the chat search box already narrows the session list (server
 * transcript search + `foldForSearch`). This paints the matching text inside
 * what is already on screen — thread-list titles/previews and the open
 * conversation — so the reader can see WHY a row matched instead of guessing.
 *
 * Matches are painted with the CSS Custom Highlight API, the same mechanism as
 * in-page find (issues/hooks/use-in-page-find.ts): ranges only, no DOM
 * mutation, so it layers over React-rendered markdown, hljs-highlighted code
 * and virtualized rows without touching any of those trees. Accent folding is
 * shared with the filter through `foldForSearch`, so a session never stays in
 * the list while nothing in it gets marked.
 *
 * Browsers without the API simply paint no tint; the filter still works.
 */

export const SEARCH_HIGHLIGHT_NAME = "multica-search-match";

function highlightApiSupported(): boolean {
  return (
    typeof CSS !== "undefined" &&
    "highlights" in CSS &&
    typeof Highlight !== "undefined"
  );
}

// Element text that is not content: never walked.
const SKIP_TAGS = new Set(["SCRIPT", "STYLE", "NOSCRIPT", "TEXTAREA"]);

/**
 * Ranges of every accent-insensitive occurrence of `query` inside `root`'s
 * text nodes, in document order. Matches are confined to a single text node —
 * a query straddling an element boundary (e.g. across a bold run) is not
 * found, the same trade-off lightweight find makes.
 */
export function collectSearchRanges(root: HTMLElement, query: string): Range[] {
  const needle = foldForSearch(query.trim());
  if (!needle) return [];

  const ranges: Range[] = [];
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
    acceptNode(node) {
      const parent = node.parentElement;
      if (!parent) return NodeFilter.FILTER_REJECT;
      if (SKIP_TAGS.has(parent.tagName)) return NodeFilter.FILTER_REJECT;
      if (parent.closest("[data-search-ignore]")) return NodeFilter.FILTER_REJECT;
      const value = node.nodeValue;
      if (!value || value.trim().length === 0) return NodeFilter.FILTER_REJECT;
      return NodeFilter.FILTER_ACCEPT;
    },
  });

  for (let node = walker.nextNode(); node; node = walker.nextNode()) {
    const textNode = node as Text;
    for (const { start, end } of findFoldedRanges(textNode.nodeValue ?? "", needle)) {
      const range = new Range();
      range.setStart(textNode, start);
      range.setEnd(textNode, end);
      ranges.push(range);
    }
  }
  return ranges;
}

// Several surfaces (thread list + conversation) search at once, and a single
// `CSS.highlights` entry cannot be owned by two of them — the last writer would
// wipe the other's spans. Ranges are therefore kept per owner and repainted
// together into one Highlight.
const rangesByOwner = new Map<string, Range[]>();

function paint(): void {
  if (!highlightApiSupported()) return;
  const all = [...rangesByOwner.values()].flat();
  if (all.length === 0) {
    CSS.highlights.delete(SEARCH_HIGHLIGHT_NAME);
    return;
  }
  CSS.highlights.set(SEARCH_HIGHLIGHT_NAME, new Highlight(...all));
}

/** Publish this owner's current match ranges (empty clears just this owner). */
export function setSearchHighlights(owner: string, ranges: Range[]): void {
  if (ranges.length === 0) rangesByOwner.delete(owner);
  else rangesByOwner.set(owner, ranges);
  paint();
}

/** Drop this owner's ranges, leaving other surfaces' matches painted. */
export function clearSearchHighlights(owner: string): void {
  rangesByOwner.delete(owner);
  paint();
}