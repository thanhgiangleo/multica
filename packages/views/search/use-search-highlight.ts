"use client";

import { useEffect, useId } from "react";

import {
  clearSearchHighlights,
  collectSearchRanges,
  setSearchHighlights,
} from "./search-highlight";

/**
 * Paint `query`'s matches inside `container` while it is mounted (see
 * search-highlight.ts). Each mounted call owns its own span set, so the thread
 * list and the conversation can highlight at the same time.
 *
 * Matches are rediscovered whenever the DOM under `container` changes: the
 * chat list is virtualized and streams replies, so rows mount, text grows and
 * code blocks finish highlighting underneath a fixed query. Bursts coalesce
 * into one frame — recomputing on every mutation would walk the container once
 * per streamed token.
 */
export function useSearchHighlight(options: {
  container: HTMLElement | null;
  query: string;
}): void {
  const { container, query } = options;
  const owner = useId();

  useEffect(() => {
    if (!container) return;
    if (query.trim() === "") {
      clearSearchHighlights(owner);
      return;
    }

    let frame = 0;
    const recompute = () => {
      setSearchHighlights(owner, collectSearchRanges(container, query));
    };
    const schedule = () => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(recompute);
    };

    recompute();
    const observer = new MutationObserver(schedule);
    observer.observe(container, {
      subtree: true,
      childList: true,
      characterData: true,
    });

    return () => {
      observer.disconnect();
      cancelAnimationFrame(frame);
      clearSearchHighlights(owner);
    };
  }, [container, owner, query]);
}