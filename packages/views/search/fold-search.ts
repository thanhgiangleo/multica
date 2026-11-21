/**
 * Text folding for search. One implementation for the whole product: the chat
 * list filter decides a session matches with `foldForSearch`, and the
 * highlighters mark the same spans with the same fold, so a query never
 * filters a session in while marking nothing inside it.
 */

/**
 * Fold a string to its unaccented lowercase form so search ignores Vietnamese
 * tone marks and case in BOTH directions: "khong" finds "không" and "Không"
 * finds "khong". NFD splits precomposed letters into base + combining marks,
 * which are then stripped; `đ` has no decomposition and is mapped by hand.
 */
export function foldForSearch(input: string): string {
  return input
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/đ/g, "d");
}

/**
 * A folded haystack plus the original offsets each folded unit came from.
 *
 * Folding can change length (NFD decomposes, marks are stripped), so matches
 * have to be found in the folded string and then mapped back. The fold is
 * applied per code point, which keeps that mapping exact: every folded unit
 * remembers which original character produced it.
 */
interface FoldedHaystack {
  folded: string;
  /** Original offset of the character that produced folded unit `i`. */
  starts: number[];
  /** Original offset just past that character. */
  ends: number[];
}

function foldHaystack(text: string): FoldedHaystack {
  const units: string[] = [];
  const starts: number[] = [];
  const ends: number[] = [];
  let offset = 0;
  for (const char of text) {
    const folded = foldForSearch(char);
    for (let i = 0; i < folded.length; i += 1) {
      starts.push(offset);
      ends.push(offset + char.length);
    }
    units.push(folded);
    offset += char.length;
  }
  return { folded: units.join(""), starts, ends };
}

export interface FoldedRange {
  /** Inclusive offset into the original string. */
  start: number;
  /** Exclusive offset into the original string. */
  end: number;
}

/**
 * Every non-overlapping occurrence of `foldedNeedle` (already folded and
 * trimmed by `foldForSearch`) in `text`, as ranges into the ORIGINAL string.
 *
 * This is the span list behind both highlighters: the React highlighter turns
 * them into `HighlightPart`s, and the CSS Custom Highlight painter turns them
 * into DOM ranges.
 */
export function findFoldedRanges(text: string, foldedNeedle: string): FoldedRange[] {
  if (!foldedNeedle) return [];
  const { folded, starts, ends } = foldHaystack(text);

  const ranges: FoldedRange[] = [];
  let from = 0;
  for (;;) {
    const at = folded.indexOf(foldedNeedle, from);
    if (at === -1) break;
    const start = starts[at];
    const end = ends[at + foldedNeedle.length - 1];
    if (start == null || end == null) break;
    if (end > start) ranges.push({ start, end });
    from = at + foldedNeedle.length;
  }
  return ranges;
}

export interface HighlightPart {
  text: string;
  highlight: boolean;
}

/**
 * Split `text` into highlighted / plain runs for `foldedNeedle` (already
 * folded and trimmed by `foldForSearch`).
 */
export function splitHighlightParts(text: string, foldedNeedle: string): HighlightPart[] {
  const ranges = findFoldedRanges(text, foldedNeedle);
  if (ranges.length === 0) return [{ text, highlight: false }];

  const parts: HighlightPart[] = [];
  let cursor = 0;
  for (const { start, end } of ranges) {
    if (start > cursor) parts.push({ text: text.slice(cursor, start), highlight: false });
    parts.push({ text: text.slice(start, end), highlight: true });
    cursor = end;
  }
  if (cursor < text.length) parts.push({ text: text.slice(cursor), highlight: false });
  return parts;
}