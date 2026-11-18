//! Mention extraction for comment content.
//!
//! Go parses mentions off goldmark's CommonMark AST, so code fences, inline code
//! spans, images and non-canonical link forms are inert for free. Rust has no
//! markdown parser, so this module reimplements the same observable contract
//! with a line scanner plus a canonical-form regex:
//!
//! * fenced code blocks (backtick/tilde, <=3 spaces indent, blockquote-aware,
//!   longer closer required, unterminated fence swallows the rest);
//! * indented code lines;
//! * HTML blocks (`<details>`-style) which stay inert until a blank line;
//! * inline code spans, including multi-backtick and multi-line spans;
//! * images (alt text is not a link) and escaped openers;
//! * the canonical inline form only: `[label](mention://type/id)` with no
//!   title, no angle-bracket destination and no surrounding whitespace.
//!
//! A mention also needs visible label text, matching Go's rule that invisible
//! labels (`[]`, `[&#32;]`, `[<script>x</script>]`) never trigger a run.

use regex::Regex;
use std::sync::LazyLock;

/// `[@Label](mention://type/id)` — same shape as Go's `util.MentionRe`, with the
/// label restricted to one line because Go's `.` never crosses a newline.
static MENTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[@?([^\[\]]*?(?:\[[^\[\]]*\][^\[\]]*?)*)\]\(mention://(member|agent|squad|issue|all)/([0-9a-fA-F-]+|all)\)")
        .expect("mention regex")
});
static RAW_HTML_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)<script\b[^>]*>.*?</script\s*>").expect("script regex"));
static HTML_COMMENT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)<!--.*?-->").expect("comment regex"));
static TAG_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)<[^>]*>").expect("tag regex"));
static SCRIPT_OPEN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)<script\b").expect("script open regex"));
static TEMPLATE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?is)<template\b[^>]*>.*?</template\s*>").expect("template regex")
});
static TEMPLATE_OPEN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)<template\b").expect("template open regex"));
static BLOCK_TAG_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)^<(?:address|article|aside|base|basefont|blockquote|body|caption|center|col|colgroup|dd|details|dialog|dir|div|dl|dt|fieldset|figcaption|figure|footer|form|frame|frameset|h[1-6]|head|header|hr|html|iframe|legend|li|link|main|menu|menuitem|nav|noframes|ol|optgroup|option|p|param|search|section|summary|table|tbody|td|tfoot|th|thead|title|tr|track|ul)(?:\s|/?>)",
    )
    .expect("block tag regex")
});

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mention {
    pub kind: String,
    pub id: String,
}

impl Mention {
    pub fn is_all(&self) -> bool {
        self.kind == "all"
    }
}

/// Go's `isNoteComment`: `/note` must be the first whitespace-delimited token.
pub fn is_note_comment(content: &str) -> bool {
    content
        .trim_start_matches([' ', '\t', '\r', '\n'])
        .split(char::is_whitespace)
        .next()
        .is_some_and(|token| token.eq_ignore_ascii_case("/note"))
}

/// Strips one blockquote marker (`>` plus one optional space).
fn strip_quote(line: &str) -> &str {
    let trimmed = line.trim_start_matches([' ', '\t']);
    let indent = line.len() - trimmed.len();
    if indent <= 3
        && let Some(rest) = trimmed.strip_prefix('>')
    {
        return rest.strip_prefix(' ').unwrap_or(rest);
    }
    line
}

fn fence_open(line: &str) -> Option<(char, usize)> {
    let stripped = strip_quote(line);
    let trimmed = stripped.trim_start_matches(' ');
    let indent = stripped.len() - trimmed.len();
    if indent > 3 {
        return None;
    }
    let first = trimmed.chars().next()?;
    if first != '`' && first != '~' {
        return None;
    }
    let run = trimmed.chars().take_while(|c| *c == first).count();
    (run >= 3).then_some((first, run))
}

fn fence_closes(line: &str, marker: char, len: usize) -> bool {
    let stripped = strip_quote(line);
    let trimmed = stripped.trim_start_matches(' ');
    let indent = stripped.len() - trimmed.len();
    if indent > 3 {
        return false;
    }
    let run = trimmed.chars().take_while(|c| *c == marker).count();
    run >= len && trimmed.chars().skip(run).all(|c| c == ' ' || c == '\t')
}

fn is_indented_code(line: &str) -> bool {
    let stripped = strip_quote(line);
    !stripped.trim().is_empty() && stripped.starts_with("    ")
}

fn starts_html_block(line: &str) -> bool {
    let stripped = strip_quote(line).trim_start_matches(' ');
    BLOCK_TAG_RE.is_match(stripped) || stripped.starts_with("<!--")
}

/// Splits content into the text segments that can hold inline links, dropping
/// fenced code, indented code and HTML blocks. Segments are paragraph-scoped so
/// an inline construct cannot span a blank line.
fn markdown_segments(content: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut paragraph = String::new();
    let mut fence: Option<(char, usize)> = None;
    let mut html_block = false;
    let mut html_comment_open = false;

    let flush = |paragraph: &mut String, segments: &mut Vec<String>| {
        if !paragraph.trim().is_empty() {
            segments.push(std::mem::take(paragraph));
        } else {
            paragraph.clear();
        }
    };

    for line in content.split('\n') {
        if let Some((marker, len)) = fence {
            if fence_closes(line, marker, len) {
                fence = None;
            }
            continue;
        }
        if html_block {
            if line.trim().is_empty() {
                html_block = false;
            }
            continue;
        }
        if html_comment_open {
            if line.contains("-->") {
                html_comment_open = false;
            }
            continue;
        }
        if let Some(open) = fence_open(line) {
            flush(&mut paragraph, &mut segments);
            fence = Some(open);
            continue;
        }
        if starts_html_block(line) {
            flush(&mut paragraph, &mut segments);
            if line.trim_start().starts_with("<!--") && !line.contains("-->") {
                html_comment_open = true;
            }
            html_block = true;
            continue;
        }
        if line.trim().is_empty() {
            flush(&mut paragraph, &mut segments);
            continue;
        }
        if is_indented_code(line) {
            flush(&mut paragraph, &mut segments);
            continue;
        }
        if !paragraph.is_empty() {
            paragraph.push('\n');
        }
        paragraph.push_str(strip_quote(line).trim_start_matches(' '));
    }
    flush(&mut paragraph, &mut segments);
    segments
}

/// Byte ranges of inline code spans within one paragraph, per CommonMark: a run
/// of N backticks opens a span that closes at the next run of exactly N.
fn code_spans(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'`' {
            i += 1;
            continue;
        }
        let open = i;
        while i < bytes.len() && bytes[i] == b'`' {
            i += 1;
        }
        let len = i - open;
        let mut j = i;
        let mut close = None;
        while j < bytes.len() {
            if bytes[j] != b'`' {
                j += 1;
                continue;
            }
            let start = j;
            while j < bytes.len() && bytes[j] == b'`' {
                j += 1;
            }
            if j - start == len {
                close = Some((start, j));
                break;
            }
        }
        if let Some((_, end)) = close {
            spans.push((open, end));
            i = end;
        }
    }
    spans
}

/// Byte ranges of image constructs `![alt](dest)` including nested brackets.
fn image_spans(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] != b'!' || bytes[i + 1] != b'[' {
            i += 1;
            continue;
        }
        let start = i;
        let mut depth = 0isize;
        let mut j = i + 1;
        let mut close = None;
        while j < bytes.len() {
            match bytes[j] {
                b'[' => depth += 1,
                b']' => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(j);
                        break;
                    }
                }
                b'\n' => break,
                _ => {}
            }
            j += 1;
        }
        let Some(close) = close else {
            i = start + 1;
            continue;
        };
        if bytes.get(close + 1) != Some(&b'(') {
            i = start + 1;
            continue;
        }
        let mut parens = 0isize;
        let mut end = None;
        let mut k = close + 1;
        while k < bytes.len() {
            match bytes[k] {
                b'(' => parens += 1,
                b')' => {
                    parens -= 1;
                    if parens == 0 {
                        end = Some(k + 1);
                        break;
                    }
                }
                b'\n' => break,
                _ => {}
            }
            k += 1;
        }
        match end {
            Some(end) => {
                spans.push((start, end));
                i = end;
            }
            None => i = start + 1,
        }
    }
    spans
}

fn is_default_ignorable(c: char) -> bool {
    matches!(c as u32,
        0x00AD | 0x034F | 0x061C | 0x115F | 0x1160 | 0x17B4 | 0x17B5
        | 0x180B..=0x180D | 0x200B..=0x200F | 0x202A..=0x202E | 0x2060..=0x206F
        | 0x3164 | 0xFE00..=0xFE0F | 0xFEFF | 0xFFA0 | 0x1D173..=0x1D17A
        | 0xE0000..=0xE0FFF)
}

fn has_visible_label(text: &str) -> bool {
    let mut first = true;
    for c in text.chars() {
        if is_default_ignorable(c) || c == '\u{2800}' {
            continue;
        }
        if c.is_alphabetic() || c.is_numeric() || (!c.is_whitespace() && !c.is_control()) {
            if first && c == '@' {
                first = false;
                continue;
            }
            return true;
        }
        if !c.is_whitespace() {
            first = false;
        }
    }
    false
}

/// Text a reader actually sees for a link label: code spans stay literal, raw
/// `<script>`/`<template>` content is stripped (never rendered), other tags are
/// unwrapped, and text pushed outside the anchor by a raw `</a>` does not count.
///
/// Backslash-escaped punctuation is held behind a private-use placeholder until
/// tag stripping is done, so `\<script>` counts as visible text rather than a
/// tag, and `\@` counts as the literal `@` Go renders.
fn visible_label_text(label: &str) -> String {
    let spans = code_spans(label);
    let mut out = String::new();
    let mut cursor = 0usize;
    let push_plain = |segment: &str, out: &mut String| {
        let (held, saved) = hold_escapes(segment);
        let without_comments = HTML_COMMENT_RE.replace_all(&held, "");
        let truncated = match RAW_CLOSE_ANCHOR_RE.find(&without_comments) {
            Some(m) => &without_comments[..m.start()],
            None => without_comments.as_ref(),
        };
        let mut stripped = TEMPLATE_RE.replace_all(truncated, "").into_owned();
        stripped = RAW_HTML_RE.replace_all(&stripped, "").into_owned();
        // A self-closing or unterminated script/template opener hides the rest.
        if let Some(m) = SCRIPT_OPEN_RE.find(&stripped) {
            stripped.truncate(m.start());
        }
        if let Some(m) = TEMPLATE_OPEN_RE.find(&stripped) {
            stripped.truncate(m.start());
        }
        stripped = TAG_RE.replace_all(&stripped, "").into_owned();
        out.push_str(&release_escapes(&decode_entities(&stripped), &saved));
    };
    for (start, end) in spans {
        push_plain(&label[cursor..start], &mut out);
        // Code-span content is literal text: no escapes, only entity decoding.
        out.push_str(&decode_entities(&label[start..end].replace('`', "")));
        cursor = end;
    }
    push_plain(&label[cursor..], &mut out);
    out
}

const ESCAPE_HOLD: char = '\u{E000}';

fn hold_escapes(text: &str) -> (String, Vec<char>) {
    let mut out = String::with_capacity(text.len());
    let mut saved = Vec::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\'
            && let Some(&next) = chars.peek()
            && next.is_ascii_punctuation()
        {
            chars.next();
            out.push(ESCAPE_HOLD);
            saved.push(next);
            continue;
        }
        out.push(c);
    }
    (out, saved)
}

fn release_escapes(text: &str, saved: &[char]) -> String {
    if saved.is_empty() {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut next = 0usize;
    for c in text.chars() {
        if c == ESCAPE_HOLD && next < saved.len() {
            out.push(saved[next]);
            next += 1;
        } else {
            out.push(c);
        }
    }
    out
}

static RAW_CLOSE_ANCHOR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)</a\s*>").expect("anchor close regex"));

/// Minimal HTML entity decoding for the label characters that decide
/// visibility: whitespace, zero-width and the escaped tag characters.
fn decode_entities(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find('&') {
        out.push_str(&rest[..pos]);
        rest = &rest[pos..];
        let Some(end) = rest.find(';') else {
            out.push_str(rest);
            return out;
        };
        let entity = &rest[1..end];
        let decoded = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            "nbsp" => Some('\u{00A0}'),
            "ZeroWidthSpace" => Some('\u{200B}'),
            "Tab" => Some('\t'),
            "NewLine" => Some('\n'),
            _ => {
                let code = entity
                    .strip_prefix("#x")
                    .or_else(|| entity.strip_prefix("#X"));
                let code = code
                    .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                    .or_else(|| entity.strip_prefix('#').and_then(|d| d.parse::<u32>().ok()));
                code.and_then(char::from_u32)
            }
        };
        match decoded {
            Some(c) => {
                out.push(c);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// Inert positions inside one paragraph: code spans and image constructs. A
/// bracket or paren inside either can never open or close a link.
fn inert_mask(text: &str) -> Vec<bool> {
    let mut mask = vec![false; text.len()];
    for (start, end) in code_spans(text).into_iter().chain(image_spans(text)) {
        for slot in mask.iter_mut().take(end.min(text.len())).skip(start) {
            *slot = true;
        }
    }
    mask
}

/// One inline link `[label](destination)` located in the paragraph. Only the
/// `<...>`-free, title-free, padding-free destination form the Go canonical
/// regex accepts can carry a mention, but every link is consumed as a unit so a
/// nested bracket pair in its label cannot be mistaken for a second link.
#[derive(Debug)]
struct InlineLink {
    start: usize,
    end: usize,
}

/// Byte index of the `]` matching the `[` at `open`, honouring nesting, escapes
/// and inert spans (code, images). `None` when the paragraph closes first.
fn matching_bracket(text: &str, open: usize, mask: &[bool]) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        if mask[i] {
            i += 1;
            continue;
        }
        match bytes[i] {
            b'\\' => {
                i += 2;
                continue;
            }
            b'[' => depth += 1,
            b']' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Index just past the `)` closing the inline destination that starts at
/// `open` (the `(` itself). `None` when the destination never closes.
fn matching_paren(text: &str, open: usize, mask: &[bool]) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        if mask[i] {
            i += 1;
            continue;
        }
        match bytes[i] {
            b'\\' => {
                i += 2;
                continue;
            }
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            b'\n' => return None,
            _ => {}
        }
        i += 1;
    }
    None
}

/// Offsets of every inline link in one paragraph, in document order.
fn inline_links(text: &str) -> Vec<InlineLink> {
    let mask = inert_mask(text);
    let bytes = text.as_bytes();
    let mut links = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if mask[i] {
            i += 1;
            continue;
        }
        match bytes[i] {
            b'\\' => {
                i += 2;
                continue;
            }
            b'!' if bytes.get(i + 1) == Some(&b'[') => {
                // Image: its alt text is not link content, and the whole
                // construct is skipped so a mention inside it stays inert.
                if let Some(close) = matching_bracket(text, i + 1, &mask)
                    && bytes.get(close + 1) == Some(&b'(')
                    && let Some(end) = matching_paren(text, close + 1, &mask)
                {
                    i = end;
                    continue;
                }
                i += 1;
            }
            b'[' => {
                let Some(close) = matching_bracket(text, i, &mask) else {
                    i += 1;
                    continue;
                };
                if bytes.get(close + 1) != Some(&b'(') {
                    i += 1;
                    continue;
                }
                let Some(end) = matching_paren(text, close + 1, &mask) else {
                    i += 1;
                    continue;
                };
                links.push(InlineLink { start: i, end });
                i = end;
            }
            _ => i += 1,
        }
    }
    links
}

/// Deduplicated mentions in document order, matching Go's `ParseMentions`.
pub fn parse_mentions(content: &str) -> Vec<Mention> {
    let mut seen = std::collections::HashSet::new();
    let mut mentions = Vec::new();
    for segment in markdown_segments(content) {
        for link in inline_links(&segment) {
            let source = &segment[link.start..link.end];
            // Canonical inline source form only: no title, no angle-bracket
            // destination, no padding around it.
            let Some(caps) = MENTION_RE
                .captures(source)
                .filter(|caps| caps.get(0).is_some_and(|m| m.as_str() == source))
            else {
                continue;
            };
            let label = caps.get(1).expect("label").as_str();
            if !has_visible_label(&visible_label_text(label)) {
                continue;
            }
            let kind = caps.get(2).expect("kind").as_str().to_string();
            let id = caps.get(3).expect("id").as_str().to_string();
            if seen.insert(format!("{kind}:{id}")) {
                mentions.push(Mention { kind, id });
            }
        }
    }
    mentions
}

pub fn has_all(mentions: &[Mention]) -> bool {
    mentions.iter().any(Mention::is_all)
}

pub fn has_agent_or_squad(mentions: &[Mention]) -> bool {
    mentions
        .iter()
        .any(|m| m.kind == "agent" || m.kind == "squad")
}

pub fn has_member(mentions: &[Mention]) -> bool {
    mentions.iter().any(|m| m.kind == "member")
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    const B: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    fn agent(id: &str) -> Mention {
        Mention {
            kind: "agent".into(),
            id: id.into(),
        }
    }

    fn parsed(content: &str) -> Vec<Mention> {
        parse_mentions(content)
    }

    fn m(kind: &str, id: &str) -> Mention {
        Mention {
            kind: kind.into(),
            id: id.into(),
        }
    }

    /// Case table ported verbatim from Go's `internal/util/mention_test.go`
    /// (86 cases, generated from the Go source to keep the expectations exact).
    #[test]
    fn matches_go_mention_cases() {
        let cases: &[(&str, &str, Vec<Mention>)] = &[
            (
                "simple agent mention",
                "[@Agent](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) please fix",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "agent name with square brackets",
                "[@David[TF]](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) please fix",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "agent name with nested brackets",
                "[@Bot[v2][beta]](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) help",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "multiple mentions with brackets",
                "[@A[1]](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) and [@B[2]](mention://agent/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)",
                vec![
                    m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                    m("agent", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                ],
            ),
            (
                "issue mention without @",
                "[MUL-123](mention://issue/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) is related",
                vec![m("issue", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "member mention",
                "[@Bob](mention://member/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) look",
                vec![m("member", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "all mention",
                "[@All](mention://all/all) heads up",
                vec![m("all", "all")],
            ),
            (
                "deduplicate same mention",
                "[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) and again [@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            ("no mentions", "just a plain comment", vec![]),
            (
                "unmatched link opener does not cross paragraph boundary",
                "[\n\n]",
                vec![],
            ),
            (
                "unmatched image opener does not cross paragraph boundary",
                "![\n\n]\n\n[@Agent](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "escaped brackets in label",
                "[@David\\[TF\\]](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) hi",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "visible agent label without optional at prefix",
                "[Agent](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "empty label is ignored",
                "[](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "whitespace label is ignored",
                "[  ](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "at prefix without a label is ignored",
                "[@](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "entity whitespace label is ignored",
                "[&#32;](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "HTML only label is ignored",
                "[<span></span>](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "script content removed by sanitization is not a visible label",
                "[<script>Agent</script>](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "script content cannot turn an at prefix into a visible label",
                "[<script>Agent</script>@](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "template content is not a visible label",
                "[<template>Agent</template>](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "template content cannot turn an at prefix into a visible label",
                "[<template>Agent</template>@](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "uppercase template with attributes is ignored",
                "[<TEMPLATE data-label=\"Agent\"><span>Agent</span></TEMPLATE>](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "self closing template still hides following label text",
                "[<template/>Agent](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "visible label after nested templates still triggers",
                "[<template><template>hidden</template></template>Bob](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "visible label before template still triggers",
                "[Bob<template>hidden</template>](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "literal template in code label stays visible",
                "[`<template>Agent</template>`](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "escaped template label stays visible",
                "[&lt;template&gt;Agent&lt;/template&gt;](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "uppercase script with attributes is ignored",
                "[<SCRIPT type=\"text/plain\">Agent</SCRIPT>](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "self closing script still hides following label text",
                "[<script/>Agent](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "nested script opener does not hide the real closing tag",
                "[<script>outer<script>inner</script>@](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "formatted script text is still hidden",
                "[<script>**Agent**</script>](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "text moved outside the mention by raw HTML does not validate its label",
                "[</a>Bob](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "visible span label still triggers",
                "[<span>Bob</span>](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "visible label after script still triggers",
                "[<script>hidden</script><span>Bob</span>](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "visible label before script still triggers",
                "[Bob<script>hidden</script>](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "style text unwrapped by sanitization stays visible",
                "[<style>Agent</style>](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "literal script in code label stays visible",
                "[`<script>Agent</script>`](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "escaped script label stays visible",
                "[&lt;script&gt;Agent&lt;/script&gt;](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "full reference link is ignored",
                "[@Agent][target]\n\n[target]: mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                vec![],
            ),
            (
                "collapsed reference link is ignored",
                "[@Agent][]\n\n[@Agent]: mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                vec![],
            ),
            (
                "shortcut reference link is ignored",
                "[@Agent]\n\n[@Agent]: mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                vec![],
            ),
            (
                "angle bracket destination is ignored",
                "[@Agent](<mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa>)",
                vec![],
            ),
            (
                "link with title is ignored",
                "[@Agent](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa \"title\")",
                vec![],
            ),
            (
                "destination whitespace is ignored",
                "[@Agent]( mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa )",
                vec![],
            ),
            (
                "entity encoded destination is ignored",
                "[@Agent](mention&#58;//agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "canonical text inside label cannot validate noncanonical outer link",
                "[@Agent `[Example](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)`](<mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa>)",
                vec![],
            ),
            (
                "mention in HTML comment label cannot validate outer link",
                "[A <!-- [X](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) -->](<mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa>)",
                vec![],
            ),
            (
                "mention in image destination cannot validate outer link",
                "[A ![x](\"[X](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\")](<mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa>)",
                vec![],
            ),
            (
                "zero width entity label is ignored",
                "[&#x200B;](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "named zero width entity label is ignored",
                "[&ZeroWidthSpace;](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "invisible format label is ignored",
                "[\u{200b}﻿](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "invisible leading text cannot turn at prefix into a label",
                "[\u{200b}@](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) [&#x200B;@](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) [ㅤ@](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "Hangul filler label is ignored",
                "[ᅟᅠㅤﾠ](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "Hangul filler entities are ignored",
                "[&#x115F;&#x1160;&#x3164;&#xFFA0;](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "braille blank label is ignored",
                "[\u{2800}](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "braille blank entity is ignored",
                "[&#x2800;](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "visible Unicode label with invisible format still triggers",
                "[@\u{200b}助手](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "visible inline code label still triggers",
                "[`Agent`](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "escaped at prefix without label is ignored",
                "[@](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa) [\\@](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "mention inside image alt text is ignored",
                "![[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)](https://example.com/image.png)",
                vec![],
            ),
            (
                "prose next to image alt mention still triggers",
                "![[@B](mention://agent/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)](https://example.com/image.png) [@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "image is ignored",
                "![@Agent](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "escaped link opener is ignored",
                "\\[@Agent](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
            (
                "autolink is ignored",
                "<mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa>",
                vec![],
            ),
            (
                "ignored reference does not hide later canonical mention",
                "[@Agent][target] then [@Agent](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n\n[target]: mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "mention inside fenced code block is ignored",
                "```\n[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n```",
                vec![],
            ),
            (
                "mention inside tilde fenced code block is ignored",
                "~~~\n[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n~~~",
                vec![],
            ),
            (
                "mention inside fenced block with info string is ignored",
                "```markdown\n[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n```",
                vec![],
            ),
            (
                "mention inside inline code span is ignored",
                "use `[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)` to mention",
                vec![],
            ),
            (
                "mention inside double-backtick span containing a backtick is ignored",
                "use ``[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)` `` to mention",
                vec![],
            ),
            (
                "mention before a fenced block is still returned",
                "[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n```\n[@B](mention://agent/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)\n```",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "mention after a closing fence is still returned",
                "```\ncode\n```\n[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "mention inside blockquote is still returned",
                "> [@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "unterminated fence suppresses everything after it",
                "```\n[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\nand more text",
                vec![],
            ),
            (
                "longer closing fence still closes and mention after it is returned",
                "```\ncode\n````\n[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "four-space indented fence is not a closer so mention stays inert",
                "```\ncode\n    ```\n[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n```",
                vec![],
            ),
            (
                "mention inside multiline code span is ignored",
                "use ``[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\nstill code`` to mention",
                vec![],
            ),
            (
                "fenced code inside a blockquote is ignored",
                "> ```\n> [@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n> ```",
                vec![],
            ),
            (
                "prose mention inside a blockquote is still returned next to quoted code",
                "> [@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n>\n> ```\n> [@B](mention://agent/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)\n> ```",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "fenced diff inside details is ignored",
                "<details>\n<summary>Agent instructions diff</summary>\n\n```diff\n- [@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n+ [@B](mention://agent/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)\n```\n\n</details>",
                vec![],
            ),
            (
                "details without blank line stays inert",
                "<details>\n<summary>Diff</summary>\n```diff\n+ [@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n```\n</details>",
                vec![],
            ),
            (
                "prose after details diff still triggers",
                "<details>\n<summary>Diff</summary>\n\n```diff\n+ [@B](mention://agent/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)\n```\n\n</details>\n\n[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "prose inside details with blank line still triggers",
                "<details>\n<summary>Request</summary>\n\n[@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n\n</details>",
                vec![m("agent", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")],
            ),
            (
                "fenced diff inside list is ignored",
                "- Instructions:\n\n  ```diff\n  + [@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)\n  ```",
                vec![],
            ),
            (
                "indented code is ignored",
                "    [@A](mention://agent/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)",
                vec![],
            ),
        ];
        for (name, content, want) in cases {
            let got = parse_mentions(content);
            assert_eq!(&got, want, "case {name}: {content}");
        }
    }

    #[test]
    fn deduplicates_in_document_order() {
        let content = format!(
            "[@A](mention://agent/{A}) and [@B](mention://agent/{B}) and [@A](mention://agent/{A})"
        );
        assert_eq!(parsed(&content), vec![agent(A), agent(B)]);
    }

    #[test]
    fn note_prefix_is_the_first_token_only() {
        for note in ["/note", "/NOTE check", "  /note\nexpiry", "\t/Note x"] {
            assert!(is_note_comment(note), "{note}");
        }
        for normal in ["/notes", "/ note", "see foo/note", "note", ""] {
            assert!(!is_note_comment(normal), "{normal}");
        }
    }

    #[test]
    fn mention_classes() {
        let mentions = parsed(&format!(
            "[@A](mention://agent/{A}) [@S](mention://squad/{A}) [@M](mention://member/{B}) [@all](mention://all/all)"
        ));
        assert!(has_agent_or_squad(&mentions));
        assert!(has_member(&mentions));
        assert!(has_all(&mentions));
        assert!(!has_all(&parsed(&format!("[@A](mention://agent/{A})"))));
    }
}
