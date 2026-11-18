//! Attachment-aware Markdown comparison; only stable attachment IDs enter the diff.
use super::SourceAttachment;
use serde_json::{Value, json};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Reference {
    id: Uuid,
    filename: String,
}

struct Candidate<'a> {
    start: usize,
    end: usize,
    url_start: usize,
    url_end: usize,
    filename: &'a str,
    url: &'a str,
}

fn delimiter(bytes: &[u8], start: usize, target: u8) -> Option<usize> {
    let mut escaped = false;
    for (index, &byte) in bytes.iter().enumerate().skip(start) {
        if escaped {
            escaped = false;
            continue;
        }
        if byte == b'\\' {
            escaped = true;
            continue;
        }
        if byte == target {
            return Some(index);
        }
    }
    None
}

fn candidate(content: &str, start: usize) -> Option<Candidate<'_>> {
    let bytes = content.as_bytes();
    if start > 0 && bytes[start - 1] == b'\\' {
        return None;
    }
    let prefix = if content[start..].starts_with("!file[") {
        6
    } else if content[start..].starts_with("![") {
        2
    } else if content[start..].starts_with('[') {
        1
    } else {
        return None;
    };
    let label_end = delimiter(bytes, start + prefix, b']')?;
    if bytes.get(label_end + 1) != Some(&b'(') {
        return None;
    }
    let url_start = label_end + 2;
    let url_end = delimiter(bytes, url_start, b')')?;
    Some(Candidate {
        start,
        end: url_end + 1,
        url_start,
        url_end,
        filename: &content[start + prefix..label_end],
        url: &content[url_start..url_end],
    })
}

fn aliases(
    captured: &[SourceAttachment],
    current: &[SourceAttachment],
) -> (HashMap<Uuid, Uuid>, HashMap<Uuid, String>) {
    let mut alias = HashMap::new();
    let mut filenames = HashMap::new();
    for item in captured.iter().chain(current) {
        let id = item.source_attachment_id.unwrap_or(item.id);
        alias.insert(id, id);
        alias.insert(item.id, id);
        filenames.insert(item.id, item.filename.clone());
        if let Some(source) = item.source_attachment_id {
            filenames.insert(source, item.filename.clone());
        }
    }
    (alias, filenames)
}

fn attachment_id(url: &str, filenames: &HashMap<Uuid, String>) -> Option<Uuid> {
    let stable = url.trim().split(['#', '?']).next()?;
    let path = if stable.starts_with("/api/") {
        stable
    } else if let Some(after_scheme) = stable
        .strip_prefix("https://")
        .or_else(|| stable.strip_prefix("http://"))
    {
        let slash = after_scheme.find('/')?;
        &after_scheme[slash..]
    } else {
        ""
    };
    if let Some(id) = path
        .strip_prefix("/api/attachments/")
        .and_then(|s| s.strip_suffix("/download"))
        && !id.contains('/')
        && let Ok(id) = Uuid::parse_str(id)
    {
        return Some(id);
    }
    let lower = stable.to_ascii_lowercase();
    filenames
        .keys()
        .find(|id| lower.contains(&id.to_string()))
        .copied()
}

fn references<'a>(
    content: &'a str,
    captured: &[SourceAttachment],
    current: &[SourceAttachment],
) -> Vec<(Candidate<'a>, Uuid, String)> {
    let (alias, filenames) = aliases(captured, current);
    let mut result = Vec::new();
    let mut offset = 0;
    while offset < content.len() {
        if !content.is_char_boundary(offset) {
            offset += 1;
            continue;
        }
        if let Some(found) = candidate(content, offset) {
            offset = found.end;
            if let Some(id) = attachment_id(found.url, &filenames) {
                let Some(id) = alias.get(&id).copied() else {
                    continue;
                };
                let label = found
                    .filename
                    .replace(r"\[", "[")
                    .replace(r"\]", "]")
                    .replace(r"\(", "(")
                    .replace(r"\)", ")")
                    .replace(r"\\", r"\");
                let filename = if label.is_empty() {
                    filenames.get(&id).cloned().unwrap_or_default()
                } else {
                    label
                };
                result.push((found, id, filename));
            }
        } else {
            offset += 1;
        }
    }
    result
}

fn standalone_range(content: &str, start: usize, end: usize) -> (usize, usize) {
    let mut line_start = content[..start].rfind('\n').map_or(0, |p| p + 1);
    let mut line_end = content[end..]
        .find('\n')
        .map_or(content.len(), |p| end + p + 1);
    let line_content_end = if content.as_bytes().get(line_end.wrapping_sub(1)) == Some(&b'\n') {
        line_end - 1
    } else {
        line_end
    };
    if !content[line_start..start].trim().is_empty()
        || !content[end..line_content_end].trim().is_empty()
    {
        return (start, end);
    }
    if line_start > 0 {
        let previous_end = line_start - 1;
        let previous_start = content[..previous_end].rfind('\n').map_or(0, |p| p + 1);
        if content[previous_start..previous_end].trim().is_empty() {
            line_start = previous_start;
            return (line_start, line_end);
        }
    }
    if line_end < content.len() {
        let next_end = content[line_end..]
            .find('\n')
            .map_or(content.len(), |p| line_end + p + 1);
        if content[line_end..next_end].trim().is_empty() {
            line_end = next_end;
        }
    }
    (line_start, line_end)
}

pub(super) fn description_projection(
    content: &str,
    captured: &[SourceAttachment],
    current: &[SourceAttachment],
) -> Option<(String, Vec<Reference>)> {
    let content = content.replace("\r\n", "\n");
    let mut body = String::new();
    let mut last = 0;
    let mut refs = Vec::new();
    for (found, id, filename) in references(&content, captured, current) {
        let (mut start, mut end) = standalone_range(&content, found.start, found.end);
        if start < last {
            start = found.start;
            end = found.end;
        }
        body.push_str(&content[last..start]);
        last = end;
        refs.push(Reference { id, filename });
    }
    body.push_str(&content[last..]);
    // Unknown download references may contain historical private locations.
    if body.contains("/api/attachments/") {
        return None;
    }
    Some((body.trim().to_owned(), refs))
}

pub(super) fn markdown_content_projection(
    content: &str,
    captured: &[SourceAttachment],
    current: &[SourceAttachment],
) -> String {
    let content = content.replace("\r\n", "\n");
    let mut result = String::new();
    let mut last = 0;
    for (found, id, _) in references(&content, captured, current) {
        result.push_str(&content[last..found.url_start]);
        result.push_str("source-context-attachment:");
        result.push_str(&id.to_string());
        last = found.url_end;
    }
    result.push_str(&content[last..]);
    result
}

pub(super) fn description_attachment_changes(
    captured: &[Reference],
    current: &[Reference],
) -> Vec<Value> {
    let mut matches: HashMap<Uuid, Vec<&Reference>> = HashMap::new();
    for item in current {
        matches.entry(item.id).or_default().push(item);
    }
    let mut matched: HashMap<Uuid, usize> = HashMap::new();
    let mut result = Vec::new();
    for item in captured {
        let index = matched.entry(item.id).or_default();
        if let Some(other) = matches.get(&item.id).and_then(|all| all.get(*index)) {
            *index += 1;
            if item.filename != other.filename {
                result.push(json!({"kind":"replaced","attachment_id":item.id,"filename":other.filename,"previous_filename":item.filename}));
            }
        } else {
            result.push(json!({"kind":"removed","attachment_id":item.id,"filename":item.filename}));
        }
    }
    let mut seen = HashMap::<Uuid, usize>::new();
    for item in current {
        *seen.entry(item.id).or_default() += 1;
        if seen[&item.id] > *matched.get(&item.id).unwrap_or(&0) {
            result.push(json!({"kind":"added","attachment_id":item.id,"filename":item.filename}));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    fn attachment(id: Uuid, source: Option<Uuid>) -> SourceAttachment {
        SourceAttachment {
            id,
            source_attachment_id: source,
            owner_type: "issue".into(),
            owner_id: Uuid::nil(),
            filename: "ảnh.png".into(),
            content_type: "image/png".into(),
            size_bytes: 4,
            created_at: "2026-11-03T00:00:00Z".into(),
        }
    }

    #[test]
    fn queries_sprint_markdown_projection_clone_and_utf8() {
        let original = Uuid::new_v4();
        let clone = Uuid::new_v4();
        let captured = [attachment(clone, Some(original))];
        let live = [attachment(original, None)];
        let before = format!(
            "Nội dung\n\n!file[ảnh.png](https://safe.invalid/api/attachments/{clone}/download?x=1)"
        );
        let now = format!("Nội dung\n\n!file[ảnh.png](/api/attachments/{original}/download)");
        let a = description_projection(&before, &captured, &live).unwrap();
        let b = description_projection(&now, &captured, &live).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.0, "Nội dung");
        assert_eq!(a.1[0].id, original);
        assert_eq!(
            markdown_content_projection(&before, &captured, &live),
            markdown_content_projection(&now, &captured, &live)
        );
        let gone = description_projection("Nội dung", &captured, &live).unwrap();
        assert_eq!(
            description_attachment_changes(&a.1, &gone.1),
            vec![json!({
                "kind":"removed","attachment_id":original,"filename":"ảnh.png"
            })]
        );
    }

    #[test]
    fn queries_sprint_markdown_rejects_unknown_download_and_preserves_external_links() {
        let id = Uuid::new_v4();
        let body = format!("[old](/api/attachments/{id}/download)");
        assert!(description_projection(&body, &[], &[]).is_none());
        assert!(markdown_content_projection(&body, &[], &[]).contains("/api/attachments/"));
        let external = "[docs](https://example.org/guide)";
        assert_eq!(
            description_projection(external, &[], &[]).unwrap().0,
            external
        );
        let escaped = format!(r"\[old](/api/attachments/{id}/download)");
        assert!(description_projection(&escaped, &[], &[]).is_none());
    }
}
