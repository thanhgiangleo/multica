//! Frozen Go request decoding for per-user issue-view preferences.
use super::super::account_catalog_write_profile::account_request;
use serde::de::IgnoredAny;
use uuid::Uuid;

const MAX_BODY: usize = 128 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Request {
    pub scope_type: String,
    pub scope_id: Option<String>,
    pub prefs: Vec<u8>,
}

fn invalid() -> String {
    "invalid request body".to_owned()
}

fn whitespace(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
    {
        index += 1;
    }
    index
}

fn string_end(bytes: &[u8], index: usize) -> Option<usize> {
    if bytes.get(index) != Some(&b'"') {
        return None;
    }
    let mut escaped = false;
    for (cursor, byte) in bytes.iter().enumerate().skip(index + 1) {
        match byte {
            b'"' if !escaped => return Some(cursor + 1),
            b'\\' if !escaped => escaped = true,
            _ => escaped = false,
        }
    }
    None
}

fn value_end(bytes: &[u8], start: usize) -> Option<usize> {
    match bytes.get(start)? {
        b'"' => string_end(bytes, start),
        b'{' | b'[' => {
            let mut stack = vec![bytes[start]];
            let mut cursor = start + 1;
            while cursor < bytes.len() {
                match bytes[cursor] {
                    b'"' => cursor = string_end(bytes, cursor)?,
                    b'{' | b'[' => {
                        stack.push(bytes[cursor]);
                        cursor += 1;
                    }
                    b'}' | b']' => {
                        stack.pop()?;
                        cursor += 1;
                        if stack.is_empty() {
                            return Some(cursor);
                        }
                    }
                    _ => cursor += 1,
                }
            }
            None
        }
        _ => {
            let mut cursor = start;
            while bytes.get(cursor).is_some_and(|byte| {
                !matches!(byte, b' ' | b'\t' | b'\r' | b'\n' | b',' | b'}' | b']')
            }) {
                cursor += 1;
            }
            Some(cursor)
        }
    }
}

pub(in crate::api) fn folded(key: &str, expected: &str) -> bool {
    key.chars()
        .map(|ch| match ch {
            'ſ' => 's',
            _ => ch.to_ascii_lowercase(),
        })
        .eq(expected.chars())
}

// pgx UUID.Scan accepts 32 hex digits or 36 bytes with the separator positions
// discarded (even when those bytes are not hyphens). uuid::Uuid::parse_str has
// different accepted forms, so keep the request boundary compatible with Go.
pub(in crate::api) fn parse_go_uuid(raw: &str) -> Option<Uuid> {
    let bytes = raw.as_bytes();
    let compact = match bytes.len() {
        32 => bytes.to_vec(),
        36 => bytes
            .iter()
            .enumerate()
            .filter_map(|(index, byte)| (!matches!(index, 8 | 13 | 18 | 23)).then_some(*byte))
            .collect(),
        _ => return None,
    };
    // Reject non-hex data, including accepted by uuid::parse_str but not pgx.
    if compact.len() != 32 || !compact.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    Uuid::parse_str(std::str::from_utf8(&compact).expect("ASCII hex digits")).ok()
}

pub(in crate::api) async fn read_prefix(mut body: axum::body::Body) -> Result<Vec<u8>, String> {
    use axum::body::HttpBody;
    use std::{future::poll_fn, pin::Pin};

    let mut bytes = Vec::new();
    let mut started = false;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        let frame = frame.map_err(|_| invalid())?;
        if let Ok(data) = frame.into_data() {
            for byte in data {
                bytes.push(byte);
                if !started && matches!(byte, b' ' | b'\t' | b'\r' | b'\n') {
                    if bytes.len() == MAX_BODY {
                        return Ok(bytes);
                    }
                    continue;
                }
                started = true;
                if in_string {
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        in_string = false;
                    }
                } else {
                    match byte {
                        b'"' => in_string = true,
                        b'{' | b'[' => depth += 1,
                        b'}' | b']' => {
                            depth = depth.saturating_sub(1);
                            if depth == 0 {
                                return Ok(bytes);
                            }
                        }
                        _ => {}
                    }
                }
                if bytes.len() == MAX_BODY {
                    return Ok(bytes);
                }
            }
        }
    }
    Ok(bytes)
}

pub(in crate::api) fn fields(body: &[u8]) -> Result<Vec<(String, Vec<u8>)>, String> {
    // MaxBytesReader bounds the first decoded value, not unread trailing bytes.
    let bytes = &body[..body.len().min(MAX_BODY)];
    account_request::<IgnoredAny>(bytes).map_err(|_| invalid())?;
    let mut fields = Vec::new();
    let mut cursor = whitespace(bytes, 0);
    if bytes.get(cursor..cursor + 4) == Some(b"null") {
        return Ok(fields);
    }
    if bytes.get(cursor) != Some(&b'{') {
        return Err(invalid());
    }
    cursor += 1;
    loop {
        cursor = whitespace(bytes, cursor);
        if bytes.get(cursor) == Some(&b'}') {
            return Ok(fields);
        }
        let end = string_end(bytes, cursor).ok_or_else(invalid)?;
        let key: String = account_request(&bytes[cursor..end]).map_err(|_| invalid())?;
        cursor = whitespace(bytes, end);
        if bytes.get(cursor) != Some(&b':') {
            return Err(invalid());
        }
        cursor = whitespace(bytes, cursor + 1);
        let end = value_end(bytes, cursor).ok_or_else(invalid)?;
        fields.push((key, bytes[cursor..end].to_vec()));
        cursor = whitespace(bytes, end);
        match bytes.get(cursor) {
            Some(b',') => cursor += 1,
            Some(b'}') => return Ok(fields),
            _ => return Err(invalid()),
        }
    }
}

pub(super) fn decode(body: &[u8]) -> Result<Request, String> {
    let mut request = Request {
        scope_type: String::new(),
        scope_id: None,
        prefs: b"{}".to_vec(),
    };
    for (key, raw) in fields(body)? {
        if folded(&key, "scope_type") {
            // json.Unmarshal into an existing Go string leaves it unchanged for null.
            if raw != b"null" {
                request.scope_type = account_request(&raw).map_err(|_| invalid())?;
            }
        } else if folded(&key, "scope_id") {
            request.scope_id = account_request(&raw).map_err(|_| invalid())?;
        } else if folded(&key, "prefs") {
            request.prefs = raw;
        }
    }
    Ok(request)
}

pub(super) fn prefs_is_object(raw: &[u8]) -> bool {
    // The outer JSON decoder has already validated this raw value. Like Go's
    // map[string]RawMessage, an object is accepted and null is rejected.
    raw.first() == Some(&b'{')
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use serde_json::Value;

    #[test]
    fn pgx_uuid_scan_request_forms() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let expected = Uuid::parse_str(id).unwrap();
        assert_eq!(parse_go_uuid(id), Some(expected));
        assert_eq!(
            parse_go_uuid("550e8400e29b41d4a716446655440000"),
            Some(expected)
        );
        assert_eq!(
            parse_go_uuid("550e8400xe29bx41d4xa716x446655440000"),
            Some(expected)
        );
        assert!(parse_go_uuid(&format!("{{{id}}}")).is_none());
        assert!(parse_go_uuid(&format!("urn:uuid:{id}")).is_none());
        assert!(parse_go_uuid("550e8400-e29b-41d4-a716-44665544000g").is_none());
    }

    #[test]
    fn matches_frozen_go_decoder_and_object_validation() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_view_preference_oracle.json"
        ))
        .unwrap();
        for case in cases {
            let label = case["label"].as_str().unwrap();
            let raw = base64::engine::general_purpose::STANDARD
                .decode(case["body"].as_str().unwrap())
                .unwrap();
            let result = decode(&raw);
            if case["decode_error"] == true {
                assert_eq!(result, Err(invalid()), "{label}");
            } else {
                let request = result.unwrap_or_else(|message| panic!("{label}: {message}"));
                assert_eq!(request.scope_type, case["scope_type"], "{label}");
                assert_eq!(
                    request.scope_id.as_deref(),
                    case["scope_id"].as_str(),
                    "{label}"
                );
                assert_eq!(
                    request.prefs,
                    base64::engine::general_purpose::STANDARD
                        .decode(case["prefs_base64"].as_str().unwrap())
                        .unwrap(),
                    "{label}"
                );
                assert_eq!(
                    prefs_is_object(&request.prefs),
                    case["prefs_is_object"] == true,
                    "{label}"
                );
            }
        }
    }
}
