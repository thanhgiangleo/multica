//! Comment request decoding follows Go's streaming encoding/json struct decoder.
use serde::{
    Deserialize,
    de::{self, MapAccess, Visitor},
};

/// Mirrors frozen Go `CreateCommentRequest`. Ordinary fields keep the zero value
/// when the key is absent or explicitly null; pointer and slice fields are the
/// only ones a null clears.
#[derive(Debug, Default)]
pub(super) struct CreateRequest {
    pub content: String,
    pub kind: String,
    pub parent_id: Option<String>,
    pub attachment_ids: Option<Vec<String>>,
    pub suppress_agent_ids: Option<Vec<String>>,
    pub steer_task_ids: Option<Vec<String>>,
    pub steer: bool,
}

#[derive(Debug, Default)]
pub(super) struct EditRequest {
    pub content: String,
    pub content_base: Option<String>,
    pub attachment_ids: Option<Vec<String>>,
    pub suppress_agent_ids: Option<Vec<String>>,
    pub expected_revision: Option<i64>,
}

pub(super) fn folded(key: &str) -> String {
    key.chars()
        .map(|c| match c {
            '\u{017f}' => 's',
            '\u{212a}' => 'k',
            c => c.to_ascii_lowercase(),
        })
        .collect()
}

pub(super) fn strings<'de, M: MapAccess<'de>>(
    map: &mut M,
    backing: &mut Vec<String>,
) -> Result<Option<Vec<String>>, M::Error> {
    let Some(items) = map.next_value::<Option<Vec<Option<String>>>>()? else {
        backing.clear();
        return Ok(None);
    };
    // encoding/json reuses slice capacity across duplicate keys. A null string
    // element retains any value already at that backing index.
    let len = items.len();
    if len == 0 {
        backing.clear();
    }
    for (index, item) in items.into_iter().enumerate() {
        if index == backing.len() {
            backing.push(item.unwrap_or_default());
        } else if let Some(value) = item {
            backing[index] = value;
        }
    }
    Ok(Some(backing[..len].to_vec()))
}

impl<'de> Deserialize<'de> for CreateRequest {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RequestVisitor;
        impl<'de> Visitor<'de> for RequestVisitor {
            type Value = CreateRequest;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object or null")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(CreateRequest::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut req = CreateRequest::default();
                let (mut attachments, mut suppressed, mut steer_tasks) =
                    (Vec::new(), Vec::new(), Vec::new());
                while let Some(key) = map.next_key::<String>()? {
                    match folded(&key).as_str() {
                        "content" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                req.content = value;
                            }
                        }
                        "type" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                req.kind = value;
                            }
                        }
                        "parent_id" => req.parent_id = map.next_value()?,
                        "attachment_ids" => {
                            req.attachment_ids = strings(&mut map, &mut attachments)?
                        }
                        "suppress_agent_ids" => {
                            req.suppress_agent_ids = strings(&mut map, &mut suppressed)?
                        }
                        "steer_task_ids" => {
                            req.steer_task_ids = strings(&mut map, &mut steer_tasks)?
                        }
                        "steer" => {
                            if let Some(value) = map.next_value::<Option<bool>>()? {
                                req.steer = value;
                            }
                        }
                        _ => {
                            map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(req)
            }
        }
        deserializer.deserialize_any(RequestVisitor)
    }
}

impl<'de> Deserialize<'de> for EditRequest {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RequestVisitor;
        impl<'de> Visitor<'de> for RequestVisitor {
            type Value = EditRequest;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object or null")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(EditRequest::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut req = EditRequest::default();
                let (mut attachments, mut suppressed) = (Vec::new(), Vec::new());
                while let Some(key) = map.next_key::<String>()? {
                    match folded(&key).as_str() {
                        "content" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                req.content = value;
                            }
                        }
                        "content_base" => req.content_base = map.next_value()?,
                        "attachment_ids" => {
                            req.attachment_ids = strings(&mut map, &mut attachments)?
                        }
                        "suppress_agent_ids" => {
                            req.suppress_agent_ids = strings(&mut map, &mut suppressed)?
                        }
                        "expected_revision" => req.expected_revision = map.next_value()?,
                        _ => {
                            map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(req)
            }
        }
        deserializer.deserialize_any(RequestVisitor)
    }
}

fn code_unit(bytes: &[u8]) -> Option<u16> {
    let escape = bytes.get(..6)?;
    if &escape[..2] != b"\\u" {
        return None;
    }
    escape[2..].iter().try_fold(0u16, |value, byte| {
        Some(value * 16 + char::from(*byte).to_digit(16)? as u16)
    })
}

pub(super) fn normalized_strings(body: &[u8]) -> Vec<u8> {
    // encoding/json replaces invalid UTF-8 bytes and unpaired UTF-16 escapes
    // INSIDE strings. Do not repair syntax, controls, escapes or outside bytes.
    let mut out = Vec::with_capacity(body.len());
    let mut in_string = false;
    let mut index = 0;
    while index < body.len() {
        let byte = body[index];
        if byte == b'"' {
            in_string = !in_string;
        } else if in_string && byte == b'\\' && index + 1 < body.len() {
            if let Some(unit) = code_unit(&body[index..]) {
                if (0xd800..=0xdbff).contains(&unit)
                    && code_unit(&body[index + 6..])
                        .is_some_and(|low| (0xdc00..=0xdfff).contains(&low))
                {
                    out.extend_from_slice(&body[index..index + 12]);
                    index += 12;
                    continue;
                }
                if (0xd800..=0xdfff).contains(&unit) {
                    out.extend_from_slice(b"\\ufffd");
                    index += 6;
                    continue;
                }
            }
            out.extend_from_slice(&body[index..index + 2]);
            index += 2;
            continue;
        } else if in_string && !byte.is_ascii() {
            let width = match byte {
                0xc2..=0xdf => 2,
                0xe0..=0xef => 3,
                0xf0..=0xf4 => 4,
                _ => 1,
            };
            let end = (index + width).min(body.len());
            if width > 1 && std::str::from_utf8(&body[index..end]).is_ok() {
                out.extend_from_slice(&body[index..end]);
                index = end;
            } else {
                // DecodeRune consumes one invalid byte, even for a truncated or
                // overlong multibyte sequence. Lossy UTF-8 conversion differs.
                out.extend_from_slice(b"\\ufffd");
                index += 1;
            }
            continue;
        }
        out.push(byte);
        index += 1;
    }
    out
}

pub(super) fn decode<T: de::DeserializeOwned>(body: &[u8]) -> Result<T, serde_json::Error> {
    // Go's Decoder reads one JSON value. It does not require EOF after that value.
    let normalized = normalized_strings(body);
    T::deserialize(&mut serde_json::Deserializer::from_slice(&normalized))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::{Value, json};

    #[test]
    fn frozen_go_decoder_cases() {
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../tests/issue_write_comment_oracle.json"))
                .unwrap();
        for case in cases {
            let body = STANDARD
                .decode(case["body_base64"].as_str().unwrap())
                .unwrap();
            let actual = match case["kind"].as_str().unwrap() {
                "create" => decode::<CreateRequest>(&body).map(|r| json!({
                    "content": r.content, "type": r.kind, "parent_id": r.parent_id,
                    "attachment_ids": r.attachment_ids, "suppress_agent_ids": r.suppress_agent_ids,
                    "steer_task_ids": r.steer_task_ids, "steer": r.steer,
                })),
                "edit" => decode::<EditRequest>(&body).map(|r| json!({
                    "content": r.content, "content_base": r.content_base,
                    "attachment_ids": r.attachment_ids, "suppress_agent_ids": r.suppress_agent_ids,
                    "expected_revision": r.expected_revision,
                })),
                other => panic!("unknown case type {other}"),
            };
            if case["error"] == true {
                assert!(actual.is_err(), "{}: {actual:?}", case["name"]);
            } else {
                assert_eq!(actual.unwrap(), case["decoded"], "{}", case["name"]);
            }
        }
    }
}
