//! Frozen Go CreateIssueRequest decoder: reject exact duplicate JSON keys,
//! then decode folded struct fields in source order from the first JSON value.
use crate::api::issue_write_comment_decode::{folded, normalized_strings, strings};
use serde::{
    Deserialize,
    de::{self, MapAccess, Visitor},
};
use serde_json::Value;
use std::{collections::HashSet, fmt};

#[derive(Debug, Default)]
pub(super) struct Request {
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub priority: String,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub parent_issue_id: Option<String>,
    pub project_id: Option<String>,
    pub stage: Option<i32>,
    pub start_date: Option<String>,
    pub due_date: Option<String>,
    pub attachment_ids: Vec<String>,
    pub label_ids: Vec<String>,
    pub properties: Option<serde_json::Map<String, Value>>,
    pub origin_type: Option<String>,
    pub origin_id: Option<String>,
    pub allow_duplicate: bool,
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct RequestVisitor;
        impl<'de> Visitor<'de> for RequestVisitor {
            type Value = Request;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an object or null")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(Request::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut req = Request::default();
                let (mut attachments, mut labels) = (Vec::new(), Vec::new());
                while let Some(key) = map.next_key::<String>()? {
                    match folded(&key).as_str() {
                        "title" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                req.title = value;
                            }
                        }
                        "description" => req.description = map.next_value()?,
                        "status" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                req.status = value;
                            }
                        }
                        "priority" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                req.priority = value;
                            }
                        }
                        "assignee_type" => req.assignee_type = map.next_value()?,
                        "assignee_id" => req.assignee_id = map.next_value()?,
                        "parent_issue_id" => req.parent_issue_id = map.next_value()?,
                        "project_id" => req.project_id = map.next_value()?,
                        "stage" => req.stage = map.next_value()?,
                        "start_date" => req.start_date = map.next_value()?,
                        "due_date" => req.due_date = map.next_value()?,
                        "attachment_ids" => {
                            req.attachment_ids =
                                strings(&mut map, &mut attachments)?.unwrap_or_default();
                        }
                        "label_ids" => {
                            req.label_ids = strings(&mut map, &mut labels)?.unwrap_or_default();
                        }
                        "properties" => {
                            // encoding/json reuses an existing non-nil map across
                            // differently cased occurrences of the same field.
                            match map.next_value::<Option<serde_json::Map<String, Value>>>()? {
                                None => req.properties = None,
                                Some(values) => req
                                    .properties
                                    .get_or_insert_with(Default::default)
                                    .extend(values),
                            }
                        }
                        "origin_type" => req.origin_type = map.next_value()?,
                        "origin_id" => req.origin_id = map.next_value()?,
                        "allow_duplicate" => {
                            if let Some(value) = map.next_value::<Option<bool>>()? {
                                req.allow_duplicate = value;
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
        d.deserialize_any(RequestVisitor)
    }
}

// Input is already syntax-checked by IgnoredAny. Track object key sets without
// parsing numeric values as f64: Go's duplicate-key pass uses UseNumber and
// accepts even an unknown 1e999. Strings are scanned as tokens, never delimiters.
fn unique_keys(body: &[u8]) -> Result<(), serde_json::Error> {
    let mut objects: Vec<HashSet<String>> = Vec::new();
    let mut index = 0;
    while index < body.len() {
        match body[index] {
            b'{' => objects.push(HashSet::new()),
            b'}' => {
                objects.pop();
            }
            b'"' => {
                let start = index;
                index += 1;
                while index < body.len() {
                    match body[index] {
                        b'\\' => index += 2,
                        b'"' => break,
                        _ => index += 1,
                    }
                }
                let end = index + 1;
                let next = body[end..].iter().find(|b| !b.is_ascii_whitespace());
                if next == Some(&b':') {
                    let key: String = serde_json::from_slice(&body[start..end])?;
                    if !objects
                        .last_mut()
                        .expect("validated object key")
                        .insert(key)
                    {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                }
            }
            _ => {}
        }
        index += 1;
    }
    Ok(())
}

pub(super) fn request(body: &[u8]) -> Result<Request, serde_json::Error> {
    let normalized = normalized_strings(body);
    let mut stream =
        serde_json::Deserializer::from_slice(&normalized).into_iter::<de::IgnoredAny>();
    stream
        .next()
        .ok_or_else(|| de::Error::custom("missing JSON value"))??;
    // Go invokes the custom duplicate-key UnmarshalJSON on ONLY the first
    // Decoder value, not on trailing values or malformed trailing bytes.
    unique_keys(&normalized[..stream.byte_offset()])?;
    Request::deserialize(&mut serde_json::Deserializer::from_slice(&normalized))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::json;

    #[test]
    fn frozen_go_create_decoder_cases() {
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../tests/issue_write_create_oracle.json"))
                .unwrap();
        for case in cases {
            let body = STANDARD
                .decode(case["body_base64"].as_str().unwrap())
                .unwrap();
            let actual = request(&body).map(|r| {
                json!({
                    "title": r.title, "description": r.description, "status": r.status,
                    "priority": r.priority, "assignee_type": r.assignee_type,
                    "assignee_id": r.assignee_id, "parent_issue_id": r.parent_issue_id,
                    "project_id": r.project_id, "stage": r.stage, "start_date": r.start_date,
                    "due_date": r.due_date, "attachment_ids": r.attachment_ids,
                    "label_ids": r.label_ids, "properties": r.properties,
                    "origin_type": r.origin_type, "origin_id": r.origin_id,
                    "allow_duplicate": r.allow_duplicate,
                })
            });
            if case["error"] == true {
                assert!(actual.is_err(), "{}: {actual:?}", case["name"]);
            } else {
                assert_eq!(actual.unwrap(), case["decoded"], "{}", case["name"]);
            }
        }
    }
}
