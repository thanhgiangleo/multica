//! Frozen Go issue update structs: ordered fields, slice reuse and struct merge.
use crate::api::{
    issue_write_comment_decode::{folded, normalized_strings, strings},
    issue_write_structure::Fields,
};
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, MapAccess, Visitor},
};
use serde_json::Value;

#[derive(Default)]
pub(in crate::api) struct UpdateRequest {
    pub expected_revision: Option<i64>,
    pub title: Option<String>,
    pub title_base: Option<String>,
    pub description: Option<String>,
    pub description_base: Option<String>,
    pub status: Option<String>,
    pub priority: Option<String>,
    pub position: Option<f64>,
    pub start_date: Option<String>,
    pub due_date: Option<String>,
    pub attachment_ids: Option<Vec<String>>,
    pub suppress_run: Option<bool>,
    pub handoff_note: Option<String>,
    pub stop_previous_assignee_runs: Option<bool>,
    pub enter_workflow_step: Option<bool>,
    pub structure: Fields,
    pub duplicate_of_issue_id: Option<String>,
}

struct UpdateSeed<'a> {
    request: &'a mut UpdateRequest,
    attachments: &'a mut Vec<String>,
}
impl<'de> DeserializeSeed<'de> for UpdateSeed<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for UpdateSeed<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("an object or null")
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        // The batch's Updates field is a value struct, not a pointer. Go null
        // leaves the already decoded struct (and its slice backing) unchanged.
        Ok(())
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        let req = self.request;
        while let Some(key) = map.next_key::<String>()? {
            match folded(&key).as_str() {
                "expected_revision" => req.expected_revision = map.next_value()?,
                "title" => req.title = map.next_value()?,
                "title_base" => req.title_base = map.next_value()?,
                "description" => req.description = map.next_value()?,
                "description_base" => req.description_base = map.next_value()?,
                "status" => req.status = map.next_value()?,
                "priority" => req.priority = map.next_value()?,
                "position" => req.position = map.next_value()?,
                "start_date" => req.start_date = map.next_value()?,
                "due_date" => req.due_date = map.next_value()?,
                "attachment_ids" => req.attachment_ids = strings(&mut map, self.attachments)?,
                "suppress_run" => {
                    if let Some(v) = map.next_value::<Option<bool>>()? {
                        req.suppress_run = Some(v);
                    }
                }
                "handoff_note" => {
                    if let Some(v) = map.next_value::<Option<String>>()? {
                        req.handoff_note = Some(v);
                    }
                }
                "stop_previous_assignee_runs" => {
                    if let Some(v) = map.next_value::<Option<bool>>()? {
                        req.stop_previous_assignee_runs = Some(v);
                    }
                }
                "enter_workflow_step" => {
                    if let Some(v) = map.next_value::<Option<bool>>()? {
                        req.enter_workflow_step = Some(v);
                    }
                }
                "parent_issue_id" => req.structure.parent_issue_id = map.next_value()?,
                "project_id" => req.structure.project_id = map.next_value()?,
                "stage" => req.structure.stage = map.next_value()?,
                "assignee_type" => req.structure.assignee_type = map.next_value()?,
                "assignee_id" => req.structure.assignee_id = map.next_value()?,
                "duplicate_of_issue_id" => req.duplicate_of_issue_id = map.next_value()?,
                _ => {
                    map.next_value::<de::IgnoredAny>()?;
                }
            }
        }
        Ok(())
    }
}
impl<'de> Deserialize<'de> for UpdateRequest {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let mut req = Self::default();
        UpdateSeed {
            request: &mut req,
            attachments: &mut Vec::new(),
        }
        .deserialize(d)?;
        Ok(req)
    }
}

#[derive(Default)]
pub(in crate::api) struct BatchRequest {
    pub ids: Option<Vec<String>>,
    pub updates: UpdateRequest,
}
impl<'de> Deserialize<'de> for BatchRequest {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct BatchVisitor;
        impl<'de> Visitor<'de> for BatchVisitor {
            type Value = BatchRequest;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object or null")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(BatchRequest::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut req = BatchRequest::default();
                let (mut ids, mut attachments) = (Vec::new(), Vec::new());
                while let Some(key) = map.next_key::<String>()? {
                    match folded(&key).as_str() {
                        "issue_ids" => req.ids = strings(&mut map, &mut ids)?,
                        "updates" => map.next_value_seed(UpdateSeed {
                            request: &mut req.updates,
                            attachments: &mut attachments,
                        })?,
                        _ => {
                            map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(req)
            }
        }
        d.deserialize_any(BatchVisitor)
    }
}

pub(in crate::api) fn decode<T: de::DeserializeOwned>(
    body: &[u8],
) -> Result<(T, Value), serde_json::Error> {
    let normalized = normalized_strings(body);
    // Update and batch use json.Unmarshal, unlike comment/quick-create's
    // streaming Decoder. Reject trailing values AND trailing malformed bytes.
    let req = serde_json::from_slice(&normalized)?;
    // Keep exact raw key spelling and last occurrence, independently of the
    // folded/merged struct. Go's nullable-field presence checks use that map.
    let raw = serde_json::from_slice(&normalized)?;
    Ok((req, raw))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::json;

    fn value(r: UpdateRequest) -> Value {
        json!({
            "expected_revision":r.expected_revision,"title":r.title,"title_base":r.title_base,
            "description":r.description,"description_base":r.description_base,
            "status":r.status,"priority":r.priority,"position":r.position,
            "start_date":r.start_date,"due_date":r.due_date,
            "attachment_ids":r.attachment_ids,"suppress_run":r.suppress_run.unwrap_or_default(),
            "handoff_note":r.handoff_note.unwrap_or_default(),
            "stop_previous_assignee_runs":r.stop_previous_assignee_runs.unwrap_or_default(),
            "enter_workflow_step":r.enter_workflow_step.unwrap_or_default(),
            "parent_issue_id":r.structure.parent_issue_id,"project_id":r.structure.project_id,
            "stage":r.structure.stage,"assignee_type":r.structure.assignee_type,
            "assignee_id":r.structure.assignee_id,"duplicate_of_issue_id":r.duplicate_of_issue_id,
        })
    }

    #[test]
    fn frozen_go_update_and_batch_decoder_cases() {
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../tests/issue_write_issue_oracle.json"))
                .unwrap();
        for case in cases {
            let body = STANDARD
                .decode(case["body_base64"].as_str().unwrap())
                .unwrap();
            let actual = if case["kind"] == "update" {
                decode::<UpdateRequest>(&body).map(|(r, raw)| json!({"request":value(r),"raw":raw}))
            } else {
                decode::<BatchRequest>(&body).map(|(r, raw)| json!({
                    "request":{"issue_ids":r.ids,"updates":value(r.updates)},
                    "raw":raw.get("updates").cloned().filter(Value::is_object).unwrap_or(Value::Null),
                }))
            };
            if case["error"] == true {
                assert!(actual.is_err(), "{}: {actual:?}", case["name"]);
            } else {
                assert_eq!(actual.unwrap(), case["decoded"], "{}", case["name"]);
            }
        }
    }
}
