//! Go struct decoding with opaque query/display RawMessages.
use super::super::{
    account_catalog_write::view_preference::{fields, folded},
    account_catalog_write_profile::account_request,
};

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Create {
    pub name: String,
    pub scope_type: String,
    pub scope_id: Option<String>,
    pub scope_variant: Option<String>,
    pub visibility: String,
    pub definition_version: i32,
    pub query: Option<Vec<u8>>,
    pub display: Option<Vec<u8>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Update {
    pub name: Option<String>,
    pub visibility: Option<String>,
    pub scope_variant: Option<String>,
    pub query: Option<Vec<u8>>,
    pub display: Option<Vec<u8>>,
    pub expected_revision: i32,
}

fn scalar<T: for<'de> serde::Deserialize<'de>>(raw: &[u8], target: &mut T) -> Result<(), String> {
    // Go non-pointer scalar fields ignore null, including duplicate null values.
    if raw != b"null" {
        *target = account_request(raw).map_err(|_| "invalid request body".to_owned())?;
    }
    Ok(())
}

fn pointer(raw: &[u8]) -> Result<Option<String>, String> {
    account_request(raw).map_err(|_| "invalid request body".to_owned())
}

impl Create {
    pub fn decode(body: &[u8]) -> Result<Self, String> {
        let mut req = Self::default();
        for (key, raw) in fields(body)? {
            if folded(&key, "name") {
                scalar(&raw, &mut req.name)?;
            } else if folded(&key, "scope_type") {
                scalar(&raw, &mut req.scope_type)?;
            } else if folded(&key, "scope_id") {
                req.scope_id = pointer(&raw)?;
            } else if folded(&key, "scope_variant") {
                req.scope_variant = pointer(&raw)?;
            } else if folded(&key, "visibility") {
                scalar(&raw, &mut req.visibility)?;
            } else if folded(&key, "definition_version") {
                scalar(&raw, &mut req.definition_version)?;
            } else if folded(&key, "query") {
                req.query = Some(raw);
            } else if folded(&key, "display") {
                req.display = Some(raw);
            }
        }
        Ok(req)
    }
}

impl Update {
    pub fn decode(body: &[u8]) -> Result<Self, String> {
        let mut req = Self::default();
        for (key, raw) in fields(body)? {
            if folded(&key, "name") {
                req.name = pointer(&raw)?;
            } else if folded(&key, "visibility") {
                req.visibility = pointer(&raw)?;
            } else if folded(&key, "scope_variant") {
                req.scope_variant = pointer(&raw)?;
            } else if folded(&key, "expected_revision") {
                scalar(&raw, &mut req.expected_revision)?;
            } else if folded(&key, "query") {
                req.query = Some(raw);
            } else if folded(&key, "display") {
                req.display = Some(raw);
            }
        }
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde_json::{Value, json};

    fn raw(value: Option<Vec<u8>>) -> Value {
        value
            .map(|raw| json!(STANDARD.encode(raw)))
            .unwrap_or(Value::Null)
    }

    #[test]
    fn saved_view_structs_match_frozen_go_oracle() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_views_oracle.json"
        ))
        .unwrap();
        for case in cases {
            let body = STANDARD.decode(case["body"].as_str().unwrap()).unwrap();
            let decoded = if case["method"] == "create" {
                Create::decode(&body).map(|req| {
                    json!({
                        "name":req.name,"scope_type":req.scope_type,"scope_id":req.scope_id,
                        "scope_variant":req.scope_variant,"visibility":req.visibility,
                        "definition_version":req.definition_version,"query":raw(req.query),
                        "display":raw(req.display)
                    })
                })
            } else {
                Update::decode(&body).map(|req| json!({
                    "name":req.name,"scope_variant":req.scope_variant,"visibility":req.visibility,
                    "expected_revision":req.expected_revision,"query":raw(req.query),
                    "display":raw(req.display)
                }))
            };
            if case["decode_error"] == true {
                assert!(decoded.is_err(), "{}: {decoded:?}", case["label"]);
            } else {
                assert_eq!(decoded.unwrap(), case["decoded"], "{}", case["label"]);
            }
        }
    }
}
