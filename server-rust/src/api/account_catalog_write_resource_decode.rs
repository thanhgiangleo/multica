//! Go request structs for project resource writes. Raw refs keep duplicate keys.
use super::super::{ApiError, error};
use axum::http::StatusCode;
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use serde_json::{Map, Value, value::RawValue};
use std::fmt;

pub(in crate::api) fn folded(key: &str, name: &str) -> bool {
    key.chars()
        .map(|ch| match ch {
            'ſ' => 's',
            'K' => 'k',
            _ => ch.to_ascii_lowercase(),
        })
        .eq(name.chars())
}

#[derive(Default)]
pub(in crate::api) struct Create {
    pub(in crate::api) resource_type: String,
    pub(in crate::api) resource_ref: Option<Box<RawValue>>,
    pub(in crate::api) label: Option<String>,
    pub(in crate::api) position: Option<i32>,
}
impl<'de> Deserialize<'de> for Create {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = Create;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a project resource object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Create, E> {
                Ok(Create::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Create, M::Error> {
                let mut request = Create::default();
                while let Some(key) = map.next_key::<String>()? {
                    if folded(&key, "resource_type") {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            request.resource_type = value;
                        }
                    } else if folded(&key, "resource_ref") {
                        // A present JSON null is a RawMessage, not a missing field.
                        request.resource_ref = Some(map.next_value()?);
                    } else if folded(&key, "label") {
                        request.label = map.next_value()?;
                    } else if folded(&key, "position") {
                        request.position = map.next_value()?;
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                    }
                }
                Ok(request)
            }
        }
        decoder.deserialize_any(Fields)
    }
}

pub(in crate::api) fn normalize(kind: &str, raw: Option<&RawValue>) -> Result<Value, ApiError> {
    let raw = raw.ok_or_else(|| error(StatusCode::BAD_REQUEST, "resource_ref is required"))?;
    let names: &[&str] = match kind {
        "github_repo" => &["url", "default_branch_hint", "ref"],
        "local_directory" => &["local_path", "daemon_id", "label", "execution_mode"],
        _ => return super::normalize(kind, &serde_json::json!({})),
    };
    let mut decoder = serde_json::Deserializer::from_str(raw.get());
    let fields = super::super::account_catalog_write_profile::decode::fields(&mut decoder, names)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid resource_ref"))?;
    let mut object = Map::new();
    for (key, value) in fields {
        if let Some(value) = value {
            object.insert(key.to_owned(), Value::String(value));
        }
    }
    super::normalize(kind, &Value::Object(object))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    #[test]
    fn resource_request_matches_go_oracle() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_resource_decode_oracle.json"
        ))
        .unwrap();
        for case in cases {
            let raw = base64::engine::general_purpose::STANDARD
                .decode(case["body"].as_str().unwrap())
                .unwrap();
            let result =
                super::super::super::account_catalog_write_profile::account_request::<Create>(&raw);
            if case["decode_error"] == true {
                assert!(result.is_err(), "{}", case["label"]);
            } else {
                let request = result.unwrap_or_else(|_| panic!("{}", case["label"]));
                let reference = request
                    .resource_ref
                    .map(|raw| serde_json::from_str::<Value>(raw.get()).unwrap())
                    .unwrap_or(Value::Null);
                assert_eq!(
                    serde_json::json!({"resource_type":request.resource_type,"resource_ref":reference,"label":request.label,"position":request.position}),
                    case["decoded"],
                    "{}",
                    case["label"]
                );
            }
        }
    }
    #[test]
    fn reference_struct_null_and_folded_scalars() {
        let raw = RawValue::from_string(
            r#"{"URL":"https://example.org/repo","url":null,"unknown":1e999999}"#.to_owned(),
        )
        .unwrap();
        assert_eq!(
            normalize("github_repo", Some(&raw)).unwrap(),
            serde_json::json!({"url":"https://example.org/repo"})
        );
        let raw = RawValue::from_string("null".to_owned()).unwrap();
        assert_eq!(
            normalize("github_repo", Some(&raw)).unwrap_err().1,
            "github_repo: url is required"
        );
        assert_eq!(
            normalize("github_repo", None).unwrap_err().1,
            "resource_ref is required"
        );
    }
}
