//! Go encoding/json semantics for metadata and reaction mutation bodies.
use super::super::issue_write_comment_decode::{decode, folded};
use super::raw_field::raw_field;
use serde::{
    Deserialize,
    de::{self, MapAccess, Visitor},
};
#[derive(Default)]
pub(super) struct ReactionRequest {
    pub emoji: String,
}

impl<'de> Deserialize<'de> for ReactionRequest {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RequestVisitor;
        impl<'de> Visitor<'de> for RequestVisitor {
            type Value = ReactionRequest;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object or null")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(ReactionRequest::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut request = ReactionRequest::default();
                while let Some(key) = map.next_key::<String>()? {
                    if folded(&key) == "emoji" {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            request.emoji = value;
                        }
                    } else {
                        map.next_value::<de::IgnoredAny>()?;
                    }
                }
                Ok(request)
            }
        }
        deserializer.deserialize_any(RequestVisitor)
    }
}

pub(super) fn metadata(body: &[u8]) -> Result<Option<Vec<u8>>, serde_json::Error> {
    raw_field(body, "value")
}

pub(in crate::api) fn emoji(body: &[u8]) -> Result<String, serde_json::Error> {
    Ok(decode::<ReactionRequest>(body)?.emoji)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::Value;

    #[test]
    fn frozen_go_scalar_decoders() {
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../tests/issue_write_scalar_oracle.json"))
                .unwrap();
        for case in cases {
            let body = STANDARD
                .decode(case["body_base64"].as_str().unwrap())
                .unwrap();
            let actual = match case["kind"].as_str().unwrap() {
                "metadata" | "property" => metadata(&body),
                "reaction" => emoji(&body).map(|value| Some(value.into_bytes())),
                other => panic!("unknown case type {other}"),
            };
            if case["error"] == true {
                assert!(actual.is_err(), "{}", case["name"]);
            } else {
                assert_eq!(
                    actual.unwrap(),
                    case["decoded_base64"]
                        .as_str()
                        .map(|value| STANDARD.decode(value).unwrap()),
                    "{}",
                    case["name"]
                );
                if case["kind"] == "metadata" {
                    let actual = super::super::validate_value(&body)
                        .err()
                        .map(|error| error.1);
                    assert_eq!(
                        actual,
                        case["validation_error"].as_str().map(str::to_owned),
                        "{}",
                        case["name"]
                    );
                }
            }
        }
    }
}
