//! Frozen Go request decoding for workspace notification preferences.
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use serde_json::{Value, json};
use std::{collections::HashMap, fmt};

#[derive(Default)]
struct NotificationPreferenceRequest {
    preferences: Option<HashMap<String, String>>,
}

impl<'de> Deserialize<'de> for NotificationPreferenceRequest {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct RequestVisitor;
        impl<'de> Visitor<'de> for RequestVisitor {
            type Value = NotificationPreferenceRequest;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a notification preference request or null")
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(NotificationPreferenceRequest::default())
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut request = NotificationPreferenceRequest::default();
                while let Some(key) = map.next_key::<String>()? {
                    // encoding/json matches struct tags with simple Unicode folding.
                    if key
                        .chars()
                        .map(|ch| match ch {
                            'ſ' => 's',
                            _ => ch.to_ascii_lowercase(),
                        })
                        .eq("preferences".chars())
                    {
                        let value = map.next_value::<Option<HashMap<String, Option<String>>>>()?;
                        match value {
                            None => request.preferences = None,
                            Some(entries) => {
                                let target = request.preferences.get_or_insert_with(HashMap::new);
                                for (name, value) in entries {
                                    target.insert(name, value.unwrap_or_default());
                                }
                            }
                        }
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                    }
                }
                Ok(request)
            }
        }
        decoder.deserialize_any(RequestVisitor)
    }
}

pub(super) fn decode(bytes: &[u8]) -> Result<Value, String> {
    let request: NotificationPreferenceRequest =
        super::super::account_catalog_write_profile::account_request(bytes)
            .map_err(|_| "invalid request body".to_owned())?;
    let preferences = request
        .preferences
        .ok_or_else(|| "preferences field is required".to_owned())?;
    for (key, value) in &preferences {
        if !matches!(
            key.as_str(),
            "assignments"
                | "status_changes"
                | "comments"
                | "mentions"
                | "updates"
                | "agent_activity"
                | "system_notifications"
        ) {
            return Err(format!("invalid preference group: {key}"));
        }
        if !matches!(value.as_str(), "all" | "muted") {
            return Err(format!("invalid preference value: {value}"));
        }
    }
    Ok(json!(preferences))
}

#[cfg(test)]
mod tests {
    use super::decode;
    use base64::Engine;
    use serde_json::Value;

    #[test]
    fn notification_decoder_matches_frozen_go_oracle() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_notification_oracle.json"
        ))
        .unwrap();
        for case in cases {
            let raw = base64::engine::general_purpose::STANDARD
                .decode(case["body"].as_str().unwrap())
                .unwrap();
            let label = case["label"].as_str().unwrap();
            if case["decode_error"] == true {
                assert_eq!(
                    decode(&raw),
                    Err("invalid request body".to_owned()),
                    "{label}"
                );
            } else if case["preferences"].is_null() {
                assert_eq!(
                    decode(&raw),
                    Err("preferences field is required".to_owned()),
                    "{label}"
                );
            } else if let Some(message) = case["validation_error"].as_str() {
                assert_eq!(decode(&raw), Err(message.to_owned()), "{label}");
            } else {
                assert_eq!(decode(&raw), Ok(case["preferences"].clone()), "{label}");
            }
        }
    }

    #[test]
    fn map_null_entries_and_repeated_keys_preserve_go_semantics() {
        assert_eq!(
            decode(br#"{"preferences":{"comments":"muted"},"PREFERENCES":{"mentions":null}}"#),
            Err("invalid preference value: ".to_owned())
        );
        assert_eq!(
            decode(br#"{"preferences":{"comments":"muted"},"preferences":null}"#),
            Err("preferences field is required".to_owned())
        );
    }
}
