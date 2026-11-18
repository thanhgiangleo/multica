//! Go encoding/json object/null semantics for the two account request structs.
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use std::fmt;

pub(in crate::api) fn fields<'de, D: Deserializer<'de>>(
    decoder: D,
    names: &'static [&'static str],
) -> Result<Vec<(&'static str, Option<String>)>, D::Error> {
    struct Fields(&'static [&'static str]);
    impl<'de> Visitor<'de> for Fields {
        type Value = Vec<(&'static str, Option<String>)>;
        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an account request object or null")
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(Vec::new())
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut fields = Vec::new();
            while let Some(key) = map.next_key::<String>()? {
                if let Some(name) = self
                    .0
                    .iter()
                    .copied()
                    .find(|name| matches_go_field(&key, name))
                {
                    fields.push((name, map.next_value::<Option<String>>()?));
                } else {
                    let _: IgnoredAny = map.next_value()?;
                }
            }
            Ok(fields)
        }
    }
    decoder.deserialize_any(Fields(names))
}

pub(in crate::api) fn matches_go_field(key: &str, name: &str) -> bool {
    // These tags are ASCII. Go's simple-fold cycle also admits long s and Kelvin K.
    let folded = key.chars().map(|ch| match ch {
        'ſ' => 's',
        'K' => 'k',
        _ => ch.to_ascii_lowercase(),
    });
    folded.eq(name.chars())
}

impl<'de> Deserialize<'de> for super::UpdateMe {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        let mut request = Self::default();
        for (key, value) in fields(
            decoder,
            &[
                "name",
                "avatar_url",
                "language",
                "profile_description",
                "timezone",
            ],
        )? {
            match key {
                "name" => request.name = value,
                "avatar_url" => request.avatar_url = value,
                "language" => request.language = value,
                "profile_description" => request.profile_description = value,
                "timezone" => request.timezone = value,
                _ => unreachable!(),
            }
        }
        Ok(request)
    }
}

impl<'de> Deserialize<'de> for super::WaitlistRequest {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        let mut request = Self::default();
        for (key, value) in fields(decoder, &["email", "reason"])? {
            // JSON null leaves a Go string field untouched, including when a
            // duplicate key follows a non-null value. Pointer fields above clear.
            if let Some(value) = value {
                match key {
                    "email" => request.email = value,
                    "reason" => request.reason = value,
                    _ => unreachable!(),
                }
            }
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{UpdateMe, WaitlistRequest};

    #[test]
    fn account_json_oracle_matches_go_1_26_9() {
        use base64::Engine;
        use serde_json::{Value, json};
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_profile_edges_oracle.json"
        ))
        .unwrap();
        for case in cases {
            let raw = base64::engine::general_purpose::STANDARD
                .decode(case["body"].as_str().unwrap())
                .unwrap();
            let label = case["label"].as_str().unwrap();
            if case["kind"] == "profile" {
                let decoded = super::super::account_request::<UpdateMe>(&raw);
                if case["decode_error"] == true {
                    assert!(decoded.is_err(), "{label}");
                } else {
                    let decoded = decoded.unwrap();
                    assert_eq!(
                        json!({
                            "name": decoded.name,
                            "avatar_url": decoded.avatar_url,
                            "language": decoded.language,
                            "profile_description": decoded.profile_description,
                            "timezone": decoded.timezone,
                        }),
                        case["decoded"],
                        "{label}"
                    );
                }
            } else {
                let decoded = super::super::account_request::<WaitlistRequest>(&raw);
                if case["decode_error"] == true {
                    assert!(decoded.is_err(), "{label}");
                } else {
                    let decoded = decoded.unwrap();
                    assert_eq!(
                        json!({"email": decoded.email, "reason": decoded.reason}),
                        case["decoded"],
                        "{label}"
                    );
                }
            }
        }
    }

    #[test]
    fn missing_null_and_repeated_strings_match_go() {
        for raw in ["null", "{}", r#"{"email":null,"reason":null}"#] {
            let request: WaitlistRequest = serde_json::from_str(raw).unwrap();
            assert_eq!(request.email, "");
            assert_eq!(request.reason, "");
        }
        let request: WaitlistRequest = serde_json::from_str(
            r#"{"email":"a@b","email":null,"reason":"one","REASON":null,"unknown":{"x":[]}}"#,
        )
        .unwrap();
        assert_eq!(request.email, "a@b");
        assert_eq!(request.reason, "one");
        let request: WaitlistRequest = serde_json::from_str(r#"{"reaſon":"folded"}"#).unwrap();
        assert_eq!(request.reason, "folded");
        let request: WaitlistRequest =
            serde_json::from_str(r#"{"EMAIL":"a@b","email":"c@d"}"#).unwrap();
        assert_eq!(request.email, "c@d");
        for raw in [
            "null",
            "{}",
            r#"{"name":null,"timezone":null}"#,
            r#"{"name":"one","NAME":null}"#,
        ] {
            let request: UpdateMe = serde_json::from_str(raw).unwrap();
            assert_eq!(request.name, None);
            assert_eq!(request.timezone, None);
        }
        for raw in [
            r#"[]"#,
            r#"[null,null]"#,
            "false",
            "12",
            r#""x""#,
            r#"{"email":12}"#,
            r#"{"reason":false}"#,
            r#"{"email":[]}"#,
            r#"{"email":{},"email":"a@b"}"#,
        ] {
            assert!(
                serde_json::from_str::<WaitlistRequest>(raw).is_err(),
                "{raw}"
            );
        }
        for raw in [
            r#"[null,null,null,null,null]"#,
            r#"{"name":[]}"#,
            r#"{"timezone":12}"#,
        ] {
            assert!(serde_json::from_str::<UpdateMe>(raw).is_err(), "{raw}");
        }
    }
}
