//! Frozen Go PAT request decoding and expiry arithmetic.
use chrono::{DateTime, Duration, Utc};
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use std::fmt;

#[derive(Default)]
pub(super) struct NewPat {
    pub(super) name: String,
    pub(super) expires_in_days: Option<i64>,
}

impl<'de> Deserialize<'de> for NewPat {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct PatVisitor;
        impl<'de> Visitor<'de> for PatVisitor {
            type Value = NewPat;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a PAT request object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(NewPat::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut req = NewPat::default();
                while let Some(key) = map.next_key::<String>()? {
                    let folded = || {
                        key.chars().map(|ch| match ch {
                            'ſ' => 's',
                            'K' => 'k',
                            _ => ch.to_ascii_lowercase(),
                        })
                    };
                    if folded().eq("name".chars()) {
                        // A null JSON value leaves a Go string unchanged, even after a duplicate.
                        if let Some(name) = map.next_value::<Option<String>>()? {
                            req.name = name;
                        }
                    } else if folded().eq("expires_in_days".chars()) {
                        req.expires_in_days = map.next_value::<Option<i64>>()?;
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                    }
                }
                Ok(req)
            }
        }
        decoder.deserialize_any(PatVisitor)
    }
}

pub(super) fn expiry(now: DateTime<Utc>, days: Option<i64>) -> Option<DateTime<Utc>> {
    let days = days.filter(|days| *days > 0)?;
    // Go multiplies time.Duration (int64 nanoseconds) with wrapping arithmetic.
    // Never turn a large positive, overflowing duration into a permanent PAT.
    let nanoseconds = days.wrapping_mul(24 * 60 * 60 * 1_000_000_000);
    now.checked_add_signed(Duration::nanoseconds(nanoseconds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use chrono::TimeZone;
    use serde_json::Value;

    #[test]
    fn go_pat_request_oracle() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_pat_oracle.json"
        ))
        .unwrap();
        let now = Utc.with_ymd_and_hms(2026, 11, 3, 0, 0, 0).unwrap();
        for case in cases {
            let raw = base64::engine::general_purpose::STANDARD
                .decode(case["body"].as_str().unwrap())
                .unwrap();
            let label = case["label"].as_str().unwrap();
            let decoded =
                super::super::super::account_catalog_write_profile::account_request::<NewPat>(&raw);
            if case["decode_error"] == true {
                assert!(decoded.is_err(), "{label}");
                continue;
            }
            let decoded = decoded.unwrap();
            assert_eq!(decoded.name, case["name"].as_str().unwrap(), "{label}");
            assert_eq!(decoded.expires_in_days, case["days"].as_i64(), "{label}");
            if decoded.name.is_empty() {
                continue;
            }
            let actual = expiry(now, decoded.expires_in_days)
                .map(|end| (end - now).num_nanoseconds().unwrap().to_string());
            assert_eq!(actual.as_deref(), case["expiry_ns"].as_str(), "{label}");
        }
    }
}
