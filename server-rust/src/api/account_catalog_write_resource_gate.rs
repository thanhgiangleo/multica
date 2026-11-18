//! Frozen Go worktree capability/version selection and structured rejection.
use chrono::{DateTime, NaiveDate, Utc};
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use serde_json::{Value, json, value::RawValue};
use std::{fmt, sync::OnceLock};

pub(super) struct Runtime {
    pub metadata: String,
    pub seen: Option<DateTime<Utc>>,
}

#[derive(Default)]
struct Capabilities(Vec<String>);
impl<'de> Deserialize<'de> for Capabilities {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = Capabilities;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("runtime metadata object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Capabilities, E> {
                Ok(Capabilities::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Capabilities, M::Error> {
                let mut storage: Vec<String> = Vec::new();
                let mut length = 0;
                while let Some(key) = map.next_key::<String>()? {
                    if super::decode::folded(&key, "capabilities") {
                        let raw: Box<RawValue> = map.next_value()?;
                        let values: Option<Vec<Option<String>>> =
                            serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
                        match values {
                            Some(values) if !values.is_empty() => {
                                length = values.len();
                                storage.resize(storage.len().max(length), String::new());
                                for (index, value) in values.into_iter().enumerate() {
                                    if let Some(value) = value {
                                        storage[index] = value;
                                    }
                                }
                            }
                            _ => {
                                storage.clear();
                                length = 0;
                            }
                        }
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                    }
                }
                storage.truncate(length);
                Ok(Capabilities(storage))
            }
        }
        decoder.deserialize_any(Fields)
    }
}

fn selection(runtimes: &[Runtime]) -> (bool, String) {
    let mut newest: Option<&Runtime> = None;
    let mut version = String::new();
    let zero = NaiveDate::from_ymd_opt(1, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    let mut version_seen = zero;
    for runtime in runtimes {
        if newest.is_none_or(|current| {
            runtime
                .seen
                .is_some_and(|seen| current.seen.is_none_or(|old| seen > old))
        }) {
            newest = Some(runtime);
        }
        let metadata: Value = serde_json::from_str(&runtime.metadata).unwrap_or(Value::Null);
        if let Some(value) = metadata["cli_version"]
            .as_str()
            .filter(|value| !value.is_empty())
            && (version.is_empty() || runtime.seen.is_some_and(|seen| seen > version_seen))
        {
            version = value.to_owned();
            version_seen = runtime.seen.unwrap_or(zero);
        }
    }
    let enabled = newest.is_some_and(|runtime| {
        serde_json::from_str::<Capabilities>(&runtime.metadata)
            .is_ok_and(|caps| caps.0.iter().any(|cap| cap == "local-worktree-v1"))
    });
    (enabled, version)
}

fn quote(value: &str) -> String {
    static PRINTABLE: OnceLock<Vec<(u32, u32)>> = OnceLock::new();
    let printable = PRINTABLE.get_or_init(|| {
        serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_go_print_ranges.json"
        ))
        .unwrap()
    });
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\x07' => out.push_str("\\a"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x0b' => out.push_str("\\v"),
            _ => {
                let code = ch as u32;
                let index = printable.partition_point(|(_, end)| *end < code);
                if printable
                    .get(index)
                    .is_some_and(|(start, _)| *start <= code)
                {
                    out.push(ch);
                } else if code < 0x20 || code == 0x7f {
                    out.push_str(&format!("\\x{code:02x}"));
                } else if code < 0x10000 {
                    out.push_str(&format!("\\u{code:04x}"));
                } else {
                    out.push_str(&format!("\\U{code:08x}"));
                }
            }
        }
    }
    out.push('"');
    out
}

pub(super) fn rejection(reference: &Value, runtimes: &[Runtime]) -> Option<Value> {
    let (enabled, version) = selection(runtimes);
    if enabled {
        return None;
    }
    let path = quote(reference["local_path"].as_str().unwrap_or_default());
    Some(json!({
        "error":format!("local_directory: {path} is set to parallel (worktree) mode, but the Multica runtime on that machine does not support it. Update the Multica app on that machine to the latest version, or keep the resource on in_place."),
        "code":"daemon_version_unsupported", "current_version":version,
        "min_version":"0.4.24", "daemon_id":reference["daemon_id"],
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn worktree_gate_matches_frozen_go_oracle() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_resource_gate_oracle.json"
        ))
        .unwrap();
        for case in cases {
            let runtimes: Vec<_> = case["runtimes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|runtime| Runtime {
                    metadata: runtime["metadata"].as_str().unwrap().to_owned(),
                    seen: runtime["seen"].as_str().map(|seen| {
                        DateTime::parse_from_rfc3339(seen)
                            .unwrap()
                            .with_timezone(&Utc)
                    }),
                })
                .collect();
            let got = rejection(&case["reference"], &runtimes);
            assert_eq!(
                got,
                case.get("rejection")
                    .filter(|value| !value.is_null())
                    .cloned(),
                "{}",
                case["label"]
            );
        }
    }
}
