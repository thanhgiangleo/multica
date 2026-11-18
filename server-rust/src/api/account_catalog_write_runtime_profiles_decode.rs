//! Go encoding/json request semantics for runtime-profile definitions.
use crate::api::account_catalog_write_profile::decode::matches_go_field;
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use std::fmt;

#[derive(Default, Debug)]
pub(super) struct CreateRequest {
    pub display_name: String,
    pub protocol_family: String,
    pub runtime_type: String,
    pub command_name: String,
    pub description: Option<String>,
    pub fixed_args: Vec<String>,
    pub enabled: Option<bool>,
}

#[derive(Default, Debug)]
pub(super) struct UpdateRequest {
    pub display_name: Option<String>,
    pub protocol_family: Option<String>,
    pub runtime_type: Option<String>,
    pub command_name: Option<String>,
    pub description: Option<String>,
    pub fixed_args: Option<Vec<String>>,
    pub enabled: Option<bool>,
}

const FIELDS: &[&str] = &[
    "display_name",
    "protocol_family",
    "runtime_type",
    "command_name",
    "description",
    "fixed_args",
    "enabled",
];

fn field(key: &str) -> Option<&'static str> {
    FIELDS
        .iter()
        .copied()
        .find(|name| key == *name)
        .or_else(|| {
            FIELDS
                .iter()
                .copied()
                .find(|name| matches_go_field(key, name))
        })
}

impl<'de> Deserialize<'de> for CreateRequest {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = CreateRequest;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a runtime profile request object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(CreateRequest::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut req = CreateRequest::default();
                while let Some(key) = map.next_key::<String>()? {
                    match field(&key) {
                        Some("display_name") => {
                            if let Some(value) = map.next_value()? {
                                req.display_name = value;
                            }
                        }
                        Some("protocol_family") => {
                            if let Some(value) = map.next_value()? {
                                req.protocol_family = value;
                            }
                        }
                        Some("runtime_type") => {
                            if let Some(value) = map.next_value()? {
                                req.runtime_type = value;
                            }
                        }
                        Some("command_name") => {
                            if let Some(value) = map.next_value()? {
                                req.command_name = value;
                            }
                        }
                        Some("description") => req.description = map.next_value()?,
                        Some("fixed_args") => {
                            req.fixed_args =
                                map.next_value::<Option<Vec<String>>>()?.unwrap_or_default()
                        }
                        Some("enabled") => req.enabled = map.next_value()?,
                        _ => {
                            let _: IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(req)
            }
        }
        decoder.deserialize_any(Fields)
    }
}

impl<'de> Deserialize<'de> for UpdateRequest {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = UpdateRequest;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a runtime profile request object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(UpdateRequest::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut req = UpdateRequest::default();
                while let Some(key) = map.next_key::<String>()? {
                    match field(&key) {
                        Some("display_name") => req.display_name = map.next_value()?,
                        Some("protocol_family") => req.protocol_family = map.next_value()?,
                        Some("runtime_type") => req.runtime_type = map.next_value()?,
                        Some("command_name") => req.command_name = map.next_value()?,
                        Some("description") => req.description = map.next_value()?,
                        Some("fixed_args") => req.fixed_args = map.next_value()?,
                        Some("enabled") => req.enabled = map.next_value()?,
                        _ => {
                            let _: IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(req)
            }
        }
        decoder.deserialize_any(Fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::account_catalog_write_profile::account_request;

    #[test]
    fn create_decodes_first_value_and_go_null_semantics() {
        let req: CreateRequest = account_request(br#"{"DISPLAY_NAME":"first","display_name":null,"command_name":"claude","fixed_args":["--one"],"FIXED_ARGS":null,"enabled":false,"ENABLED":null,"unknown":{}} trailing"#).unwrap();
        assert_eq!(req.display_name, "first");
        assert_eq!(req.command_name, "claude");
        assert!(req.fixed_args.is_empty());
        assert_eq!(req.enabled, None);
        let req: CreateRequest = account_request(b"null garbage").unwrap();
        assert_eq!(req.display_name, "");
        let req: CreateRequest =
            account_request(r#"{"diſplay_name":"Folded","FIXED_ARGS":["-a"]}"#.as_bytes()).unwrap();
        assert_eq!(req.display_name, "Folded");
        assert_eq!(req.fixed_args, ["-a"]);
        for raw in [
            br#"{"fixed_args":[1]}"#.as_slice(),
            br#"{"enabled":"false"}"#.as_slice(),
            b"[1]".as_slice(),
        ] {
            assert!(account_request::<CreateRequest>(raw).is_err());
        }
    }

    #[test]
    fn update_pointer_null_and_repeated_fields() {
        let req: UpdateRequest = account_request(br#"{"runtime_type":"codex","RUNTIME_TYPE":null,"fixed_args":["-v"],"FIXED_ARGS":null,"display_name":"a","DISPLAY_NAME":null,"enabled":true,"ENABLED":null} trailing"#).unwrap();
        assert_eq!(req.runtime_type, None);
        assert_eq!(req.fixed_args, None);
        assert_eq!(req.display_name, None);
        assert_eq!(req.enabled, None);
        assert!(account_request::<UpdateRequest>(br#"{"fixed_args":[null]}"#).is_err());
    }
}
