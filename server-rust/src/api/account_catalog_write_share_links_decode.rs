//! Go's first-value JSON decode for share-link creation and joining.
use crate::api::account_catalog_write_profile::decode::matches_go_field;
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use std::fmt;

#[derive(Default, Debug)]
pub(super) struct CreateRequest {
    pub role: String,
    pub expires_in: Option<i64>,
    pub max_uses: Option<i64>,
}
#[derive(Default, Debug)]
pub(super) struct JoinRequest {
    pub code: String,
}

impl<'de> Deserialize<'de> for CreateRequest {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = CreateRequest;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a share link request object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(CreateRequest::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut req = CreateRequest::default();
                while let Some(key) = map.next_key::<String>()? {
                    let field = ["role", "expires_in", "max_uses"]
                        .into_iter()
                        .find(|name| matches_go_field(&key, name));
                    match field {
                        Some("role") => {
                            if let Some(value) = map.next_value()? {
                                req.role = value;
                            }
                        }
                        Some("expires_in") => req.expires_in = map.next_value()?,
                        Some("max_uses") => req.max_uses = map.next_value()?,
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
impl<'de> Deserialize<'de> for JoinRequest {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = JoinRequest;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a share link join object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(JoinRequest::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut req = JoinRequest::default();
                while let Some(key) = map.next_key::<String>()? {
                    if matches_go_field(&key, "code") {
                        if let Some(value) = map.next_value()? {
                            req.code = value;
                        }
                    } else {
                        let _: IgnoredAny = map.next_value()?;
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
    fn create_preserves_go_null_and_repeated_field_behavior() {
        let req: CreateRequest = account_request(br#"{"ROLE":"admin","role":null,"EXPIRES_IN":72,"expires_in":null,"MAX_USES":2,"max_uses":4} trailing"#).unwrap();
        assert_eq!(req.role, "admin");
        assert_eq!(req.expires_in, None);
        assert_eq!(req.max_uses, Some(4));
        let req: CreateRequest = account_request(b"null trailing").unwrap();
        assert!(req.role.is_empty());
        for raw in [
            br#"{"role":1}"#.as_slice(),
            br#"{"expires_in":1.1}"#.as_slice(),
            b"[]".as_slice(),
        ] {
            assert!(account_request::<CreateRequest>(raw).is_err());
        }
    }
    #[test]
    fn join_retains_scalar_on_null_and_folds_key() {
        let req: JoinRequest =
            account_request(br#"{"CODE":"first","code":null,"CoDe":"second"} trailing"#).unwrap();
        assert_eq!(req.code, "second");
        assert!(account_request::<JoinRequest>(br#"{"code":null,"CODE":1}"#).is_err());
    }
}
