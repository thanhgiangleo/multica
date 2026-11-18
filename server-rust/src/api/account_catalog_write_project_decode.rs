//! Frozen Go project-create request struct decoding; resource refs remain raw JSON.
use super::super::account_catalog_write_resources::decode::Create as Resource;
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use std::fmt;

#[derive(Default)]
pub(super) struct Create {
    pub title: String,
    pub description: Option<String>,
    pub icon: Option<String>,
    pub status: String,
    pub priority: String,
    pub lead_type: Option<String>,
    pub lead_id: Option<String>,
    pub start_date: Option<String>,
    pub due_date: Option<String>,
    pub workflow_id: Option<String>,
    pub resources: Vec<Resource>,
}

impl<'de> Deserialize<'de> for Create {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = Create;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a project object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Create, E> {
                Ok(Create::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Create, M::Error> {
                let mut req = Create::default();
                while let Some(key) = map.next_key::<String>()? {
                    let field = [
                        "title",
                        "description",
                        "icon",
                        "status",
                        "priority",
                        "lead_type",
                        "lead_id",
                        "start_date",
                        "due_date",
                        "workflow_id",
                        "resources",
                    ]
                    .into_iter()
                    .find(|field| {
                        super::super::account_catalog_write_resources::decode::folded(&key, field)
                    });
                    match field {
                        Some("title") => {
                            if let Some(value) = map.next_value()? {
                                req.title = value;
                            }
                        }
                        Some("status") => {
                            if let Some(value) = map.next_value()? {
                                req.status = value;
                            }
                        }
                        Some("priority") => {
                            if let Some(value) = map.next_value()? {
                                req.priority = value;
                            }
                        }
                        Some("description") => req.description = map.next_value()?,
                        Some("icon") => req.icon = map.next_value()?,
                        Some("lead_type") => req.lead_type = map.next_value()?,
                        Some("lead_id") => req.lead_id = map.next_value()?,
                        Some("start_date") => req.start_date = map.next_value()?,
                        Some("due_date") => req.due_date = map.next_value()?,
                        Some("workflow_id") => req.workflow_id = map.next_value()?,
                        Some("resources") => {
                            req.resources = map
                                .next_value::<Option<Vec<Resource>>>()?
                                .unwrap_or_default()
                        }
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
    fn create_preserves_go_fields_and_resource_raw_refs() {
        let request: Create = account_request(br#"{"TITLE":"demo","title":null,"RESOURCEs":[{"resource_type":"github_repo","resource_ref":{"url":"old","url":"new"}}],"resources":null,"description":"initial","DESCRIPTION":null} trailing"#).unwrap();
        assert_eq!(request.title, "demo");
        assert!(request.resources.is_empty());
        assert!(request.description.is_none());
        let request: Create = account_request(br#"{"title":"demo","resources":[null,{"Resource_Type":"github_repo","resource_ref":{"url":"https://example.org","url":null}}]}"#).unwrap();
        assert_eq!(request.resources[0].resource_type, "");
        assert_eq!(
            request.resources[1].resource_ref.as_ref().unwrap().get(),
            r#"{"url":"https://example.org","url":null}"#
        );
    }
}
