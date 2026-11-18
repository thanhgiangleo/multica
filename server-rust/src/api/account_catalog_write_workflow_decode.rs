//! Frozen Go encoding/json semantics for workflow definition writes.
use super::super::account_catalog_write_profile::decode::matches_go_field;
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use std::{collections::HashMap, fmt};

#[derive(Default, Debug)]
pub(super) struct WriteRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub steps: Option<Steps>,
    pub status_mapping: HashMap<String, String>,
    pub dry_run: bool,
}
#[derive(Default, Debug)]
pub(super) struct StepInput {
    pub status_key: String,
    pub new_status: Option<NewStatus>,
    pub handler: Option<HandlerInput>,
    pub instructions: String,
    pub next_status_key: String,
    pub back_status_key: String,
}
#[derive(Default, Debug)]
pub(super) struct NewStatus {
    pub name: String,
    pub category: String,
    pub color: String,
}
#[derive(Default, Debug)]
pub(super) struct HandlerInput {
    pub r#type: String,
    pub id: String,
}

// Go resets slice length but reuses backing elements while decoding repeated fields.
#[derive(Default, Debug)]
pub(super) struct Steps {
    backing: Vec<StepInput>,
    len: usize,
}
impl std::ops::Deref for Steps {
    type Target = [StepInput];
    fn deref(&self) -> &Self::Target {
        &self.backing[..self.len]
    }
}
impl IntoIterator for Steps {
    type Item = StepInput;
    type IntoIter = std::vec::IntoIter<StepInput>;
    fn into_iter(mut self) -> Self::IntoIter {
        self.backing.truncate(self.len);
        self.backing.into_iter()
    }
}
struct ReuseStep<'a>(Option<&'a mut StepInput>);
impl<'de> DeserializeSeed<'de> for ReuseStep<'_> {
    type Value = Option<StepInput>;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        if let Some(value) = self.0 {
            StepInput::deserialize_in_place(decoder, value)?;
            Ok(None)
        } else {
            StepInput::deserialize(decoder).map(Some)
        }
    }
}
impl<'de> Deserialize<'de> for Steps {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        let mut value = Self::default();
        Self::deserialize_in_place(decoder, &mut value)?;
        Ok(value)
    }
    fn deserialize_in_place<D: Deserializer<'de>>(
        decoder: D,
        value: &mut Self,
    ) -> Result<(), D::Error> {
        struct StepsVisitor<'a>(&'a mut Steps);
        impl<'de> Visitor<'de> for StepsVisitor<'_> {
            type Value = ();
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a workflow step array")
            }
            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<(), S::Error> {
                let mut index = 0;
                while let Some(next) =
                    seq.next_element_seed(ReuseStep(self.0.backing.get_mut(index)))?
                {
                    if let Some(next) = next {
                        self.0.backing.push(next);
                    }
                    index += 1;
                }
                // An empty JSON array replaces Go's slice with a fresh empty slice.
                if index == 0 {
                    self.0.backing.clear();
                }
                self.0.len = index;
                Ok(())
            }
        }
        decoder.deserialize_seq(StepsVisitor(value))
    }
}

fn scalar<'de, M: MapAccess<'de>>(map: &mut M, value: &mut String) -> Result<(), M::Error> {
    if let Some(next) = map.next_value::<Option<String>>()? {
        *value = next;
    }
    Ok(())
}

macro_rules! object {
    ($ty:ty, $visitor:ident, $($name:literal => $apply:expr),+ $(,)?) => {
        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
                let mut value = Self::default();
                Self::deserialize_in_place(decoder, &mut value)?;
                Ok(value)
            }
            fn deserialize_in_place<D: Deserializer<'de>>(decoder: D, value: &mut Self) -> Result<(), D::Error> {
                struct $visitor<'a>(&'a mut $ty);
                impl<'de> Visitor<'de> for $visitor<'_> {
                    type Value = ();
                    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        f.write_str("a workflow object or null")
                    }
                    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                        Ok(())
                    }
                    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                        let req = self.0;
                        while let Some(key) = map.next_key::<String>()? {
                            // Go matches tagged fields exactly first, then by simple Unicode fold.
                            if let Some(field) = [$($name),+].into_iter().find(|field| key == **field)
                                .or_else(|| [$($name),+].into_iter().find(|field| matches_go_field(&key, field))) {
                                match field {
                                    $($name => { ($apply)(req, &mut map)?; },)+
                                    _ => unreachable!(),
                                }
                            } else {
                                let _: IgnoredAny = map.next_value()?;
                            }
                        }
                        Ok(())
                    }
                }
                decoder.deserialize_any($visitor(value))
            }
        }
    }
}

#[derive(Default, Debug)]
pub(in crate::api) struct SetProjectWorkflowRequest {
    pub workflow_id: Option<String>,
    pub status_mapping: HashMap<String, String>,
    pub dry_run: bool,
}

#[derive(Default, Debug)]
pub(super) struct HandoffRequest {
    pub(super) handler: HandlerInput,
    pub(super) instructions: String,
    pub(super) next_status_key: String,
    pub(super) back_status_key: String,
    pub(super) copy_for_project_id: String,
    // The path supplies the actual status, but Go still decodes this field.
    pub(super) status_key: String,
}

// Decode a repeated embedded handler in place (null retains Go's struct fields).
struct MergeValue<'a, T>(&'a mut T);
impl<'de, T: Deserialize<'de>> DeserializeSeed<'de> for MergeValue<'_, T> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        T::deserialize_in_place(decoder, self.0)
    }
}

// Duplicate pointer-to-struct fields merge into the existing Go struct, unless null resets it.
struct MergePointer<'a, T>(&'a mut Option<T>);
impl<'de, T: Default + Deserialize<'de>> DeserializeSeed<'de> for MergePointer<'_, T> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        struct PointerVisitor<'a, T>(&'a mut Option<T>);
        impl<'de, T: Default + Deserialize<'de>> Visitor<'de> for PointerVisitor<'_, T> {
            type Value = ();
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a workflow object or null")
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<(), E> {
                *self.0 = None;
                Ok(())
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
                self.visit_none()
            }
            fn visit_some<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
                T::deserialize_in_place(decoder, self.0.get_or_insert_with(T::default))
            }
        }
        decoder.deserialize_option(PointerVisitor(self.0))
    }
}

// The map is reused across duplicate keys in Go, unlike a slice (which is replaced).
fn merge_mapping<'de, M: MapAccess<'de>>(
    map: &mut M,
    target: &mut HashMap<String, String>,
) -> Result<(), M::Error> {
    if let Some(next) = map.next_value::<Option<HashMap<String, Option<String>>>>()? {
        target.extend(
            next.into_iter()
                .map(|(key, val)| (key, val.unwrap_or_default())),
        );
    } else {
        target.clear();
    }
    Ok(())
}

object!(WriteRequest, WriteVisitor,
    "name" => |req: &mut WriteRequest, map: &mut M| { req.name = map.next_value()?; Ok(()) },
    "description" => |req: &mut WriteRequest, map: &mut M| { req.description = map.next_value()?; Ok(()) },
    "steps" => |req: &mut WriteRequest, map: &mut M| map.next_value_seed(MergePointer(&mut req.steps)),
    "status_mapping" => |req: &mut WriteRequest, map: &mut M| merge_mapping(map, &mut req.status_mapping),
    "dry_run" => |req: &mut WriteRequest, map: &mut M| {
        if let Some(value) = map.next_value::<Option<bool>>()? { req.dry_run = value; }
        Ok(())
    },
);
object!(SetProjectWorkflowRequest, SetProjectWorkflowVisitor,
    "workflow_id" => |req: &mut SetProjectWorkflowRequest, map: &mut M| { req.workflow_id = map.next_value()?; Ok(()) },
    "status_mapping" => |req: &mut SetProjectWorkflowRequest, map: &mut M| merge_mapping(map, &mut req.status_mapping),
    "dry_run" => |req: &mut SetProjectWorkflowRequest, map: &mut M| {
        if let Some(value) = map.next_value::<Option<bool>>()? { req.dry_run = value; }
        Ok(())
    },
);
object!(HandoffRequest, HandoffVisitor,
    "status_key" => |req: &mut HandoffRequest, map: &mut M| scalar(map, &mut req.status_key),
    "handler" => |req: &mut HandoffRequest, map: &mut M| map.next_value_seed(MergeValue(&mut req.handler)),
    "instructions" => |req: &mut HandoffRequest, map: &mut M| scalar(map, &mut req.instructions),
    "next_status_key" => |req: &mut HandoffRequest, map: &mut M| scalar(map, &mut req.next_status_key),
    "back_status_key" => |req: &mut HandoffRequest, map: &mut M| scalar(map, &mut req.back_status_key),
    "copy_for_project_id" => |req: &mut HandoffRequest, map: &mut M| scalar(map, &mut req.copy_for_project_id),
);
object!(StepInput, StepVisitor,
    "status_key" => |req: &mut StepInput, map: &mut M| scalar(map, &mut req.status_key),
    "new_status" => |req: &mut StepInput, map: &mut M| map.next_value_seed(MergePointer(&mut req.new_status)),
    "handler" => |req: &mut StepInput, map: &mut M| map.next_value_seed(MergePointer(&mut req.handler)),
    "instructions" => |req: &mut StepInput, map: &mut M| scalar(map, &mut req.instructions),
    "next_status_key" => |req: &mut StepInput, map: &mut M| scalar(map, &mut req.next_status_key),
    "back_status_key" => |req: &mut StepInput, map: &mut M| scalar(map, &mut req.back_status_key),
);
object!(NewStatus, NewStatusVisitor,
    "name" => |req: &mut NewStatus, map: &mut M| scalar(map, &mut req.name),
    "category" => |req: &mut NewStatus, map: &mut M| scalar(map, &mut req.category),
    "color" => |req: &mut NewStatus, map: &mut M| scalar(map, &mut req.color),
);
object!(HandlerInput, HandlerVisitor,
    "type" => |req: &mut HandlerInput, map: &mut M| scalar(map, &mut req.r#type),
    "id" => |req: &mut HandlerInput, map: &mut M| scalar(map, &mut req.id),
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::account_catalog_write_profile::account_request;

    #[test]
    fn go_project_workflow_and_handoff_decoding() {
        let req: SetProjectWorkflowRequest = account_request(
            br#"{"WORKFLOW_ID":"first","workflow_id":null,"DRY_RUN":true,"dry_run":null,"status_mapping":{"todo":"done"},"STATUS_MAPPING":{"blocked":null}} trailing"#,
        ).unwrap();
        assert_eq!(req.workflow_id, None);
        assert!(req.dry_run);
        assert_eq!(
            req.status_mapping.get("todo").map(String::as_str),
            Some("done")
        );
        assert_eq!(
            req.status_mapping.get("blocked").map(String::as_str),
            Some("")
        );
        let req: SetProjectWorkflowRequest = account_request(b"null trailing").unwrap();
        assert!(req.workflow_id.is_none());
        assert!(req.status_mapping.is_empty());

        let req: HandoffRequest = account_request(
            br#"{"HANDLER":{"TYPE":"member","ID":"first"},"handler":{"type":null,"id":"second"},"instructions":"one","INSTRUCTIONS":null,"NEXT_STATUS_KEY":"todo","copy_for_project_id":"project"} trailing"#,
        ).unwrap();
        assert_eq!(req.handler.r#type, "member");
        assert_eq!(req.handler.id, "second");
        assert_eq!(req.instructions, "one");
        assert_eq!(req.next_status_key, "todo");
        assert_eq!(req.copy_for_project_id, "project");
        let req: HandoffRequest =
            account_request(br#"{"handler":{"type":"member"},"HANDLER":null}"#).unwrap();
        assert_eq!(req.handler.r#type, "member");
        let req: HandoffRequest = account_request(b"null trailing").unwrap();
        assert!(req.handler.r#type.is_empty());

        for body in [
            br#"{"workflow_id":12}"#.as_slice(),
            br#"{"status_mapping":{"todo":42}}"#,
            br#"{"dry_run":"true"}"#,
            br#"{"status_mapping":[]}"#,
        ] {
            assert!(account_request::<SetProjectWorkflowRequest>(body).is_err());
        }
        for body in [
            br#"{"handler":{"type":42}}"#.as_slice(),
            br#"{"handler":[]}"#,
            br#"{"instructions":false}"#,
            br#"{"status_key":7}"#,
        ] {
            assert!(account_request::<HandoffRequest>(body).is_err());
        }
    }

    #[test]
    fn go_workflow_write_request_decoding() {
        let body = br#"{"NAME":"old","nAmE":null,"steps":[null,{"STATUS_KEY":"todo","status_key":null,"handler":{"TYPE":"agent","type":null}}],"DRY_RUN":true,"dry_run":null,"status_mapping":{"todo":"done"},"STATUS_MAPPING":{"blocked":null}}tail"#;
        let req: WriteRequest = account_request(body).unwrap();
        assert_eq!(req.name.as_deref(), None);
        assert!(req.dry_run);
        assert_eq!(req.steps.as_ref().unwrap()[0].status_key, "");
        assert_eq!(req.steps.as_ref().unwrap()[1].status_key, "todo");
        assert_eq!(
            req.steps.as_ref().unwrap()[1]
                .handler
                .as_ref()
                .unwrap()
                .r#type,
            "agent"
        );
        assert_eq!(
            req.status_mapping.get("todo").map(String::as_str),
            Some("done")
        );
        assert_eq!(
            req.status_mapping.get("blocked").map(String::as_str),
            Some("")
        );
        let nested: WriteRequest = account_request(br#"{"steps":[{"new_status":{"name":"X"},"NEW_STATUS":{"category":"started","name":null},"handler":{"type":"member"},"HANDLER":{"id":"user"}}]}"#).unwrap();
        let step = &nested.steps.unwrap()[0];
        assert_eq!(step.new_status.as_ref().unwrap().name, "X");
        assert_eq!(step.new_status.as_ref().unwrap().category, "started");
        assert_eq!(step.handler.as_ref().unwrap().r#type, "member");
        assert_eq!(step.handler.as_ref().unwrap().id, "user");
        let reused: WriteRequest = account_request(br#"{"steps":[{"status_key":"todo","handler":{"type":"agent"}},{"status_key":"done"}],"STEPS":[{"instructions":"resume"}]}"#).unwrap();
        assert_eq!(reused.steps.as_ref().unwrap().len(), 1);
        assert_eq!(reused.steps.as_ref().unwrap()[0].status_key, "todo");
        assert_eq!(reused.steps.as_ref().unwrap()[0].instructions, "resume");
        assert_eq!(
            reused.steps.as_ref().unwrap()[0]
                .handler
                .as_ref()
                .unwrap()
                .r#type,
            "agent"
        );
        for body in [
            br#"{"steps":{}}"#.as_slice(),
            br#"{"status_mapping":{"todo":1}}"#,
            br#"{"dry_run":"true"}"#,
        ] {
            assert!(account_request::<WriteRequest>(body).is_err());
        }
    }
}
