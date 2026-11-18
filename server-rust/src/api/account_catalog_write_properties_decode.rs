//! Go encoding/json field folding and duplicate pointer-to-struct merge semantics.
use super::{Config, Create, OptionInput, Update};
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use std::fmt;

fn folded(key: &str, expected: &str) -> bool {
    key.chars()
        .map(|ch| match ch {
            'ſ' => 's',
            'K' => 'k',
            _ => ch.to_ascii_lowercase(),
        })
        .eq(expected.chars())
}

struct OptionSeed<'a>(&'a mut OptionInput);
impl<'de> DeserializeSeed<'de> for OptionSeed<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        for (field, value) in super::super::account_catalog_write_profile::decode::fields(
            decoder,
            &["id", "name", "color"],
        )? {
            if let Some(value) = value {
                match field {
                    "id" => self.0.id = value,
                    "name" => self.0.name = value,
                    "color" => self.0.color = value,
                    _ => unreachable!(),
                }
            }
        }
        Ok(())
    }
}

struct OptionsSeed<'a>(&'a mut Config);
impl<'de> DeserializeSeed<'de> for OptionsSeed<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        struct OptionsVisitor<'a>(&'a mut Config);
        impl<'de> Visitor<'de> for OptionsVisitor<'_> {
            type Value = ();
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a property option array or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
                self.0.options.clear();
                self.0.option_tail.clear();
                Ok(())
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
                // Go reuses backing-array structs, even past a previous shorter slice.
                // Empty arrays and null reset that storage; null struct elements do not.
                self.0.options.append(&mut self.0.option_tail);
                let mut index = 0;
                loop {
                    let mut fresh = OptionInput::default();
                    let option = self.0.options.get_mut(index).unwrap_or(&mut fresh);
                    if sequence.next_element_seed(OptionSeed(option))?.is_none() {
                        break;
                    }
                    if index == self.0.options.len() {
                        self.0.options.push(fresh);
                    }
                    index += 1;
                }
                if index == 0 {
                    self.0.options.clear();
                } else {
                    self.0.option_tail = self.0.options.split_off(index);
                }
                Ok(())
            }
        }
        decoder.deserialize_any(OptionsVisitor(self.0))
    }
}

struct ConfigSeed<'a>(&'a mut Option<Config>);
impl<'de> DeserializeSeed<'de> for ConfigSeed<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        struct ConfigVisitor<'a>(&'a mut Option<Config>);
        impl<'de> Visitor<'de> for ConfigVisitor<'_> {
            type Value = ();
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a property config object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
                *self.0 = None;
                Ok(())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
                // A second non-null Go pointer-to-struct object updates the existing
                // config. An omitted options field must not erase its prior slice.
                let config = self.0.get_or_insert_with(Config::default);
                while let Some(key) = map.next_key::<String>()? {
                    if folded(&key, "options") {
                        map.next_value_seed(OptionsSeed(config))?;
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                    }
                }
                Ok(())
            }
        }
        decoder.deserialize_any(ConfigVisitor(self.0))
    }
}

impl<'de> Deserialize<'de> for Create {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct CreateVisitor;
        impl<'de> Visitor<'de> for CreateVisitor {
            type Value = Create;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a property create object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Create, E> {
                Ok(Create::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Create, M::Error> {
                let mut request = Create::default();
                while let Some(key) = map.next_key::<String>()? {
                    if folded(&key, "config") {
                        map.next_value_seed(ConfigSeed(&mut request.config))?;
                    } else if let Some(field) = ["name", "type", "description", "icon"]
                        .into_iter()
                        .find(|field| folded(&key, field))
                    {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            match field {
                                "name" => request.name = value,
                                "type" => request.kind = value,
                                "description" => request.description = value,
                                "icon" => request.icon = value,
                                _ => unreachable!(),
                            }
                        }
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                    }
                }
                Ok(request)
            }
        }
        decoder.deserialize_any(CreateVisitor)
    }
}

impl<'de> Deserialize<'de> for Update {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct UpdateVisitor;
        impl<'de> Visitor<'de> for UpdateVisitor {
            type Value = Update;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a property update object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Update, E> {
                Ok(Update::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Update, M::Error> {
                let mut request = Update::default();
                while let Some(key) = map.next_key::<String>()? {
                    if folded(&key, "config") {
                        map.next_value_seed(ConfigSeed(&mut request.config))?;
                    } else if folded(&key, "archived") {
                        request.archived = map.next_value()?;
                    } else if let Some(field) = ["name", "description", "icon"]
                        .into_iter()
                        .find(|field| folded(&key, field))
                    {
                        let value = map.next_value()?;
                        match field {
                            "name" => request.name = value,
                            "description" => request.description = value,
                            "icon" => request.icon = value,
                            _ => unreachable!(),
                        }
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                    }
                }
                Ok(request)
            }
        }
        decoder.deserialize_any(UpdateVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use serde_json::Value;

    #[test]
    fn matches_frozen_go_property_request_oracle() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_properties_oracle.json"
        ))
        .unwrap();
        for case in cases {
            let label = format!("{} {}", case["method"], case["label"]);
            let raw = base64::engine::general_purpose::STANDARD
                .decode(case["body"].as_str().unwrap())
                .unwrap();
            let result = if case["method"] == "create" {
                super::super::super::account_catalog_write_profile::account_request::<Create>(&raw)
                    .map(|value| serde_json::to_value(value).unwrap())
            } else {
                super::super::super::account_catalog_write_profile::account_request::<Update>(&raw)
                    .map(|value| serde_json::to_value(value).unwrap())
            };
            if case["decode_error"] == true {
                assert!(result.is_err(), "{label}");
            } else {
                assert_eq!(
                    result.unwrap_or_else(|_| panic!("{label}")),
                    case["decoded"],
                    "{label}"
                );
            }
        }
    }
}
