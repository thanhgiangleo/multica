//! Select/checkbox grouping preserves unset, unavailable and typed values.
use super::*;

pub(super) struct Property {
    id: String,
    kind: String,
    options: Vec<String>,
}
impl Property {
    pub(super) async fn resolve(
        db: &impl GenericClient,
        ws: Uuid,
        raw: &str,
    ) -> Result<Self, TableError> {
        let id = Uuid::parse_str(raw).map_err(|_| invalid("invalid group.property_id"))?;
        let row = db.query_opt("SELECT type,config,archived_at IS NOT NULL AS archived FROM issue_property WHERE workspace_id=$1 AND id=$2", &[&ws,&id]).await
            .map_err(|e| query_failure(e,"failed to resolve table group"))?
            .ok_or_else(|| unsupported("property_not_found", "The grouped property no longer exists."))?;
        if row.get::<_, bool>("archived") {
            return Err(unsupported(
                "property_archived",
                "The grouped property is archived.",
            ));
        }
        let kind: String = row.get("type");
        if !matches!(kind.as_str(), "select" | "checkbox") {
            return Err(unsupported(
                "property_type_unsupported",
                "This property type cannot be used for grouping.",
            ));
        }
        let config: Value = row.get("config");
        let options = config
            .get("options")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|v| v.get("id").and_then(Value::as_str).map(str::to_owned))
            .collect();
        Ok(Self {
            id: id.to_string(),
            kind,
            options,
        })
    }
    pub(super) fn expression(&self, args: &mut Args) -> String {
        let key = arg(args, self.id.clone());
        if self.kind == "select" {
            let active = arg(args, self.options.clone());
            format!(
                "CASE WHEN NOT (i.properties ? {key}::text) THEN 'unset:' WHEN jsonb_typeof(i.properties -> {key}::text)='string' AND i.properties ->> {key}::text=ANY({active}::text[]) THEN 'value:' || (i.properties ->> {key}::text) WHEN jsonb_typeof(i.properties -> {key}::text)='string' THEN 'unavailable:' || (i.properties ->> {key}::text) ELSE 'unavailable:' END"
            )
        } else {
            format!(
                "CASE WHEN NOT (i.properties ? {key}::text) THEN 'unset:' WHEN jsonb_typeof(i.properties -> {key}::text)='boolean' THEN 'value:' || (i.properties ->> {key}::text) ELSE 'unavailable:' END"
            )
        }
    }
    pub(super) fn expected(&self) -> Vec<String> {
        let mut result = if self.kind == "select" {
            self.options.iter().map(|s| format!("value:{s}")).collect()
        } else {
            vec!["value:false".into(), "value:true".into()]
        };
        result.push("unset:".into());
        result
    }
    pub(super) fn order(&self, args: &mut Args) -> String {
        if self.kind == "select" {
            let options = arg(
                args,
                self.options
                    .iter()
                    .map(|s| format!("value:{s}"))
                    .collect::<Vec<_>>(),
            );
            format!(
                "CASE WHEN group_value LIKE 'value:%' THEN COALESCE(array_position({options}::text[],group_value),100000) WHEN group_value LIKE 'unavailable:%' THEN 100001 ELSE 100002 END"
            )
        } else {
            "CASE group_value WHEN 'value:false' THEN 0 WHEN 'value:true' THEN 1 WHEN 'unavailable:' THEN 2 ELSE 3 END".into()
        }
    }
    pub(super) fn descriptor(&self, raw: &str, count: i64) -> Result<Value, TableError> {
        let (state, value) = raw
            .split_once(':')
            .ok_or_else(|| db_failure("failed to resolve table group"))?;
        let mut body =
            json!({"kind":"property", "actor":null, "property_id":self.id, "value_state":state});
        match state {
            "unset" => (),
            "unavailable" => {
                if !value.is_empty() {
                    body["value"] = json!(value);
                }
            }
            "value" if self.kind == "checkbox" => {
                body["value"] = json!(
                    value
                        .parse::<bool>()
                        .map_err(|_| db_failure("failed to resolve table group"))?
                )
            }
            "value" => body["value"] = json!(value),
            _ => return Err(db_failure("failed to resolve table group")),
        }
        Ok(
            json!({"key":format!("property:{}:{state}:{}",self.id,URL_SAFE_NO_PAD.encode(value)),"value":body,"count":count}),
        )
    }
    pub(super) fn predicate(&self, raw: &str, args: &mut Args) -> Result<String, TableError> {
        let prefix = format!("property:{}:", self.id);
        let (state, encoded) = raw
            .strip_prefix(&prefix)
            .and_then(|v| v.split_once(':'))
            .ok_or_else(|| invalid("invalid group_key"))?;
        let decoded = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| invalid("invalid group_key"))?;
        let value = String::from_utf8(decoded).map_err(|_| invalid("invalid group_key"))?;
        let key = arg(args, self.id.clone());
        match state {
            "unset" if value.is_empty() => Ok(format!("NOT (i.properties ? {key}::text)")),
            "value" => {
                let kind = if self.kind == "select" {
                    if !self.options.contains(&value) {
                        return Err(invalid("invalid group_key"));
                    }
                    "string"
                } else {
                    if !matches!(value.as_str(), "true" | "false") {
                        return Err(invalid("invalid group_key"));
                    }
                    "boolean"
                };
                let value = arg(args, value);
                Ok(format!(
                    "jsonb_typeof(i.properties -> {key}::text)='{kind}' AND i.properties ->> {key}::text={value}::text"
                ))
            }
            "unavailable" if self.kind == "select" && !value.is_empty() => {
                let value = arg(args, value);
                Ok(format!(
                    "jsonb_typeof(i.properties -> {key}::text)='string' AND i.properties ->> {key}::text={value}::text"
                ))
            }
            "unavailable" => {
                let kind = if self.kind == "select" {
                    "string"
                } else {
                    "boolean"
                };
                Ok(format!(
                    "i.properties ? {key}::text AND jsonb_typeof(i.properties -> {key}::text)<>'{kind}'"
                ))
            }
            _ => Err(invalid("invalid group_key")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_keys_preserve_typed_false_and_empty_states() {
        let property = Property {
            id: Uuid::nil().to_string(),
            kind: "checkbox".into(),
            options: vec![],
        };
        assert_eq!(property.expected(), ["value:false", "value:true", "unset:"]);
        let descriptor = property.descriptor("value:false", 2).unwrap();
        assert_eq!(descriptor["value"]["value"], false);
        assert_eq!(descriptor["count"], 2);
        let mut args = Args::new();
        let sql = property
            .predicate(descriptor["key"].as_str().unwrap(), &mut args)
            .unwrap();
        assert!(sql.contains("='boolean'"));
        assert_eq!(args.len(), 2);
        let unset = property.descriptor("unset:", 0).unwrap();
        assert!(unset["value"].get("value").is_none());
        assert!(
            property
                .predicate(
                    &format!("property:{}:unset:eA", property.id),
                    &mut Args::new()
                )
                .is_err()
        );
    }

    #[test]
    fn select_keys_bind_deleted_strings_without_interpolating_them() {
        let property = Property {
            id: Uuid::nil().to_string(),
            kind: "select".into(),
            options: vec!["red".into(), "blue".into()],
        };
        assert_eq!(property.expected(), ["value:red", "value:blue", "unset:"]);
        let raw = "deleted'; SELECT secret";
        let descriptor = property
            .descriptor(&format!("unavailable:{raw}"), 1)
            .unwrap();
        assert_eq!(descriptor["value"]["value"], raw);
        let mut args = Args::new();
        let sql = property
            .predicate(descriptor["key"].as_str().unwrap(), &mut args)
            .unwrap();
        assert!(!sql.contains(raw));
        assert_eq!(args.len(), 2);
        for key in [
            format!(
                "property:{}:value:{}",
                property.id,
                URL_SAFE_NO_PAD.encode("deleted")
            ),
            format!("property:{}:unset:*", property.id),
            format!("property:{}:unknown:", property.id),
            format!("property:{}:unset:", Uuid::new_v4()),
        ] {
            assert!(property.predicate(&key, &mut Args::new()).is_err());
        }
    }
}
