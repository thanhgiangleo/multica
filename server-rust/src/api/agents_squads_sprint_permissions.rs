//! Go V1 invocation permissions; teams are stored but never grant access here.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub(super) struct Target {
    pub target_type: String,
    pub target_id: Uuid,
}
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Permission {
    pub mode: String,
    pub targets: Vec<Target>,
}
impl Permission {
    pub fn visibility(&self) -> &'static str {
        if self.mode == "public_to" && self.targets.iter().any(|t| t.target_type == "workspace") {
            "workspace"
        } else {
            "private"
        }
    }
}
fn text<'a>(
    fields: &'a serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>, ApiError> {
    match fields.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        _ => Err(bad("invalid request body")),
    }
}
pub(super) fn validate_shape(fields: &serde_json::Map<String, Value>) -> Result<(), ApiError> {
    text(fields, "permission_mode")?;
    text(fields, "visibility")?;
    if let Some(raw) = fields.get("invocation_targets").filter(|v| !v.is_null()) {
        let raw = raw.as_array().ok_or_else(|| bad("invalid request body"))?;
        for item in raw {
            if !item.is_object()
                || item
                    .get("target_type")
                    .is_some_and(|v| !v.is_null() && !v.is_string())
                || item
                    .get("target_id")
                    .is_some_and(|v| !v.is_null() && !v.is_string())
            {
                return Err(bad("invalid request body"));
            }
        }
    }
    Ok(())
}
pub(super) fn parse(
    ws: Uuid,
    fields: &serde_json::Map<String, Value>,
    create: bool,
) -> Result<Option<Permission>, ApiError> {
    validate_shape(fields)?;
    let mode = text(fields, "permission_mode")?;
    let legacy = text(fields, "visibility")?;
    // Create's nil permission_mode uses legacy visibility. Update distinguishes
    // an explicitly null mode (private) from an absent mode, matching Go raw fields.
    let has_mode = if create {
        mode.is_some()
    } else {
        fields.contains_key("permission_mode")
    };
    let workspace = || Target {
        target_type: "workspace".into(),
        target_id: ws,
    };
    if !has_mode {
        let legacy = if create {
            Some(legacy.unwrap_or("private"))
        } else {
            legacy
        };
        return match legacy {
            None => Ok(None),
            Some("workspace") => Ok(Some(Permission {
                mode: "public_to".into(),
                targets: vec![workspace()],
            })),
            Some("private" | "") => Ok(Some(Permission {
                mode: "private".into(),
                targets: vec![],
            })),
            _ => Err(bad("visibility must be 'private' or 'workspace'")),
        };
    }
    let mode = match mode {
        None | Some("" | "private") => "private",
        Some("public_to") => "public_to",
        _ => return Err(bad("permission_mode must be 'private' or 'public_to'")),
    };
    let mut targets = Vec::new();
    // Private discards targets, but reject invalid JSON container types just as
    // decoding Go's typed []AgentInvocationTargetDTO does before normalization.
    let raw = match fields.get("invocation_targets") {
        None | Some(Value::Null) => &[][..],
        Some(Value::Array(raw)) => raw.as_slice(),
        _ => return Err(bad("invalid request body")),
    };
    for item in raw {
        if !item.is_object() {
            return Err(bad("invalid request body"));
        }
        if item
            .get("target_type")
            .is_some_and(|v| !v.is_null() && !v.is_string())
            || item
                .get("target_id")
                .is_some_and(|v| !v.is_null() && !v.is_string())
        {
            return Err(bad("invalid request body"));
        }
    }
    if mode == "private" {
        return Ok(Some(Permission {
            mode: mode.into(),
            targets,
        }));
    }
    let mut seen = BTreeSet::new();
    for item in raw {
        let target = match item.get("target_type").and_then(Value::as_str) {
            Some("workspace") => workspace(),
            Some(kind @ ("member" | "team")) => {
                let id = item
                    .get("target_id")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| bad("member/team invocation target requires target_id"))?;
                Target {
                    target_type: kind.into(),
                    target_id: parse_uuid(id)?,
                }
            }
            _ => {
                return Err(bad(
                    "invocation target_type must be 'workspace', 'member', or 'team'",
                ));
            }
        };
        if seen.insert(target.clone()) {
            targets.push(target);
        }
    }
    if targets.is_empty() {
        targets.push(workspace());
    }
    Ok(Some(Permission {
        mode: mode.into(),
        targets,
    }))
}
fn parse_uuid(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| bad("member/team invocation target_id is not a valid uuid"))
}

pub(super) fn changes(
    ws: Uuid,
    fields: &serde_json::Map<String, Value>,
    current: &Permission,
) -> bool {
    if !fields.contains_key("permission_mode") && !fields.contains_key("invocation_targets") {
        return text(fields, "visibility")
            .ok()
            .flatten()
            .is_some_and(|vis| {
                let submitted = if vis == "workspace" {
                    "workspace"
                } else {
                    "private"
                };
                submitted != current.visibility()
            });
    }
    let Ok(Some(want)) = parse(ws, fields, false) else {
        return false;
    };
    want.mode != current.mode
        || want.targets.into_iter().collect::<BTreeSet<_>>()
            != current.targets.iter().cloned().collect::<BTreeSet<_>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fields(value: Value) -> serde_json::Map<String, Value> {
        value.as_object().unwrap().clone()
    }
    #[test]
    fn permission_defaults_normalization_and_legacy_noop() {
        let ws = Uuid::new_v4();
        let member = Uuid::new_v4();
        let empty = fields(json!({}));
        assert_eq!(parse(ws, &empty, true).unwrap().unwrap().mode, "private");
        assert!(parse(ws, &empty, false).unwrap().is_none());
        let public = parse(
            ws,
            &fields(json!({"permission_mode":"public_to","invocation_targets":[]})),
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(public.visibility(), "workspace");
        assert_eq!(public.targets[0].target_id, ws);
        let private = parse(
            ws,
            &fields(json!({"permission_mode":null,"visibility":"workspace"})),
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(private.mode, "private");
        let current=parse(ws,&fields(json!({"permission_mode":"public_to","invocation_targets":[{"target_type":"member","target_id":member},{"target_type":"member","target_id":member}]})),false).unwrap().unwrap();
        assert_eq!(current.targets.len(), 1);
        assert_eq!(current.visibility(), "private");
        assert!(!changes(
            ws,
            &fields(json!({"visibility":"private"})),
            &current
        ));
        assert!(!changes(
            ws,
            &fields(json!({"permission_mode":"invalid"})),
            &current
        ));
        assert!(changes(
            ws,
            &fields(json!({"permission_mode":"private"})),
            &current
        ));
        assert!(changes(
            ws,
            &fields(json!({"visibility":"workspace"})),
            &current
        ));
    }
    #[test]
    fn target_validation_never_uses_foreign_workspace_selector() {
        let ws = Uuid::new_v4();
        let p=parse(ws,&fields(json!({"permission_mode":"public_to","invocation_targets":[{"target_type":"workspace","target_id":"foreign"}]})),false).unwrap().unwrap();
        assert_eq!(p.targets[0].target_id, ws);
        for target in [
            json!({"target_type":"team"}),
            json!({"target_type":"member","target_id":"invalid"}),
            json!({"target_type":"unknown"}),
        ] {
            assert!(
                parse(
                    ws,
                    &fields(json!({"permission_mode":"public_to","invocation_targets":[target]})),
                    false
                )
                .is_err()
            );
        }
        assert!(
            parse(
                ws,
                &fields(json!({"permission_mode":"private","invocation_targets":"bad"})),
                false
            )
            .is_err()
        );
    }
}
