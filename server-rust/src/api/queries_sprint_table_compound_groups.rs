//! Compound primary-axis headers and status cells, with optional visible columns.
use super::*;

pub(super) struct Compound {
    primary: Box<Resolved>,
    secondary: Box<Resolved>,
    visible: Option<Vec<String>>,
}
impl Compound {
    pub(super) async fn resolve(
        db: &impl GenericClient,
        ws: Uuid,
        group: &Group,
    ) -> Result<Self, TableError> {
        if !matches!(group.secondary.as_str(), "status" | "status_category") {
            return Err(unsupported(
                "secondary_group_unsupported",
                "Only status is supported as a secondary group.",
            ));
        }
        if !matches!(group.primary.as_str(), "assignee" | "project" | "parent") {
            return Err(unsupported(
                "primary_group_unsupported",
                "This primary group is not supported.",
            ));
        }
        let primary = Box::new(
            Box::pin(resolve(
                db,
                ws,
                &Group {
                    kind: group.primary.clone(),
                    ..Group::default()
                },
                false,
            ))
            .await?,
        );
        let secondary = Box::new(
            Box::pin(resolve(
                db,
                ws,
                &Group {
                    kind: group.secondary.clone(),
                    category_format: group.category_format.clone(),
                    ..Group::default()
                },
                false,
            ))
            .await?,
        );
        let mut visible = None;
        if let Some(values) = &group.secondary_values {
            let mut seen = std::collections::HashSet::new();
            let mut normalized = Vec::new();
            for input in values {
                if !seen.insert(input.as_str()) {
                    return Err(invalid("duplicate group.secondary_values"));
                }
                let value = if group.secondary == "status_category" && !secondary.legacy_categories
                {
                    category(input)
                        .ok_or_else(|| invalid("invalid group.secondary_values"))?
                        .to_owned()
                } else {
                    input.clone()
                };
                let valid = if group.secondary == "status_category" {
                    if secondary.legacy_categories {
                        BEHAVIORS.contains(&value.as_str())
                    } else {
                        CATEGORIES.contains(&value.as_str())
                    }
                } else {
                    BEHAVIORS.contains(&value.as_str())
                        || secondary.custom_categories.contains_key(&value)
                };
                if !valid {
                    return Err(invalid("invalid group.secondary_values"));
                }
                if !normalized.contains(&value) {
                    normalized.push(value);
                }
            }
            visible = Some(normalized);
        }
        Ok(Self {
            primary,
            secondary,
            visible,
        })
    }
    pub(super) fn primary(&self) -> &Resolved {
        &self.primary
    }
    pub(super) fn visible_keys(&self) -> Option<Vec<String>> {
        self.visible.as_ref().map(|values| {
            if self.secondary.kind == "status_category" {
                values
                    .iter()
                    .flat_map(|v| {
                        self.secondary
                            .category_keys
                            .get(v)
                            .cloned()
                            .unwrap_or_default()
                    })
                    .collect()
            } else {
                values.clone()
            }
        })
    }
    pub(super) fn predicate(&self, key: &str, args: &mut Args) -> Result<String, TableError> {
        let (encoded, status) = key
            .strip_prefix("compound:")
            .and_then(|raw| {
                raw.split_once(if self.secondary.kind == "status_category" {
                    ":status_category:"
                } else {
                    ":status:"
                })
            })
            .ok_or_else(|| invalid("invalid group_key"))?;
        let status =
            if self.secondary.kind == "status_category" && !self.secondary.legacy_categories {
                category(status)
                    .ok_or_else(|| invalid("invalid group_key"))?
                    .to_owned()
            } else {
                status.to_owned()
            };
        let valid = if self.secondary.kind == "status_category" {
            if self.secondary.legacy_categories {
                BEHAVIORS.contains(&status.as_str())
            } else {
                CATEGORIES.contains(&status.as_str())
            }
        } else {
            BEHAVIORS.contains(&status.as_str())
                || self.secondary.custom_categories.contains_key(&status)
        };
        if !valid {
            return Err(invalid("invalid group_key"));
        }
        let decoded = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| invalid("invalid group_key"))?;
        let decoded = String::from_utf8(decoded).map_err(|_| invalid("invalid group_key"))?;
        let primary = self.primary.predicate(&decoded, args)?;
        if self.secondary.kind == "status_category" {
            let keys = self
                .secondary
                .category_keys
                .get(&status)
                .cloned()
                .unwrap_or_default();
            Ok(format!(
                "({primary}) AND i.status=ANY({}::text[])",
                arg(args, keys)
            ))
        } else {
            Ok(format!(
                "({primary}) AND i.status={}::text",
                arg(args, status)
            ))
        }
    }
    pub(super) fn descriptor(
        &self,
        raw: &str,
        count: i64,
        parent: Value,
        secondary_counts: &Value,
    ) -> Result<Value, TableError> {
        let mut primary = self.primary.descriptor(raw, count, parent)?;
        let mut statuses = BEHAVIORS.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        statuses.extend(self.secondary.custom_categories.keys().cloned());
        if self.secondary.kind == "status_category" {
            statuses = if self.secondary.legacy_categories {
                BEHAVIORS.iter().map(|s| s.to_string()).collect()
            } else {
                CATEGORIES.iter().map(|s| s.to_string()).collect()
            };
        }
        let encoded = URL_SAFE_NO_PAD.encode(
            primary["key"]
                .as_str()
                .ok_or_else(|| db_failure("failed to resolve table group"))?,
        );
        let axis = &self.secondary.kind;
        primary["secondary_groups"]=Value::Array(statuses.into_iter().map(|status| {
            json!({"key":format!("compound:{encoded}:{axis}:{status}"),"value":{"kind":"status","status":status,"actor":null},"count":secondary_counts.get(&status).and_then(Value::as_i64).unwrap_or(0)})
        }).collect());
        Ok(primary)
    }
    pub(super) fn cte(&self, where_: &str, args: &mut Args) -> String {
        let primary = self.primary.expression(args);
        let secondary = self.secondary.expression(args);
        if let Some(visible) = &self.visible {
            let visible_ref = arg(args, visible.clone());
            let excluded = if self.primary.kind == "parent" {
                let keys = arg(args, self.visible_keys().unwrap_or_default());
                format!(
                    ", promoted_parents AS (SELECT DISTINCT child.parent_issue_id AS id FROM membership child WHERE child.parent_issue_id IS NOT NULL AND child.status=ANY({keys}::text[]))"
                )
            } else {
                String::new()
            };
            let condition = if self.primary.kind == "parent" {
                "NOT (i.parent_issue_id IS NULL AND EXISTS(SELECT 1 FROM promoted_parents p WHERE p.id=i.id))"
            } else {
                "TRUE"
            };
            format!(
                "membership AS NOT MATERIALIZED (SELECT i.* FROM issue i WHERE {where_}){excluded}, cells AS (SELECT {primary} AS group_value,{secondary} AS secondary_value,count(*) FILTER (WHERE {condition})::bigint AS cell_count FROM membership i GROUP BY 1,2), grouped AS (SELECT group_value,sum(cell_count)::bigint AS issue_count,COALESCE(sum(cell_count) FILTER (WHERE secondary_value=ANY({visible_ref}::text[])),0)::bigint AS visible_count,jsonb_object_agg(secondary_value,cell_count)::jsonb AS secondary_counts FROM cells GROUP BY group_value HAVING COALESCE(sum(cell_count) FILTER (WHERE secondary_value=ANY({visible_ref}::text[])),0)>0)"
            )
        } else {
            format!(
                "cells AS (SELECT {primary} AS group_value,{secondary} AS secondary_value,count(*)::bigint AS cell_count FROM issue i WHERE {where_} GROUP BY 1,2), grouped AS (SELECT group_value,sum(cell_count)::bigint AS issue_count,sum(cell_count)::bigint AS visible_count,jsonb_object_agg(secondary_value,cell_count)::jsonb AS secondary_counts FROM cells GROUP BY group_value)"
            )
        }
    }
}
