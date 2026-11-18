//! Group headers and row predicates from the frozen Go table contract.
#[path = "queries_sprint_table_compound_groups.rs"]
mod compound_groups;
#[path = "queries_sprint_table_property_groups.rs"]
mod property_groups;
use super::*;

const BEHAVIORS: [&str; 7] = [
    "backlog",
    "todo",
    "in_progress",
    "in_review",
    "blocked",
    "done",
    "cancelled",
];
const CATEGORIES: [&str; 4] = ["unstarted", "started", "done", "closed"];

fn category(status: &str) -> Option<&'static str> {
    match status {
        "backlog" | "todo" | "unstarted" => Some("unstarted"),
        "in_progress" | "in_review" | "blocked" | "started" => Some("started"),
        "done" | "completed" => Some("done"),
        "cancelled" | "canceled" | "closed" => Some("closed"),
        _ => None,
    }
}
fn wire_category(status: &str, category: &str) -> String {
    if BEHAVIORS.contains(&status) {
        return status.to_owned();
    }
    match category {
        "unstarted" => "todo",
        "started" => "in_progress",
        "done" => "done",
        "closed" => "cancelled",
        _ => category,
    }
    .to_owned()
}

#[derive(Default)]
pub(super) struct Resolved {
    kind: String,
    status_order: Vec<String>,
    legacy_categories: bool,
    custom_categories: BTreeMap<String, String>,
    category_keys: BTreeMap<String, Vec<String>>,
    property: Option<property_groups::Property>,
    compound: Option<compound_groups::Compound>,
}
pub(super) fn identity(group: &Group) -> String {
    let base = if group.kind == "property" {
        format!(
            "group:property:{}:empty={}",
            group.property_id, group.include_empty
        )
    } else {
        format!("group:{}", group.kind)
    };
    if group.kind == "compound" {
        let mut identity = format!("group:compound:{}:{}", group.primary, group.secondary);
        if let Some(values) = &group.secondary_values {
            identity.push_str(&format!(":visible={}", values.join(",")));
        }
        if group.secondary == "status_category" {
            identity.push_str(&format!(
                ":category_format={}",
                if group.category_format.is_empty() {
                    "legacy"
                } else {
                    &group.category_format
                }
            ));
        }
        return identity;
    }
    if group.kind == "status_category" {
        format!(
            "{base}:category_format={}",
            if group.category_format.is_empty() {
                "legacy"
            } else {
                &group.category_format
            }
        )
    } else {
        base
    }
}
fn unsupported(code: &str, message: &str) -> TableError {
    TableError(
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({"error":"unsupported_group","code":code,"message":message}),
    )
}
pub(super) fn normalize_key(
    group: &Group,
    key: Option<&str>,
) -> Result<Option<String>, TableError> {
    let key = key.map(str::trim).filter(|s| !s.is_empty());
    if group.kind == "none" {
        if key.is_some() {
            return Err(invalid("group_key must be empty when group.kind=none"));
        }
        Ok(None)
    } else {
        key.map(|s| Some(s.to_owned()))
            .ok_or_else(|| invalid("group_key is required"))
    }
}
pub(super) async fn resolve(
    db: &impl GenericClient,
    ws: Uuid,
    group: &Group,
    allow_none: bool,
) -> Result<Resolved, TableError> {
    if !group.category_format.is_empty()
        && (group.category_format != "lifecycle"
            || !(group.kind == "status_category"
                || (group.kind == "compound" && group.secondary == "status_category")))
    {
        return Err(invalid(
            "category_format=lifecycle requires status_category grouping",
        ));
    }
    if group.kind != "compound" && group.secondary_values.is_some() {
        return Err(invalid(
            "group.secondary_values requires group.kind=compound",
        ));
    }
    let mut result = Resolved {
        kind: group.kind.clone(),
        legacy_categories: group.category_format.is_empty(),
        ..Default::default()
    };
    match group.kind.as_str() {
        "none" if allow_none => (),
        "none" => return Err(invalid("group.kind=none is not valid for group headers")),
        "status" => {
            let entries = db.query("WITH builtin(key,category,rank) AS (VALUES ('backlog','unstarted',0),('todo','unstarted',1),('in_progress','started',2),('in_review','started',3),('blocked','started',4),('done','done',5),('cancelled','closed',6)), entries AS (SELECT key,category,position,is_system FROM issue_status WHERE workspace_id=$1 UNION ALL SELECT b.key,b.category,0,true FROM builtin b WHERE NOT EXISTS(SELECT 1 FROM issue_status s WHERE s.workspace_id=$1 AND s.key=b.key)) SELECT e.key,e.category FROM entries e LEFT JOIN builtin b ON b.key=e.key ORDER BY CASE e.category WHEN 'unstarted' THEN 0 WHEN 'started' THEN 1 WHEN 'done' THEN 2 WHEN 'closed' THEN 3 ELSE 0 END,e.position,CASE WHEN e.is_system THEN 0 ELSE 1 END,CASE WHEN e.is_system THEN COALESCE(b.rank,0) ELSE 0 END,e.key", &[&ws])
                .await.map_err(|e| query_failure(e,"failed to resolve table group"))?;
            for entry in entries {
                let key: String = entry.get("key");
                let category: String = entry.get("category");
                if !BEHAVIORS.contains(&key.as_str()) && CATEGORIES.contains(&category.as_str()) {
                    result.custom_categories.insert(key.clone(), category);
                }
                result.status_order.push(key);
            }
        }
        "status_category" => {
            for status in BEHAVIORS {
                let bucket = if result.legacy_categories {
                    status
                } else {
                    category(status).expect("builtin")
                };
                result
                    .category_keys
                    .entry(bucket.to_owned())
                    .or_default()
                    .push(status.to_owned());
            }
            let entries = db.query("SELECT key,category FROM issue_status WHERE workspace_id=$1 AND key<>ALL($2::text[]) AND category=ANY($3::text[]) ORDER BY key", &[&ws,&BEHAVIORS.map(str::to_owned).to_vec(),&CATEGORIES.map(str::to_owned).to_vec()])
                .await.map_err(|e| query_failure(e, "failed to resolve table group"))?;
            for entry in entries {
                let key: String = entry.get("key");
                let stored: String = entry.get("category");
                let bucket = category(&stored).unwrap_or(&stored);
                let bucket = if result.legacy_categories {
                    wire_category(&key, bucket)
                } else {
                    bucket.to_owned()
                };
                result
                    .category_keys
                    .entry(bucket.clone())
                    .or_default()
                    .push(key.clone());
                result.custom_categories.insert(key, bucket);
            }
        }
        "assignee" | "project" | "parent" => (),
        "property" => {
            result.property =
                Some(property_groups::Property::resolve(db, ws, &group.property_id).await?)
        }
        "compound" => {
            result.compound = Some(compound_groups::Compound::resolve(db, ws, group).await?)
        }
        _ => {
            return Err(unsupported(
                "group_kind_unsupported",
                "This group type is not supported.",
            ));
        }
    }
    Ok(result)
}
impl Resolved {
    pub(super) fn promoted_keys(&self) -> Option<Vec<String>> {
        self.compound
            .as_ref()
            .filter(|c| c.primary().kind == "parent")
            .and_then(|c| c.visible_keys())
    }
    fn expression(&self, args: &mut Args) -> String {
        if let Some(compound) = &self.compound {
            return compound.primary().expression(args);
        }
        if let Some(property) = &self.property {
            return property.expression(args);
        }
        if self.kind == "status_category" {
            let mut categories = self.custom_categories.clone();
            for status in BEHAVIORS {
                categories.insert(
                    status.to_owned(),
                    if self.legacy_categories {
                        status.to_owned()
                    } else {
                        category(status).expect("builtin").to_owned()
                    },
                );
            }
            let mut expression = "CASE i.status".to_owned();
            for (status, category) in categories {
                expression.push_str(&format!(
                    " WHEN {}::text THEN {}::text",
                    arg(args, status),
                    arg(args, category)
                ));
            }
            expression.push_str(" ELSE i.status END");
            return expression;
        }
        match self.kind.as_str() {
            "status" => "i.status",
            "assignee" => {
                "CASE WHEN i.assignee_type IS NULL OR i.assignee_id IS NULL THEN '__unassigned__' ELSE i.assignee_type || ':' || i.assignee_id::text END"
            }
            "project" => "COALESCE(i.project_id::text, '__no_project__')",
            "parent" => "COALESCE(i.parent_issue_id::text, '__no_parent__')",
            _ => unreachable!("resolved group"),
        }.to_owned()
    }
    fn sort(&self) -> &'static str {
        if let Some(compound) = &self.compound {
            return compound.primary().sort();
        }
        match self.kind.as_str() {
            "assignee" => {
                "LOWER(COALESCE(CASE split_part(group_value, ':', 1) WHEN 'member' THEN (SELECT u.name FROM \"user\" u WHERE u.id=split_part(group_value, ':', 2)::uuid) WHEN 'agent' THEN (SELECT a.name FROM agent a WHERE a.workspace_id=$1 AND a.id=split_part(group_value, ':', 2)::uuid) WHEN 'squad' THEN (SELECT s.name FROM squad s WHERE s.workspace_id=$1 AND s.id=split_part(group_value, ':', 2)::uuid) END, ''))"
            }
            "project" => {
                "CASE WHEN group_value='__no_project__' THEN '' ELSE LOWER(COALESCE((SELECT p.title FROM project p WHERE p.workspace_id=$1 AND p.id=group_value::uuid),'')) END"
            }
            "parent" => {
                "CASE WHEN group_value='__no_parent__' THEN '' ELSE LOWER(COALESCE((SELECT p.title FROM issue p WHERE p.workspace_id=$1 AND p.id=group_value::uuid),'')) END"
            }
            _ => "group_value",
        }
    }
    fn order(&self, args: &mut Args) -> String {
        if let Some(compound) = &self.compound {
            return compound.primary().order(args);
        }
        if let Some(property) = &self.property {
            return property.order(args);
        }
        match self.kind.as_str() {
            "status" => format!("COALESCE(array_position({}::text[], group_value),100000)",arg(args,self.status_order.clone())),
            "status_category" if self.legacy_categories => format!("COALESCE(array_position({}::text[], group_value),100000)", arg(args, BEHAVIORS.map(str::to_owned).to_vec())),
            "status_category" => "CASE group_value WHEN 'unstarted' THEN 0 WHEN 'started' THEN 1 WHEN 'done' THEN 2 WHEN 'closed' THEN 3 ELSE 4 END".into(),
            "assignee" => "CASE split_part(group_value, ':', 1) WHEN 'member' THEN 0 WHEN 'agent' THEN 1 WHEN 'squad' THEN 2 ELSE 3 END".into(),
            "project" => "CASE WHEN group_value='__no_project__' THEN 0 ELSE 1 END".into(),
            "parent" => "CASE WHEN group_value='__no_parent__' THEN 0 ELSE 1 END".into(),
            _ => unreachable!("resolved group"),
        }
    }
    pub(super) fn predicate(&self, key: &str, args: &mut Args) -> Result<String, TableError> {
        if let Some(compound) = &self.compound {
            return compound.predicate(key, args);
        }
        if let Some(property) = &self.property {
            return property.predicate(key, args);
        }
        match self.kind.as_str() {
            "none" if key.is_empty() => Ok("TRUE".into()),
            "status" => {
                let status = key
                    .strip_prefix("status:")
                    .filter(|s| !s.is_empty() && s.len() <= 64)
                    .ok_or_else(|| invalid("invalid group_key"))?;
                Ok(format!("i.status={}::text", arg(args, status.to_owned())))
            }
            "status_category" => {
                let raw = key
                    .strip_prefix("status_category:")
                    .ok_or_else(|| invalid("invalid group_key"))?;
                let bucket = if self.legacy_categories {
                    if !BEHAVIORS.contains(&raw) {
                        return Err(invalid("invalid group_key"));
                    }
                    raw
                } else {
                    category(raw)
                        .filter(|v| CATEGORIES.contains(v))
                        .ok_or_else(|| invalid("invalid group_key"))?
                };
                let keys = self.category_keys.get(bucket).cloned().unwrap_or_default();
                Ok(format!("i.status=ANY({}::text[])", arg(args, keys)))
            }
            "assignee" => {
                let raw = key
                    .strip_prefix("assignee:")
                    .ok_or_else(|| invalid("invalid group_key"))?;
                if raw == "unassigned" {
                    return Ok("i.assignee_type IS NULL AND i.assignee_id IS NULL".into());
                }
                let (kind, id) = raw
                    .split_once(':')
                    .ok_or_else(|| invalid("invalid group_key"))?;
                if !matches!(kind, "member" | "agent" | "squad") {
                    return Err(invalid("invalid group_key"));
                }
                let id = Uuid::parse_str(id).map_err(|_| invalid("invalid group_key"))?;
                Ok(format!(
                    "i.assignee_type={}::text AND i.assignee_id={}::uuid",
                    arg(args, kind.to_owned()),
                    arg(args, id)
                ))
            }
            "project" | "parent" => {
                let raw = key
                    .strip_prefix(&format!("{}:", self.kind))
                    .ok_or_else(|| invalid("invalid group_key"))?;
                let column = if self.kind == "project" {
                    "i.project_id"
                } else {
                    "i.parent_issue_id"
                };
                if raw == "none" {
                    return Ok(format!("{column} IS NULL"));
                }
                let id = Uuid::parse_str(raw).map_err(|_| invalid("invalid group_key"))?;
                Ok(format!("{column}={}::uuid", arg(args, id)))
            }
            _ => Err(invalid("invalid group_key")),
        }
    }
    fn descriptor_counts(
        &self,
        raw: &str,
        count: i64,
        parent: Value,
        secondary_counts: &Value,
    ) -> Result<Value, TableError> {
        if let Some(compound) = &self.compound {
            return compound.descriptor(raw, count, parent, secondary_counts);
        }
        self.descriptor(raw, count, parent)
    }
    fn descriptor(&self, raw: &str, count: i64, parent: Value) -> Result<Value, TableError> {
        if let Some(property) = &self.property {
            return property.descriptor(raw, count);
        }
        let mut value = json!({"kind":self.kind,"actor":null});
        let key = match self.kind.as_str() {
            "status" if !raw.is_empty() => {
                value["status"] = json!(raw);
                format!("status:{raw}")
            }
            "status_category"
                if (self.legacy_categories && BEHAVIORS.contains(&raw))
                    || (!self.legacy_categories && CATEGORIES.contains(&raw)) =>
            {
                value["kind"] = json!("status");
                value["status"] = json!(raw);
                format!("status_category:{raw}")
            }
            "assignee" if raw == "__unassigned__" => "assignee:unassigned".into(),
            "assignee" => {
                let (kind, id) = raw
                    .split_once(':')
                    .ok_or_else(|| db_failure("failed to resolve table group"))?;
                if !matches!(kind, "member" | "agent" | "squad") || Uuid::parse_str(id).is_err() {
                    return Err(db_failure("failed to resolve table group"));
                }
                value["actor"] = json!({"type":kind,"id":id});
                format!("assignee:{raw}")
            }
            "project" | "parent" => {
                let none = if self.kind == "project" {
                    "__no_project__"
                } else {
                    "__no_parent__"
                };
                if raw == none {
                    if self.kind == "parent" {
                        value["value_state"] = json!("unset");
                    }
                    format!("{}:none", self.kind)
                } else {
                    Uuid::parse_str(raw)
                        .map_err(|_| db_failure("failed to resolve table group"))?;
                    if self.kind == "project" {
                        value["project_id"] = json!(raw);
                    } else {
                        value["parent_id"] = json!(raw);
                        value["value_state"] = json!(if parent.is_null() {
                            "unavailable"
                        } else {
                            "value"
                        });
                        if !parent.is_null() {
                            value["parent"] = parent;
                        }
                    }
                    format!("{}:{raw}", self.kind)
                }
            }
            _ => return Err(db_failure("failed to resolve table group")),
        };
        Ok(json!({"key":key,"value":value,"count":count}))
    }
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct GroupsRequest {
    query: Query,
    group: Group,
    page: Page,
}
#[derive(Deserialize, Serialize)]
struct GroupsCursor {
    v: u8,
    query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_order: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_sort_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_cursor_key: Option<String>,
}
pub(super) fn routes() -> Router<ApiState> {
    Router::new().route(
        "/api/issues/table/groups",
        post(groups).layer(DefaultBodyLimit::max(1 << 20)),
    )
}
async fn groups(
    state: State<ApiState>,
    ws: Extension<WorkspaceId>,
    user: Extension<Identity>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<Value>, TableError> {
    tokio::time::timeout(
        std::time::Duration::from_secs(8),
        execute(state, ws, user, body),
    )
    .await
    .map_err(|_| query_timeout())?
}
async fn execute(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<Value>, TableError> {
    let body = body.map_err(|_| invalid("invalid issue table query"))?;
    let mut decoder = serde_json::Deserializer::from_slice(&body);
    let req = GroupsRequest::deserialize(&mut decoder)
        .map_err(|_| invalid("invalid issue table query"))?;
    decoder
        .end()
        .map_err(|_| invalid("invalid issue table query: request must contain one JSON object"))?;
    let limit = page_limit(req.page.limit)?;
    let fp = fingerprint(ws, &req.query);
    let identity = identity(&req.group);
    let cursor=req.page.cursor.as_deref().filter(|s|!s.trim().is_empty()).map(|s| {
        let bytes=URL_SAFE_NO_PAD.decode(s).map_err(|_|invalid("invalid cursor"))?;
        let c:GroupsCursor=serde_json::from_slice(&bytes).map_err(|_|invalid("invalid cursor"))?;
        if c.v!=1 {return Err(invalid("invalid cursor"));}
        if c.query!=fp || c.group_key.as_deref()!=Some(identity.as_str()) || c.parent_id.is_some() {return Err(TableError(StatusCode::CONFLICT,json!({"error":"cursor_query_mismatch","message":"cursor does not belong to this table query"})));}
        if c.group_order.is_none() || c.group_sort_key.is_none() || c.group_cursor_key.is_none() {return Err(invalid("invalid cursor"));}
        Ok(c)
    }).transpose()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| db_failure("failed to start table query"))?;
    let tx = db
        .build_transaction()
        .read_only(true)
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await
        .map_err(|_| db_failure("failed to start table query"))?;
    tx.batch_execute("SET LOCAL statement_timeout = '8s'")
        .await
        .map_err(|e| query_failure(e, "failed to start table query"))?;
    let mut args = Args::new();
    let where_ = compile(&*tx, ws, user, &req.query, &mut args).await?;
    let group = resolve(&*tx, ws, &req.group, false).await?;
    let expression = group.expression(&mut args);
    let order = group.order(&mut args);
    let primary_kind = group
        .compound
        .as_ref()
        .map_or(group.kind.as_str(), |c| c.primary().kind.as_str());
    let context = if primary_kind == "parent" {
        "CASE WHEN group_value='__no_parent__' THEN NULL::jsonb ELSE (SELECT jsonb_build_object('id',p.id::text,'number',p.number,'identifier',w.issue_prefix || '-' || p.number::text,'title',p.title,'status',p.status) FROM issue p JOIN workspace w ON w.id=p.workspace_id WHERE p.workspace_id=$1 AND p.id=group_value::uuid) END"
    } else {
        "NULL::jsonb"
    };
    let predicate = if let Some(c) = &cursor {
        let order = arg(&mut args, c.group_order.expect("checked cursor"));
        let sort = arg(&mut args, c.group_sort_key.clone().expect("checked cursor"));
        let key = arg(
            &mut args,
            c.group_cursor_key.clone().expect("checked cursor"),
        );
        format!(
            "(group_order>{order}::int OR (group_order={order}::int AND (group_sort>{sort}::text OR (group_sort={sort}::text AND group_value>{key}::text))))"
        )
    } else {
        "TRUE".into()
    };
    let limit_ref = arg(&mut args, limit + 1);
    let mut grouped = format!(
        "grouped AS (SELECT {expression} AS group_value,count(*)::bigint AS issue_count FROM issue i WHERE {where_} GROUP BY 1)"
    );
    if req.group.include_empty
        && let Some(property) = &group.property
    {
        let expected = arg(&mut args, property.expected());
        grouped = format!(
            "actual AS (SELECT {expression} AS group_value,count(*)::bigint AS issue_count FROM issue i WHERE {where_} GROUP BY 1), expected AS (SELECT unnest({expected}::text[]) AS group_value), grouped AS (SELECT e.group_value,COALESCE(a.issue_count,0)::bigint AS issue_count FROM expected e LEFT JOIN actual a USING(group_value) UNION ALL SELECT a.group_value,a.issue_count FROM actual a WHERE NOT (a.group_value=ANY({expected}::text[])))"
        );
    }
    let (visible, secondary) = if let Some(compound) = &group.compound {
        grouped = compound.cte(&where_, &mut args);
        ("visible_count", "secondary_counts")
    } else {
        ("issue_count", "'{}'::jsonb")
    };
    let sql = format!(
        "WITH {grouped}, sorted AS (SELECT group_value,issue_count,({visible})::bigint AS visible_count,({secondary})::jsonb AS secondary_counts,({})::text AS group_sort,({context})::jsonb AS group_context FROM grouped), ranked AS (SELECT *,({order})::int AS group_order,sum(visible_count) OVER ()::bigint AS total FROM sorted) SELECT * FROM ranked WHERE {predicate} ORDER BY group_order,group_sort,group_value LIMIT {limit_ref}",
        group.sort()
    );
    let mut rows = tx
        .query(&sql, &refs(&args))
        .await
        .map_err(|e| query_failure(e, "failed to list table groups"))?;
    let total = rows.first().map_or(0, |r| r.get::<_, i64>("total"));
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next_cursor = if more {
        let row = rows.last().expect("positive limit");
        Some(
            URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&GroupsCursor {
                    v: 1,
                    query: fp.clone(),
                    group_key: Some(identity),
                    parent_id: None,
                    group_order: Some(row.get("group_order")),
                    group_sort_key: Some(row.get("group_sort")),
                    group_cursor_key: Some(row.get("group_value")),
                })
                .map_err(|_| db_failure("failed to encode table cursor"))?,
            ),
        )
    } else {
        None
    };
    let groups = rows
        .iter()
        .map(|r| {
            group.descriptor_counts(
                r.get("group_value"),
                r.get("issue_count"),
                r.get::<_, Option<Value>>("group_context")
                    .unwrap_or(Value::Null),
                &r.get::<_, Value>("secondary_counts"),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    tx.commit()
        .await
        .map_err(|_| db_failure("failed to finish table query"))?;
    Ok(GoJson(
        json!({"query_fingerprint":fp,"total":total,"groups":groups,"next_cursor":next_cursor}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursor_identity_binds_all_group_axes() {
        let mut group = Group {
            kind: "property".into(),
            property_id: Uuid::nil().to_string(),
            include_empty: true,
            ..Default::default()
        };
        assert_eq!(
            identity(&group),
            format!("group:property:{}:empty=true", Uuid::nil())
        );
        group.kind = "status_category".into();
        assert_eq!(
            identity(&group),
            "group:status_category:category_format=legacy"
        );
        group.category_format = "lifecycle".into();
        assert_eq!(
            identity(&group),
            "group:status_category:category_format=lifecycle"
        );
        group.kind = "compound".into();
        group.primary = "parent".into();
        group.secondary = "status_category".into();
        group.secondary_values = Some(vec!["todo".into(), "shipped".into()]);
        assert_eq!(
            identity(&group),
            "group:compound:parent:status_category:visible=todo,shipped:category_format=lifecycle"
        );
        group.secondary_values = Some(vec![]);
        assert_eq!(
            identity(&group),
            "group:compound:parent:status_category:visible=:category_format=lifecycle"
        );
        assert_eq!(category("canceled"), Some("closed"));
        assert_eq!(wire_category("shipped", "done"), "done");
    }

    #[test]
    fn group_keys_are_typed_and_bound() {
        for kind in ["status", "assignee", "project", "parent"] {
            let g = Resolved {
                kind: kind.into(),
                status_order: vec![],
                ..Default::default()
            };
            assert!(g.predicate("bogus", &mut Args::new()).is_err());
        }
        let g = Resolved {
            kind: "status".into(),
            status_order: vec![],
            ..Default::default()
        };
        let mut args = Args::new();
        assert_eq!(
            g.predicate("status:'; SELECT *", &mut args).unwrap(),
            "i.status=$1::text"
        );
        assert_eq!(args.len(), 1);
        let g = Group {
            kind: "none".into(),
            ..Default::default()
        };
        assert_eq!(normalize_key(&g, Some(" ")).unwrap(), None);
        assert!(normalize_key(&g, Some("status:todo")).is_err());
    }
}
