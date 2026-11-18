//! Disjunctive table facets, evaluated in one bounded read-only snapshot.
use super::super::Actor;
use super::*;
use std::collections::HashSet;

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Facet {
    kind: String,
    property_id: String,
}
impl Facet {
    fn identity(&self) -> String {
        if self.kind == "property" {
            format!("property:{}", self.property_id)
        } else {
            self.kind.clone()
        }
    }
    fn response(&self, values: Vec<Value>) -> Value {
        let mut response = json!({"kind":self.kind,"values":values});
        if !self.property_id.is_empty() {
            response["property_id"] = json!(self.property_id);
        }
        response
    }
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FacetsRequest {
    query: Query,
    facets: Vec<Facet>,
    include_total: Option<bool>,
}

fn without_facet(query: &Query, facet: &Facet) -> Query {
    let mut query = query.clone();
    let filters = &mut query.filters;
    match facet.kind.as_str() {
        "status" => filters.statuses.clear(),
        "priority" => filters.priorities.clear(),
        "assignee" => {
            filters.assignees = None;
            filters.include_no_assignee = false;
        }
        "creator" => filters.creators.clear(),
        "project" => {
            filters.project_ids.clear();
            filters.include_no_project = false;
        }
        "label" => filters.label_ids.clear(),
        "property" => {
            filters.properties.remove(&facet.property_id);
        }
        "working_agents" => {
            filters.working_issue_ids = None;
            filters.working_only = false;
        }
        _ => (),
    }
    query
}
fn base_expression(query: &Query, facet: &Facet) -> Option<&'static str> {
    let f = &query.filters;
    match facet.kind.as_str() {
        "status" if f.statuses.is_empty() => Some("i.status"),
        "priority" if f.priorities.is_empty() => Some("i.priority"),
        "assignee" if option_empty(&f.assignees) && !f.include_no_assignee => Some(
            "CASE WHEN i.assignee_type IS NULL OR i.assignee_id IS NULL THEN '__none__' ELSE i.assignee_type || ':' || i.assignee_id::text END",
        ),
        "creator" if f.creators.is_empty() => Some("i.creator_type || ':' || i.creator_id::text"),
        "project" if f.project_ids.is_empty() && !f.include_no_project => {
            Some("COALESCE(i.project_id::text, '__none__')")
        }
        _ => None,
    }
}
fn unsupported(code: &str, message: &str) -> TableError {
    TableError(
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({"error":"unsupported_group","code":code,"message":message}),
    )
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route(
        "/api/issues/table/facets",
        post(facets).layer(DefaultBodyLimit::max(1 << 20)),
    )
}
async fn facets(
    state: State<ApiState>,
    workspace: Extension<WorkspaceId>,
    user: Extension<Identity>,
    actor: Extension<Actor>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<Value>, TableError> {
    tokio::time::timeout(
        std::time::Duration::from_secs(8),
        execute_facets(state, workspace, user, actor, body),
    )
    .await
    .map_err(|_| query_timeout())?
}
async fn execute_facets(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<Value>, TableError> {
    let body = body.map_err(|_| invalid("invalid issue table query"))?;
    let mut decoder = serde_json::Deserializer::from_slice(&body);
    let mut req = FacetsRequest::deserialize(&mut decoder)
        .map_err(|_| invalid("invalid issue table query"))?;
    decoder
        .end()
        .map_err(|_| invalid("invalid issue table query: request must contain one JSON object"))?;
    if req.facets.is_empty() || req.facets.len() > 32 {
        return Err(invalid("facets must contain between 1 and 32 entries"));
    }
    let mut seen = HashSet::new();
    for facet in &mut req.facets {
        facet.kind = facet.kind.trim().to_owned();
        facet.property_id = facet.property_id.trim().to_owned();
        if !seen.insert(facet.identity()) {
            return Err(invalid("duplicate table facet"));
        }
    }
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
    let mut base_args = Args::new();
    let base = compile(&*tx, ws, user, &req.query, &mut base_args).await?;
    let include_total = req.include_total.unwrap_or(true);
    let mut total = 0i64;
    let mut responses: Vec<Value> = req.facets.iter().map(|f| f.response(vec![])).collect();
    let batch: Vec<(usize, &str)> = req
        .facets
        .iter()
        .enumerate()
        .filter_map(|(i, facet)| base_expression(&req.query, facet).map(|e| (i, e)))
        .collect();
    if !batch.is_empty() {
        let mut markers = Vec::new();
        let mut values = Vec::new();
        let mut sets = Vec::new();
        for (i, expression) in &batch {
            markers.push(format!("WHEN GROUPING({expression})=0 THEN {i}"));
            values.push(format!(
                "WHEN GROUPING({expression})=0 THEN ({expression})::text"
            ));
            sets.push(format!("({expression})"));
        }
        if include_total {
            sets.push("()".into());
        }
        let sql = format!(
            "SELECT CASE {} ELSE -1 END, CASE {} ELSE '' END, count(*)::bigint FROM issue i WHERE {base} GROUP BY GROUPING SETS ({})",
            markers.join(" "),
            values.join(" "),
            sets.join(",")
        );
        for row in tx
            .query(&sql, &refs(&base_args))
            .await
            .map_err(|e| query_failure(e, "failed to list table facets"))?
        {
            let index: i32 = row.get(0);
            let count: i64 = row.get(2);
            if index == -1 {
                total = count;
            } else {
                responses[index as usize]["values"]
                    .as_array_mut()
                    .expect("array")
                    .push(json!({"key":row.get::<_,String>(1),"count":count}));
            }
        }
    } else if include_total {
        total = tx
            .query_one(
                &format!("SELECT count(*)::bigint FROM issue i WHERE {base}"),
                &refs(&base_args),
            )
            .await
            .map_err(|e| query_failure(e, "failed to count table facets"))?
            .get(0);
    }
    for (index, facet) in req.facets.iter().enumerate() {
        if base_expression(&req.query, facet).is_some() {
            continue;
        }
        let mut args = Args::new();
        let where_ = compile(&*tx, ws, user, &without_facet(&req.query, facet), &mut args).await?;
        let sql = match facet.kind.as_str() {
            "status" => {
                format!("SELECT i.status, count(*)::bigint FROM issue i WHERE {where_} GROUP BY 1")
            }
            "priority" => format!(
                "SELECT i.priority, count(*)::bigint FROM issue i WHERE {where_} GROUP BY 1"
            ),
            "assignee" => format!(
                "SELECT CASE WHEN i.assignee_type IS NULL OR i.assignee_id IS NULL THEN '__none__' ELSE i.assignee_type || ':' || i.assignee_id::text END, count(*)::bigint FROM issue i WHERE {where_} GROUP BY 1"
            ),
            "creator" => format!(
                "SELECT i.creator_type || ':' || i.creator_id::text, count(*)::bigint FROM issue i WHERE {where_} GROUP BY 1"
            ),
            "project" => format!(
                "SELECT COALESCE(i.project_id::text, '__none__'), count(*)::bigint FROM issue i WHERE {where_} GROUP BY 1"
            ),
            "label" => format!(
                "SELECT itl.label_id::text, count(DISTINCT i.id)::bigint FROM issue i JOIN issue_to_label itl ON itl.issue_id=i.id WHERE {where_} GROUP BY 1"
            ),
            "property" => {
                let id = Uuid::parse_str(&facet.property_id)
                    .map_err(|_| invalid("invalid facets.property_id"))?;
                let property = tx.query_opt("SELECT type,archived_at IS NOT NULL AS archived FROM issue_property WHERE id=$1 AND workspace_id=$2", &[&id,&ws]).await
                    .map_err(|e| query_failure(e,"failed to resolve table facet"))?
                    .ok_or_else(|| unsupported("property_not_found","The faceted property no longer exists."))?;
                if property.get::<_, bool>("archived") {
                    return Err(unsupported(
                        "property_archived",
                        "The faceted property is archived.",
                    ));
                }
                let key = arg(&mut args, id.to_string());
                match property.get::<_, String>("type").as_str() {
                    "select" | "actor" => format!(
                        "SELECT COALESCE(i.properties ->> {key}::text,'__none__'), count(*)::bigint FROM issue i WHERE {where_} AND (jsonb_typeof(i.properties -> {key}::text)='string' OR NOT (i.properties ? {key}::text)) GROUP BY 1"
                    ),
                    "checkbox" => format!(
                        "SELECT COALESCE(i.properties ->> {key}::text,'__none__'), count(*)::bigint FROM issue i WHERE {where_} AND (jsonb_typeof(i.properties -> {key}::text)='boolean' OR NOT (i.properties ? {key}::text)) GROUP BY 1"
                    ),
                    "text" | "url" | "date" | "number" | "multi_text" | "multi_url" => format!(
                        "SELECT CASE WHEN i.properties ? {key}::text THEN '__set__' ELSE '__none__' END, count(*)::bigint FROM issue i WHERE {where_} GROUP BY 1"
                    ),
                    "multi_select" | "multi_actor" => format!(
                        "SELECT COALESCE(p.value,'__none__'), count(DISTINCT i.id)::bigint FROM issue i JOIN LATERAL (SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(i.properties -> {key}::text)='array' THEN i.properties -> {key}::text ELSE '[]'::jsonb END) AS value UNION ALL SELECT NULL WHERE NOT (i.properties ? {key}::text)) p(value) ON TRUE WHERE {where_} GROUP BY 1"
                    ),
                    _ => {
                        return Err(unsupported(
                            "property_type_unsupported",
                            "This property type cannot be used as a filter facet.",
                        ));
                    }
                }
            }
            "working_agents" => {
                let user_arg = arg(&mut args, user);
                let is_agent = arg(&mut args, matches!(actor, Actor::Task(_)));
                // Match the frozen Go view gate, not the stricter invocation gate.
                // Neither inaccessible nor archived/system agents may disclose identity here.
                format!(
                    "WITH candidates AS MATERIALIZED (SELECT a.id FROM agent a WHERE a.workspace_id=$1 AND a.kind='user' AND a.archived_at IS NULL AND ({is_agent}::boolean OR a.owner_id={user_arg}::uuid OR EXISTS (SELECT 1 FROM member m WHERE m.workspace_id=$1 AND m.user_id={user_arg}::uuid AND m.role IN ('owner','admin')) OR (a.permission_mode='public_to' AND EXISTS (SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND (t.target_type='workspace' OR (t.target_type='member' AND t.target_id={user_arg}::uuid)))))), running AS MATERIALIZED (SELECT atq.agent_id,atq.issue_id FROM candidates c JOIN agent_task_queue atq ON atq.agent_id=c.id AND atq.status='running' WHERE atq.chat_session_id IS NULL AND atq.autopilot_run_id IS NULL AND atq.issue_id IS NOT NULL) SELECT r.agent_id::text,count(*)::bigint FROM running r WHERE EXISTS (SELECT 1 FROM issue i WHERE i.id=r.issue_id AND {where_}) GROUP BY r.agent_id"
                )
            }
            _ => return Err(invalid("invalid facets.kind")),
        };
        let values = tx
            .query(&sql, &refs(&args))
            .await
            .map_err(|e| query_failure(e, "failed to list table facets"))?
            .iter()
            .map(|r| json!({"key":r.get::<_,String>(0),"count":r.get::<_,i64>(1)}))
            .collect();
        responses[index] = facet.response(values);
    }
    for response in &mut responses {
        response["values"]
            .as_array_mut()
            .expect("array")
            .sort_by(|a, b| a["key"].as_str().cmp(&b["key"].as_str()));
    }
    tx.commit()
        .await
        .map_err(|e| query_failure(e, "failed to finish table query"))?;
    Ok(GoJson(
        json!({"query_fingerprint":fingerprint(ws,&req.query),"total":total,"facets":responses}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn facets_remove_only_their_own_dimension() {
        let mut query: Query = serde_json::from_value(json!({"scope":{"kind":"workspace"},"filters":{"statuses":["todo"],"assignees":[],"include_no_assignee":true,"project_ids":["x"],"include_no_project":true,"properties":{"a":["x"],"b":["y"]},"working_only":true,"working_issue_ids":[]}})).unwrap();
        for kind in [
            "status",
            "priority",
            "assignee",
            "creator",
            "project",
            "label",
            "property",
            "working_agents",
        ] {
            let f = Facet {
                kind: kind.into(),
                property_id: "a".into(),
            };
            let dropped = without_facet(&query, &f);
            assert_eq!(dropped.scope.kind, "workspace");
            if kind != "status" {
                assert_eq!(dropped.filters.statuses, query.filters.statuses);
            }
            if kind == "property" {
                assert!(!dropped.filters.properties.contains_key("a"));
                assert!(dropped.filters.properties.contains_key("b"));
            }
            if kind == "assignee" {
                assert!(dropped.filters.assignees.is_none());
                assert!(!dropped.filters.include_no_assignee);
            }
            if kind == "working_agents" {
                assert!(dropped.filters.working_issue_ids.is_none());
                assert!(!dropped.filters.working_only);
            }
        }
        query.filters.assignees = Some(vec![]);
        query.filters.include_no_assignee = false;
        assert!(
            base_expression(
                &query,
                &Facet {
                    kind: "assignee".into(),
                    ..Facet::default()
                }
            )
            .is_some()
        );
        assert!(
            serde_json::from_value::<FacetsRequest>(json!({"facets":[],"unexpected":true}))
                .is_err()
        );
    }
}
