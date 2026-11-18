//! Archived inbox facets from the frozen Go query, scoped to one member and workspace.
use super::{
    ApiError, ApiState, GoJson, Identity,
    catalog::{WorkspaceId, parse_params},
    error,
};
use axum::{
    extract::{Extension, RawQuery, State},
    http::StatusCode,
};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize)]
struct Facets {
    statuses: HashMap<String, i64>,
    priorities: HashMap<String, i64>,
    actors: HashMap<String, i64>,
    unread_count: i64,
}

pub(super) struct Filters {
    pub(super) statuses: Vec<String>,
    pub(super) priorities: Vec<String>,
    pub(super) actors: Vec<String>,
    pub(super) unread_only: bool,
}

pub(super) fn filters(raw: Option<&str>) -> Result<Filters, ApiError> {
    let q = parse_params(raw);
    let parse = |key| -> Result<Vec<String>, ApiError> {
        let Some(value) = q.get(key).filter(|value| !value.is_empty()) else {
            return Ok(Vec::new());
        };
        if value.len() > 8192 {
            return Err(error(StatusCode::BAD_REQUEST, "filter is too long"));
        }
        let mut values: Vec<String> = value.split(',').map(str::to_owned).collect();
        if values.len() > 100 {
            return Err(error(StatusCode::BAD_REQUEST, "too many filter values"));
        }
        if values.iter().any(String::is_empty) {
            return Err(error(StatusCode::BAD_REQUEST, "empty filter value"));
        }
        values.sort();
        values.dedup();
        Ok(values)
    };
    let unread_only = match q.get("unread_only").map(String::as_str).unwrap_or("") {
        "" | "false" => false,
        "true" => true,
        _ => return Err(error(StatusCode::BAD_REQUEST, "invalid unread_only")),
    };
    Ok(Filters {
        statuses: parse("statuses")?,
        priorities: parse("priorities")?,
        actors: parse("actors")?,
        unread_only,
    })
}

pub(super) async fn get(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<impl Serialize>, ApiError> {
    let Filters {
        statuses,
        priorities,
        actors,
        unread_only,
    } = filters(raw.as_deref())?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to load archived inbox filters",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let rows = db
        .query(
            include_str!("archived_facets.sql"),
            &[
                &ws as &(dyn tokio_postgres::types::ToSql + Sync),
                &user,
                &statuses,
                &priorities,
                &actors,
                &unread_only,
            ],
        )
        .await
        .map_err(|_| fail())?;
    let mut result = Facets {
        statuses: HashMap::new(),
        priorities: HashMap::new(),
        actors: HashMap::new(),
        unread_count: 0,
    };
    for row in rows {
        let key: String = row.get("key");
        let count: i64 = row.get("count");
        match row.get::<_, &str>("dimension") {
            "statuses" => {
                result.statuses.insert(key, count);
            }
            "priorities" => {
                result.priorities.insert(key, count);
            }
            "actors" => {
                result.actors.insert(key, count);
            }
            "unread" => result.unread_count = count,
            _ => {}
        }
    }
    Ok(GoJson(result))
}

#[cfg(test)]
mod tests {
    use super::filters;
    #[test]
    fn filter_validation() {
        assert!(filters(Some("statuses=done,,todo")).is_err());
        assert!(filters(Some("unread_only=1")).is_err());
        assert!(filters(Some(&format!("actors={}", "x,".repeat(101)))).is_err());
        assert_eq!(
            filters(Some("actors=system,member%3Aa,system"))
                .unwrap()
                .actors,
            vec!["member:a", "system"]
        );
    }
}
