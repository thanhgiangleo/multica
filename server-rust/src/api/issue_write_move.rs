//! Relative ordering follows frozen Go's float arithmetic and delegates writes.
use super::{
    ApiState, Identity, actor::Actor, catalog::WorkspaceId, issue_read::load_issue,
    issue_write::WriteError, issue_write_update,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::Response,
    routing::post,
};
use serde_json::Value;
use uuid::Uuid;

fn error(status: StatusCode, message: &'static str) -> WriteError {
    WriteError(status, message.into())
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/issues/{id}/move", post(move_issue))
}

fn anchor(value: &Value, field: &str) -> Result<Option<Uuid>, WriteError> {
    if value.is_null() {
        return Ok(None);
    }
    let value = value.as_str().filter(|s| !s.is_empty()).ok_or_else(|| {
        WriteError(
            StatusCode::BAD_REQUEST,
            format!("{field} must be a UUID or null"),
        )
    })?;
    Uuid::parse_str(value)
        .map(Some)
        .map_err(|_| WriteError(StatusCode::BAD_REQUEST, format!("invalid {field}")))
}

fn position(current: f64, before: Option<f64>, after: Option<f64>) -> Result<f64, WriteError> {
    let conflict = |message| error(StatusCode::CONFLICT, message);
    match (before, after) {
        (Some(before), Some(after)) => {
            if before.partial_cmp(&after) != Some(std::cmp::Ordering::Less) {
                return Err(conflict("move anchors are stale or out of order"));
            }
            let value = before + (after - before) / 2.0;
            if !(value.is_finite() && value > before && value < after) {
                return Err(conflict("move anchors are too close; refresh and retry"));
            }
            Ok(value)
        }
        (Some(before), None) => {
            let value = before + 1.0;
            if !value.is_finite() {
                return Err(conflict("move position is out of range"));
            }
            Ok(value)
        }
        (None, Some(after)) => {
            let value = after - 1.0;
            if !value.is_finite() {
                return Err(conflict("move position is out of range"));
            }
            Ok(value)
        }
        (None, None) => Ok(current),
    }
}

fn encode_position(value: f64) -> Result<Value, WriteError> {
    if !value.is_finite() {
        return Err(error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encode move",
        ));
    }
    serde_json::to_value(value)
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to encode move"))
}

async fn move_issue(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<Response, WriteError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to resolve move anchor",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let current = load_issue(&db, ws, &raw).await?;
    let id: Uuid = current.get("id");
    // Capture the moved issue position before resolving either anchor. The
    // delegated update uses this original snapshot, just like Go's loader.
    let current_position: f64 = db
        .query_one(
            "SELECT position FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| fail())?
        .get(0);
    let parsed: Value = serde_json::from_slice(&body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    // Go accepts null as a nil map; it then fails the required anchor checks.
    let mut fields = if parsed.is_null() {
        serde_json::Map::new()
    } else {
        parsed
            .as_object()
            .cloned()
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid request body"))?
    };
    for field in fields.keys() {
        if ![
            "status",
            "assignee_type",
            "assignee_id",
            "parent_issue_id",
            "project_id",
            "before_id",
            "after_id",
            "expected_revision",
            "stop_previous_assignee_runs",
        ]
        .contains(&field.as_str())
        {
            return Err(WriteError(
                StatusCode::BAD_REQUEST,
                format!("unsupported move field: {field}"),
            ));
        }
    }
    for field in ["before_id", "after_id"] {
        if !fields.contains_key(field) {
            return Err(WriteError(
                StatusCode::BAD_REQUEST,
                format!("{field} is required"),
            ));
        }
    }
    if let Some(value) = fields.get("project_id")
        && let Some(project) = anchor(value, "project_id")?
        && !db
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM project WHERE workspace_id=$1 AND id=$2)",
                &[&ws, &project],
            )
            .await
            .map_err(|_| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to validate move project",
                )
            })?
            .get::<_, bool>(0)
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "project not found in this workspace",
        ));
    }
    let before = anchor(&fields["before_id"], "before_id")?;
    let after = anchor(&fields["after_id"], "after_id")?;
    if before == Some(id) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "before_id cannot be the moved issue",
        ));
    }
    if after == Some(id) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "after_id cannot be the moved issue",
        ));
    }
    if before.is_some() && before == after {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "move anchors must be distinct",
        ));
    }
    let mut values = [None, None];
    for (index, anchor) in [before, after].into_iter().enumerate() {
        if let Some(anchor) = anchor {
            values[index] = Some(
                db.query_opt(
                    "SELECT position FROM issue WHERE workspace_id=$1 AND id=$2",
                    &[&ws, &anchor],
                )
                .await
                .map_err(|_| fail())?
                .ok_or_else(|| {
                    error(
                        StatusCode::BAD_REQUEST,
                        "move anchor not found in this workspace",
                    )
                })?
                .get::<_, f64>(0),
            );
        }
    }
    // Go uses the position loaded with the issue, not a second read after
    // resolving anchors. A concurrent position change must not silently alter
    // the value derived for a no-anchor move.
    let next = position(current_position, values[0], values[1])?;
    let encoded = encode_position(next)?;
    fields.remove("before_id");
    fields.remove("after_id");
    fields.insert("position".into(), encoded);
    // Same two-phase anchor/read boundary as Go. CAS protects only the moved
    // issue; this does not promise adjacency or rebalance concurrent writers.
    drop(db);
    issue_write_update::update(
        State(state),
        Extension(WorkspaceId(ws)),
        Extension(Identity(user)),
        Extension(actor),
        Path(raw),
        Bytes::from(Value::Object(fields).to_string()),
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_float_gap_and_one_sided_rounding() {
        assert_eq!(position(7.0, None, None).unwrap(), 7.0);
        // Go's float64 JSON encoder errors rather than writing null for NaN.
        assert_eq!(
            encode_position(position(f64::NAN, None, None).unwrap())
                .unwrap_err()
                .0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(position(7.0, Some(1.0), Some(3.0)).unwrap(), 2.0);
        assert_eq!(position(7.0, Some(1.0), None).unwrap(), 2.0);
        assert_eq!(position(7.0, None, Some(3.0)).unwrap(), 2.0);
        for pair in [
            (2.0, 1.0),
            (1.0, 1.0),
            (1.0, f64::from_bits(1.0_f64.to_bits() + 1)),
            (-f64::MAX, f64::MAX),
        ] {
            assert_eq!(
                position(0.0, Some(pair.0), Some(pair.1)).unwrap_err().0,
                StatusCode::CONFLICT
            );
        }
        assert!(position(0.0, Some(f64::INFINITY), None).is_err());
        assert!(position(0.0, None, Some(f64::NAN)).is_err());
        // Go only tests finite one-sided output, not strict progress.
        assert_eq!(position(0.0, Some(f64::MAX), None).unwrap(), f64::MAX);
    }
}
