//! Explicit issue subscriptions and durable subtree opt-outs.
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_read::load_issue,
    issue_write::{IssueSubscriberEvent, write_actor},
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
};
use serde_json::{Value, json};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}/subscribe", post(subscribe))
        .route("/api/issues/{id}/unsubscribe", post(unsubscribe))
        .route(
            "/api/issues/{id}/unsubscribe/subtree",
            post(unsubscribe_subtree),
        )
}

#[derive(Default)]
struct Request {
    user_id: Option<String>,
    user_type: Option<String>,
}

fn decode(body: &[u8]) -> Request {
    let mut request = Request::default();
    // Go ignores Decode's error here. Syntax errors leave the entire struct
    // untouched, but type errors preserve earlier values AND continue decoding
    // later fields. A Value map would also lose duplicate-key order/case-folding.
    let _ = super::issue_write::raw_field::fields(body, |key, value| {
        let field = match key {
            "user_id" => &mut request.user_id,
            "user_type" => &mut request.user_type,
            _ => return,
        };
        let normalized = super::issue_write_comment_decode::normalized_strings(value);
        match serde_json::from_slice::<Option<String>>(&normalized) {
            Ok(value) => *field = value,
            // encoding/json allocates a nil *string before reporting a type
            // error. An existing pointer retains its string in that case.
            Err(_) => {
                field.get_or_insert_with(String::new);
            }
        }
    });
    request
}

fn target(
    body: &[u8],
    caller_type: &'static str,
    caller: Uuid,
) -> Result<(String, Uuid, String), ApiError> {
    let fields = decode(body);
    let raw = fields
        .user_id
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| caller.to_string());
    let kind = fields
        .user_type
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| caller_type.into());
    let denied = || {
        error(
            StatusCode::FORBIDDEN,
            "target user is not a member of this workspace",
        )
    };
    let id = Uuid::parse_str(&raw).map_err(|_| denied())?;
    if !matches!(kind.as_str(), "member" | "agent") {
        return Err(denied());
    }
    Ok((kind, id, raw))
}

async fn subscribe(
    State(s): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(u)): Extension<Identity>,
    Extension(a): Extension<Actor>,
    Path(id): Path<String>,
    b: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    mutate(s, ws, u, a, id, b, Mode::Subscribe).await
}
async fn unsubscribe(
    State(s): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(u)): Extension<Identity>,
    Extension(a): Extension<Actor>,
    Path(id): Path<String>,
    b: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    mutate(s, ws, u, a, id, b, Mode::Unsubscribe).await
}
async fn unsubscribe_subtree(
    State(s): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(u)): Extension<Identity>,
    Extension(a): Extension<Actor>,
    Path(id): Path<String>,
    b: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    mutate(s, ws, u, a, id, b, Mode::Subtree).await
}
#[derive(Clone, Copy)]
enum Mode {
    Subscribe,
    Unsubscribe,
    Subtree,
}

async fn mutate(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    raw: String,
    body: Bytes,
    mode: Mode,
) -> Result<GoJson<Value>, ApiError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            if matches!(mode, Mode::Subscribe) {
                "failed to subscribe"
            } else {
                "failed to unsubscribe"
            },
        )
    };
    let denied = || {
        error(
            StatusCode::FORBIDDEN,
            "target user is not a member of this workspace",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let issue = load_issue(&db, ws, &raw).await?;
    let id: Uuid = issue.get("id");
    let (actor_type, actor_id, _) = write_actor(actor, user)?;
    let (kind, target, raw_target) = target(&body, actor_type, actor_id)?;
    let tx = db.transaction().await.map_err(|_| fail())?;
    // Same lock VALUE and order as Go delegated subscriptions and member revoke.
    // Applying it to explicit operations also makes their membership check hold
    // until commit rather than leaving subscriptions behind a concurrent revoke.
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtext($1::uuid::text),hashtext($2::uuid::text))",
        &[&ws, &target],
    )
    .await
    .map_err(|_| fail())?;
    let valid = if kind == "member" {
        tx.query_opt(
            "SELECT id FROM member WHERE user_id=$1 AND workspace_id=$2 FOR SHARE",
            &[&target, &ws],
        )
        .await
    } else {
        tx.query_opt(
            "SELECT id FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user' FOR SHARE",
            &[&target, &ws],
        )
        .await
    }
    .map_err(|_| fail())?
    .is_some();
    if !valid {
        return Err(denied());
    }
    if tx
        .query_opt(
            "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2 FOR KEY SHARE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| fail())?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "issue not found"));
    }
    let removed = match mode {
        Mode::Subscribe => {
            tx.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason) VALUES($1,$2,$3,'manual') ON CONFLICT(issue_id,user_type,user_id) DO UPDATE SET unsubscribed_at=NULL,opt_out_scope=NULL,reason=EXCLUDED.reason", &[&id, &kind, &target]).await.map_err(|_| fail())?;
            vec![raw]
        }
        Mode::Unsubscribe => {
            tx.execute("UPDATE issue_subscriber SET unsubscribed_at=now(),opt_out_scope='issue' WHERE issue_id=$1 AND user_type=$2 AND user_id=$3 AND unsubscribed_at IS NULL", &[&id, &kind, &target]).await.map_err(|_| fail())?;
            vec![raw]
        }
        Mode::Subtree => tx
            .query(
                include_str!("issue_write_subscriber_subtree.sql"),
                &[&id, &kind, &target, &ws],
            )
            .await
            .map_err(|_| fail())?
            .iter()
            .map(|r| r.get::<_, Uuid>(0).to_string())
            .collect(),
    };
    tx.commit().await.map_err(|_| fail())?;
    let subscribing = matches!(mode, Mode::Subscribe);
    for issue in removed {
        // Go preserves request spelling on subscribe, canonicalizes unsubscribe.
        let mut payload = json!({"issue_id":issue,"user_type":kind,"user_id":if subscribing { raw_target.clone() } else { target.to_string() }});
        if subscribing {
            payload["reason"] = json!("manual");
        }
        sink.publish_subscriber_changed(IssueSubscriberEvent {
            event_type: if subscribing {
                "subscriber:added"
            } else {
                "subscriber:removed"
            },
            workspace_id: ws,
            actor_type,
            actor_id,
            payload,
        });
    }
    Ok(GoJson(json!({"subscribed":subscribing})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    #[test]
    fn subscriber_decoder_matches_go_oracle() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/issue_write_delete_subscribe_oracle.json"
        ))
        .unwrap();
        for case in cases.iter().filter(|c| c["kind"] == "subscriber") {
            let body = STANDARD
                .decode(case["body_base64"].as_str().unwrap())
                .unwrap();
            let request = decode(&body);
            assert_eq!(json!(request.user_id), case["user_id"], "{case}");
            assert_eq!(json!(request.user_type), case["user_type"], "{case}");
        }
    }

    #[test]
    fn target_matches_lenient_go_body_contract() {
        let caller = Uuid::new_v4();
        for body in [
            b"".as_slice(),
            b"garbage",
            b"null",
            b"[]",
            b"{\"user_id\":27}",
        ] {
            let (kind, id, _) = target(body, "agent", caller).unwrap();
            assert_eq!((kind.as_str(), id), ("agent", caller));
        }
        let other = Uuid::new_v4();
        let body = format!(r#"{{"user_id":"{other}"}} trailing-body"#);
        assert_eq!(target(body.as_bytes(), "member", caller).unwrap().1, other);
        assert!(target(br#"{"user_id":"broken"}"#, "member", caller).is_err());
        assert!(target(br#"{"user_type":"machine"}"#, "member", caller).is_err());
    }
}
