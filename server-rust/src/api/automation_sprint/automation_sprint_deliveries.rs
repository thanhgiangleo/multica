//! Durable replay admission. Read history stays in autopilot_history_read.
use super::*;
use axum::{http::HeaderMap, routing::post};

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route(
        "/api/autopilots/{id}/deliveries/{deliveryId}/replay",
        post(replay),
    )
}

fn response(row: &Row) -> Value {
    let mut value = json!({
        "id":row.get::<_,Uuid>("id"),
        "workspace_id":row.get::<_,Uuid>("workspace_id"),
        "autopilot_id":row.get::<_,Uuid>("autopilot_id"),
        "trigger_id":row.get::<_,Uuid>("trigger_id"),
        "provider":row.get::<_,String>("provider"),
        "event":row.get::<_,String>("event"),
        "dedupe_key":row.get::<_,Option<String>>("dedupe_key"),
        "dedupe_source":row.get::<_,Option<String>>("dedupe_source"),
        "signature_status":row.get::<_,String>("signature_status"),
        "status":row.get::<_,String>("status"),
        "attempt_count":row.get::<_,i32>("attempt_count"),
        "dispatch_attempts":row.get::<_,i32>("dispatch_attempts"),
        "available_at":time(row,"available_at"),
        "content_type":row.get::<_,Option<String>>("content_type"),
        "response_status":row.get::<_,Option<i32>>("response_status"),
        "autopilot_run_id":row.get::<_,Option<Uuid>>("autopilot_run_id"),
        "replayed_from_delivery_id":row.get::<_,Option<Uuid>>("replayed_from_delivery_id"),
        "error":row.get::<_,Option<String>>("error"),
        "reason_code":row.get::<_,Option<String>>("reason_code"),
        "replay_idempotency_key":row.get::<_,Option<String>>("replay_idempotency_key"),
        "received_at":time(row,"received_at"),
        "last_attempt_at":time(row,"last_attempt_at"),
        "created_at":time(row,"created_at"),
        "selected_headers":row.get::<_,Value>("selected_headers")
    });
    if let Some(raw) = row
        .get::<_, Option<Vec<u8>>>("raw_body")
        .filter(|b| !b.is_empty())
    {
        value["raw_body"] = json!(String::from_utf8_lossy(&raw));
    }
    if let Some(body) = row.get::<_, Option<String>>("response_body") {
        value["response_body"] = json!(body);
    }
    value
}

async fn load_delivery(
    db: &impl GenericClient,
    ws: Uuid,
    ap: Uuid,
    raw: &str,
) -> Result<Row, ApiError> {
    let id = parse_id(raw, "invalid delivery id")?;
    db.query_opt(
        "SELECT * FROM webhook_delivery WHERE id=$1 AND workspace_id=$2 AND autopilot_id=$3",
        &[&id, &ws, &ap],
    )
    .await
    .map_err(|_| fail("failed to load delivery"))?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "delivery not found"))
}

// Matches the Go normalization precedence, including explicit envelopes before headers.
pub(super) fn normalized_event(raw: &[u8], headers: &Value) -> Result<String, ApiError> {
    let raw = raw.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(raw);
    let body: Value = serde_json::from_slice(raw)
        .map_err(|_| bad("stored body no longer parses: invalid json"))?;
    if !body.is_object() && !body.is_array() {
        return Err(bad(
            "stored body no longer parses: body must be a JSON object or array",
        ));
    }
    let field = |key| text(&body, key).filter(|s| !s.is_empty());
    if let Some(event) = field("event") {
        return Ok(event.to_owned());
    }
    let header = |key| text(headers, key).filter(|s| !s.is_empty());
    if let Some(event) = header("x-github-event") {
        return Ok(match field("action") {
            Some(action) => format!("github.{event}.{action}"),
            None => format!("github.{event}"),
        });
    }
    if let Some(event) = header("x-gitlab-event") {
        return Ok(format!("gitlab.{event}"));
    }
    Ok(header("x-event-type")
        .or_else(|| field("type"))
        .or_else(|| field("action"))
        .unwrap_or("webhook.received")
        .to_owned())
}

async fn replay(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, delivery)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to create replay delivery"))?;
    let ap = load(&db, ws, &raw).await?;
    required_writer(&db, &ap, ws, user, &actor).await?;
    let id: Uuid = ap.get("id");
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to create replay delivery"))?;
    // Serialize with archive/update; revalidate access and active state under the lock.
    let ap = load_locked(&tx, ws, id).await?;
    required_writer(&tx, &ap, ws, user, &actor).await?;
    let original = load_delivery(&tx, ws, id, &delivery).await?;
    if original.get::<_, String>("status") == "rejected"
        || original.get::<_, String>("signature_status") == "invalid"
    {
        return Err(bad(
            "cannot replay a delivery that failed signature verification",
        ));
    }
    let raw = original
        .get::<_, Option<Vec<u8>>>("raw_body")
        .filter(|r| !r.is_empty())
        .ok_or_else(|| bad("original delivery has no raw body to replay"))?;
    if ap.get::<_, String>("status") != "active" {
        return Err(bad("autopilot is not active"));
    }
    let trigger: Uuid = original.get("trigger_id");
    let row = tx
        .query_opt(
            "SELECT enabled FROM autopilot_trigger WHERE id=$1 AND autopilot_id=$2 FOR SHARE",
            &[&trigger, &id],
        )
        .await
        .map_err(|_| fail("failed to load trigger"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "trigger not found"))?;
    if !row.get::<_, bool>(0) {
        return Err(bad("trigger is disabled"));
    }
    let selected: Value = original.get("selected_headers");
    let event = normalized_event(&raw, &selected)?;
    let key = headers
        .get("idempotency-key")
        .map(|v| v.to_str().map(str::trim))
        .transpose()
        .map_err(|_| bad("invalid Idempotency-Key"))?
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    if key.len() > 255 {
        return Err(bad("Idempotency-Key is too long"));
    }
    let from: Uuid = original.get("id");
    let provider: String = original.get("provider");
    let content: Option<String> = original.get("content_type");
    let inserted = tx.query_opt("INSERT INTO webhook_delivery(workspace_id,autopilot_id,trigger_id,provider,event,signature_status,status,selected_headers,content_type,raw_body,replayed_from_delivery_id,replay_idempotency_key) VALUES ($1,$2,$3,$4,$5,'not_required','queued',$6,$7,$8,$9,$10) ON CONFLICT (replayed_from_delivery_id,replay_idempotency_key) WHERE replayed_from_delivery_id IS NOT NULL AND replay_idempotency_key IS NOT NULL DO NOTHING RETURNING *", &[&ws,&id,&trigger,&provider,&event,&selected,&content,&raw,&from,&key])
        .await.map_err(|_|fail("failed to create replay delivery"))?;
    let replay = match inserted {
        Some(row) => row,
        None => tx.query_one("SELECT * FROM webhook_delivery WHERE replayed_from_delivery_id=$1 AND replay_idempotency_key=$2 AND workspace_id=$3 AND autopilot_id=$4", &[&from,&key,&ws,&id])
            .await.map_err(|_|fail("failed to inspect replay request"))?,
    };
    let result = response(&replay);
    tx.commit()
        .await
        .map_err(|_| fail("failed to create replay delivery"))?;
    // The queued row is the durable worker signal. No task is claimed or dispatched here.
    Ok((StatusCode::ACCEPTED, GoJson(result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn normalization_matches_go_precedence_and_shape() {
        let headers = json!({"x-github-event":"pull_request","x-event-type":"ignored"});
        assert_eq!(
            normalized_event(br#"{"event":"explicit","eventPayload":null}"#, &headers).unwrap(),
            "explicit"
        );
        assert_eq!(
            normalized_event(br#"{"action":"opened"}"#, &headers).unwrap(),
            "github.pull_request.opened"
        );
        assert_eq!(
            normalized_event(b"\xef\xbb\xbf[]", &json!({})).unwrap(),
            "webhook.received"
        );
        assert_eq!(
            normalized_event(br#"{"type":"job"}"#, &json!({})).unwrap(),
            "job"
        );
        for raw in [
            b"null".as_slice(),
            b"true",
            b"7",
            b"\"scalar\"",
            b"",
            b"{}{}",
        ] {
            assert!(normalized_event(raw, &headers).is_err(), "{raw:?}");
        }
    }
}
