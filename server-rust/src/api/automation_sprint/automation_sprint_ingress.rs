//! Public, persist-first webhook ingress. Active delivery admission remains unavailable
//! until the shared task runtime supplies a recoverable worker and quota adapter.
use super::*;
use axum::{
    body::{Body, to_bytes},
    extract::ConnectInfo,
    http::HeaderMap,
    routing::post,
};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

type HmacSha256 = Hmac<Sha256>;
const MAX_BODY: usize = 256 * 1024;
const WINDOW: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(in crate::api) struct IngressLimits(Mutex<HashMap<String, VecDeque<Instant>>>);

impl IngressLimits {
    pub(super) fn check(&self, key: String, limit: usize, consume: bool) -> bool {
        let now = Instant::now();
        let Ok(mut state) = self.0.lock() else {
            return false;
        };
        // Sweep old keys as well as old timestamps, or arbitrary token probes
        // would grow the process indefinitely even after their window expires.
        state.retain(|_, hits| {
            hits.retain(|time| now.duration_since(*time) < WINDOW);
            !hits.is_empty()
        });
        if state.get(&key).is_some_and(|hits| hits.len() >= limit) {
            return false;
        }
        if consume {
            state.entry(key).or_default().push_back(now);
        }
        true
    }
}

pub(in crate::api) fn public_routes() -> Router<ApiState> {
    Router::new().route("/api/webhooks/autopilots/{token}", post(ingest))
}

fn selected_headers(headers: &HeaderMap) -> Value {
    let mut selected = serde_json::Map::new();
    for key in [
        "user-agent",
        "x-github-event",
        "x-github-delivery",
        "x-gitlab-event",
        "x-event-type",
        "idempotency-key",
    ] {
        if let Some(value) = headers
            .get(key)
            .and_then(|v| v.to_str().ok())
            .filter(|v| !v.is_empty())
        {
            selected.insert(key.to_owned(), json!(value));
        }
    }
    if headers.contains_key("x-hub-signature-256") {
        selected.insert("x-hub-signature-256-present".into(), json!(true));
    }
    Value::Object(selected)
}

fn signature_status(secret: Option<&str>, headers: &HeaderMap, body: &[u8]) -> &'static str {
    let Some(secret) = secret.filter(|s| !s.is_empty()) else {
        return "not_required";
    };
    let Some(value) = headers
        .get("x-hub-signature-256")
        .and_then(|v| v.to_str().ok())
    else {
        return "missing";
    };
    let Some(value) = value.strip_prefix("sha256=") else {
        return "invalid";
    };
    let Ok(digest) = hex::decode(value) else {
        return "invalid";
    };
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(body);
    if mac.verify_slice(&digest).is_ok() {
        "valid"
    } else {
        "invalid"
    }
}

fn dedupe(provider: &str, headers: &HeaderMap) -> (Option<String>, Option<String>) {
    let get = |name| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
    };
    if provider == "github"
        && let Some(key) = get("x-github-delivery")
    {
        return (Some(key.into()), Some("x-github-delivery".into()));
    }
    if let Some(key) = get("idempotency-key") {
        return (Some(key.into()), Some("idempotency-key".into()));
    }
    if let Some(key) = get("x-github-delivery") {
        return (Some(key.into()), Some("x-github-delivery".into()));
    }
    (None, None)
}

pub(super) fn event_allowed(filters: Option<&Value>, event: &str, payload: &Value) -> bool {
    let Some(filters) = filters else { return true };
    let Some(filters) = filters.as_array() else {
        return false;
    };
    if filters.is_empty() {
        return true;
    }
    let parts: Vec<&str> = event.split('.').collect();
    let known = matches!(parts[0], "github" | "gitlab" | "bitbucket" | "gitea");
    let name = if known {
        parts.get(1).copied().unwrap_or("")
    } else {
        parts[0]
    };
    let suffix = if known {
        parts.get(2..).unwrap_or(&[]).join(".")
    } else {
        parts.get(1..).unwrap_or(&[]).join(".")
    };
    filters.iter().any(|filter| {
        if filter.as_object().is_none() || filter.get("event").and_then(Value::as_str) != Some(name)
        {
            return false;
        }
        match filter.get("actions") {
            None | Some(Value::Null) => true,
            Some(Value::Array(actions)) if actions.is_empty() => true,
            Some(Value::Array(actions)) => actions.iter().filter_map(Value::as_str).any(|action| {
                !action.is_empty()
                    && (action == suffix.trim()
                        || ["action", "state", "conclusion", "status"]
                            .iter()
                            .any(|field| {
                                payload.get(field).and_then(Value::as_str).map(str::trim)
                                    == Some(action)
                            }))
            }),
            _ => false,
        }
    })
}

fn reply(status: StatusCode, body: Value) -> (StatusCode, GoJson<Value>) {
    (status, GoJson(body))
}

async fn ingest(
    State(state): State<ApiState>,
    Extension(ConnectInfo(peer)): Extension<ConnectInfo<SocketAddr>>,
    Path(token): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let limits: &Arc<IngressLimits> = &state.automation_sprint_limits;
    let ip = peer.ip().to_string();
    if !limits.check(format!("absolute:{ip}"), 600, true)
        || !limits.check(format!("invalid:{ip}"), 30, false)
    {
        return Err(error(StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded"));
    }
    if token.is_empty() || token.len() > 255 {
        limits.check(format!("invalid:{ip}"), 30, true);
        return Err(error(StatusCode::NOT_FOUND, "webhook not found"));
    }
    let db = state.pool.get().await.map_err(|_| fail("internal error"))?;
    let trigger = db.query_opt(
        "SELECT id,autopilot_id FROM autopilot_trigger WHERE webhook_token=$1 AND kind='webhook'",
        &[&token],
    ).await.map_err(|_| fail("internal error"))?;
    let Some(trigger) = trigger else {
        limits.check(format!("invalid:{ip}"), 30, true);
        return Err(error(StatusCode::NOT_FOUND, "webhook not found"));
    };
    let ap_id: Uuid = trigger.get("autopilot_id");
    let trigger_id: Uuid = trigger.get("id");
    drop(db);
    let token_hash = hex::encode(Sha256::digest(token.as_bytes()));
    if !limits.check(format!("token:{token_hash}"), 60, true) {
        return Err(error(StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded"));
    }
    let raw = to_bytes(body, MAX_BODY + 1)
        .await
        .map_err(|_| error(StatusCode::PAYLOAD_TOO_LARGE, "payload too large"))?;
    if raw.len() > MAX_BODY {
        return Err(error(StatusCode::PAYLOAD_TOO_LARGE, "payload too large"));
    }
    let clean = raw.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(&raw);
    let payload: Value = serde_json::from_slice(clean).map_err(|_| bad("invalid json"))?;
    if !payload.is_object() && !payload.is_array() {
        return Err(bad("body must be a JSON object or array"));
    }
    let selected = selected_headers(&headers);
    let event = super::automation_sprint_deliveries::normalized_event(&raw, &selected)?;
    let content = headers
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.split(';').next())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let mut db = state.pool.get().await.map_err(|_| fail("internal error"))?;
    let tx = db.transaction().await.map_err(|_| fail("internal error"))?;
    // All concurrent deliveries and trigger edits serialize on the autopilot.
    // Re-read the token under that lock to refuse rotated/removed credentials.
    let ap = tx
        .query_opt("SELECT * FROM autopilot WHERE id=$1 FOR UPDATE", &[&ap_id])
        .await
        .map_err(|_| fail("internal error"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "webhook not found"))?;
    let trigger = tx.query_opt("SELECT * FROM autopilot_trigger WHERE id=$1 AND autopilot_id=$2 AND kind='webhook' AND webhook_token=$3 FOR SHARE", &[&trigger_id, &ap_id, &token])
        .await.map_err(|_| fail("internal error"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "webhook not found"))?;
    // The autopilot row lock serializes replicas. Count only inbound attempts,
    // not operator replays, using the existing autopilot/created_at index.
    // Keep the process-local limits above for unknown credentials and duplicate
    // floods; this durable ceiling bounds *new* deliveries across processes.
    let recent: i64 = tx.query_one(
        "SELECT count(*) FROM (SELECT 1 FROM webhook_delivery WHERE autopilot_id=$1 AND trigger_id=$2 AND replayed_from_delivery_id IS NULL AND created_at>clock_timestamp()-interval '60 seconds' ORDER BY created_at DESC LIMIT 60) recent",
        &[&ap_id,&trigger_id],
    ).await.map_err(|_| fail("internal error"))?.get(0);
    if recent >= 60 {
        return Err(error(StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded"));
    }
    let ws: Uuid = ap.get("workspace_id");
    let provider: String = trigger.get("provider");
    let (key, source) = dedupe(&provider, &headers);
    // Dedupe may be attacker-supplied; reject unbounded strings before touching DB.
    if key.as_ref().is_some_and(|k| k.len() > 255) {
        return Err(bad("dedupe key is too long"));
    }
    let signature = signature_status(
        trigger
            .get::<_, Option<String>>("signing_secret")
            .as_deref(),
        &headers,
        &raw,
    );
    let status: &str;
    let mut response_status: StatusCode;
    let mut body: Value;
    if signature == "invalid" || signature == "missing" {
        let reason = if signature == "missing" {
            "missing_signature"
        } else {
            "invalid_signature"
        };
        status = "rejected";
        response_status = StatusCode::UNAUTHORIZED;
        body = json!({"status":"rejected","reason":reason});
        limits.check(format!("invalid:{ip}"), 30, true);
    } else if !trigger.get::<_, bool>("enabled") {
        status = "ignored";
        response_status = StatusCode::OK;
        body = json!({"status":"ignored","reason":"trigger_disabled"});
    } else if ap.get::<_, String>("status") != "active" {
        status = "ignored";
        response_status = StatusCode::OK;
        let reason = if ap.get::<_, String>("status") == "archived" {
            "autopilot_archived"
        } else {
            "autopilot_paused"
        };
        body = json!({"status":"ignored","reason":reason});
    } else {
        let event_payload = payload
            .get("eventPayload")
            .filter(|_| {
                payload
                    .get("event")
                    .and_then(Value::as_str)
                    .is_some_and(|event| !event.is_empty())
            })
            .unwrap_or(&payload);
        if !event_allowed(
            trigger.get::<_, Option<Value>>("event_filters").as_ref(),
            &event,
            event_payload,
        ) {
            status = "ignored";
            response_status = StatusCode::OK;
            body = json!({"status":"ignored","reason":"event_filtered","event":event});
        } else {
            // Admission/worker/quotas are not integrated. Persist recoverable
            // work, but never claim accepted until an actual run exists.
            status = "queued";
            response_status = StatusCode::SERVICE_UNAVAILABLE;
            body = json!({"error":"autopilot admission unavailable"});
        }
    }
    let raw: &[u8] = &raw;
    let row = tx.query_opt("INSERT INTO webhook_delivery(workspace_id,autopilot_id,trigger_id,provider,event,dedupe_key,dedupe_source,signature_status,status,selected_headers,content_type,raw_body) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (trigger_id,dedupe_key) WHERE dedupe_key IS NOT NULL AND status NOT IN ('rejected','failed') DO NOTHING RETURNING id", &[&ws,&ap_id,&trigger_id,&provider,&event,&key,&source,&signature,&status,&selected,&content,&raw])
        .await.map_err(|_| fail("internal error"))?;
    let delivery_id: Uuid;
    if let Some(inserted) = row {
        delivery_id = inserted.get(0);
        body["delivery_id"] = json!(delivery_id);
        let encoded = body.to_string();
        tx.execute(
            "UPDATE webhook_delivery SET response_status=$2,response_body=$3,error=$4 WHERE id=$1",
            &[
                &delivery_id,
                &i32::from(response_status.as_u16()),
                &encoded,
                &if status == "queued" {
                    Some("autopilot admission unavailable")
                } else {
                    None::<&str>
                },
            ],
        )
        .await
        .map_err(|_| fail("internal error"))?;
    } else {
        let previous = tx.query_one("UPDATE webhook_delivery SET attempt_count=attempt_count+1,last_attempt_at=now() WHERE trigger_id=$1 AND dedupe_key=$2 AND status NOT IN ('rejected','failed') RETURNING id,status,autopilot_run_id", &[&trigger_id,&key])
            .await.map_err(|_| fail("internal error"))?;
        delivery_id = previous.get("id");
        let run: Option<Uuid> = previous.get("autopilot_run_id");
        if previous.get::<_, String>("status") == "queued" && run.is_none() {
            response_status = StatusCode::SERVICE_UNAVAILABLE;
            body = json!({"error":"autopilot admission unavailable","delivery_id":delivery_id});
        } else {
            body = json!({"status":"duplicate","delivery_id":delivery_id});
            if let Some(run) = run {
                body["run_id"] = json!(run);
            }
            response_status = StatusCode::OK;
        }
    }
    tx.commit().await.map_err(|_| fail("internal error"))?;
    Ok(reply(response_status, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signature_dedupe_and_filter_fail_closed() {
        let body = br#"{"action":"opened"}"#;
        let mut headers = HeaderMap::new();
        headers.insert("idempotency-key", "manual".parse().unwrap());
        headers.insert("x-github-delivery", "gh".parse().unwrap());
        assert_eq!(dedupe("github", &headers).0.as_deref(), Some("gh"));
        assert_eq!(dedupe("generic", &headers).0.as_deref(), Some("manual"));
        assert_eq!(signature_status(Some("secret"), &headers, body), "missing");
        let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
        mac.update(body);
        headers.insert(
            "x-hub-signature-256",
            format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
                .parse()
                .unwrap(),
        );
        assert_eq!(signature_status(Some("secret"), &headers, body), "valid");
        assert_eq!(
            signature_status(Some("secret"), &headers, b"tampered"),
            "invalid"
        );
        assert!(!selected_headers(&headers).to_string().contains("sha256="));
        let filters = json!([{"event":"pull_request","actions":["closed"]},{"event":"pull_request","actions":["opened"]}]);
        assert!(event_allowed(
            Some(&filters),
            "github.pull_request.opened",
            &json!({})
        ));
        assert!(!event_allowed(
            Some(&json!({"bad":true})),
            "github.pull_request.opened",
            &json!({})
        ));
    }
}
