//! Operational realtime health: only live collectors can provide a snapshot.
//! The integrator owns the collector and must not substitute zero counters.
use super::{ApiError, ApiState, error};
use axum::{
    Json, Router,
    extract::{ConnectInfo, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use std::{net::SocketAddr, sync::Arc};

pub trait RealtimeMetrics: Send + Sync {
    fn snapshot(&self) -> Result<Value, String>;
}
pub type SharedRealtimeMetrics = Arc<dyn RealtimeMetrics>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/health/realtime", get(realtime))
}

fn has_forwarding_headers(headers: &HeaderMap) -> bool {
    [
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-proto",
        "x-real-ip",
        "forwarded",
    ]
    .iter()
    .any(|name| {
        headers
            .get_all(*name)
            .iter()
            .any(|v| !v.as_bytes().iter().all(u8::is_ascii_whitespace))
    })
}

fn authorized(
    headers: &HeaderMap,
    peer: Option<SocketAddr>,
    token: Option<&str>,
) -> Result<(), StatusCode> {
    if let Some(token) = token.map(str::trim).filter(|token| !token.is_empty()) {
        let auth = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let Some((scheme, supplied)) = auth.split_once(' ') else {
            return Err(StatusCode::UNAUTHORIZED);
        };
        if !scheme.eq_ignore_ascii_case("Bearer") || supplied.trim().is_empty() {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let supplied = supplied.trim().as_bytes();
        let mut expected = Hmac::<Sha256>::new_from_slice(token.as_bytes())
            .map_err(|_| StatusCode::UNAUTHORIZED)?;
        expected.update(b"realtime-metrics-auth");
        let mut candidate =
            Hmac::<Sha256>::new_from_slice(supplied).map_err(|_| StatusCode::UNAUTHORIZED)?;
        candidate.update(b"realtime-metrics-auth");
        if candidate
            .verify_slice(&expected.finalize().into_bytes())
            .is_err()
        {
            return Err(StatusCode::UNAUTHORIZED);
        }
        return Ok(());
    }
    if peer.is_some_and(|peer| peer.ip().is_loopback()) && !has_forwarding_headers(headers) {
        Ok(())
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// Match the frozen Go operational surface. A half-populated collector is not
// evidence of a healthy realtime relay, even when its few counters are valid.
fn counters(value: &Value, fields: &[&str]) -> bool {
    let Some(map) = value.as_object() else {
        return false;
    };
    fields.iter().all(|field| {
        map.get(*field)
            .and_then(Value::as_i64)
            .is_some_and(|n| n >= 0)
    })
}

fn counter_map(value: Option<&Value>) -> bool {
    value.and_then(Value::as_object).is_some_and(|map| {
        map.len() <= 256
            && map.iter().all(|(key, value)| {
                !key.is_empty() && key.len() <= 128 && value.as_i64().is_some_and(|n| n >= 0)
            })
    })
}

fn complete_snapshot(snapshot: &Value) -> bool {
    let Some(top) = snapshot.as_object() else {
        return false;
    };
    if !counters(
        snapshot,
        &[
            "connects_total",
            "disconnects_total",
            "active_connections",
            "slow_evictions_total",
            "messages_sent_total",
            "messages_dropped_total",
            "membership_removals_dropped_total",
            "inbound_too_large_total",
        ],
    ) || ![
        "events_sent_by_type",
        "subscribes_total",
        "unsubscribes_total",
        "subscribe_denied_total",
        "active_scope_rooms",
    ]
    .iter()
    .all(|key| counter_map(top.get(*key)))
    {
        return false;
    }
    let Some(redis) = top.get("redis").and_then(Value::as_object) else {
        return false;
    };
    if redis.get("connected").and_then(Value::as_bool).is_none()
        || redis.get("node_id").and_then(Value::as_str).is_none()
        || redis.get("last_error").and_then(Value::as_str).is_none()
        || !counters(
            &Value::Object(redis.clone()),
            &[
                "xadd_total",
                "xadd_errors",
                "xread_total",
                "xread_errors",
                "ack_total",
                "last_xadd_lag_micros",
                "mirror_primary_errors",
                "mirror_secondary_errors",
                "mirror_divergence_total",
                "stream_trimmed_total",
                "stream_missing_total",
                "retention_errors",
                "streams_without_ttl",
                "used_memory_bytes",
                "max_memory_bytes",
                "evicted_keys",
            ],
        )
    {
        return false;
    }
    let Some(streams) = redis.get("streams").and_then(Value::as_object) else {
        return false;
    };
    if streams.len() > 256
        || !streams.values().all(|stream| {
            stream
                .get("entries")
                .and_then(Value::as_i64)
                .is_some_and(|n| n >= 0)
                && stream
                    .get("pttl_millis")
                    .and_then(Value::as_i64)
                    .is_some_and(|n| n >= -2)
        })
    {
        return false;
    }
    let Some(daemon) = top.get("daemonws") else {
        return false;
    };
    counters(
        daemon,
        &[
            "connects_total",
            "disconnects_total",
            "active_connections",
            "slow_evictions_total",
            "wakeup_published_total",
            "wakeup_publish_errors",
            "wakeup_received_total",
            "wakeup_delivered_hit_total",
            "wakeup_delivered_miss_total",
            "runtime_gone_delivered_hit_total",
            "runtime_gone_delivered_miss_total",
            "runtime_gone_published_total",
            "runtime_gone_publish_errors",
            "runtime_gone_received_total",
        ],
    )
}

async fn realtime(
    State(state): State<ApiState>,
    peer: Result<ConnectInfo<SocketAddr>, axum::extract::rejection::ExtensionRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Err(status) = authorized(
        &headers,
        peer.ok().map(|p| p.0),
        state.realtime_token.as_deref(),
    ) {
        let mut response = status.into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"metrics\""),
            );
        }
        return Ok(response);
    }
    let metrics = state.realtime_metrics.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "realtime metrics unavailable",
        )
    })?;
    let snapshot = metrics.snapshot().map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "realtime metrics unavailable",
        )
    })?;
    if !complete_snapshot(&snapshot) {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "realtime metrics incomplete",
        ));
    }
    let mut response = Json(snapshot).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn health_requires_real_complete_counters() {
        let mut valid = serde_json::json!({
            "connects_total": 3, "disconnects_total": 2, "active_connections": 1,
            "slow_evictions_total": 0, "messages_sent_total": 7,
            "messages_dropped_total": 0, "membership_removals_dropped_total": 0,
            "inbound_too_large_total": 0,
            "events_sent_by_type": {}, "subscribes_total": {},
            "unsubscribes_total": {}, "subscribe_denied_total": {},
            "active_scope_rooms": {},
            "redis": {"connected": false, "node_id": "", "last_error": "",
                "xadd_total": 0, "xadd_errors": 0, "xread_total": 0,
                "xread_errors": 0, "ack_total": 0, "last_xadd_lag_micros": 0,
                "mirror_primary_errors": 0, "mirror_secondary_errors": 0,
                "mirror_divergence_total": 0, "stream_trimmed_total": 0,
                "stream_missing_total": 0, "retention_errors": 0,
                "streams_without_ttl": 0, "used_memory_bytes": 0,
                "max_memory_bytes": 0, "evicted_keys": 0,
                "streams": {"relay": {"entries": 1, "pttl_millis": -1}}},
            "daemonws": {"connects_total": 2, "disconnects_total": 1,
                "active_connections": 1, "slow_evictions_total": 0,
                "wakeup_published_total": 4, "wakeup_publish_errors": 0,
                "wakeup_received_total": 0, "wakeup_delivered_hit_total": 0,
                "wakeup_delivered_miss_total": 0,
                "runtime_gone_delivered_hit_total": 0,
                "runtime_gone_delivered_miss_total": 0,
                "runtime_gone_published_total": 0,
                "runtime_gone_publish_errors": 0,
                "runtime_gone_received_total": 0}
        });
        assert!(complete_snapshot(&valid));
        for pointer in [
            "/daemonws",
            "/redis",
            "/events_sent_by_type",
            "/messages_sent_total",
            "/daemonws/wakeup_received_total",
            "/redis/streams",
        ] {
            let mut missing = valid.clone();
            let (parent, name) = pointer.rsplit_once('/').unwrap();
            missing
                .pointer_mut(parent)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(name);
            assert!(!complete_snapshot(&missing), "accepted missing {pointer}");
        }
        valid["daemonws"]["connects_total"] = serde_json::json!(-1);
        assert!(!complete_snapshot(&valid));
        valid["daemonws"]["connects_total"] = serde_json::json!(2);
        valid["redis"]["streams"]["relay"]["pttl_millis"] = serde_json::json!(-3);
        assert!(!complete_snapshot(&valid));
        valid["redis"]["streams"]["relay"]["pttl_millis"] = serde_json::json!(-1);
        valid["subscribes_total"]["workspace"] = serde_json::json!("0");
        assert!(!complete_snapshot(&valid));
    }

    #[test]
    fn token_and_peer_policy() {
        let mut h = HeaderMap::new();
        let local = "127.0.0.1:8080".parse().unwrap();
        let remote = "203.0.113.1:8080".parse().unwrap();
        assert_eq!(authorized(&h, None, None), Err(StatusCode::NOT_FOUND));
        assert_eq!(
            authorized(&h, Some(remote), None),
            Err(StatusCode::NOT_FOUND)
        );
        assert_eq!(authorized(&h, Some(local), None), Ok(()));
        h.insert("x-forwarded-for", HeaderValue::from_static("203.0.113.1"));
        assert_eq!(
            authorized(&h, Some(local), None),
            Err(StatusCode::NOT_FOUND)
        );
        h.remove("x-forwarded-for");
        assert_eq!(
            authorized(&h, Some(local), Some("secret")),
            Err(StatusCode::UNAUTHORIZED)
        );
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret"),
        );
        assert_eq!(authorized(&h, Some(remote), Some("secret")), Ok(()));
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secrex"),
        );
        assert_eq!(
            authorized(&h, Some(local), Some("secret")),
            Err(StatusCode::UNAUTHORIZED)
        );
    }
}
