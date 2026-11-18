//! Daemon usage ingestion, preserving correction semantics and nullable
//! provider-authoritative cost. The rollup worker consumes updated_at.
use super::{
    ApiError, ApiState, GoJson, daemon::DaemonIdentity, error, json,
    runtime_chat_daemon::task_access,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/daemon/tasks/{taskId}/usage", post(report))
}

#[derive(Default)]
struct Usage {
    provider: String,
    model: String,
    input_tokens: i64,
    output_tokens: i64,
    cache_read_tokens: i64,
    cache_write_tokens: i64,
    cost_usd_ticks: i64,
}
impl<'de> Deserialize<'de> for Usage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UsageVisitor;
        impl<'de> Visitor<'de> for UsageVisitor {
            type Value = Usage;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("object or null")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(Usage::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut usage = Usage::default();
                while let Some(key) = map.next_key::<String>()? {
                    let key = key.to_ascii_lowercase();
                    match key.as_str() {
                        "provider" => {
                            if let Some(value) = map.next_value()? {
                                usage.provider = value;
                            }
                        }
                        "model" => {
                            if let Some(value) = map.next_value()? {
                                usage.model = value;
                            }
                        }
                        "input_tokens" => {
                            if let Some(value) = map.next_value()? {
                                usage.input_tokens = value;
                            }
                        }
                        "output_tokens" => {
                            if let Some(value) = map.next_value()? {
                                usage.output_tokens = value;
                            }
                        }
                        "cache_read_tokens" => {
                            if let Some(value) = map.next_value()? {
                                usage.cache_read_tokens = value;
                            }
                        }
                        "cache_write_tokens" => {
                            if let Some(value) = map.next_value()? {
                                usage.cache_write_tokens = value;
                            }
                        }
                        "cost_usd_ticks" => {
                            if let Some(value) = map.next_value()? {
                                usage.cost_usd_ticks = value;
                            }
                        }
                        _ => {
                            map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(usage)
            }
        }
        deserializer.deserialize_any(UsageVisitor)
    }
}

#[derive(Default)]
struct Report(Vec<Usage>);
impl<'de> Deserialize<'de> for Report {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ReportVisitor;
        impl<'de> Visitor<'de> for ReportVisitor {
            type Value = Report;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("object or null")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(Report::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut report = Report::default();
                while let Some(key) = map.next_key::<String>()? {
                    if key.eq_ignore_ascii_case("usage") {
                        report.0 = map.next_value::<Option<Vec<Usage>>>()?.unwrap_or_default();
                    } else {
                        map.next_value::<de::IgnoredAny>()?;
                    }
                }
                Ok(report)
            }
        }
        deserializer.deserialize_any(ReportVisitor)
    }
}

async fn report(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task_id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load task"))?;
    let (task, _, _) = task_access(&**db, &identity, id).await?;
    let mut items = serde_json::Deserializer::from_slice(&body).into_iter::<Report>();
    let report = items
        .next()
        .and_then(Result::ok)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let runtime_provider: Option<String> = task.get("runtime_provider");
    let runtime_provider = runtime_provider.unwrap_or_default().trim().to_lowercase();
    for usage in report.0 {
        let provider = if usage.provider.trim().is_empty() {
            runtime_provider.clone()
        } else {
            usage.provider.trim().to_lowercase()
        };
        let cost = (usage.cost_usd_ticks > 0).then_some(usage.cost_usd_ticks);
        // Go continues after an individual upsert failure, acknowledging the rest.
        let _ = db.execute("INSERT INTO task_usage(task_id,provider,model,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,cost_usd_ticks,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now()) ON CONFLICT(task_id,provider,model) DO UPDATE SET input_tokens=EXCLUDED.input_tokens,output_tokens=EXCLUDED.output_tokens,cache_read_tokens=EXCLUDED.cache_read_tokens,cache_write_tokens=EXCLUDED.cache_write_tokens,cost_usd_ticks=EXCLUDED.cost_usd_ticks,updated_at=now()", &[&id,&provider,&usage.model,&usage.input_tokens,&usage.output_tokens,&usage.cache_read_tokens,&usage.cache_write_tokens,&cost]).await;
    }
    Ok(GoJson(json!({"status":"ok"})))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn usage_wire_case_null_and_known_fields() {
        let mut values = serde_json::Deserializer::from_slice(
            br#"{"USAGE":[{"Provider":" CoDeX ","INPUT_TOKENS":5,"cost_usd_ticks":null},null]} {}"#,
        )
        .into_iter::<Report>();
        let report = values.next().unwrap().unwrap();
        assert_eq!(report.0.len(), 2);
        assert_eq!(report.0[0].input_tokens, 5);
        assert_eq!(report.0[0].cost_usd_ticks, 0);
        assert!(serde_json::from_str::<Report>(r#"{"usage":[{"model":3}]}"#).is_err());
    }
}
