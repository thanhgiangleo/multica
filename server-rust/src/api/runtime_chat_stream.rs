//! Daemon task transcript ingest. One atomic batch insert; only committed rows
//! become ordered typed events, scoped to issue workspace or chat owner.
use super::{
    ApiError, ApiState, GoJson, daemon::DaemonIdentity, error, json,
    runtime_chat_daemon::task_access, runtime_chat_events::Event, runtime_chat_redact,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use serde_json::Value;
use sha1::{Digest, Sha1};
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/daemon/tasks/{taskId}/messages", post(messages))
}

#[derive(Default)]
struct Message {
    seq: i32,
    kind: String,
    tool: String,
    call_id: String,
    content: String,
    input: Option<Value>,
    output: String,
    created_at: Option<DateTime<Utc>>,
    output_truncated: Option<bool>,
    idempotency_key: String,
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MessageVisitor;
        impl<'de> Visitor<'de> for MessageVisitor {
            type Value = Message;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("object or null")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(Message::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut message = Message::default();
                while let Some(key) = map.next_key::<String>()? {
                    let lower = key.to_ascii_lowercase();
                    match lower.as_str() {
                        "seq" => {
                            if let Some(value) = map.next_value::<Option<i64>>()? {
                                message.seq = value as i32;
                            }
                        }
                        "type" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                message.kind = value;
                            }
                        }
                        "tool" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                message.tool = value;
                            }
                        }
                        "call_id" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                message.call_id = value;
                            }
                        }
                        "content" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                message.content = value;
                            }
                        }
                        "input" => {
                            let value = map.next_value::<Value>()?;
                            if !value.is_null() && !value.is_object() {
                                return Err(de::Error::custom("input must be an object"));
                            }
                            if let (Some(Value::Object(existing)), Value::Object(next)) =
                                (&mut message.input, &value)
                            {
                                existing.extend(next.clone());
                            } else {
                                message.input = if value.is_object() { Some(value) } else { None };
                            }
                        }
                        "output" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                message.output = value;
                            }
                        }
                        "created_at" => {
                            message.created_at = map
                                .next_value::<Option<String>>()?
                                .map(|raw| {
                                    let timestamp = DateTime::parse_from_rfc3339(&raw)
                                        .map_err(de::Error::custom)?;
                                    if timestamp.timestamp_subsec_nanos() >= 1_000_000_000 {
                                        return Err(de::Error::custom("invalid timestamp second"));
                                    }
                                    Ok::<_, M::Error>(timestamp.with_timezone(&Utc))
                                })
                                .transpose()
                                .map_err(de::Error::custom)?;
                        }
                        "output_truncated" => message.output_truncated = map.next_value()?,
                        "idempotency_key" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                message.idempotency_key = value;
                            }
                        }
                        _ => {
                            map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(message)
            }
        }
        deserializer.deserialize_any(MessageVisitor)
    }
}

#[derive(Default)]
struct Batch(Vec<Message>);
impl<'de> Deserialize<'de> for Batch {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BatchVisitor;
        impl<'de> Visitor<'de> for BatchVisitor {
            type Value = Batch;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("object or null")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(Batch::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut batch = Batch::default();
                while let Some(key) = map.next_key::<String>()? {
                    if key.eq_ignore_ascii_case("messages") {
                        batch.0 = map
                            .next_value::<Option<Vec<Message>>>()?
                            .unwrap_or_default();
                    } else {
                        map.next_value::<de::IgnoredAny>()?;
                    }
                }
                Ok(batch)
            }
        }
        deserializer.deserialize_any(BatchVisitor)
    }
}

fn decode(raw: &[u8]) -> Result<Batch, ApiError> {
    // Go json.Decoder.Decode consumes one JSON value and ignores trailing values.
    let mut items = serde_json::Deserializer::from_slice(raw).into_iter::<Batch>();
    items
        .next()
        .and_then(Result::ok)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid request body"))
}

fn message_id(task: Uuid, key: &str) -> Uuid {
    let mut hash = Sha1::new();
    hash.update(task.as_bytes());
    hash.update(key.as_bytes());
    let mut bytes: [u8; 16] = hash.finalize()[..16].try_into().unwrap();
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn rfc3339_nano(timestamp: DateTime<Utc>) -> String {
    let formatted = timestamp.to_rfc3339_opts(SecondsFormat::Nanos, true);
    format!(
        "{}Z",
        formatted
            .trim_end_matches('Z')
            .trim_end_matches('0')
            .trim_end_matches('.')
    )
}

fn timestamps(messages: &[Message]) -> Vec<String> {
    let now = Utc::now();
    let mut times = Vec::with_capacity(messages.len());
    for message in messages {
        let Some(timestamp) = message
            .created_at
            .filter(|t| (*t - now).abs() <= chrono::Duration::seconds(120))
        else {
            return vec![String::new(); messages.len()];
        };
        times.push(rfc3339_nano(timestamp));
    }
    times
}

fn payload(row: &Row, task: Uuid, issue: Option<Uuid>) -> Value {
    let optional = |name| row.get::<_, Option<String>>(name).unwrap_or_default();
    let created_at: DateTime<Utc> = row.get("created_at");
    let mut value = json!({
        "task_id":task,"seq":row.get::<_,i32>("seq"),"type":row.get::<_,String>("type"),
        "created_at":rfc3339_nano(created_at),
    });
    if let Some(issue) = issue {
        value["issue_id"] = json!(issue);
    }
    for (field, stored) in [
        ("tool", "tool"),
        ("call_id", "call_id"),
        ("content", "content"),
        ("output", "output"),
    ] {
        let text = optional(stored);
        if !text.is_empty() {
            value[field] = json!(text);
        }
    }
    if let Some(input) = row
        .get::<_, Option<Value>>("input")
        .filter(|value| value.as_object().is_some_and(|map| !map.is_empty()))
    {
        value["input"] = input;
    }
    if let Some(truncated) = row.get::<_, Option<bool>>("output_truncated") {
        value["output_truncated"] = json!(truncated);
    }
    value
}

async fn messages(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let mut batch = decode(&body)?;
    if batch.0.is_empty() {
        return Ok(GoJson(json!({"status":"ok"})));
    }
    let task_id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task_id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load task"))?;
    let (task, workspace, owner) = task_access(&**db, &identity, task_id).await?;
    let issue: Option<Uuid> = task.get("issue_id");
    let session: Option<Uuid> = task.get("chat_session_id");
    // UUID v5 uses SHA-1, identical to Go uuid.NewSHA1 on the task UUID namespace.
    let times = timestamps(&batch.0);
    let mut ids = Vec::with_capacity(batch.0.len());
    let mut seqs = Vec::with_capacity(batch.0.len());
    let mut kinds = Vec::with_capacity(batch.0.len());
    let mut tools = Vec::with_capacity(batch.0.len());
    let mut call_ids = Vec::with_capacity(batch.0.len());
    let mut contents = Vec::with_capacity(batch.0.len());
    let mut inputs = Vec::with_capacity(batch.0.len());
    let mut outputs = Vec::with_capacity(batch.0.len());
    let mut truncations = Vec::with_capacity(batch.0.len());
    for msg in &mut batch.0 {
        if msg.idempotency_key.len() > 512 {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "message idempotency_key exceeds 512 bytes",
            ));
        }
        ids.push(if msg.idempotency_key.is_empty() {
            Uuid::now_v7()
        } else {
            message_id(task_id, &msg.idempotency_key)
        });
        seqs.push(msg.seq);
        kinds.push(msg.kind.replace('\0', ""));
        tools.push(msg.tool.replace('\0', ""));
        call_ids.push(msg.call_id.replace('\0', ""));
        contents.push(runtime_chat_redact::text(&msg.content));
        outputs.push(runtime_chat_redact::text(&msg.output));
        inputs.push(
            msg.input
                .as_ref()
                .map(|input| runtime_chat_redact::input_map(input).to_string())
                .unwrap_or_default(),
        );
        truncations.push(
            msg.output_truncated
                .map(|v| v.to_string())
                .unwrap_or_default(),
        );
    }
    let created = db.query(
        "WITH incoming AS (SELECT unnest($1::uuid[]) AS id,unnest($2::int4[]) AS seq,unnest($3::text[]) AS type,unnest($4::text[]) AS tool,unnest($5::text[]) AS call_id,unnest($6::text[]) AS content,unnest($7::text[]) AS input,unnest($8::text[]) AS output,unnest($9::text[]) AS created_at,unnest($10::text[]) AS output_truncated),inserted AS (INSERT INTO task_message(id,task_id,seq,type,tool,content,input,output,created_at,output_truncated,call_id) SELECT m.id,$11,m.seq,m.type,NULLIF(m.tool,''),NULLIF(m.content,''),NULLIF(m.input,'')::jsonb,NULLIF(m.output,''),COALESCE(NULLIF(m.created_at,'')::timestamptz,now()),NULLIF(m.output_truncated,'')::bool,NULLIF(m.call_id,'') FROM incoming m ON CONFLICT(id) DO NOTHING RETURNING *) SELECT * FROM inserted ORDER BY seq ASC",
        &[&ids,&seqs,&kinds,&tools,&call_ids,&contents,&inputs,&outputs,&times,&truncations,&task_id],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to persist task message"))?;
    if issue.is_some() || session.is_some() {
        for row in created {
            state.events.publish(Event {
                event_type: "task:message".into(),
                workspace_id: workspace,
                actor_type: "system".into(),
                actor_id: Uuid::nil(),
                chat_session_id: session,
                chat_session_owner_id: owner,
                payload: payload(&row, task_id, issue),
            });
        }
    }
    Ok(GoJson(json!({"status":"ok"})))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wire_decoder_matches_go_field_case_null_and_tail_rules() {
        let batch = decode(br#"{"MESSAGES":[{"seq":1,"TYPE":"text","Content":"one","CONTENT":"two","output_truncated":false,"unknown":3},null]} {}"#).unwrap();
        assert_eq!(batch.0.len(), 2);
        assert_eq!(batch.0[0].content, "two");
        assert_eq!(batch.0[0].output_truncated, Some(false));
        assert_eq!(batch.0[1].seq, 0);
        assert!(decode(br#"{"messages":[{"input":5}]}"#).is_err());
        assert!(decode(br#"{"messages":{}}"#).is_err());
    }
    #[test]
    fn repeated_input_maps_merge_and_null_clears_them() {
        let batch = decode(br#"{"messages":[{"input":{"first":1},"INPUT":{"second":2}},{"input":{"first":1},"input":null}]}"#).unwrap();
        assert_eq!(batch.0[0].input, Some(json!({"first":1,"second":2})));
        assert!(batch.0[1].input.is_none());
        assert!(decode(br#"{"messages":[{"created_at":"2025-01-01T00:00:60Z"}]}"#).is_err());
    }

    #[test]
    fn idempotent_uuid_and_rfc3339nano_match_go() {
        let namespace = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        assert_eq!(
            message_id(namespace, "www.widgets.com").to_string(),
            "21f7f8de-8051-5b89-8680-0195ef798b6a"
        );
        for (raw, expected) in [
            ("2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"),
            ("2025-01-01T00:00:00.123Z", "2025-01-01T00:00:00.123Z"),
            ("2025-01-01T00:00:00.123456Z", "2025-01-01T00:00:00.123456Z"),
        ] {
            let value = DateTime::parse_from_rfc3339(raw)
                .unwrap()
                .with_timezone(&Utc);
            assert_eq!(rfc3339_nano(value), expected);
        }
    }

    #[test]
    fn implausible_clock_falls_back_for_whole_batch() {
        let batch = decode(br#"{"messages":[{"created_at":"2020-01-01T00:00:00Z"},{}]}"#).unwrap();
        assert_eq!(timestamps(&batch.0), vec!["", ""]);
    }
}
