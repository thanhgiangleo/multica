//! Fleet owns mcn_ status; local DB must still recognize a non-shadow owner.
use super::{ApiError, ApiState, error, temporarily_disabled};
use axum::http::StatusCode;
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use std::{fmt, time::Duration};
use uuid::Uuid;

#[derive(Clone)]
pub(super) struct CloudVerifier {
    url: String,
    client: reqwest::Client,
}

#[derive(Default)]
struct Verdict {
    valid: Option<bool>,
    reason: Option<String>,
    owner_id: Option<String>,
    instance_id: Option<String>,
    instance_record_id: Option<String>,
}

impl<'de> Deserialize<'de> for Verdict {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Object;
        impl<'de> Visitor<'de> for Object {
            type Value = Verdict;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Fleet verdict object")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Verdict, M::Error> {
                let mut verdict = Verdict::default();
                while let Some(key) = map.next_key::<String>()? {
                    // encoding/json matches case-insensitively and processes
                    // duplicate keys in order. Null leaves primitive fields alone.
                    let folded: String = key
                        .chars()
                        .map(|c| match c {
                            '\u{017f}' => 's',
                            '\u{212a}' => 'k',
                            c => c.to_ascii_lowercase(),
                        })
                        .collect();
                    if folded == "valid" {
                        if let Some(value) = map.next_value::<Option<bool>>()? {
                            verdict.valid = Some(value);
                        }
                    } else {
                        let field = match folded.as_str() {
                            "reason" => &mut verdict.reason,
                            "owner_id" => &mut verdict.owner_id,
                            "instance_id" => &mut verdict.instance_id,
                            "instance_record_id" => &mut verdict.instance_record_id,
                            _ => {
                                map.next_value::<IgnoredAny>()?;
                                continue;
                            }
                        };
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            *field = Some(value);
                        }
                    }
                }
                Ok(verdict)
            }
        }
        decoder.deserialize_map(Object)
    }
}

fn unavailable() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "cloud pat verifier unavailable",
    )
}

impl CloudVerifier {
    pub fn new(base: &str) -> Result<Option<Self>, reqwest::Error> {
        let base = base.trim().trim_end_matches('/');
        if base.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            url: format!("{base}/api/v1/pat/verify"),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?,
        }))
    }

    async fn owner(&self, token: &str) -> Result<String, ApiError> {
        let body =
            serde_json::to_vec(&serde_json::json!({"token": token})).map_err(|_| unavailable())?;
        if body.len() > 4096 {
            return Err(unavailable());
        }
        let mut response = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| unavailable())?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(unavailable());
        }
        let mut raw = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
            if raw.len() + chunk.len() > 65536 {
                return Err(unavailable());
            }
            raw.extend_from_slice(&chunk);
        }
        // Go unmarshals JSON null into a zero-valued struct (valid=false).
        // The map-only visitor rejects arrays, including empty arrays.
        let parsed = serde_json::from_slice::<Option<Verdict>>(&raw)
            .map_err(|_| unavailable())?
            .unwrap_or_default();
        // These fields are validated as strings even though only owner is used.
        let _ = (
            &parsed.reason,
            &parsed.instance_id,
            &parsed.instance_record_id,
        );
        if parsed.valid != Some(true) {
            return Err(error(StatusCode::UNAUTHORIZED, "invalid token"));
        }
        let owner = parsed.owner_id.unwrap_or_default();
        if owner.is_empty() {
            return Err(unavailable());
        }
        Ok(owner)
    }
}

pub(super) async fn authenticate(state: &ApiState, token: &str) -> Result<Uuid, ApiError> {
    let verifier = state
        .cloud
        .as_ref()
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid token"))?;
    // No Redis in this slice: every request revalidates Fleet and local owner.
    // Do not introduce an in-process cache with a different revocation contract.
    let owner = verifier.owner(token).await?;
    let id =
        Uuid::parse_str(&owner).map_err(|_| error(StatusCode::UNAUTHORIZED, "invalid token"))?;
    let db = state.pool.get().await.map_err(|_| unavailable())?;
    let exists = db
        .query_opt(
            "SELECT 1 FROM \"user\" WHERE id=$1 AND NOT zbs_is_shadow",
            &[&id],
        )
        .await
        .map_err(|_| unavailable())?;
    if exists.is_none() {
        return Err(error(StatusCode::UNAUTHORIZED, "invalid token"));
    }
    if temporarily_disabled(id, "") {
        return Err(error(StatusCode::FORBIDDEN, "account disabled"));
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, extract::State, http::HeaderMap, response::IntoResponse, routing::post};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    type Reply = Arc<Mutex<(StatusCode, String)>>;

    #[tokio::test]
    async fn fleet_protocol_is_bounded_and_fail_closed() {
        assert!(CloudVerifier::new(" \n ").unwrap().is_none());
        let reply: Reply = Arc::new(Mutex::new((StatusCode::OK, String::new())));
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let app = Router::new()
            .route(
                "/api/v1/pat/verify",
                post(
                    move |State(reply): State<Reply>, headers: HeaderMap, body: String| {
                        count.fetch_add(1, Ordering::SeqCst);
                        async move {
                            assert_eq!(headers["content-type"], "application/json");
                            assert_eq!(headers["accept"], "application/json");
                            assert!(!headers.contains_key("authorization"));
                            assert!(!headers.contains_key("cookie"));
                            let data: serde_json::Value = serde_json::from_str(&body).unwrap();
                            assert_eq!(data, serde_json::json!({"token":"mcn_synthetic"}));
                            let value = reply.lock().unwrap().clone();
                            value.into_response()
                        }
                    },
                ),
            )
            .with_state(reply.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        let verifier = CloudVerifier::new(&format!(" http://{address}/// "))
            .unwrap()
            .unwrap();
        for (body, expected) in [
            (r#"{"valid":true,"owner_id":"owner"}"#, None),
            (r#"{"VALID":true,"OWNER_ID":"owner"}"#, None),
            (
                r#"{"valid":false,"VALID":true,"owner_id":"old","OWNER_ID":"owner","owner_id":null,"valid":null}"#,
                None,
            ),
            (r#"{"valid":true,"owner_id":"owner","reaſon":null}"#, None),
            (
                r#"{"valid":true,"valid":false,"owner_id":"owner"}"#,
                Some(StatusCode::UNAUTHORIZED),
            ),
            (
                r#"{"valid":true,"owner_id":"owner","reaſon":3}"#,
                Some(StatusCode::SERVICE_UNAVAILABLE),
            ),
            (
                r#"{"valid":true,"owner_id":"owner","reason":null,"instance_id":null,"instance_record_id":null}"#,
                None,
            ),
            (
                r#"{"valid":false,"reason":"revoked"}"#,
                Some(StatusCode::UNAUTHORIZED),
            ),
            (r#"{"valid":null}"#, Some(StatusCode::UNAUTHORIZED)),
            ("null", Some(StatusCode::UNAUTHORIZED)),
            ("{}", Some(StatusCode::UNAUTHORIZED)),
            (r#"{"valid":true}"#, Some(StatusCode::SERVICE_UNAVAILABLE)),
            (
                r#"{"valid":true,"owner_id":3}"#,
                Some(StatusCode::SERVICE_UNAVAILABLE),
            ),
            (
                r#"{"valid":true,"owner_id":"owner","instance_id":3}"#,
                Some(StatusCode::SERVICE_UNAVAILABLE),
            ),
            (
                r#"{"valid":"true","owner_id":"owner"}"#,
                Some(StatusCode::SERVICE_UNAVAILABLE),
            ),
            ("{broken", Some(StatusCode::SERVICE_UNAVAILABLE)),
            ("[]", Some(StatusCode::SERVICE_UNAVAILABLE)),
            ("{} {}", Some(StatusCode::SERVICE_UNAVAILABLE)),
        ] {
            *reply.lock().unwrap() = (StatusCode::OK, body.into());
            let result = verifier.owner("mcn_synthetic").await;
            match expected {
                Some(status) => assert_eq!(result.unwrap_err().0, status, "{body}"),
                None => assert_eq!(result.unwrap(), "owner"),
            }
        }
        for code in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            *reply.lock().unwrap() = (code, "synthetic upstream detail".into());
            let err = verifier.owner("mcn_synthetic").await.unwrap_err();
            assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(err.1, "cloud pat verifier unavailable");
        }
        *reply.lock().unwrap() = (StatusCode::OK, " ".repeat(65537));
        assert_eq!(
            verifier.owner("mcn_synthetic").await.unwrap_err().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        let before = calls.load(Ordering::SeqCst);
        assert_eq!(
            verifier.owner(&"mcn_".repeat(1100)).await.unwrap_err().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            before,
            "oversize token reached Fleet"
        );
        stop.send(()).unwrap();
        server.await.unwrap();
        assert_eq!(
            verifier.owner("mcn_synthetic").await.unwrap_err().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
