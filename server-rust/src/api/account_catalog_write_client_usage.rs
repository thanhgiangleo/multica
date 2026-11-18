//! User-scoped, daily client-usage writes against the frozen Go contract.
use super::account_catalog_write::view_preference::parse_go_uuid;
use super::account_catalog_write_profile::decode::matches_go_field;
use super::{
    Actor, ApiError, ApiState, Identity, WorkspaceSelectors, catalog::parse_params, error,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, State},
    http::{HeaderMap, StatusCode, Uri},
    routing::post,
};
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use serde_json::Value;
use std::{collections::HashMap, fmt};
use uuid::Uuid;

const UPSERT: &str = r#"INSERT INTO client_usage_daily (
    user_id,client_type,install_id,activity_date,workspace_id,client_version,os,
    first_active_at,last_active_at,runtime_probed_at,probe_result,runtime_count,
    provider_summary,online_count,offline_count
) VALUES ($1,$2,$3,(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date,$4,$5,$6,
    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,
    CASE WHEN $7::boolean THEN CURRENT_TIMESTAMP ELSE NULL END,$8,$9,$10,$11,$12)
ON CONFLICT (user_id,client_type,install_id,activity_date) DO UPDATE SET
    workspace_id=COALESCE(EXCLUDED.workspace_id,client_usage_daily.workspace_id),
    client_version=EXCLUDED.client_version, os=EXCLUDED.os,
    last_active_at=EXCLUDED.last_active_at,
    runtime_probed_at=CASE WHEN $7::boolean THEN EXCLUDED.runtime_probed_at ELSE client_usage_daily.runtime_probed_at END,
    probe_result=CASE WHEN $7::boolean THEN EXCLUDED.probe_result ELSE client_usage_daily.probe_result END,
    runtime_count=CASE WHEN $7::boolean THEN EXCLUDED.runtime_count ELSE client_usage_daily.runtime_count END,
    provider_summary=CASE WHEN $7::boolean THEN EXCLUDED.provider_summary ELSE client_usage_daily.provider_summary END,
    online_count=CASE WHEN $7::boolean THEN EXCLUDED.online_count ELSE client_usage_daily.online_count END,
    offline_count=CASE WHEN $7::boolean THEN EXCLUDED.offline_count ELSE client_usage_daily.offline_count END,
    updated_at=CURRENT_TIMESTAMP"#;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/client-usage", post(upsert))
        .layer(DefaultBodyLimit::disable())
}

#[derive(Default)]
struct Request {
    install_id: String,
    runtime: Option<Probe>,
}

#[derive(Default)]
struct Probe {
    probe_result: String,
    runtime_count: Option<i32>,
    provider_summary: Option<HashMap<String, i32>>,
    online_count: Option<i32>,
    offline_count: Option<i32>,
}

// encoding/json decodes repeated object keys into the existing Go struct and
// merges repeated map values. A null struct pointer clears it; null on a
// non-pointer scalar leaves its previous value intact.
enum ProbeField {
    Result(Option<String>),
    Count(Option<i32>),
    Providers(Option<Vec<(String, i32)>>),
    Online(Option<i32>),
    Offline(Option<i32>),
}

struct ProbeFields(Vec<ProbeField>);

impl<'de> Deserialize<'de> for ProbeFields {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = ProbeFields;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a runtime probe object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut fields = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    let field = if matches_go_field(&key, "probe_result") {
                        ProbeField::Result(map.next_value()?)
                    } else if matches_go_field(&key, "runtime_count") {
                        ProbeField::Count(map.next_value()?)
                    } else if matches_go_field(&key, "provider_summary") {
                        ProbeField::Providers(
                            map.next_value::<Option<ProviderFields>>()?.map(|v| v.0),
                        )
                    } else if matches_go_field(&key, "online_count") {
                        ProbeField::Online(map.next_value()?)
                    } else if matches_go_field(&key, "offline_count") {
                        ProbeField::Offline(map.next_value()?)
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                        return Err(serde::de::Error::custom("unknown runtime field"));
                    };
                    fields.push(field);
                }
                Ok(ProbeFields(fields))
            }
        }
        decoder.deserialize_map(Fields)
    }
}

struct ProviderFields(Vec<(String, i32)>);
impl<'de> Deserialize<'de> for ProviderFields {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = ProviderFields;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a provider count object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut entries = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    entries.push((key, map.next_value::<Option<i32>>()?.unwrap_or(0)));
                }
                Ok(ProviderFields(entries))
            }
        }
        decoder.deserialize_map(Fields)
    }
}

impl Probe {
    fn apply(&mut self, fields: ProbeFields) {
        for field in fields.0 {
            match field {
                ProbeField::Result(Some(value)) => self.probe_result = value,
                ProbeField::Result(None) => {}
                ProbeField::Count(value) => self.runtime_count = value,
                ProbeField::Providers(Some(entries)) => {
                    let providers = self.provider_summary.get_or_insert_with(HashMap::new);
                    providers.extend(entries);
                }
                ProbeField::Providers(None) => self.provider_summary = None,
                ProbeField::Online(value) => self.online_count = value,
                ProbeField::Offline(value) => self.offline_count = value,
            }
        }
    }
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = Request;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a client usage request object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(Request::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut request = Request::default();
                while let Some(key) = map.next_key::<String>()? {
                    if matches_go_field(&key, "install_id") {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            request.install_id = value;
                        }
                    } else if matches_go_field(&key, "runtime") {
                        if let Some(fields) = map.next_value::<Option<ProbeFields>>()? {
                            request
                                .runtime
                                .get_or_insert_with(Probe::default)
                                .apply(fields);
                        } else {
                            request.runtime = None;
                        }
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                        return Err(serde::de::Error::custom("unknown client usage field"));
                    }
                }
                Ok(request)
            }
        }
        decoder.deserialize_any(Fields)
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

type ValidatedProbe = (String, Option<i32>, Option<Value>, Option<i32>, Option<i32>);

fn validate_probe(probe: &Probe) -> Result<ValidatedProbe, ApiError> {
    let result = probe.probe_result.trim().to_ascii_lowercase();
    match result.as_str() {
        "error" => {
            if probe.runtime_count.is_some()
                || probe.provider_summary.is_some()
                || probe.online_count.is_some()
                || probe.offline_count.is_some()
            {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "failed runtime probes must not include counts",
                ));
            }
            Ok((result, None, None, None, None))
        }
        "success" => {
            let (Some(count), Some(providers), Some(online), Some(offline)) = (
                probe.runtime_count,
                probe.provider_summary.as_ref(),
                probe.online_count,
                probe.offline_count,
            ) else {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "successful runtime probes require all counts",
                ));
            };
            if !(0..=1000).contains(&count)
                || online < 0
                || offline < 0
                || online as i64 + offline as i64 != count as i64
            {
                return Err(error(StatusCode::BAD_REQUEST, "invalid runtime counts"));
            }
            if providers.len() > 32 {
                return Err(error(StatusCode::BAD_REQUEST, "too many runtime providers"));
            }
            let mut total = 0i64;
            for (name, value) in providers {
                if name.is_empty()
                    || name.len() > 64
                    || !name.bytes().enumerate().all(|(i, b)| {
                        if i == 0 {
                            b.is_ascii_lowercase() || b.is_ascii_digit()
                        } else {
                            b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-'
                        }
                    })
                    || !(0..=1000).contains(value)
                {
                    return Err(error(
                        StatusCode::BAD_REQUEST,
                        "invalid runtime provider summary",
                    ));
                }
                total += i64::from(*value);
            }
            if total != i64::from(count) {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "runtime provider counts do not match runtime_count",
                ));
            }
            let value = serde_json::to_value(providers)
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid runtime provider summary"))?;
            Ok((
                result,
                Some(count),
                Some(value),
                Some(online),
                Some(offline),
            ))
        }
        _ => Err(error(
            StatusCode::BAD_REQUEST,
            "runtime probe_result must be success or error",
        )),
    }
}

async fn upsert(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(selectors): Extension<WorkspaceSelectors>,
    headers: HeaderMap,
    uri: Uri,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<StatusCode, ApiError> {
    actor.require_human()?;
    let body = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if body.len() > 16 * 1024 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid request body"));
    }
    let req: Request = super::account_catalog_write_profile::full_request(&body)?;
    let install = parse_go_uuid(req.install_id.trim())
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid install_id"))?;
    let platform = header(&headers, "x-client-platform")
        .trim()
        .to_ascii_lowercase();
    if platform != "web" && platform != "desktop" {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "client platform must be web or desktop",
        ));
    }
    let version = header(&headers, "x-client-version").trim();
    let version = if version.is_empty() {
        "unknown"
    } else {
        version
    };
    if version.len() > 64 || !version.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid client version"));
    }
    let os = header(&headers, "x-client-os").trim().to_ascii_lowercase();
    let os = match os.as_str() {
        "macos" | "windows" | "linux" | "ios" | "android" | "chromeos" => os.as_str(),
        _ => "unknown",
    };
    let probe = if let Some(probe) = &req.runtime {
        if platform != "desktop" {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "runtime data is only accepted from desktop",
            ));
        }
        Some(validate_probe(probe)?)
    } else {
        None
    };
    let workspace = workspace(&state, &selectors, &uri).await?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to record client usage",
        )
    })?;
    let failed = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to record client usage",
        )
    };
    let tx = db.transaction().await.map_err(|_| failed())?;
    if let Some(ws) = workspace {
        // Deletion holds the exclusive workspace row lock before detaching reports.
        if tx
            .query_opt("SELECT id FROM workspace WHERE id=$1 FOR KEY SHARE", &[&ws])
            .await
            .map_err(|_| failed())?
            .is_none()
        {
            return Err(error(StatusCode::FORBIDDEN, "workspace not found"));
        }
        if tx
            .query_opt(
                "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &user],
            )
            .await
            .map_err(|_| failed())?
            .is_none()
        {
            return Err(error(StatusCode::FORBIDDEN, "workspace not found"));
        }
    }
    let has_probe = probe.is_some();
    let (result, count, providers, online, offline) = probe
        .map(|(r, c, p, o, f)| (Some(r), c, p, o, f))
        .unwrap_or((None, None, None, None, None));
    tx.execute(
        UPSERT,
        &[
            &user, &platform, &install, &workspace, &version, &os, &has_probe, &result, &count,
            &providers, &online, &offline,
        ],
    )
    .await
    .map_err(|_| failed())?;
    tx.commit().await.map_err(|_| failed())?;
    Ok(StatusCode::NO_CONTENT)
}

async fn workspace(
    state: &ApiState,
    selectors: &WorkspaceSelectors,
    uri: &Uri,
) -> Result<Option<Uuid>, ApiError> {
    let params = parse_params(uri.query());
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to record client usage",
        )
    })?;
    for slug in [
        selectors.slug.as_deref(),
        params.get("workspace_slug").map(String::as_str),
    ] {
        if let Some(slug) = slug.filter(|s| !s.is_empty())
            && let Ok(Some(row)) = db
                .query_opt("SELECT id FROM workspace WHERE slug=$1", &[&slug])
                .await
        {
            return Ok(Some(row.get::<_, Uuid>(0)));
        }
    }
    for id in [
        selectors.id.as_deref(),
        params.get("workspace_id").map(String::as_str),
    ] {
        if let Some(id) = id.filter(|s| !s.is_empty()) {
            return parse_go_uuid(id)
                .map(Some)
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid workspace id"));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod decoder_tests {
    use super::*;

    #[test]
    fn go_object_merge_and_folded_fields() {
        let req: Request = super::super::account_catalog_write_profile::full_request(
            br#"{"INSTALL_ID":"first","install_id":null,"InSTALL_Id":"last",
                "RUNTIME":{"PROBE_RESULT":"success","runtime_count":2,"provider_summary":{"claude":1},"online_count":1},
                "runtime":{"provider_summary":{"claude":null,"codex":2},"OFFLINE_COUNT":1}}"#,
        ).unwrap();
        assert_eq!(req.install_id, "last");
        let runtime = req.runtime.unwrap();
        assert_eq!(runtime.probe_result, "success");
        assert_eq!(runtime.runtime_count, Some(2));
        assert_eq!(runtime.online_count, Some(1));
        assert_eq!(runtime.offline_count, Some(1));
        assert_eq!(
            runtime.provider_summary.unwrap(),
            HashMap::from([("claude".into(), 0), ("codex".into(), 2)])
        );
        for body in [
            br#"{"runtime":{"probe_result":"success","unknown":0}}"#.as_slice(),
            br#"{"runtime":{"probe_result":false}}"#,
            br#"{"extra":{}}"#,
        ] {
            assert!(
                super::super::account_catalog_write_profile::full_request::<Request>(body).is_err()
            );
        }
    }

    #[test]
    fn go_null_reset_and_map_merge() {
        let request: Request = serde_json::from_str(
            r#"{"runtime":{"probe_result":"success","provider_summary":{"a":1}},"RUNTIME":null,"runtime":{"PROBE_RESULT":"error"}}"#,
        ).unwrap();
        assert_eq!(request.runtime.as_ref().unwrap().probe_result, "error");
        assert!(request.runtime.unwrap().provider_summary.is_none());
        let request: Request = serde_json::from_str(
            r#"{"runtime":{"probe_result":"success","provider_summary":{"a":1}},"runtime":{"provider_summary":{}}}"#,
        ).unwrap();
        assert_eq!(request.runtime.unwrap().provider_summary.unwrap()["a"], 1);
    }
}
