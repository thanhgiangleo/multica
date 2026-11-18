//! Account profile and cloud-waitlist mutations against the frozen Go contract.
use super::{Actor, ApiError, ApiState, GoJson, Identity, User, error, timestamp};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, State},
    http::StatusCode,
    routing::{patch, post},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use tokio_postgres::Row;

#[path = "account_catalog_write_profile_decode.rs"]
pub(super) mod decode;
#[path = "account_catalog_write_json.rs"]
mod json;
#[path = "account_catalog_write_mail.rs"]
mod mail;
#[path = "account_catalog_write_timezone.rs"]
mod timezone;

// json.Decoder.Decode reads one value and permits trailing input; Go's
// handlers also parse JSON without requiring a Content-Type header.
pub(super) fn account_request<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, ApiError> {
    if !json::depth_ok(body) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid request body"));
    }
    let normalized = json::normalize_strings(body);
    T::deserialize(&mut serde_json::Deserializer::from_slice(&normalized))
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))
}

// json.Unmarshal (unlike json.Decoder.Decode) requires the entire body to be one value.
pub(super) fn full_request<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, ApiError> {
    if !json::depth_ok(body) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid request body"));
    }
    serde_json::from_slice(&json::normalize_strings(body))
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/me", patch(update_me)).route(
        "/api/me/onboarding/cloud-waitlist",
        post(join_cloud_waitlist),
    )
}

#[derive(Default)]
struct UpdateMe {
    name: Option<String>,
    avatar_url: Option<String>,
    language: Option<String>,
    profile_description: Option<String>,
    timezone: Option<String>,
}

pub(super) const USER_COLUMNS: &str = "id, name, email, avatar_url, language, timezone, onboarded_at, onboarding_questionnaire, profile_description, created_at, updated_at";

pub(super) fn user_response(row: &Row) -> User {
    User {
        id: row.get("id"),
        name: row.get("name"),
        email: row.get("email"),
        avatar_url: row.get("avatar_url"),
        language: row.get("language"),
        timezone: row.get("timezone"),
        onboarded_at: row
            .get::<_, Option<DateTime<Utc>>>("onboarded_at")
            .map(timestamp),
        onboarding_questionnaire: row.get::<_, Value>("onboarding_questionnaire"),
        profile_description: row.get("profile_description"),
        created_at: timestamp(row.get("created_at")),
        updated_at: timestamp(row.get("updated_at")),
    }
}

async fn update_me(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<User>, ApiError> {
    actor.require_human()?;
    let body = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req: UpdateMe = account_request(&body)?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "user not found"))?;
    let current = db
        .query_opt(
            &format!("SELECT {USER_COLUMNS} FROM \"user\" WHERE id=$1"),
            &[&user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "user not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "user not found"))?;
    let name = req.name.map(|s| s.trim().to_owned());
    if name.as_deref().is_some_and(str::is_empty) {
        return Err(error(StatusCode::BAD_REQUEST, "name is required"));
    }
    let current_avatar: Option<String> = current.get("avatar_url");
    let avatar = match req.avatar_url {
        Some(raw) => Some(
            super::account_catalog_write_avatar::accept(
                &state,
                &**db,
                &raw,
                current_avatar.as_deref().unwrap_or(""),
            )
            .await
            .map_err(|message| error(StatusCode::FORBIDDEN, message))?,
        ),
        None => None,
    };
    let language = req.language.map(|s| s.trim().to_owned());
    if language
        .as_deref()
        .is_some_and(|s| !matches!(s, "en" | "zh-Hans" | "zh-Hant" | "ko" | "ja" | "fr"))
    {
        return Err(error(StatusCode::BAD_REQUEST, "unsupported language"));
    }
    let description = req.profile_description.map(|s| s.trim().to_owned());
    if description
        .as_deref()
        .is_some_and(|s| s.chars().count() > 2000)
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "profile_description exceeds 2000 characters",
        ));
    }
    let timezone = req.timezone.map(|s| s.trim().to_owned());
    if timezone
        .as_deref()
        .is_some_and(|s| !s.is_empty() && !valid_timezone(s))
    {
        return Err(error(StatusCode::BAD_REQUEST, "invalid timezone"));
    }
    let row = db.query_one(&format!("UPDATE \"user\" SET name=COALESCE($2,name), avatar_url=COALESCE($3,avatar_url), language=COALESCE($4,language), profile_description=COALESCE($5,profile_description), timezone=CASE WHEN $6::text IS NULL THEN timezone WHEN $6::text='' THEN NULL ELSE $6::text END, updated_at=now() WHERE id=$1 RETURNING {USER_COLUMNS}"),
        &[&user, &name, &avatar, &language, &description, &timezone]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update user"))?;
    Ok(GoJson(user_response(&row)))
}

fn valid_timezone(s: &str) -> bool {
    timezone::valid(s)
}

#[derive(Default)]
struct WaitlistRequest {
    email: String,
    reason: String,
}

async fn join_cloud_waitlist(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<User>, ApiError> {
    actor.require_human()?;
    let body = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req: WaitlistRequest = account_request(&body)?;
    // Go strings.ToLower uses simple Unicode case mapping, not contextual or
    // multi-character special casing (notably Greek sigma and dotted capital I).
    let email: String = req
        .email
        .trim()
        .chars()
        .map(|ch| ch.to_lowercase().next().unwrap_or(ch))
        .collect();
    if email.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "email is required"));
    }
    if email.len() > 254 {
        return Err(error(StatusCode::BAD_REQUEST, "email is too long"));
    }
    if !valid_email(&email) {
        return Err(error(StatusCode::BAD_REQUEST, "email is invalid"));
    }
    let reason = req.reason.trim();
    if reason.len() > 500 {
        return Err(error(StatusCode::BAD_REQUEST, "reason is too long"));
    }
    let reason = (!reason.is_empty()).then_some(reason);
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to join waitlist"))?;
    let row = db.query_opt(&format!("UPDATE \"user\" SET cloud_waitlist_email=$2, cloud_waitlist_reason=$3, updated_at=now() WHERE id=$1 RETURNING {USER_COLUMNS}"), &[&user, &email, &reason]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to join waitlist"))?
        .ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to join waitlist"))?;
    let result = user_response(&row);
    drop(db);
    super::account_catalog_write_onboarding::record(
        &state,
        vec![super::account_catalog_write_onboarding::OnboardingMetric::CloudWaitlistJoined],
    )
    .await;
    Ok(GoJson(result))
}

fn valid_email(s: &str) -> bool {
    mail::valid_address(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn profile_validation() {
        assert!(valid_timezone("Asia/Ho_Chi_Minh"));
        assert!(!valid_timezone("../../etc/passwd"));
        assert!(!valid_timezone("Unknown/City"));
        assert!(valid_email("person@example.com"));
        assert!(valid_email("person@localhost"));
    }
}
