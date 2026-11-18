//! Installation token rotation/revocation: one-time plaintext, persistent SHA-256 only.
use super::{Actor, ApiError, ApiState, GoJson, Identity, error};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::GenericClient;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route(
        "/api/workspaces/{id}/plugins/{installation_id}/token",
        post(rotate).delete(revoke),
    )
}

pub(super) async fn admin(
    db: &impl GenericClient,
    user: Uuid,
    actor: &Actor,
    raw: &str,
    enabled: bool,
) -> Result<Uuid, ApiError> {
    actor.require_workspace_url(raw)?;
    let ws =
        Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
    let row = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    actor.require_human()?;
    let role: String = row.get("role");
    if !matches!(role.as_str(), "owner" | "admin") {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if !enabled {
        return Err(error(
            StatusCode::FORBIDDEN,
            "Plugin management is not enabled",
        ));
    }
    Ok(ws)
}
fn parse_install(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::NOT_FOUND, "plugin installation not found"))
}
fn token() -> Result<String, ApiError> {
    let mut entropy = [0u8; 32];
    getrandom::fill(&mut entropy)
        .map_err(|_| error(StatusCode::BAD_GATEWAY, "failed to issue the Plugin token"))?;
    Ok(format!("mpi_{}", URL_SAFE_NO_PAD.encode(entropy)))
}
pub(super) fn signing_secret(key: &[u8; 32], installation: Uuid) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts 32 bytes");
    mac.update(b"multica-plugin-hook-signature:v1:");
    mac.update(installation.to_string().as_bytes());
    format!("whsec_{}", hex::encode(mac.finalize().into_bytes()))
}
async fn rotate(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, raw_install)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to issue the Plugin token",
        )
    })?;
    let ws = admin(&**db, user, &actor, &raw, state.extensions_sprint_enabled).await?;
    let installation = parse_install(&raw_install)?;
    // Signing value prepared before changing database token hash. Hook key never persisted.
    let signing = state
        .extensions_sprint_signing_key
        .as_ref()
        .map(|key| signing_secret(key, installation));
    let token = token()?;
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    let rows = db
        .execute(
            "UPDATE plugin_installation SET token_hash=$1,token_rotated_at=now(),updated_at=now() WHERE id=$2 AND workspace_id=$3",
            &[&hash, &installation, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::BAD_GATEWAY, "failed to issue the Plugin token"))?;
    if rows == 0 {
        return Err(error(
            StatusCode::NOT_FOUND,
            "plugin installation not found",
        ));
    }
    let mut response = json!({"token":token});
    if let Some(signing) = signing {
        response["signing_secret"] = json!(signing);
    }
    Ok(GoJson(response))
}
async fn revoke(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, raw_install)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to revoke the Plugin token",
        )
    })?;
    let ws = admin(&**db, user, &actor, &raw, state.extensions_sprint_enabled).await?;
    let installation = parse_install(&raw_install)?;
    let rows = db
        .execute(
            "UPDATE plugin_installation SET token_hash=NULL,updated_at=now() WHERE id=$1 AND workspace_id=$2",
            &[&installation, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::BAD_GATEWAY, "failed to revoke the Plugin token"))?;
    if rows == 0 {
        return Err(error(
            StatusCode::NOT_FOUND,
            "plugin installation not found",
        ));
    }
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn token_entropy_and_signing_secret_are_installation_bound() {
        let first = token().unwrap();
        let second = token().unwrap();
        assert!(first.starts_with("mpi_") && second.starts_with("mpi_"));
        assert_ne!(first, second);
        assert_eq!(URL_SAFE_NO_PAD.decode(&first[4..]).unwrap().len(), 32);
        let id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        assert_ne!(
            signing_secret(&[7; 32], id),
            signing_secret(&[7; 32], Uuid::new_v4())
        );
        assert_ne!(signing_secret(&[7; 32], id), signing_secret(&[8; 32], id));
        assert_eq!(
            signing_secret(&[0; 32], id),
            "whsec_1af7a35faa804a939671546dba7d0ca48c11e3f926992a6fe223e5b29d2d14f8"
        );
    }
}
