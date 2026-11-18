//! Installation reads, inert reactivation, disable and atomic dependent cleanup.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity, error, extensions_sprint_install,
    extensions_sprint_token, timestamp,
};
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{delete, get, post, put},
};
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use serde_json::{Map, Value, json};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/workspaces/{id}/plugins", get(list))
        .route(
            "/api/workspaces/{id}/plugins/{installation_id}/config",
            put(configure),
        )
        .route(
            "/api/workspaces/{id}/plugins/{installation_id}/enable",
            post(enable),
        )
        .route(
            "/api/workspaces/{id}/plugins/{installation_id}/disable",
            post(disable),
        )
        .route(
            "/api/workspaces/{id}/plugins/{installation_id}",
            delete(uninstall),
        )
}
fn unavailable() -> ApiError {
    error(StatusCode::BAD_GATEWAY, "failed to update the Plugin")
}
fn missing() -> ApiError {
    error(StatusCode::NOT_FOUND, "plugin installation not found")
}
fn parse_id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| missing())
}

// Explicit projections: neither unknown config keys, secret-typed values, token
// hashes nor arbitrary hook fields are a part of an installation response.
pub(super) fn project_config(
    manifest: &Value,
    config: &Value,
) -> Result<(Vec<Value>, Value), ApiError> {
    let mut fields = Vec::new();
    let mut visible = serde_json::Map::new();
    let schema = match manifest.get("config") {
        None | Some(Value::Null) => None,
        Some(Value::Object(fields)) => Some(fields),
        Some(_) => return Err(unavailable()),
    };
    if let Some(schema) = schema {
        for (key, field) in schema {
            let kind = field
                .get("type")
                .and_then(Value::as_str)
                .ok_or_else(unavailable)?;
            let mut projected = json!({"key":key,"type":kind,
                "label":field.get("label").and_then(Value::as_str).unwrap_or(""),
                "required":field.get("required").and_then(Value::as_bool).unwrap_or(false)});
            for name in ["description", "options", "placeholder", "multiline"] {
                if let Some(value) = field.get(name) {
                    projected[name] = value.clone();
                }
            }
            fields.push(projected);
            if matches!(kind, "string" | "number" | "bool" | "enum")
                && let Some(value) = config.get(key)
            {
                visible.insert(key.clone(), value.clone());
            }
        }
    }
    Ok((fields, Value::Object(visible)))
}

pub(super) fn visible_config(manifest: &Value, config: &Value) -> Result<Value, ApiError> {
    project_config(manifest, config).map(|(_, config)| config)
}

fn descriptive_payload(manifest: &Value, config: &Value) -> Result<Value, ApiError> {
    let (fields, visible) = project_config(manifest, config)?;
    let contributes = &manifest["contributes"];
    let array = |name: &str| -> Result<Vec<Value>, ApiError> {
        match contributes.get(name) {
            None | Some(Value::Null) => Ok(Vec::new()),
            Some(Value::Array(values)) => Ok(values.clone()),
            _ => Err(unavailable()),
        }
    };
    let mut hooks = Vec::new();
    for hook in array("hooks")? {
        let mut value = json!({"key":hook["key"], "name":hook["name"],
            "description":hook.get("description").and_then(Value::as_str).unwrap_or(""),
            "triggers":hook.get("triggers").cloned().unwrap_or_else(||json!([])),
            "transport":hook["transport"]["type"]});
        if hook["events"].as_array().is_some_and(|v| !v.is_empty()) {
            value["events"] = hook["events"].clone();
        }
        hooks.push(value);
    }
    let name = manifest
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(unavailable)?;
    let mut out = json!({"name":name,"config_schema":fields,"config":visible,
        "surfaces":array("surfaces")?,"hooks":hooks,"resources":array("resources")?});
    if manifest["description"]
        .as_str()
        .is_some_and(|v| !v.is_empty())
    {
        out["description"] = manifest["description"].clone();
    }
    Ok(out)
}

pub(super) fn payload(
    row: &Row,
    secrets: Vec<String>,
    schedules: &[Row],
) -> Result<Value, ApiError> {
    let mut out = descriptive_payload(
        &row.get::<_, Value>("manifest"),
        &row.get::<_, Value>("config"),
    )?;
    for schedule in schedules {
        let key: &str = schedule.get("hook_key");
        if let Some(hook) = out["hooks"]
            .as_array_mut()
            .and_then(|hooks| hooks.iter_mut().find(|h| h["key"] == key))
        {
            let mut value = json!({"cron":schedule.get::<_,String>("cron_expression"),"timezone":schedule.get::<_,String>("timezone")});
            if let Some(next) =
                schedule.get::<_, Option<chrono::DateTime<chrono::Utc>>>("next_run_at")
            {
                value["next_run_at"] = json!(timestamp(next));
            }
            hook["schedule"] = value;
        }
    }
    let scopes: Value = row.get("granted_scopes");
    let base = json!({"id":row.get::<_,Uuid>("id"),"plugin_key":row.get::<_,String>("plugin_key"),
        "version":row.get::<_,String>("version"),"package_version_id":row.get::<_,Uuid>("package_version_id"),
        "enabled":row.get::<_,bool>("enabled"),"granted_scopes":scopes,"configured_secrets":secrets,
        "created_at":timestamp(row.get("created_at")),"updated_at":timestamp(row.get("updated_at"))});
    out.as_object_mut()
        .expect("object projection")
        .extend(base.as_object().expect("object projection").clone());
    Ok(out)
}

async fn load_payload(db: &impl GenericClient, row: &Row) -> Result<Value, ApiError> {
    let installation: Uuid = row.get("id");
    let secrets = db
        .query(
            "SELECT key FROM plugin_secret WHERE installation_id=$1 ORDER BY key",
            &[&installation],
        )
        .await
        .map_err(|_| unavailable())?
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    let schedules = db.query("SELECT hook_key,cron_expression,timezone,next_run_at FROM plugin_hook_schedule WHERE installation_id=$1 ORDER BY hook_key", &[&installation])
        .await.map_err(|_|unavailable())?;
    payload(row, secrets, &schedules)
}

async fn list(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_workspace_url(&raw)?;
    let ws = Uuid::parse_str(&raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    // One consistent snapshot avoids pairing old config with newly removed secrets.
    let tx = db
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(|_| unavailable())?;
    if tx
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| unavailable())?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "workspace not found"));
    }
    if !state.extensions_sprint_enabled {
        return Err(error(
            StatusCode::FORBIDDEN,
            "Plugin management is not enabled",
        ));
    }
    let rows = tx
        .query(
            "SELECT * FROM plugin_installation WHERE workspace_id=$1 ORDER BY created_at,id",
            &[&ws],
        )
        .await
        .map_err(|_| unavailable())?;
    let mut plugins = Vec::with_capacity(rows.len());
    for row in &rows {
        plugins.push(load_payload(&*tx, row).await?);
    }
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(GoJson(json!({"plugins":plugins})))
}

async fn enable(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, id)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    set_enabled(state, user, actor, raw, id, true).await
}
async fn disable(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, id)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    set_enabled(state, user, actor, raw, id, false).await
}
fn invalid_config(message: &'static str) -> ApiError {
    error(StatusCode::BAD_REQUEST, message)
}

fn normalize_config(field: &Value, value: &Value, key: &str) -> Result<Value, ApiError> {
    let _ = key; // Never include untrusted config keys or submitted values in error text.
    match field.get("type").and_then(Value::as_str) {
        Some("string") => {
            let text = value
                .as_str()
                .ok_or_else(|| invalid_config("config field must be a string"))?;
            if text.len() > 4096 {
                return Err(invalid_config("config field exceeds 4096 bytes"));
            }
        }
        Some("number") if value.is_number() => {}
        Some("number") => return Err(invalid_config("config field must be a number")),
        Some("bool") if value.is_boolean() => {}
        Some("bool") => return Err(invalid_config("config field must be a boolean")),
        Some("enum") => {
            let text = value
                .as_str()
                .ok_or_else(|| invalid_config("config field must be a string"))?;
            if !field
                .get("options")
                .and_then(Value::as_array)
                .is_some_and(|options| options.iter().any(|option| option.as_str() == Some(text)))
            {
                return Err(invalid_config(
                    "config field must be one of the declared options",
                ));
            }
        }
        _ => return Err(invalid_config("config field has an unsupported type")),
    }
    Ok(value.clone())
}

// Frozen Go secretbox wire format: random 12-byte nonce || AES-256-GCM ciphertext || tag.
// No AAD or key-id prefix in v1. Deployment must inject a separate decoded config key.
fn seal_config_secret(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>, ApiError> {
    let key = UnboundKey::new(&aead::AES_256_GCM, key).map_err(|_| unavailable())?;
    let key = LessSafeKey::new(key);
    let mut nonce = [0; 12];
    getrandom::fill(&mut nonce).map_err(|_| unavailable())?;
    let mut ciphertext = plaintext.to_vec();
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::empty(),
        &mut ciphertext,
    )
    .map_err(|_| unavailable())?;
    let mut sealed = Vec::with_capacity(nonce.len() + ciphertext.len());
    sealed.extend_from_slice(&nonce);
    sealed.extend_from_slice(&ciphertext);
    Ok(sealed)
}

struct ConfigUpdate {
    plain: Value,
    secrets: Vec<(String, String)>,
}

fn config_update(manifest: &Value, stored: &Value, body: &Value) -> Result<ConfigUpdate, ApiError> {
    if !body.is_object() {
        return Err(invalid_config("invalid request body"));
    }
    let empty = Map::new();
    let schema = match manifest.get("config") {
        None => &empty,
        Some(Value::Object(schema)) => schema,
        _ => return Err(invalid_config("stored plugin manifest is unreadable")),
    };
    let values = match body.get("values") {
        None | Some(Value::Null) => &empty,
        Some(Value::Object(values)) => values,
        _ => return Err(invalid_config("invalid request body")),
    };
    let stored = stored.as_object().ok_or_else(unavailable)?;
    let mut plain = Map::new();
    // Re-project stored values as well: historical/undeclared keys and a field
    // that became secret must never be copied back into plaintext config.
    for (key, field) in schema {
        if matches!(
            field.get("type").and_then(Value::as_str),
            Some("string" | "number" | "bool" | "enum")
        ) && let Some(value) = stored.get(key)
        {
            plain.insert(key.clone(), normalize_config(field, value, key)?);
        }
    }
    let mut secrets = Vec::new();
    for (key, value) in values {
        let field = schema
            .get(key)
            .ok_or_else(|| invalid_config("unknown config field"))?;
        if field.get("type").and_then(Value::as_str) == Some("secret") {
            let text = value
                .as_str()
                .ok_or_else(|| invalid_config("config field must be a string"))?;
            if text.len() > 8192 {
                return Err(invalid_config("config field exceeds 8192 bytes"));
            }
            secrets.push((key.clone(), text.to_owned()));
        } else {
            plain.insert(key.clone(), normalize_config(field, value, key)?);
        }
    }
    Ok(ConfigUpdate {
        plain: Value::Object(plain),
        secrets,
    })
}

async fn configure(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<GoJson<Value>, ApiError> {
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    let tx = db.transaction().await.map_err(|_| unavailable())?;
    let ws =
        extensions_sprint_token::admin(&*tx, user, &actor, &raw, state.extensions_sprint_enabled)
            .await?;
    // Keep consent/role valid through commit, including concurrent member removal.
    if tx.query_opt("SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 AND role IN ('owner','admin') FOR SHARE", &[&ws, &user]).await.map_err(|_|unavailable())?.is_none() {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    let id = parse_id(&id)?;
    // Read after locking, never merge over a pre-transaction installation snapshot.
    let row = tx
        .query_opt(
            "SELECT * FROM plugin_installation WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(missing)?;
    let update = config_update(
        &row.get::<_, Value>("manifest"),
        &row.get::<_, Value>("config"),
        &body,
    )?;
    let key = if update.secrets.is_empty() {
        None
    } else {
        Some(state.extensions_sprint_config_key.as_ref().ok_or_else(|| {
            error(
                StatusCode::BAD_GATEWAY,
                "plugin secrets are disabled: MULTICA_PLUGIN_SECRET_KEY is not configured",
            )
        })?)
    };
    for (name, text) in &update.secrets {
        if text.is_empty() {
            tx.execute(
                "DELETE FROM plugin_secret WHERE installation_id=$1 AND key=$2",
                &[&id, &name],
            )
            .await
            .map_err(|_| unavailable())?;
        } else {
            let ciphertext =
                seal_config_secret(key.expect("secret writes require a key"), text.as_bytes())?;
            tx.execute("INSERT INTO plugin_secret(installation_id,key,ciphertext) VALUES($1,$2,$3) ON CONFLICT(installation_id,key) DO UPDATE SET ciphertext=EXCLUDED.ciphertext,updated_at=now()", &[&id, &name, &ciphertext]).await.map_err(|_|unavailable())?;
        }
    }
    let row = tx.query_one("UPDATE plugin_installation SET config=$3,updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *", &[&id, &ws, &update.plain]).await.map_err(|_|unavailable())?;
    let result = load_payload(&*tx, &row).await?;
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(GoJson(result))
}

async fn set_enabled(
    state: ApiState,
    user: Uuid,
    actor: Actor,
    raw: String,
    id: String,
    enabled: bool,
) -> Result<GoJson<Value>, ApiError> {
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    let ws =
        extensions_sprint_token::admin(&**db, user, &actor, &raw, state.extensions_sprint_enabled)
            .await?;
    let id = parse_id(&id)?;
    let tx = db.transaction().await.map_err(|_| unavailable())?;
    let row = tx
        .query_opt(
            "SELECT * FROM plugin_installation WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(missing)?;
    let row = if row.get::<_, bool>("enabled") != enabled {
        if enabled {
            // There is no runtime/scheduler adapter in this slice. Reactivating a
            // hook/skill/surface package would falsely promise restored effects.
            extensions_sprint_install::ensure_inert(&row.get::<_, Value>("manifest"))?;
            if tx.query_opt("SELECT 1 FROM plugin_hook_schedule WHERE installation_id=$1 UNION ALL SELECT 1 FROM skill WHERE plugin_installation_id=$1 LIMIT 1", &[&id]).await.map_err(|_|unavailable())?.is_some() {
                return Err(error(StatusCode::UNPROCESSABLE_ENTITY,"this Plugin has contributions this Rust slice cannot reactivate yet"));
            }
        }
        let updated = tx.query_one("UPDATE plugin_installation SET enabled=$3,updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *", &[&id,&ws,&enabled]).await.map_err(|_|unavailable())?;
        if !enabled {
            tx.execute("UPDATE plugin_hook_schedule SET enabled=false,next_run_at=NULL,updated_at=now() WHERE installation_id=$1", &[&id]).await.map_err(|_|unavailable())?;
        }
        updated
    } else {
        row
    };
    let result = load_payload(&*tx, &row).await?;
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(GoJson(result))
}

async fn uninstall(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    let ws =
        extensions_sprint_token::admin(&**db, user, &actor, &raw, state.extensions_sprint_enabled)
            .await?;
    let id = parse_id(&id)?;
    let tx = db.transaction().await.map_err(|_| unavailable())?;
    // Every storage mutation holds FOR SHARE on this same row through commit.
    // Lock before leaf deletes so no concurrent writer can leave orphan storage.
    tx.query_opt(
        "SELECT id FROM plugin_installation WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
        &[&id, &ws],
    )
    .await
    .map_err(|_| unavailable())?
    .ok_or_else(missing)?;
    for statement in [
        "DELETE FROM plugin_hook_schedule WHERE installation_id=$1",
        "DELETE FROM plugin_storage WHERE installation_id=$1",
        "DELETE FROM plugin_secret WHERE installation_id=$1",
        "DELETE FROM plugin_invocation WHERE installation_id=$1",
        "DELETE FROM agent_skill WHERE skill_id IN (SELECT id FROM skill WHERE plugin_installation_id=$1)",
        "DELETE FROM skill_file WHERE skill_id IN (SELECT id FROM skill WHERE plugin_installation_id=$1)",
        "DELETE FROM skill_to_label WHERE skill_id IN (SELECT id FROM skill WHERE plugin_installation_id=$1)",
        "DELETE FROM skill WHERE plugin_installation_id=$1",
        "DELETE FROM plugin_installation WHERE id=$1",
    ] {
        tx.execute(statement, &[&id])
            .await
            .map_err(|_| unavailable())?;
    }
    tx.commit().await.map_err(|_| unavailable())?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_values_follow_frozen_go_type_and_byte_limits() {
        for (field, good, bad) in [
            (
                json!({"type":"string"}),
                json!("é".repeat(2048)),
                json!("é".repeat(2049)),
            ),
            (json!({"type":"number"}), json!(1.25), json!("1.25")),
            (json!({"type":"bool"}), json!(false), json!(0)),
            (
                json!({"type":"enum","options":["a","b"]}),
                json!("b"),
                json!("c"),
            ),
        ] {
            assert_eq!(normalize_config(&field, &good, "safe").unwrap(), good);
            assert!(normalize_config(&field, &bad, "safe").is_err());
            assert!(normalize_config(&field, &Value::Null, "safe").is_err());
        }
        assert!(normalize_config(&json!({"type":"future"}), &json!(1), "safe").is_err());
    }

    #[test]
    fn config_partial_merge_routes_secrets_without_plaintext_or_echo() {
        let manifest = json!({"config":{
            "text":{"type":"string"},"count":{"type":"number"},"token":{"type":"secret"}}});
        let update = config_update(
            &manifest,
            &json!({"text":"keep","token":"stale-secret","orphan":"stale"}),
            &json!({"values":{"count":2,"token":"synthetic-secret"}}),
        )
        .unwrap();
        assert_eq!(update.plain, json!({"text":"keep","count":2}));
        assert_eq!(
            update.secrets,
            vec![("token".to_owned(), "synthetic-secret".to_owned())]
        );
        for body in [
            json!([]),
            json!({"values":[]}),
            json!({"values":{"unknown":"synthetic-secret"}}),
            json!({"values":{"token":true}}),
            json!({"values":{"token":"é".repeat(4097)}}),
        ] {
            let failure = config_update(&manifest, &json!({}), &body).err().unwrap();
            assert!(!failure.1.to_string().contains("synthetic-secret"));
        }
        let maximum = config_update(
            &manifest,
            &json!({}),
            &json!({"values":{"token":"é".repeat(4096)}}),
        )
        .unwrap();
        assert_eq!(maximum.secrets[0].1.len(), 8192);
        assert_eq!(
            config_update(&manifest, &json!({"text":"keep"}), &json!({"values":null}))
                .unwrap()
                .plain,
            json!({"text":"keep"})
        );
        assert!(config_update(&json!({"config":[]}), &json!({}), &json!({})).is_err());
    }

    fn open_test_secret(key: &[u8; 32], sealed: &[u8]) -> Option<Vec<u8>> {
        if sealed.len() < 28 {
            return None;
        }
        let key = LessSafeKey::new(UnboundKey::new(&aead::AES_256_GCM, key).ok()?);
        let nonce = Nonce::try_assume_unique_for_key(&sealed[..12]).ok()?;
        let mut ciphertext = sealed[12..].to_vec();
        key.open_in_place(nonce, Aad::empty(), &mut ciphertext)
            .ok()
            .map(|p| p.to_vec())
    }

    #[test]
    fn config_secretbox_nonce_format_randomness_and_integrity() {
        let key = [9; 32];
        let first = seal_config_secret(&key, b"synthetic-value").unwrap();
        let second = seal_config_secret(&key, b"synthetic-value").unwrap();
        assert_ne!(first, second);
        assert_eq!(first.len(), 12 + 15 + 16);
        assert_eq!(open_test_secret(&key, &first).unwrap(), b"synthetic-value");
        assert!(open_test_secret(&[8; 32], &first).is_none());
        for index in [0, 12, first.len() - 1] {
            let mut changed = first.clone();
            changed[index] ^= 1;
            assert!(open_test_secret(&key, &changed).is_none());
        }
        assert!(open_test_secret(&key, &first[..27]).is_none());
        // NIST AES-256-GCM vector, independently compatible with Go cipher.NewGCM.
        let vector=hex::decode("000000000000000000000000cea7403d4d606b6e074ec5d3baf39d18d0d1c8a799996bf0265b98b5d48ab919").unwrap();
        assert_eq!(open_test_secret(&[0; 32], &vector).unwrap(), vec![0; 16]);
    }

    #[test]
    fn descriptive_projection_drops_secrets_and_unrecognized_config() {
        let manifest = json!({"name":"Synthetic","config":{
            "safe":{"type":"string","label":"Visible"},
            "secret":{"type":"secret","label":"Secret"},
            "future":{"type":"unknown","label":"Future"}},
            "contributes":{"hooks":[{"key":"run","name":"Run","transport":{"type":"http","url":"https://example.invalid/private"},"input_schema":{"secret":"not returned"},"triggers":["manual"]}]}});
        let out=descriptive_payload(&manifest,&json!({"safe":"visible","secret":"never-echo","future":"never-echo","orphan":"never-echo"})).unwrap();
        assert_eq!(out["config"], json!({"safe":"visible"}));
        assert_eq!(out["config_schema"].as_array().unwrap().len(), 3);
        assert_eq!(out["hooks"][0]["transport"], "http");
        assert!(!out.to_string().contains("never-echo"));
        assert!(!out.to_string().contains("example.invalid/private"));
        assert!(out["hooks"][0].get("input_schema").is_none());
    }
}
