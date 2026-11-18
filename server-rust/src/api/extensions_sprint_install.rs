//! Bounded install path for inert read/storage-only published packages. Unsupported
//! contributions and upgrades fail closed rather than creating half-installed state.
use super::{
    ApiError, ApiState, GoJson, Identity, error, extensions_sprint_lifecycle,
    extensions_sprint_token,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
};
use regex::Regex;
use serde::{
    Deserialize, Serialize,
    de::{self, MapAccess, Visitor},
    ser::SerializeMap,
};
use serde_json::{Value, json};
use std::{collections::HashSet, fmt};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    manifest_version: u32,
    key: String,
    name: String,
    #[serde(default)]
    description: String,
    version: String,
    author: Author,
    #[serde(default)]
    icon: String,
    scopes: Vec<String>,
    #[serde(default)]
    config: ConfigFields,
    contributes: Contributes,
}

#[derive(Default)]
struct ConfigFields(Vec<(String, ConfigField)>);

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ConfigField {
    #[serde(rename = "type")]
    kind: String,
    label: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    description: String,
    #[serde(default, skip_serializing_if = "is_false")]
    required: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    options: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    placeholder: String,
    #[serde(default, skip_serializing_if = "is_false")]
    multiline: bool,
}
fn is_false(value: &bool) -> bool {
    !*value
}

impl Serialize for ConfigFields {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut object = serializer.serialize_map(Some(self.0.len()))?;
        for (key, field) in &self.0 {
            object.serialize_entry(key, field)?;
        }
        object.end()
    }
}
impl<'de> Deserialize<'de> for ConfigFields {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FieldsVisitor;
        impl<'de> Visitor<'de> for FieldsVisitor {
            type Value = ConfigFields;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a config field object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut fields = Vec::new();
                let mut seen = HashSet::new();
                while let Some((key, value)) = map.next_entry::<String, ConfigField>()? {
                    if !seen.insert(key.clone()) {
                        return Err(de::Error::custom("duplicate config field"));
                    }
                    fields.push((key, value));
                }
                Ok(ConfigFields(fields))
            }
        }
        deserializer.deserialize_map(FieldsVisitor)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Author {
    name: String,
    #[serde(default)]
    url: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Contributes {
    #[serde(default)]
    surfaces: Vec<Value>,
    #[serde(default)]
    hooks: Vec<Value>,
    #[serde(default)]
    resources: Vec<Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviewRequest {
    version_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstallRequest {
    version_id: String,
    granted_scopes: Vec<String>,
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/workspaces/{id}/plugins/preview", post(preview))
        .route("/api/workspaces/{id}/plugins", post(install))
}
fn invalid(message: &'static str) -> ApiError {
    error(StatusCode::BAD_REQUEST, message)
}
fn incompatible(message: &'static str) -> ApiError {
    error(StatusCode::UNPROCESSABLE_ENTITY, message)
}
fn unavailable() -> ApiError {
    error(StatusCode::BAD_GATEWAY, "failed to load the Plugin package")
}
fn text(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= max
        && !value.contains(['\r', '\n', '\0'])
}
pub(super) fn ensure_inert(value: &Value) -> Result<(), ApiError> {
    parse_manifest(value).map(|_| ())
}
fn parse_manifest(value: &Value) -> Result<Manifest, ApiError> {
    let raw = serde_json::to_vec(value).map_err(|_| invalid("invalid Plugin manifest"))?;
    parse_manifest_bytes(&raw)
}
fn parse_manifest_bytes(raw: &[u8]) -> Result<Manifest, ApiError> {
    if raw.len() > (1 << 20) {
        return Err(invalid("Plugin manifest exceeds 1048576 bytes"));
    }
    let manifest: Manifest =
        serde_json::from_slice(raw).map_err(|_| invalid("invalid Plugin manifest"))?;
    if manifest.manifest_version != 1 {
        return Err(invalid("unsupported Plugin manifest version"));
    }
    if manifest.key.len() > 255
        || manifest.key.split('.').count() < 2
        || !manifest.key.split('.').all(|part| {
            !part.is_empty()
                && part.as_bytes()[0].is_ascii_lowercase()
                && part
                    .bytes()
                    .last()
                    .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                && part
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && !part.contains("--")
        })
    {
        return Err(invalid("invalid Plugin key"));
    }
    if !text(&manifest.name, 160) || !text(&manifest.author.name, 160) {
        return Err(invalid("invalid Plugin display name"));
    }
    if manifest.description.len() > 2000 || manifest.description.contains(['\r', '\0']) {
        return Err(invalid("invalid Plugin description"));
    }
    let semver=Regex::new(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$").expect("static regex");
    if manifest.version.len() > 64 || !semver.is_match(&manifest.version) {
        return Err(invalid("invalid Plugin version"));
    }
    if manifest.scopes.is_empty() || manifest.scopes.len() > 64 {
        return Err(invalid("invalid Plugin scope list"));
    }
    let mut seen = HashSet::new();
    for scope in &manifest.scopes {
        if !seen.insert(scope) {
            return Err(invalid("duplicate Plugin scope"));
        }
        if !matches!(
            scope.as_str(),
            "storage:user" | "storage:workspace" | "issues:read" | "comments:read"
        ) {
            return Err(incompatible(
                "this Plugin declares scopes this Rust slice cannot enforce yet",
            ));
        }
    }
    validate_config_fields(&manifest.config)?;
    // Config forms are host-rendered; executable contributions remain unsupported.
    if !manifest.icon.is_empty()
        || !manifest.author.url.is_empty()
        || !manifest.contributes.surfaces.is_empty()
        || !manifest.contributes.hooks.is_empty()
        || !manifest.contributes.resources.is_empty()
    {
        return Err(incompatible(
            "this Plugin declares contributions or config this Rust slice cannot install yet",
        ));
    }
    Ok(manifest)
}

fn validate_config_fields(config: &ConfigFields) -> Result<(), ApiError> {
    if config.0.len() > 32 {
        return Err(invalid("Plugin config exceeds 32 fields"));
    }
    let key_pattern = Regex::new(r"^[a-z][a-z0-9]*(?:[_-][a-z0-9]+)*$").expect("static regex");
    for (key, field) in &config.0 {
        if !key_pattern.is_match(key)
            || !text(&field.label, 160)
            || field.description.len() > 500
            || field.placeholder.len() > 160
        {
            return Err(invalid("invalid Plugin config field"));
        }
        match field.kind.as_str() {
            "string" | "number" | "bool" | "secret" => {
                if !field.options.is_empty() {
                    return Err(invalid("config options require an enum field"));
                }
            }
            "enum" => {
                if field.options.is_empty() || field.options.len() > 64 {
                    return Err(invalid("invalid Plugin enum options"));
                }
                let mut seen = HashSet::new();
                for option in &field.options {
                    if !text(option, 160) || !seen.insert(option) {
                        return Err(invalid("invalid or duplicate Plugin enum option"));
                    }
                }
            }
            _ => return Err(invalid("unsupported Plugin config type")),
        }
        if field.multiline && field.kind != "string" {
            return Err(invalid("multiline requires a string field"));
        }
    }
    Ok(())
}
pub(super) struct PublishedManifest {
    pub key: String,
    pub name: String,
    pub version: String,
    pub value: Value,
    pub canonical: Vec<u8>,
}

pub(super) fn publish_manifest(raw: &[u8]) -> Result<PublishedManifest, ApiError> {
    // Decode straight into the strict struct before Value can erase duplicate fields.
    let manifest = parse_manifest_bytes(raw)?;
    let wire: Value =
        serde_json::from_slice(raw).map_err(|_| invalid("invalid Plugin manifest"))?;
    if wire.get("config").is_some_and(|config| !config.is_object()) {
        return Err(invalid("Plugin config must be an object"));
    }
    #[derive(Serialize)]
    struct CanonicalAuthor<'a> {
        name: &'a str,
    }
    #[derive(Serialize)]
    struct Canonical<'a> {
        manifest_version: u32,
        key: &'a str,
        name: &'a str,
        #[serde(skip_serializing_if = "str::is_empty")]
        description: &'a str,
        version: &'a str,
        author: CanonicalAuthor<'a>,
        scopes: &'a [String],
        config: &'a ConfigFields,
        contributes: serde_json::Map<String, Value>,
    }
    // Match frozen Go's typed manifest field and config declaration order,
    // plus encoding/json escaping. No executable artifact is accepted here.
    let canonical = serde_json::to_string(&Canonical {
        manifest_version: 1,
        key: &manifest.key,
        name: &manifest.name,
        description: &manifest.description,
        version: &manifest.version,
        author: CanonicalAuthor {
            name: &manifest.author.name,
        },
        scopes: &manifest.scopes,
        config: &manifest.config,
        contributes: serde_json::Map::new(),
    })
    .map_err(|_| invalid("invalid Plugin manifest"))?
    .replace('<', "\\u003c")
    .replace('>', "\\u003e")
    .replace('&', "\\u0026")
    .replace('\u{2028}', "\\u2028")
    .replace('\u{2029}', "\\u2029")
    .into_bytes();
    let value =
        serde_json::from_slice(&canonical).map_err(|_| invalid("invalid Plugin manifest"))?;
    Ok(PublishedManifest {
        key: manifest.key,
        name: manifest.name,
        version: manifest.version,
        value,
        canonical,
    })
}

async fn version(db: &impl GenericClient, ws: Uuid, id: Uuid) -> Result<Row, ApiError> {
    db.query_opt("SELECT v.id,v.version,v.manifest,v.digest,p.plugin_key FROM plugin_package_version v JOIN plugin_package p ON p.id=v.package_id AND p.workspace_id=v.workspace_id WHERE v.id=$1 AND v.workspace_id=$2", &[&id,&ws]).await
        .map_err(|_|unavailable())?.ok_or_else(||error(StatusCode::NOT_FOUND,"Plugin package version not found"))
}
fn validated(row: &Row) -> Result<Manifest, ApiError> {
    let manifest = parse_manifest(&row.get::<_, Value>("manifest"))?;
    if manifest.key != row.get::<_, String>("plugin_key")
        || manifest.version != row.get::<_, String>("version")
    {
        return Err(invalid(
            "published Plugin manifest does not match its package version",
        ));
    }
    Ok(manifest)
}
async fn preview(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<super::Actor>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let db = state.pool.get().await.map_err(|_| unavailable())?;
    let ws =
        extensions_sprint_token::admin(&**db, user, &actor, &raw, state.extensions_sprint_enabled)
            .await?;
    let req: PreviewRequest =
        serde_json::from_slice(&bytes).map_err(|_| invalid("invalid request body"))?;
    let id = Uuid::parse_str(&req.version_id)
        .map_err(|_| error(StatusCode::NOT_FOUND, "Plugin package version not found"))?;
    let row = version(&**db, ws, id).await?;
    let manifest = validated(&row)?;
    let existing=db.query_opt("SELECT version,granted_scopes FROM plugin_installation WHERE workspace_id=$1 AND plugin_key=$2", &[&ws,&manifest.key]).await.map_err(|_|unavailable())?;
    let snapshot: Value = row.get("manifest");
    let (config_schema, _) = extensions_sprint_lifecycle::project_config(&snapshot, &json!({}))?;
    let mut response = json!({"manifest":snapshot,"scopes":manifest.scopes,
        "config_schema":config_schema,"version_id":id,"version":row.get::<_,String>("version"),
        "digest":row.get::<_,String>("digest"),"installed":existing.is_some()});
    if let Some(existing) = existing {
        response["installed_version"] = json!(existing.get::<_, String>("version"));
        let have: Value = existing.get("granted_scopes");
        let added: Vec<String> = manifest
            .scopes
            .iter()
            .filter(|s| !have.as_array().is_some_and(|a| a.contains(&json!(s))))
            .cloned()
            .collect();
        if !added.is_empty() {
            response["added_scopes"] = json!(added);
        }
    }
    Ok(GoJson(response))
}
async fn install(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<super::Actor>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let mut db = state.pool.get().await.map_err(|_| unavailable())?;
    let ws =
        extensions_sprint_token::admin(&**db, user, &actor, &raw, state.extensions_sprint_enabled)
            .await?;
    let req: InstallRequest =
        serde_json::from_slice(&bytes).map_err(|_| invalid("invalid request body"))?;
    let id = Uuid::parse_str(&req.version_id)
        .map_err(|_| error(StatusCode::NOT_FOUND, "Plugin package version not found"))?;
    let tx = db.transaction().await.map_err(|_| unavailable())?;
    if tx
        .query_opt("SELECT id FROM workspace WHERE id=$1 FOR KEY SHARE", &[&ws])
        .await
        .map_err(|_| unavailable())?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "workspace not found"));
    }
    // Same per-package lock as frozen Go publish/delete, held through commit.
    let provisional = version(&*tx, ws, id).await?;
    let key: String = provisional.get("plugin_key");
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1::text,0))",
        &[&format!("{ws}:{key}")],
    )
    .await
    .map_err(|_| unavailable())?;
    let row = version(&*tx, ws, id).await?;
    let manifest = validated(&row)?;
    let wanted: HashSet<_> = manifest.scopes.iter().collect();
    let granted: HashSet<_> = req.granted_scopes.iter().collect();
    if wanted != granted || wanted.len() != req.granted_scopes.len() {
        return Err(error(
            StatusCode::CONFLICT,
            "granted_scopes must match the manifest scopes exactly",
        ));
    }
    let scopes = json!(manifest.scopes);
    let row=tx.query_one("INSERT INTO plugin_installation(workspace_id,plugin_key,package_version_id,version,manifest,granted_scopes,installed_by) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id,plugin_key,package_version_id,version,manifest,granted_scopes,enabled,config,created_at,updated_at",
        &[&ws,&manifest.key,&id,&row.get::<_,String>("version"),&row.get::<_,Value>("manifest"),&scopes,&user]).await.map_err(|e| {
            if e.code()==Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
                error(StatusCode::CONFLICT,"this Plugin is already installed in this workspace")
            } else {error(StatusCode::BAD_GATEWAY,"failed to install the Plugin")}
        })?;
    let payload = extensions_sprint_lifecycle::payload(&row, Vec::new(), &[])?;
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::BAD_GATEWAY, "failed to install the Plugin"))?;
    Ok((StatusCode::CREATED, GoJson(payload)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inert_publish_canonical_is_typed_and_html_safe() {
        let manifest = publish_manifest(br#"{"manifest_version":1,"key":"example.safe","name":"<Safe & sound>","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["issues:read"],"contributes":{"hooks":[],"surfaces":[],"resources":[]}}"#).unwrap();
        assert_eq!(String::from_utf8(manifest.canonical).unwrap(), r#"{"manifest_version":1,"key":"example.safe","name":"<Safe & sound>","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["issues:read"],"config":{},"contributes":{}}"#.replace('<', &format!("{}u003c", char::from(92))).replace('>', &format!("{}u003e", char::from(92))).replace('&', &format!("{}u0026", char::from(92))));
    }

    #[test]
    fn config_declarations_are_strict_bounded_and_canonical() {
        let prefix = r#"{"manifest_version":1,"key":"example.safe","name":"Safe","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["storage:workspace"],"contributes":{},"config":"#;
        let config = r#"{"z_token":{"type":"secret","label":"Token","required":true},"a_text":{"type":"string","label":"Text","multiline":true},"choice":{"type":"enum","label":"Choice","options":["one","two"]}}"#;
        let manifest = publish_manifest(format!("{prefix}{config}}}").as_bytes()).unwrap();
        let canonical = String::from_utf8(manifest.canonical).unwrap();
        assert!(canonical.contains(&format!("\"config\":{config}")));
        for bad in [
            "null".to_owned(), "[]".into(),
            r#"{"token":{"type":"secret","label":"Token"},"token":{"type":"secret","label":"Again"}}"#.into(),
            r#"{"token":{"type":"secret","type":"string","label":"Token"}}"#.into(),
            r#"{"bad__key":{"type":"string","label":"Text"}}"#.into(),
            r#"{"choice":{"type":"enum","label":"Choice"}}"#.into(),
            format!(r#"{{"token":{{"type":"string","label":"{}"}}}}"#, "x".repeat(161)),
            serde_json::to_string(&(0..33).map(|i| (format!("field{i}"), json!({"type":"bool","label":"Flag"}))).collect::<serde_json::Map<_,_>>()).unwrap(),
        ] {
            assert!(publish_manifest(format!("{prefix}{bad}}}").as_bytes()).is_err(), "accepted {bad}");
        }
    }

    #[test]
    fn fail_closed_manifest_and_exact_scope_set() {
        let valid = json!({"manifest_version":1,"key":"example.synthetic","name":"Synthetic",
            "version":"1.0.0","author":{"name":"Synthetic"},
            "scopes":["storage:workspace"],"contributes":{}});
        assert!(parse_manifest(&valid).is_ok());
        for changed in [
            json!({"scopes":["storage:workspace","storage:workspace"]}),
            json!({"scopes":["issues:write"]}),
            json!({"contributes":{"hooks":[{"key":"run"}]}}),
            json!({"config":{"fields":[{"key":"secret","type":"secret"}]}}),
            json!({"config":{"token":{"type":"secret","label":"Token","options":["x"]}}}),
            json!({"config":{"token":{"type":"number","label":"Token","multiline":true}}}),
            json!({"config":{"token":{"type":"enum","label":"Token","options":["x","x"]}}}),
            json!({"config":{"token":{"type":"string","label":"Token","unknown":true}}}),
            json!({"author":{"name":"Synthetic","url":"https://example.invalid"}}),
            json!({"key":"-invalid.example"}),
        ] {
            let mut bad = valid.clone();
            for (key, value) in changed.as_object().unwrap() {
                bad[key] = value.clone();
            }
            assert!(parse_manifest(&bad).is_err(), "{bad}");
        }
    }
}
