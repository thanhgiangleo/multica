//! Scoped daemon bundle resolution. Only enabled, requested workspace skills are read.
use super::{
    ApiError, ApiState, GoJson,
    daemon::{self, DaemonIdentity},
    error, runtime_chat_daemon,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use tokio_postgres::IsolationLevel;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route(
        "/api/daemon/runtimes/{runtimeId}/tasks/{taskId}/skill-bundles/resolve",
        post(resolve),
    )
}

#[derive(Clone, Debug, Serialize)]
pub struct BundleFile {
    pub path: String,
    pub content: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub sha256: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub size_bytes: usize,
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Serialize)]
pub struct Bundle {
    pub id: String,
    pub source: String,
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub hash: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub size_bytes: usize,
    pub content: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<BundleFile>,
}

impl Bundle {
    /// The manifest's file list in ITS order (path-sorted) plus the count, which
    /// `with_manifest` folds into the hash and then discards. A claim's skill
    /// refs carry both, so they have to be recoverable after the fold.
    pub fn manifest_refs(&self) -> (Vec<BundleFile>, usize) {
        let mut files = self.files.clone();
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let count = files.len();
        (files, count)
    }

    /// Frozen Go skillbundle.BuildManifest v1: UTF-8 byte lengths, never rune counts.
    pub fn with_manifest(mut self) -> Self {
        fn part(hash: &mut Sha256, value: &str) {
            hash.update(format!("{}:", value.len()).as_bytes());
            hash.update(value.as_bytes());
            hash.update(b"\n");
        }
        let mut hash = Sha256::new();
        for value in [
            "v1",
            &self.source,
            &self.id,
            &self.name,
            &self.description,
            &self.content,
        ] {
            part(&mut hash, value);
        }
        self.size_bytes = self.content.len();
        // Hash order is independent of file input order; preserve response file order.
        let mut order: Vec<usize> = (0..self.files.len()).collect();
        order.sort_by(|a, b| self.files[*a].path.cmp(&self.files[*b].path));
        for index in order {
            let file = &mut self.files[index];
            file.sha256 = format!(
                "sha256:{}",
                hex::encode(Sha256::digest(file.content.as_bytes()))
            );
            file.size_bytes = file.content.len();
            part(&mut hash, &file.path);
            part(&mut hash, &file.sha256);
            part(&mut hash, &file.content);
            self.size_bytes += file.size_bytes;
        }
        self.hash = format!("sha256:{}", hex::encode(hash.finalize()));
        self
    }
}

fn supporting(path: &str, content: &str) -> BundleFile {
    BundleFile {
        path: path.into(),
        content: content.into(),
        sha256: String::new(),
        size_bytes: 0,
    }
}

pub(super) fn builtin_skills() -> Vec<Bundle> {
    const PLATFORM: &str = include_str!("runtime_skills_sprint_builtin/multica-platform/SKILL.md");
    const ONBOARDING: &str =
        include_str!("runtime_skills_sprint_builtin/multica-onboarding/SKILL.md");
    const LEGACY: &str =
        include_str!("runtime_skills_sprint_builtin/multica-working-on-issues/SKILL.md");
    let mut files = vec![
        supporting(
            "references/agents.md",
            include_str!("runtime_skills_sprint_builtin/multica-platform/references/agents.md"),
        ),
        supporting(
            "references/autopilots.md",
            include_str!("runtime_skills_sprint_builtin/multica-platform/references/autopilots.md"),
        ),
        supporting(
            "references/issues.md",
            include_str!("runtime_skills_sprint_builtin/multica-platform/references/issues.md"),
        ),
        supporting(
            "references/mentions.md",
            include_str!("runtime_skills_sprint_builtin/multica-platform/references/mentions.md"),
        ),
        supporting(
            "references/projects.md",
            include_str!("runtime_skills_sprint_builtin/multica-platform/references/projects.md"),
        ),
        supporting(
            "references/runtimes.md",
            include_str!("runtime_skills_sprint_builtin/multica-platform/references/runtimes.md"),
        ),
        supporting(
            "references/skill-import.md",
            include_str!(
                "runtime_skills_sprint_builtin/multica-platform/references/skill-import.md"
            ),
        ),
        supporting(
            "references/squads.md",
            include_str!("runtime_skills_sprint_builtin/multica-platform/references/squads.md"),
        ),
    ];
    files.sort_by(|a, b| a.path.cmp(&b.path));
    [
        ("multica-onboarding", ONBOARDING, Vec::new()),
        ("multica-platform", PLATFORM, files),
        ("multica-working-on-issues", LEGACY, Vec::new()),
    ]
    .into_iter()
    .map(|(name, content, files)| Bundle {
        id: format!("builtin:{name}"),
        source: "builtin".into(),
        name: name.into(),
        description: String::new(),
        content: content.into(),
        files,
        hash: String::new(),
        size_bytes: 0,
    })
    .collect()
}

/// Built-in skills visible to one agent. A skill listed in the system-key map
/// is scoped to that built-in agent (only `mika` has one today); an unlisted
/// skill is universal. The legacy `multica-working-on-issues` stub survives
/// only for runtimes that still redirect unknown platform-skill names onto it.
pub(super) fn builtin_skills_for(system_key: &str, legacy_redirects: bool) -> Vec<Bundle> {
    const MIKA_SYSTEM_KEY: &str = "mika";
    builtin_skills()
        .into_iter()
        .filter(|bundle| match bundle.name.as_str() {
            "multica-onboarding" => system_key == MIKA_SYSTEM_KEY,
            "multica-working-on-issues" => legacy_redirects,
            _ => true,
        })
        .collect()
}

#[derive(Default)]
struct ResolveRequest {
    skills: Option<Vec<RequestedRef>>,
}

#[derive(Default)]
struct RequestedRef {
    id: Option<String>,
    source: Option<String>,
    hash: Option<String>,
}

// encoding/json.Decode accepts null, case-insensitive fields and trailing JSON.
// Preserve wire order so the last spelling of a duplicate field wins.
impl<'de> Deserialize<'de> for ResolveRequest {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = ResolveRequest;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(ResolveRequest::default())
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut value = ResolveRequest::default();
                while let Some(key) = map.next_key::<String>()? {
                    if key.eq_ignore_ascii_case("skills") {
                        value.skills = map.next_value()?;
                    } else {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }
                Ok(value)
            }
        }
        decoder.deserialize_any(Visitor)
    }
}

impl<'de> Deserialize<'de> for RequestedRef {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = RequestedRef;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(RequestedRef::default())
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut value = RequestedRef::default();
                while let Some(key) = map.next_key::<String>()? {
                    let target = if key.eq_ignore_ascii_case("id") {
                        Some(&mut value.id)
                    } else if key.eq_ignore_ascii_case("source") {
                        Some(&mut value.source)
                    } else if key.eq_ignore_ascii_case("hash") {
                        Some(&mut value.hash)
                    } else {
                        None
                    };
                    if let Some(target) = target {
                        // A JSON null does not clear a Go string field.
                        if let Some(text) = map.next_value::<Option<String>>()? {
                            *target = Some(text);
                        }
                    } else {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }
                Ok(value)
            }
        }
        decoder.deserialize_any(Visitor)
    }
}

fn failed(_: tokio_postgres::Error) -> ApiError {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to load skill bundles",
    )
}

fn runtime_binding(identity: &DaemonIdentity, daemon: Option<&str>, owner: Option<Uuid>) -> bool {
    match identity {
        DaemonIdentity::User(user) => owner == Some(*user),
        DaemonIdentity::Bound {
            daemon: bound,
            owner: bound_owner,
            ..
        } => {
            !bound.trim().is_empty()
                && daemon == Some(bound.as_str())
                && (bound.starts_with("qoder-managed-")
                    || bound_owner.is_some() && *bound_owner == owner)
        }
    }
}

async fn resolve(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path((runtime_raw, task_raw)): Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let runtime = Uuid::parse_str(&runtime_raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid runtime_id"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtime"))?;
    // One consistent snapshot: never hash a skill body together with files from a later edit.
    let tx = db
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(failed)?;
    let rt = tx
        .query_opt(
            "SELECT workspace_id,daemon_id,owner_id FROM agent_runtime WHERE id=$1",
            &[&runtime],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtime"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?;
    let ws: Uuid = rt.get("workspace_id");
    daemon::workspace_access(&*tx, &identity, &ws.to_string()).await?;
    if !runtime_binding(
        &identity,
        rt.get::<_, Option<String>>("daemon_id").as_deref(),
        rt.get("owner_id"),
    ) {
        return Err(error(StatusCode::NOT_FOUND, "runtime not found"));
    }
    let task_id = Uuid::parse_str(&task_raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task_id"))?;
    let (task, task_ws, _) = runtime_chat_daemon::task_access(&*tx, &identity, task_id).await?;
    if task_ws != ws
        || task.get::<_, Option<Uuid>>("runtime_id") != Some(runtime)
        || runtime_raw != runtime.to_string()
    {
        return Err(error(StatusCode::NOT_FOUND, "task not found"));
    }
    if !matches!(
        task.get::<_, String>("status").as_str(),
        "dispatched" | "waiting_local_directory"
    ) {
        return Err(error(StatusCode::CONFLICT, "task is not preparing"));
    }
    let mut decoder = serde_json::Deserializer::from_slice(&body);
    let req = ResolveRequest::deserialize(&mut decoder)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let refs = req.skills.unwrap_or_default();
    if refs.iter().any(|r| {
        r.id.as_deref().unwrap_or_default().is_empty()
            || r.source.as_deref().unwrap_or_default().is_empty()
            || r.hash.as_deref().unwrap_or_default().is_empty()
    }) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid skill ref"));
    }
    let ids: Vec<Uuid> = refs
        .iter()
        .filter(|r| r.source.as_deref() == Some("workspace"))
        .filter_map(|r| Uuid::parse_str(r.id.as_deref().unwrap_or_default()).ok())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let agent: Uuid = task.get("agent_id");
    let mut allowed: HashMap<(String, String), Bundle> = HashMap::new();
    if !ids.is_empty() {
        let rows = tx.query("SELECT s.id,s.name,s.description,s.content FROM skill s JOIN agent_skill ask ON ask.skill_id=s.id WHERE ask.agent_id=$1 AND ask.enabled=TRUE AND s.id=ANY($2::uuid[]) AND s.workspace_id=$3 ORDER BY s.name ASC", &[&agent,&ids,&ws]).await.map_err(failed)?;
        let loaded_ids: Vec<Uuid> = rows.iter().map(|r| r.get("id")).collect();
        let file_rows = tx.query("SELECT skill_id,path,content FROM skill_file WHERE skill_id=ANY($1::uuid[]) ORDER BY skill_id,path", &[&loaded_ids]).await.map_err(failed)?;
        let mut files: HashMap<Uuid, Vec<BundleFile>> = HashMap::new();
        for row in file_rows {
            files
                .entry(row.get("skill_id"))
                .or_default()
                .push(supporting(row.get("path"), row.get("content")));
        }
        for row in rows {
            let id: Uuid = row.get("id");
            let bundle = Bundle {
                id: id.to_string(),
                source: "workspace".into(),
                name: row.get("name"),
                description: row.get("description"),
                content: row.get("content"),
                files: files.remove(&id).unwrap_or_default(),
                hash: String::new(),
                size_bytes: 0,
            }
            .with_manifest();
            allowed.insert((bundle.source.clone(), bundle.id.clone()), bundle);
        }
    }
    if refs.iter().any(|r| r.source.as_deref() == Some("builtin")) {
        let wanted: HashSet<&str> = refs
            .iter()
            .filter(|r| r.source.as_deref() == Some("builtin"))
            .filter_map(|r| r.id.as_deref())
            .collect();
        for bundle in builtin_skills()
            .into_iter()
            .filter(|b| wanted.contains(b.id.as_str()))
        {
            let bundle = bundle.with_manifest();
            allowed.insert((bundle.source.clone(), bundle.id.clone()), bundle);
        }
    }
    let mut bundles = Vec::with_capacity(refs.len());
    for reference in refs {
        let key = (
            reference.source.unwrap_or_default(),
            reference.id.unwrap_or_default(),
        );
        let bundle = allowed
            .get(&key)
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "skill bundle not found"))?;
        // Frozen Go has no plugin producer; unknown/plugin sources stay not-found.
        bundles.push(bundle.clone());
    }
    tx.commit().await.map_err(failed)?;
    Ok(GoJson(json!({"bundles":bundles})).into_response())
}

/// A skill as a task claim hands it over: the definition plus supporting files.
#[derive(Clone, Debug, Serialize)]
pub struct AgentSkillData {
    pub id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub source: String,
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub hash: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub size_bytes: usize,
    pub content: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<AgentSkillFileData>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AgentSkillFileData {
    pub path: String,
    pub content: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub sha256: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub size_bytes: usize,
}

/// The ref form: the manifest only, so a daemon can ask for the bundle later.
#[derive(Clone, Debug, Serialize)]
pub struct AgentSkillRefData {
    pub id: String,
    pub source: String,
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub hash: String,
    pub size_bytes: usize,
    pub file_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<AgentSkillFileRefData>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AgentSkillFileRefData {
    pub path: String,
    pub sha256: String,
    pub size_bytes: usize,
}

const SOURCE_BUILTIN: &str = "builtin";
const SOURCE_WORKSPACE: &str = "workspace";

/// Go `BuiltinSkillID`: a builtin has no database row, so its name is its id.
pub(super) fn builtin_skill_id(name: &str) -> String {
    format!("{SOURCE_BUILTIN}:{name}")
}

/// Go `BuildAgentSkillBundles`: every skill gets a manifest, and the same
/// manifest is compressed into a ref. Both forms come out of one fold so the
/// hash a ref advertises is always the hash of the bundle beside it.
pub(super) fn build_agent_skill_bundles(
    skills: Vec<AgentSkillData>,
) -> (Vec<AgentSkillData>, Vec<AgentSkillRefData>) {
    let mut bundles = Vec::with_capacity(skills.len());
    let mut refs = Vec::with_capacity(skills.len());
    for mut skill in skills {
        // An unnamed source is inferred: an id means it came from the workspace
        // library, no id means it is a builtin.
        if skill.source.is_empty() {
            skill.source = if skill.id.is_empty() {
                SOURCE_BUILTIN.into()
            } else {
                SOURCE_WORKSPACE.into()
            };
        }
        if skill.id.is_empty() && skill.source == SOURCE_BUILTIN {
            skill.id = builtin_skill_id(&skill.name);
        }
        let bundle = Bundle {
            id: skill.id.clone(),
            source: skill.source.clone(),
            name: skill.name.clone(),
            description: skill.description.clone(),
            content: skill.content.clone(),
            files: skill
                .files
                .iter()
                .map(|file| supporting(&file.path, &file.content))
                .collect(),
            hash: String::new(),
            size_bytes: 0,
        }
        .with_manifest();
        let (ref_files, file_count) = bundle.manifest_refs();
        // Back-fill each file with the hash and size the manifest computed for
        // it, matched by path because the manifest sorts.
        for file in &mut skill.files {
            if let Some(stamp) = ref_files.iter().find(|stamp| stamp.path == file.path) {
                file.sha256 = stamp.sha256.clone();
                file.size_bytes = stamp.size_bytes;
            }
        }
        refs.push(AgentSkillRefData {
            id: skill.id.clone(),
            source: skill.source.clone(),
            name: skill.name.clone(),
            description: skill.description.clone(),
            hash: bundle.hash.clone(),
            size_bytes: bundle.size_bytes,
            file_count,
            files: ref_files
                .into_iter()
                .map(|file| AgentSkillFileRefData {
                    path: file.path,
                    sha256: file.sha256,
                    size_bytes: file.size_bytes,
                })
                .collect(),
        });
        skill.hash = bundle.hash;
        skill.size_bytes = bundle.size_bytes;
        skill.files = bundle
            .files
            .iter()
            .map(|file| AgentSkillFileData {
                path: file.path.clone(),
                content: file.content.clone(),
                sha256: file.sha256.clone(),
                size_bytes: file.size_bytes,
            })
            .collect();
        skill.source = bundle.source;
        skill.id = bundle.id;
        bundles.push(skill);
    }
    (bundles, refs)
}

/// Go `LoadAgentSkills` on top of `agent_skills`/`skill_files`: one batched
/// file read, assembled in the order the skills were loaded so the wire order
/// matches Go's.
pub(super) async fn load_agent_skills(
    db: &impl deadpool_postgres::GenericClient,
    agent: Uuid,
) -> Result<Vec<AgentSkillData>, String> {
    let rows = super::daemon_claim_load::agent_skills(db, agent).await?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<Uuid> = rows.iter().map(|row| row.id).collect();
    let files = super::daemon_claim_load::skill_files(db, &ids).await?;
    let mut by_skill: HashMap<Uuid, Vec<AgentSkillFileData>> = HashMap::new();
    for file in files {
        by_skill
            .entry(file.skill_id)
            .or_default()
            .push(AgentSkillFileData {
                path: file.path,
                content: file.content,
                sha256: String::new(),
                size_bytes: 0,
            });
    }
    Ok(rows
        .into_iter()
        .map(|row| AgentSkillData {
            id: row.id.to_string(),
            // Go's skillsWithFiles leaves Source empty for workspace skills,
            // and the field is omitempty on the wire; BuildAgentSkillBundles
            // infers the workspace source from a non-empty id instead.
            source: String::new(),
            name: row.name,
            description: row.description,
            hash: String::new(),
            size_bytes: 0,
            content: row.content,
            files: by_skill.remove(&row.id).unwrap_or_default(),
        })
        .collect())
}

/// The built-in skills as claim payloads, in the order `builtin_skills`
/// returns them (which is the order Go's loader produces).
/// Go `BuiltinSkills` as claim payloads, in the order Go's loader produces
/// them. Built-ins carry neither an id nor a source here: Go's embedded-file
/// loader fills in only name/content/files, so the plain skills list omits
/// both fields on the wire. `build_agent_skill_bundles` is what derives the
/// `builtin:<name>` id and the `builtin` source for the bundle path.
pub(super) fn builtin_skill_data(system_key: &str, legacy_redirects: bool) -> Vec<AgentSkillData> {
    builtin_skills_for(system_key, legacy_redirects)
        .into_iter()
        .map(|bundle| AgentSkillData {
            id: String::new(),
            source: String::new(),
            name: bundle.name,
            description: bundle.description,
            hash: String::new(),
            size_bytes: 0,
            content: bundle.content,
            files: bundle
                .files
                .into_iter()
                .map(|file| AgentSkillFileData {
                    path: file.path,
                    content: file.content,
                    sha256: String::new(),
                    size_bytes: 0,
                })
                .collect(),
        })
        .collect()
}

/// Go `LoadAgentSkillBundles`: the agent's own skills plus its builtins, each
/// rendered as a bundle and a ref.
pub(super) async fn load_agent_skill_bundles(
    db: &impl deadpool_postgres::GenericClient,
    agent: Uuid,
    system_key: &str,
    legacy_redirects: bool,
) -> Result<(Vec<AgentSkillData>, Vec<AgentSkillRefData>), String> {
    let mut skills = load_agent_skills(db, agent).await?;
    skills.extend(builtin_skill_data(system_key, legacy_redirects));
    Ok(build_agent_skill_bundles(skills))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_skills_sprint_bundle_manifest_matches_frozen_go_vectors() {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/runtime_skills_sprint_vectors.json"
        ))
        .unwrap();
        for vector in vectors.as_array().unwrap() {
            let sk = &vector["skill"];
            let files = sk["Files"]
                .as_array()
                .map(|files| {
                    files
                        .iter()
                        .map(|f| {
                            supporting(f["Path"].as_str().unwrap(), f["Content"].as_str().unwrap())
                        })
                        .collect()
                })
                .unwrap_or_default();
            let bundle = Bundle {
                id: sk["ID"].as_str().unwrap().into(),
                source: sk["Source"].as_str().unwrap().into(),
                name: sk["Name"].as_str().unwrap().into(),
                description: sk["Description"].as_str().unwrap().into(),
                content: sk["Content"].as_str().unwrap().into(),
                files,
                hash: String::new(),
                size_bytes: 0,
            }
            .with_manifest();
            assert_eq!(bundle.hash, vector["manifest"]["Hash"], "{}", bundle.name);
            assert_eq!(json!(bundle.size_bytes), vector["manifest"]["SizeBytes"]);
            assert_eq!(json!(bundle.files.len()), vector["manifest"]["FileCount"]);
            for file in &bundle.files {
                let expected = vector["manifest"]["Files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|f| f["Path"] == file.path)
                    .unwrap();
                assert_eq!(file.sha256, expected["SHA256"]);
                assert_eq!(json!(file.size_bytes), expected["SizeBytes"]);
            }
            let mut reversed = bundle.clone();
            reversed.files.reverse();
            assert_eq!(bundle.hash, reversed.with_manifest().hash);
            let mut changed = bundle.clone();
            changed.content.push('x');
            assert_ne!(bundle.hash, changed.with_manifest().hash);
            if bundle.source == "builtin" {
                let embedded = builtin_skills()
                    .into_iter()
                    .find(|b| b.id == bundle.id)
                    .unwrap()
                    .with_manifest();
                assert_eq!(embedded.hash, bundle.hash);
            }
        }
    }

    #[test]
    fn runtime_skills_sprint_bundle_decoder_matches_go_structs() {
        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"Skills":[{"ID":"old","id":"new","id":null,"Source":"workspace","HASH":"stale","unknown":[1]}]} {"trailing":true}"#,
        );
        let req = ResolveRequest::deserialize(&mut decoder).unwrap();
        let refs = req.skills.unwrap();
        assert_eq!(refs[0].id.as_deref(), Some("new"));
        assert_eq!(refs[0].source.as_deref(), Some("workspace"));
        assert_eq!(refs[0].hash.as_deref(), Some("stale"));
        for raw in [
            "null",
            "{}",
            r#"{"skills":null}"#,
            r#"{"Skills":[{}],"skills":null}"#,
        ] {
            assert!(
                serde_json::from_str::<ResolveRequest>(raw)
                    .unwrap()
                    .skills
                    .is_none()
            );
        }
        for raw in ["[]", "42", r#"{"skills":42}"#, r#"{"skills":[{"id":42}]}"#] {
            assert!(
                serde_json::from_str::<ResolveRequest>(raw).is_err(),
                "{raw}"
            );
        }
    }
}
