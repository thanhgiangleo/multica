//! Workspace-private published package catalog and transactional deletion.
//! Package versions remain immutable; installed versions cannot be deleted.
use super::{
    ApiError, ApiState, GoJson, Identity, error, extensions_sprint_install,
    extensions_sprint_token, timestamp,
};
use axum::{
    Router,
    extract::{DefaultBodyLimit, Extension, Multipart, Path, State},
    http::StatusCode,
    routing::get,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    io::{Cursor, Read},
};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/workspaces/{id}/plugins/packages",
            get(list).post(publish),
        )
        .route(
            "/api/workspaces/{id}/plugins/packages/{package_id}",
            axum::routing::delete(remove),
        )
        .layer(DefaultBodyLimit::max(MAX_ARCHIVE + 64 * 1024))
}

fn failed() -> ApiError {
    error(
        StatusCode::BAD_GATEWAY,
        "failed to access published Plugins",
    )
}

async fn package(db: &impl GenericClient, ws: Uuid, id: Uuid) -> Result<Row, ApiError> {
    db.query_opt(
        "SELECT id,plugin_key,name,created_at FROM plugin_package WHERE id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .map_err(|_| failed())?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "plugin package not found"))
}

async fn summary(db: &impl GenericClient, ws: Uuid, package: &Row) -> Result<Value, ApiError> {
    let id: Uuid = package.get("id");
    let key: String = package.get("plugin_key");
    let installed = db.query_opt(
        "SELECT package_version_id FROM plugin_installation WHERE workspace_id=$1 AND plugin_key=$2",
        &[&ws, &key],
    ).await.map_err(|_| failed())?.map(|row| row.get::<_, Uuid>(0));
    let versions = db.query(
        "SELECT id,version,digest,size_bytes,created_at FROM plugin_package_version WHERE package_id=$1 AND workspace_id=$2 ORDER BY created_at DESC, id DESC",
        &[&id, &ws],
    ).await.map_err(|_| failed())?;
    let versions: Vec<Value> = versions.iter().map(|row| {
        let version_id: Uuid = row.get("id");
        json!({"id":version_id,"version":row.get::<_,String>("version"),
            "digest":row.get::<_,String>("digest"),"size_bytes":row.get::<_,i64>("size_bytes"),
            "published_at":timestamp(row.get("created_at")),"installed":installed==Some(version_id)})
    }).collect();
    Ok(
        json!({"id":id,"plugin_key":key,"name":package.get::<_, String>("name"),
        "versions":versions,"created_at":timestamp(package.get("created_at"))}),
    )
}

const MAX_ARCHIVE: usize = 2 << 20;
const MAX_FILE: usize = 1 << 20;

fn invalid_bundle() -> ApiError {
    error(StatusCode::BAD_REQUEST, "plugin package is invalid")
}

fn zip_entry_count(archive: &[u8]) -> Result<usize, ApiError> {
    // zip 2.x stores central-directory entries in an IndexMap keyed by name.
    // Duplicate names collapse before ZipArchive::len(), so validate raw count
    // and directory bounds before allowing the library to resolve file names.
    let end = archive
        .windows(4)
        .enumerate()
        .rev()
        .find_map(|(offset, signature)| {
            if signature == b"PK\x05\x06"
                && archive.get(offset + 20..offset + 22).is_some_and(|bytes| {
                    offset + 22 + u16::from_le_bytes([bytes[0], bytes[1]]) as usize == archive.len()
                })
            {
                Some(offset)
            } else {
                None
            }
        })
        .ok_or_else(invalid_bundle)?;
    let u16_at = |offset: usize| -> Result<usize, ApiError> {
        let bytes: [u8; 2] = archive
            .get(offset..offset + 2)
            .ok_or_else(invalid_bundle)?
            .try_into()
            .map_err(|_| invalid_bundle())?;
        Ok(u16::from_le_bytes(bytes) as usize)
    };
    let u32_at = |offset: usize| -> Result<usize, ApiError> {
        let bytes: [u8; 4] = archive
            .get(offset..offset + 4)
            .ok_or_else(invalid_bundle)?
            .try_into()
            .map_err(|_| invalid_bundle())?;
        Ok(u32::from_le_bytes(bytes) as usize)
    };
    let count = u16_at(end + 10)?;
    if u16_at(end + 4)? != 0
        || u16_at(end + 6)? != 0
        || u16_at(end + 8)? != count
        || count == 0
        || count > 512
    {
        return Err(invalid_bundle());
    }
    let size = u32_at(end + 12)?;
    let start = u32_at(end + 16)?;
    if size == u32::MAX as usize
        || start == u32::MAX as usize
        || start.checked_add(size) != Some(end)
    {
        return Err(invalid_bundle());
    }
    let mut cursor = start;
    for _ in 0..count {
        if archive.get(cursor..cursor + 4) != Some(b"PK\x01\x02") {
            return Err(invalid_bundle());
        }
        let length = 46usize
            .checked_add(u16_at(cursor + 28)?)
            .and_then(|n| n.checked_add(u16_at(cursor + 30).ok()?))
            .and_then(|n| n.checked_add(u16_at(cursor + 32).ok()?))
            .ok_or_else(invalid_bundle)?;
        cursor = cursor.checked_add(length).ok_or_else(invalid_bundle)?;
        if cursor > end {
            return Err(invalid_bundle());
        }
    }
    if cursor != end {
        return Err(invalid_bundle());
    }
    Ok(count)
}

fn bundle_manifest(
    archive: &[u8],
) -> Result<extensions_sprint_install::PublishedManifest, ApiError> {
    if archive.is_empty() || archive.len() > MAX_ARCHIVE {
        return Err(invalid_bundle());
    }
    let count = zip_entry_count(archive)?;
    let mut zip = zip::ZipArchive::new(Cursor::new(archive)).map_err(|_| invalid_bundle())?;
    if zip.len() != count {
        return Err(invalid_bundle());
    }
    let mut files = HashSet::new();
    for index in 0..zip.len() {
        let entry = zip.by_index(index).map_err(|_| invalid_bundle())?;
        let raw = std::str::from_utf8(entry.name_raw()).map_err(|_| invalid_bundle())?;
        let name = raw.strip_suffix('/').unwrap_or(raw);
        if name.is_empty()
            || name.starts_with('/')
            || name.contains('\\')
            || name
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || entry
                .unix_mode()
                .is_some_and(|mode| mode & 0o170000 == 0o120000)
        {
            return Err(invalid_bundle());
        }
        if !entry.is_dir() && !files.insert(name.to_owned()) {
            return Err(invalid_bundle());
        }
    }
    let root = "multica.plugin.json";
    let name = if files.contains(root) {
        root.to_owned()
    } else {
        let matches: Vec<_> = files
            .iter()
            .filter(|name| name.ends_with("/multica.plugin.json") && name.split('/').count() == 2)
            .collect();
        if matches.len() != 1 {
            return Err(invalid_bundle());
        }
        (*matches[0]).clone()
    };
    let mut file = zip.by_name(&name).map_err(|_| invalid_bundle())?;
    if file.size() > MAX_FILE as u64 {
        return Err(invalid_bundle());
    }
    let mut raw = Vec::new();
    file.by_ref()
        .take(MAX_FILE as u64 + 1)
        .read_to_end(&mut raw)
        .map_err(|_| invalid_bundle())?;
    if raw.len() > MAX_FILE {
        return Err(invalid_bundle());
    }
    extensions_sprint_install::publish_manifest(&raw)
}

async fn publish(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<super::Actor>,
    Path(raw): Path<String>,
    mut multipart: Multipart,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let mut db = state.pool.get().await.map_err(|_| failed())?;
    let ws =
        extensions_sprint_token::admin(&**db, user, &actor, &raw, state.extensions_sprint_enabled)
            .await?;
    let mut archive = None;
    while let Some(mut field) = multipart.next_field().await.map_err(|_| invalid_bundle())? {
        if field.name() != Some("bundle") || archive.is_some() {
            return Err(invalid_bundle());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = field.chunk().await.map_err(|_| invalid_bundle())? {
            if chunk.len() > MAX_ARCHIVE - bytes.len() {
                return Err(error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "the Plugin package is too large",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        archive = Some(bytes);
    }
    let bundle = bundle_manifest(archive.as_deref().ok_or_else(invalid_bundle)?)?;
    let mut hasher = Sha256::new();
    hasher.update(&bundle.canonical);
    let digest = hex::encode(hasher.finalize());
    let tx = db.transaction().await.map_err(|_| failed())?;
    if tx
        .query_opt("SELECT id FROM workspace WHERE id=$1 FOR KEY SHARE", &[&ws])
        .await
        .map_err(|_| failed())?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "workspace not found"));
    }
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1::text,0))",
        &[&format!("{ws}:{}", bundle.key)],
    )
    .await
    .map_err(|_| failed())?;
    let row = tx.query_one(
        "INSERT INTO plugin_package(workspace_id,plugin_key,name,created_by) VALUES($1,$2,$3,$4) \
         ON CONFLICT (workspace_id,plugin_key) DO UPDATE SET name=EXCLUDED.name,updated_at=now() \
         RETURNING id,plugin_key,name,created_at",
        &[&ws,&bundle.key,&bundle.name,&user],
    ).await.map_err(|_| failed())?;
    let package_id: Uuid = row.get("id");
    tx.execute(
        "INSERT INTO plugin_package_version(package_id,workspace_id,version,manifest,digest,size_bytes,published_by) \
         VALUES($1,$2,$3,$4,$5,0,$6)",
        &[&package_id,&ws,&bundle.version,&bundle.value,&digest,&user],
    ).await.map_err(|err| {
        if err.code()==Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
            error(StatusCode::CONFLICT, "this Plugin version is already published")
        } else { failed() }
    })?;
    let payload = summary(&*tx, ws, &row).await?;
    tx.commit().await.map_err(|_| failed())?;
    Ok((StatusCode::CREATED, GoJson(payload)))
}

async fn list(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<super::Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let mut db = state.pool.get().await.map_err(|_| failed())?;
    actor.require_workspace_url(&raw)?;
    let ws = Uuid::parse_str(&raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
    let tx = db
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(|_| failed())?;
    if tx
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| failed())?
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
    let rows = tx.query(
        "SELECT id,plugin_key,name,created_at FROM plugin_package WHERE workspace_id=$1 ORDER BY name ASC, id ASC",
        &[&ws],
    ).await.map_err(|_| failed())?;
    let mut packages = Vec::with_capacity(rows.len());
    for row in &rows {
        packages.push(summary(&*tx, ws, row).await?);
    }
    tx.commit().await.map_err(|_| failed())?;
    Ok(GoJson(json!({"packages":packages})))
}

async fn remove(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<super::Actor>,
    Path((raw, package_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let mut db = state.pool.get().await.map_err(|_| failed())?;
    let ws =
        extensions_sprint_token::admin(&**db, user, &actor, &raw, state.extensions_sprint_enabled)
            .await?;
    let id = Uuid::parse_str(&package_id)
        .map_err(|_| error(StatusCode::NOT_FOUND, "plugin package not found"))?;
    // First read only supplies the advisory lock key; re-read the row after the lock.
    let key: String = package(&**db, ws, id).await?.get("plugin_key");
    let tx = db.transaction().await.map_err(|_| failed())?;
    if tx
        .query_opt("SELECT id FROM workspace WHERE id=$1 FOR KEY SHARE", &[&ws])
        .await
        .map_err(|_| failed())?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "workspace not found"));
    }
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1::text,0))",
        &[&format!("{ws}:{key}")],
    )
    .await
    .map_err(|_| failed())?;
    let current = package(&*tx, ws, id).await?;
    if current.get::<_, String>("plugin_key") != key {
        return Err(error(
            StatusCode::CONFLICT,
            "plugin package changed during deletion",
        ));
    }
    let installed: i64 = tx.query_one(
        "SELECT count(*) FROM plugin_installation WHERE package_version_id IN (SELECT id FROM plugin_package_version WHERE package_id=$1 AND workspace_id=$2)",
        &[&id, &ws],
    ).await.map_err(|_| failed())?.get(0);
    if installed != 0 {
        return Err(error(
            StatusCode::CONFLICT,
            "this plugin is still installed; uninstall it before deleting the published package",
        ));
    }
    tx.execute("DELETE FROM plugin_package_file WHERE version_id IN (SELECT id FROM plugin_package_version WHERE package_id=$1 AND workspace_id=$2)", &[&id,&ws])
        .await.map_err(|_| failed())?;
    tx.execute(
        "DELETE FROM plugin_package_version WHERE package_id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .map_err(|_| failed())?;
    if tx
        .execute(
            "DELETE FROM plugin_package WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| failed())?
        != 1
    {
        return Err(error(StatusCode::NOT_FOUND, "plugin package not found"));
    }
    tx.commit().await.map_err(|_| failed())?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::{ZipWriter, write::SimpleFileOptions};

    fn archive(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        for (path, content) in files {
            writer
                .start_file(*path, SimpleFileOptions::default())
                .unwrap();
            writer.write_all(content).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    #[test]
    fn inert_bundle_rejects_traversal_duplicate_large_and_executable_manifests() {
        let manifest: &[u8] = br#"{"manifest_version":1,"key":"example.safe","name":"Safe","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["issues:read"],"contributes":{}}"#;
        assert_eq!(
            bundle_manifest(&archive(&[("plugin/multica.plugin.json", manifest)]))
                .unwrap()
                .key,
            "example.safe"
        );
        for files in [
            vec![("../multica.plugin.json", manifest)],
            vec![("two/nested/multica.plugin.json", manifest)],
            vec![
                ("multica.plugin.json", manifest),
                ("../hidden", b"anything"),
            ],
        ] {
            assert!(bundle_manifest(&archive(&files)).is_err());
        }
        for body in [
            br#"{"manifest_version":1,"key":"example.safe","key":"example.bad","name":"Safe","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["issues:read"],"contributes":{}}"#.as_slice(),
            br#"{"manifest_version":1,"key":"example.safe","name":"Safe","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["issues:write"],"contributes":{}}"#,
            br#"{"manifest_version":1,"key":"example.safe","name":"Safe","version":"1.0.0","author":{"name":"Synthetic"},"scopes":["issues:read"],"contributes":{"surfaces":[{"entry":"panel.js"}]}}"#,
        ] {
            assert!(bundle_manifest(&archive(&[("multica.plugin.json",body)])).is_err());
        }
        let large = vec![b'a'; MAX_FILE + 1];
        assert!(bundle_manifest(&archive(&[("multica.plugin.json", &large)])).is_err());

        let mut duplicate = archive(&[
            ("multica.plugin.json", manifest),
            ("multica.plugin.jsxx", b"ignored"),
        ]);
        let second_name = b"multica.plugin.jsxx";
        let occurrences: Vec<_> = duplicate
            .windows(second_name.len())
            .enumerate()
            .filter_map(|(position, window)| (window == second_name).then_some(position))
            .collect();
        assert_eq!(occurrences.len(), 2);
        for position in occurrences {
            duplicate[position..position + second_name.len()]
                .copy_from_slice(b"multica.plugin.json");
        }
        assert_eq!(zip_entry_count(&duplicate).unwrap(), 2);
        assert!(bundle_manifest(&duplicate).is_err());

        let mut symlink = ZipWriter::new(Cursor::new(Vec::new()));
        symlink
            .start_file("multica.plugin.json", SimpleFileOptions::default())
            .unwrap();
        symlink.write_all(manifest).unwrap();
        symlink
            .add_symlink("unused-link", "../escape", SimpleFileOptions::default())
            .unwrap();
        assert!(bundle_manifest(&symlink.finish().unwrap().into_inner()).is_err());

        let mut too_many = ZipWriter::new(Cursor::new(Vec::new()));
        too_many
            .start_file("multica.plugin.json", SimpleFileOptions::default())
            .unwrap();
        too_many.write_all(manifest).unwrap();
        for index in 0..512 {
            too_many
                .start_file(format!("unused-{index}"), SimpleFileOptions::default())
                .unwrap();
        }
        assert!(bundle_manifest(&too_many.finish().unwrap().into_inner()).is_err());
    }
}
