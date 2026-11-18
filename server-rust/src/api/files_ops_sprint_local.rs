//! Directory-fd anchored local storage. A symlink swap after path validation
//! must not redirect reads, publishes or cleanup outside the service root.
use super::{LocalObjects, ObjectStore};
use std::{
    ffi::{CStr, CString, OsStr},
    fs::{DirBuilder, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{
            ffi::OsStrExt,
            fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
        },
    },
    path::Path,
};

fn cstr(part: &OsStr) -> Result<CString, String> {
    CString::new(part.as_bytes()).map_err(|_| "NUL in object key".into())
}

fn require_private_directory(file: &File, label: &str) -> Result<(), String> {
    let metadata = file.metadata().map_err(|e| e.to_string())?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(format!("{label} must be owned by the service and private"));
    }
    Ok(())
}

pub(super) fn open_or_create_root(root: &Path) -> Result<File, String> {
    let parent = root
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    // Mode is applied by mkdir itself, not by chmod on a path that could
    // be replaced after creation. A racing pre-existing root stays untouched.
    match DirBuilder::new().mode(0o700).create(root) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.to_string()),
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(root)
        .map_err(|e| e.to_string())?;
    require_private_directory(&file, "upload root")?;
    Ok(file)
}

fn open_dir(parent: &File, name: &CStr, create: bool) -> Result<File, String> {
    let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    let mut fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 && create && std::io::Error::last_os_error().kind() == std::io::ErrorKind::NotFound {
        let rc = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
        if rc < 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::AlreadyExists {
            return Err(std::io::Error::last_os_error().to_string());
        }
        fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    }
    if fd < 0 {
        let err = std::io::Error::last_os_error();
        if !create && err.kind() == std::io::ErrorKind::NotFound {
            return Err("object parent missing".into());
        }
        return Err(err.to_string());
    }
    let dir = unsafe { File::from_raw_fd(fd) };
    let metadata = dir.metadata().map_err(|e| e.to_string())?;
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
        return Err("object directory must be owned by the service and private".into());
    }
    Ok(dir)
}

fn parent_and_leaf(
    store: &LocalObjects,
    key: &str,
    create: bool,
) -> Result<(File, CString), String> {
    // The existing parser rejects traversal and internal sidecar/temp keys.
    store.path(key)?;
    let path = Path::new(key);
    let mut parent = store.root_fd.try_clone().map_err(|e| e.to_string())?;
    require_private_directory(&parent, "upload root")?;
    for part in path.parent().ok_or("invalid object key")?.components() {
        parent = open_dir(&parent, &cstr(part.as_os_str())?, create)?;
    }
    Ok((parent, cstr(path.file_name().ok_or("invalid object key")?)?))
}

fn open_file(parent: &File, name: &CStr, flags: i32, mode: libc::mode_t) -> Result<File, String> {
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            mode,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn unlink(parent: &File, name: &CStr) -> Result<(), String> {
    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) } == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.kind() == std::io::ErrorKind::NotFound {
        Ok(())
    } else {
        Err(err.to_string())
    }
}

fn extra_names(leaf: &CStr) -> Result<(CString, CString), String> {
    let text = leaf.to_bytes();
    let mut temp = Vec::with_capacity(text.len() + 5);
    temp.push(b'.');
    temp.extend_from_slice(text);
    temp.extend_from_slice(b".tmp");
    let mut meta = Vec::with_capacity(text.len() + 10);
    meta.extend_from_slice(text);
    meta.extend_from_slice(b".meta.json");
    Ok((
        CString::new(temp).map_err(|e| e.to_string())?,
        CString::new(meta).map_err(|e| e.to_string())?,
    ))
}

pub(super) fn read(
    store: &LocalObjects,
    key: &str,
    start: u64,
    limit: usize,
    expected_size: Option<u64>,
) -> Result<Vec<u8>, String> {
    let (parent, leaf) = parent_and_leaf(store, key, false)?;
    let mut file = open_file(&parent, &leaf, libc::O_RDONLY, 0)?;
    let metadata = file.metadata().map_err(|e| e.to_string())?;
    // A private directory does not make an imported object private if it is
    // hard-linked outside the root or readable by another local account.
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
    {
        return Err("object must be a private, service-owned regular file".into());
    }
    if expected_size.is_some_and(|size| metadata.len() != size) {
        return Err("object size differs from ledger".into());
    }
    file.seek(SeekFrom::Start(start))
        .map_err(|e| e.to_string())?;
    let mut data = Vec::new();
    file.take(limit as u64)
        .read_to_end(&mut data)
        .map_err(|e| e.to_string())?;
    Ok(data)
}

pub(super) fn write(
    store: &LocalObjects,
    key: &str,
    data: &[u8],
    content_type: &str,
    filename: &str,
) -> Result<(), String> {
    let (parent, leaf) = parent_and_leaf(store, key, true)?;
    let (temp, meta) = extra_names(&leaf)?;
    let mut owns_temp = false;
    let mut owns_meta = false;
    let result = (|| {
        let mut file = open_file(
            &parent,
            &temp,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )?;
        owns_temp = true;
        file.write_all(data).map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        let mut sidecar = open_file(
            &parent,
            &meta,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )?;
        owns_meta = true;
        let bytes = serde_json::to_vec(
            &serde_json::json!({"filename": filename, "content_type": content_type}),
        )
        .map_err(|e| e.to_string())?;
        sidecar.write_all(&bytes).map_err(|e| e.to_string())?;
        sidecar.sync_all().map_err(|e| e.to_string())?;
        if unsafe {
            libc::linkat(
                parent.as_raw_fd(),
                temp.as_ptr(),
                parent.as_raw_fd(),
                leaf.as_ptr(),
                0,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error().to_string());
        }
        // Never roll back the published object on a late durability failure:
        // another writer may already observe it and retries must not clobber it.
        Ok(())
    })();
    if owns_temp {
        let _ = unlink(&parent, &temp);
    }
    if result.is_err() && owns_meta {
        let _ = unlink(&parent, &meta);
    }
    // The DB may commit immediately after write returns. Durably publish the
    // directory entry (and the temp removal) before the ledger is finalized.
    if result.is_ok() {
        parent.sync_all().map_err(|e| e.to_string())?;
    }
    result
}

pub(super) fn remove(store: &LocalObjects, key: &str) -> Result<(), String> {
    let (parent, leaf) = match parent_and_leaf(store, key, false) {
        Ok(value) => value,
        Err(e) if e == "object parent missing" => return Ok(()),
        Err(e) => return Err(e),
    };
    let (temp, meta) = extra_names(&leaf)?;
    // The DB ledger authenticates the object key, not the current inode. If a
    // local operator/import replaced a file with a symlink, hard link or
    // foreign/shared file, retain the intent for repair rather than deleting
    // that inode. Validate all three names before unlinking any of them.
    for name in [&leaf, &meta, &temp] {
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if fd < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::NotFound {
                continue;
            }
            return Err(error.to_string());
        }
        let file = unsafe { File::from_raw_fd(fd) };
        let metadata = file.metadata().map_err(|e| e.to_string())?;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
            || metadata.nlink() != 1
        {
            return Err("object cleanup requires private, service-owned regular files".into());
        }
    }
    for name in [&leaf, &meta, &temp] {
        unlink(&parent, name)?;
    }
    // Keep the cleanup intent until directory removal is durable.
    parent.sync_all().map_err(|e| e.to_string())
}

impl ObjectStore for LocalObjects {
    fn is_local(&self) -> bool {
        true
    }
    fn object_url(&self, key: &str) -> Result<String, String> {
        self.path(key)?;
        Ok(format!(
            "{}/uploads/{key}",
            self.base_url.as_deref().unwrap_or("")
        ))
    }
    fn key_from_url(&self, url: &str) -> Result<String, String> {
        let relative = if let Some(base) = &self.base_url {
            url.strip_prefix(&format!("{base}/uploads/"))
        } else {
            url.strip_prefix("/uploads/")
        }
        .ok_or("object URL is not in configured local storage".to_owned())?;
        self.path(relative)?;
        Ok(relative.to_owned())
    }
    fn read(&self, key: &str, start: u64, limit: usize) -> Result<Vec<u8>, String> {
        read(self, key, start, limit, None)
    }
    fn read_verified(
        &self,
        key: &str,
        start: u64,
        limit: usize,
        expected_size: u64,
    ) -> Result<Vec<u8>, String> {
        read(self, key, start, limit, Some(expected_size))
    }
    fn write(
        &self,
        key: &str,
        data: &[u8],
        content_type: &str,
        filename: &str,
    ) -> Result<(), String> {
        write(self, key, data, content_type, filename)
    }
    fn remove(&self, key: &str) -> Result<(), String> {
        remove(self, key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn verified_read_rejects_stale_ledger_for_full_and_partial_ranges() {
        let root = std::env::temp_dir().join(format!("files-ops-size-{}", Uuid::now_v7()));
        let store = LocalObjects::new(root.clone(), None).unwrap();
        let key = "users/owner/item.txt";
        store.write(key, b"abcd", "text/plain", "item.txt").unwrap();
        assert!(store.read_verified(key, 0, 3, 3).is_err());
        assert!(store.read_verified(key, 1, 2, 3).is_err());
        assert_eq!(store.read_verified(key, 1, 2, 4).unwrap(), b"bc");
        store.remove(key).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_symlink_and_shared_or_foreign_upload_roots() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let base = std::env::temp_dir().join(format!("files-ops-private-root-{}", Uuid::now_v7()));
        let root = base.join("root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(LocalObjects::new(root.clone(), None).is_err());
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        LocalObjects::new(root.clone(), None).unwrap();
        let link = base.join("link");
        symlink(&root, &link).unwrap();
        assert!(LocalObjects::new(link, None).is_err());
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn refuses_shared_or_foreign_object_directories_without_changing_them() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::temp_dir().join(format!("files-ops-private-child-{}", Uuid::now_v7()));
        let root = base.join("root");
        let store = LocalObjects::new(root.clone(), None).unwrap();
        let workspace = root.join("workspaces");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::set_permissions(&workspace, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(
            store
                .write("workspaces/one/item", b"private", "text/plain", "item")
                .is_err()
        );
        assert!(!root.join("workspaces/one/item").exists());
        assert_eq!(
            std::fs::metadata(&workspace).unwrap().permissions().mode() & 0o777,
            0o777
        );
        std::fs::set_permissions(&workspace, std::fs::Permissions::from_mode(0o700)).unwrap();
        store
            .write("workspaces/one/item", b"private", "text/plain", "item")
            .unwrap();
        std::fs::set_permissions(
            root.join("workspaces/one"),
            std::fs::Permissions::from_mode(0o750),
        )
        .unwrap();
        assert!(store.read("workspaces/one/item", 0, 20).is_err());
        assert!(store.remove("workspaces/one/item").is_err());
        std::fs::set_permissions(
            root.join("workspaces/one"),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        assert_eq!(
            store.read("workspaces/one/item", 0, 20).unwrap(),
            b"private"
        );
        store.remove("workspaces/one/item").unwrap();
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn root_permission_changes_revoke_all_object_operations() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("files-ops-root-mode-{}", Uuid::now_v7()));
        let store = LocalObjects::new(root.clone(), None).unwrap();
        store
            .write("users/one/item", b"private", "text/plain", "item")
            .unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(store.read("users/one/item", 0, 20).is_err());
        assert!(
            store
                .write("users/one/other", b"new", "text/plain", "other")
                .is_err()
        );
        assert!(store.remove("users/one/item").is_err());
        assert!(!root.join("users/one/other").exists());
        assert!(LocalObjects::new(root.clone(), None).is_err());
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o777
        );
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(store.read("users/one/item", 0, 20).unwrap(), b"private");
        store.remove("users/one/item").unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_shared_foreign_and_hardlinked_objects_without_exposing_bytes() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("files-ops-object-mode-{}", Uuid::now_v7()));
        let store = LocalObjects::new(root.clone(), None).unwrap();
        let key = "users/one/item";
        store.write(key, b"private", "text/plain", "item").unwrap();
        let leaf = root.join(key);
        let linked = root.join("users/one/linked");
        std::fs::hard_link(&leaf, &linked).unwrap();
        assert!(store.read(key, 0, 10).is_err());
        std::fs::remove_file(linked).unwrap();
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(store.read(key, 0, 10).is_err());
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(store.read(key, 0, 10).unwrap(), b"private");
        store.remove(key).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cleanup_refuses_poisoned_object_and_sidecar_without_partial_deletion() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let root =
            std::env::temp_dir().join(format!("files-ops-cleanup-poison-{}", Uuid::now_v7()));
        let store = LocalObjects::new(root.clone(), None).unwrap();
        let key = "users/one/item";
        store.write(key, b"private", "text/plain", "item").unwrap();
        let leaf = root.join(key);
        let sidecar = root.join("users/one/item.meta.json");
        let outside = root
            .parent()
            .unwrap()
            .join(format!("files-ops-outside-{}", Uuid::now_v7()));
        std::fs::write(&outside, b"outside").unwrap();
        std::fs::remove_file(&sidecar).unwrap();
        symlink(&outside, &sidecar).unwrap();
        assert!(store.remove(key).is_err());
        assert_eq!(std::fs::read(&leaf).unwrap(), b"private");
        assert_eq!(std::fs::read(&outside).unwrap(), b"outside");
        std::fs::remove_file(&sidecar).unwrap();
        std::fs::hard_link(&outside, &sidecar).unwrap();
        assert!(store.remove(key).is_err());
        assert!(leaf.exists());
        std::fs::remove_file(&sidecar).unwrap();
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(store.remove(key).is_err());
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o600)).unwrap();
        store.remove(key).unwrap();
        assert!(!leaf.exists());
        std::fs::remove_file(outside).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn newly_created_upload_root_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::temp_dir().join(format!("files-ops-new-root-{}", Uuid::now_v7()));
        let root = base.join("nested/root");
        let store = LocalObjects::new(root.clone(), None).unwrap();
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        store
            .write("users/one/item", b"private", "text/plain", "item")
            .unwrap();
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn root_fd_stays_confined_after_root_replacement() {
        let base = std::env::temp_dir().join(format!("files-ops-root-swap-{}", Uuid::now_v7()));
        let root = base.join("objects");
        let displaced = base.join("original");
        let store = LocalObjects::new(root.clone(), None).unwrap();
        store
            .write("users/one/item.png", b"original", "image/png", "item.png")
            .unwrap();
        std::fs::rename(&root, &displaced).unwrap();
        std::fs::create_dir_all(root.join("users/one")).unwrap();
        std::fs::write(root.join("users/one/item.png"), b"outside").unwrap();

        assert_eq!(
            store.read("users/one/item.png", 0, 100).unwrap(),
            b"original"
        );
        store.remove("users/one/item.png").unwrap();
        assert_eq!(
            std::fs::read(root.join("users/one/item.png")).unwrap(),
            b"outside"
        );
        store
            .write("users/one/second.png", b"inside", "image/png", "second.png")
            .unwrap();
        assert_eq!(
            std::fs::read(displaced.join("users/one/second.png")).unwrap(),
            b"inside"
        );
        assert!(!root.join("users/one/second.png").exists());
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn root_symlink_replacement_cannot_redirect_object_access() {
        let base = std::env::temp_dir().join(format!("files-ops-root-link-{}", Uuid::now_v7()));
        let root = base.join("objects");
        let displaced = base.join("original");
        let outside = base.join("outside");
        let store = LocalObjects::new(root.clone(), None).unwrap();
        std::fs::create_dir_all(outside.join("users/one")).unwrap();
        std::fs::write(outside.join("users/one/item"), b"outside").unwrap();
        store
            .write("users/one/item", b"inside", "text/plain", "item")
            .unwrap();
        std::fs::rename(&root, &displaced).unwrap();
        std::os::unix::fs::symlink(&outside, &root).unwrap();
        assert_eq!(store.read("users/one/item", 0, 10).unwrap(), b"inside");
        store
            .write("users/one/other", b"new", "text/plain", "other")
            .unwrap();
        store.remove("users/one/item").unwrap();
        assert_eq!(
            std::fs::read(outside.join("users/one/item")).unwrap(),
            b"outside"
        );
        assert_eq!(
            std::fs::read(displaced.join("users/one/other")).unwrap(),
            b"new"
        );
        std::fs::remove_file(root).unwrap();
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn concurrent_immutable_writes_never_replace_first_object() {
        let root = std::env::temp_dir().join(format!("files-ops-concurrent-{}", Uuid::now_v7()));
        let store = LocalObjects::new(root.clone(), None).unwrap();
        let writers: Vec<_> = (0..8)
            .map(|id| {
                let store = store.clone();
                std::thread::spawn(move || {
                    store.write(
                        "users/one/object",
                        &[id],
                        "application/octet-stream",
                        "object",
                    )
                })
            })
            .collect();
        let results: Vec<_> = writers
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let bytes = store.read("users/one/object", 0, 10).unwrap();
        assert_eq!(bytes.len(), 1);
        store.remove("users/one/object").unwrap();
        assert!(!root.join("users/one/object").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn parent_fd_stays_confined_after_directory_swap() {
        let root = std::env::temp_dir().join(format!("files-ops-fd-race-{}", Uuid::now_v7()));
        let outside = std::env::temp_dir().join(format!("files-ops-fd-other-{}", Uuid::now_v7()));
        let store = LocalObjects::new(root.clone(), None).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir_all(root.join("users/original")).unwrap();
        for path in [root.join("users"), root.join("users/original")] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("item.png"), b"private").unwrap();
        let (parent, leaf) = parent_and_leaf(&store, "users/original/item.png", false).unwrap();
        std::fs::rename(root.join("users/original"), root.join("users/renamed")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("users/original")).unwrap();
        let mut object = open_file(
            &parent,
            &leaf,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )
        .unwrap();
        object.write_all(b"new").unwrap();
        assert_eq!(std::fs::read(outside.join("item.png")).unwrap(), b"private");
        assert_eq!(
            std::fs::read(root.join("users/renamed/item.png")).unwrap(),
            b"new"
        );
        unlink(&parent, &leaf).unwrap();
        assert_eq!(std::fs::read(outside.join("item.png")).unwrap(), b"private");
        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(outside).unwrap();
    }
}
