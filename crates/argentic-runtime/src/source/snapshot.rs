use super::{archive::hex, file_url, invalid, io, read_regular, sha256, Files, MAX_JSON_TOTAL};
use crate::persistence::{failure, Result};
use cap_std::{ambient_authority, fs::Dir};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

const SNAPSHOTS: &str = "agsdl-snapshots";
const INDEX: &str = "files.json";
const PROJECT: &str = "project";

/// A document location and, for packaged sources, its durable snapshot identity.
/// The private host data directory owns the snapshot. Dropping this handle does
/// not remove files or change their identity.
pub struct RetainedSource {
    pub(super) location: PathBuf,
    pub(super) id: Option<String>,
}

impl RetainedSource {
    pub(crate) fn document_path(&self) -> &Path {
        &self.location
    }

    pub fn resource_root(&self) -> &Path {
        self.location.parent().unwrap()
    }

    /// Bind the document base and snapshot identity to the host-created scope.
    /// Persist this scope as the Start capture's `callScopeJson`.
    pub fn bind_scope(&self, scope: &mut Value) -> Result<()> {
        let object = scope
            .as_object_mut()
            .ok_or_else(|| invalid("scope", "", "Expected a host scope object"))?;
        object.insert(
            "documentLocation".into(),
            Value::from(file_url(&self.location)?.as_str()),
        );
        if let Some(id) = &self.id {
            object.insert("projectSnapshot".into(), Value::from(id.as_str()));
        } else {
            object.remove("projectSnapshot");
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
struct Manifest {
    document: String,
    files: BTreeMap<String, Entry>,
}

#[derive(Deserialize, Serialize)]
struct Entry {
    size: u64,
    sha256: String,
    executable: bool,
}

fn unavailable() -> crate::persistence::dto::EngineFailure {
    failure(
        "CONTENT_UNAVAILABLE",
        "projectSnapshot",
        "The captured project snapshot is missing or has changed",
    )
}

pub(super) fn retain(
    host_data: &Path,
    authored_sha256: &str,
    files: &Files,
) -> Result<RetainedSource> {
    let data = host_data.canonicalize().map_err(|e| io(host_data, e))?;
    let parent = data.join(SNAPSHOTS);
    fs::create_dir_all(&parent).map_err(|e| io(&parent, e))?;
    if parent.canonicalize().map_err(|e| io(&parent, e))? != parent {
        return Err(unavailable());
    }
    let manifest = Manifest {
        document: authored_sha256.into(),
        files: files
            .iter()
            .map(|(name, file)| {
                (
                    name.clone(),
                    Entry {
                        size: file.bytes.len() as u64,
                        sha256: sha256(&file.bytes),
                        executable: file.executable,
                    },
                )
            })
            .collect(),
    };
    let bytes = serde_json::to_vec(&manifest)?;
    let id = sha256(&bytes);
    let path = parent.join(&id);
    if !path.exists() {
        let staging = tempfile::tempdir_in(&parent).map_err(|e| io(&parent, e))?;
        let root = staging.path().join(PROJECT);
        #[cfg(unix)]
        let mut directories =
            std::collections::BTreeSet::from([root.clone(), staging.path().to_owned()]);
        fs::create_dir(&root).map_err(|e| io(&root, e))?;
        for (name, file) in files {
            let target = root.join(name);
            fs::create_dir_all(target.parent().unwrap()).map_err(|e| io(&target, e))?;
            #[cfg(unix)]
            directories.extend(
                target
                    .ancestors()
                    .skip(1)
                    .take_while(|dir| *dir != root)
                    .map(Path::to_owned),
            );
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(if file.executable { 0o700 } else { 0o600 });
            }
            let mut output = options.open(&target).map_err(|e| io(&target, e))?;
            output
                .write_all(&file.bytes)
                .and_then(|()| output.sync_all())
                .map_err(|e| io(&target, e))?;
        }
        let index = staging.path().join(INDEX);
        let mut output = fs::File::create(&index).map_err(|e| io(&index, e))?;
        output
            .write_all(&bytes)
            .and_then(|()| output.sync_all())
            .map_err(|e| io(&index, e))?;
        #[cfg(unix)]
        for directory in directories.iter().rev() {
            sync_directory(directory)?;
        }
        if let Err(error) = fs::rename(staging.path(), &path) {
            if !path.exists() {
                return Err(io(&path, error));
            }
        }
    }
    #[cfg(unix)]
    {
        sync_directory(&parent)?;
        sync_directory(&data)?;
    }
    let snapshot = Snapshot::open(&data, &id, authored_sha256)?;
    // Refuse to publish an already missing or damaged copy. Runtime reads also
    // verify the exact bytes, including after an embedding host restarts.
    let root =
        Dir::open_ambient_dir(&snapshot.root, ambient_authority()).map_err(|_| unavailable())?;
    for (name, entry) in &snapshot.manifest.files {
        let file = read_regular(&root, Path::new(name), entry.size).map_err(|_| unavailable())?;
        if sha256(&file.bytes) != entry.sha256 || cfg!(unix) && file.executable != entry.executable
        {
            return Err(unavailable());
        }
    }
    Ok(RetainedSource {
        location: snapshot.root.join(super::MANIFEST),
        id: Some(id),
    })
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|e| io(path, e))
}

pub(crate) struct Snapshot {
    pub root: PathBuf,
    uri_root: PathBuf,
    manifest: Manifest,
}

impl Snapshot {
    fn open(data: &Path, id: &str, authored_sha256: &str) -> Result<Self> {
        if !hex(id, 64) {
            return Err(unavailable());
        }
        let directory = data.join(SNAPSHOTS).join(id);
        let root = directory.join(PROJECT);
        if root.canonicalize().ok().as_ref() != Some(&root) {
            return Err(unavailable());
        }
        let dir =
            Dir::open_ambient_dir(&directory, ambient_authority()).map_err(|_| unavailable())?;
        let index =
            read_regular(&dir, Path::new(INDEX), MAX_JSON_TOTAL).map_err(|_| unavailable())?;
        if sha256(&index.bytes) != id {
            return Err(unavailable());
        }
        let manifest: Manifest = serde_json::from_slice(&index.bytes).map_err(|_| unavailable())?;
        if manifest.document != authored_sha256 {
            return Err(unavailable());
        }
        // Windows file URLs round-trip to DOS paths, while canonicalize may
        // return the same location with a verbatim volume prefix.
        let uri_root = file_url(&root.join(super::MANIFEST))?
            .to_file_path()
            .map_err(|_| unavailable())?
            .parent()
            .ok_or_else(unavailable)?
            .to_owned();
        Ok(Self {
            root,
            uri_root,
            manifest,
        })
    }

    pub fn from_capture(
        data: &Path,
        capture: &crate::persistence::start030::StartCapture030,
    ) -> Result<Option<Self>> {
        let private: Value = serde_json::from_str(&capture.private_capture_json)?;
        let Some(scope) = private.get("callScopeJson").and_then(Value::as_str) else {
            return Ok(None);
        };
        let scope: Value = serde_json::from_str(scope)?;
        let Some(id) = scope.get("projectSnapshot") else {
            return Ok(None);
        };
        let data = data.canonicalize().map_err(|_| unavailable())?;
        let snapshot = Self::open(
            &data,
            id.as_str().ok_or_else(unavailable)?,
            &capture.source.source_sha256,
        )?;
        if crate::capture::document_location(&scope)?.as_ref()
            != Some(&file_url(&snapshot.root.join(super::MANIFEST))?)
        {
            return Err(unavailable());
        }
        Ok(Some(snapshot))
    }

    /// Whether a path names a location inside the retained project.
    pub fn contains(&self, path: &Path) -> bool {
        path.starts_with(&self.uri_root) || path.starts_with(&self.root)
    }

    pub fn member<'a>(&self, path: &'a Path) -> Option<(&'a Path, &str)> {
        let relative = path.strip_prefix(&self.uri_root).ok()?;
        let name = relative.to_str()?.replace(std::path::MAIN_SEPARATOR, "/");
        self.manifest
            .files
            .get(&name)
            .map(|entry| (relative, entry.sha256.as_str()))
    }
}
