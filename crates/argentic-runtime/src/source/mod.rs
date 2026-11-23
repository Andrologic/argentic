//! Local AgSDL input and portable archives. Opening never starts a provider,
//! resolves secret values, executes a command or retrieves an external URI.
mod archive;
#[cfg(test)]
mod archive_writer_tests;
#[cfg(test)]
mod conformance050_tests;
mod project;
#[cfg(test)]
mod project_tests;
mod snapshot;

pub use snapshot::RetainedSource;
pub(crate) use snapshot::Snapshot;

use crate::persistence::{dto::EngineFailure, failure, Result};
use cap_std::fs::Dir;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
};
use url::Url;

const MANIFEST: &str = "agsdl.json";
const MAX_FILES: usize = 8192;
const MAX_TOTAL: u64 = 256 * 1024 * 1024;
const MAX_JSON: u64 = 1024 * 1024;
const MAX_JSON_TOTAL: u64 = 8 * MAX_JSON;

type Files = BTreeMap<String, ProjectFile>;

struct ProjectFile {
    bytes: Vec<u8>,
    executable: bool,
}

/// One of the original files selected by a project or archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedFile<'a> {
    /// Portable UTF-8 path relative to the project root, separated by `/`.
    pub path: &'a str,
    /// Exact bytes held by the opened source.
    pub bytes: &'a [u8],
    /// Whether the project or archive marks this file executable.
    pub executable: bool,
}

/// Host-owned input. For an uploaded archive use [Self::open_archive_bytes].
/// Retain a project under the host's private storage before admitting a run.
pub struct OpenedSource {
    authored: String,
    origin: Option<PathBuf>,
    project: Option<project::Project>,
}

impl OpenedSource {
    /// Open a local JSON document, a project directory or manifest, or an archive.
    /// Only a trusted local caller should select a path on the host machine.
    pub fn open_path(path: &Path) -> Result<Self> {
        // Rebuild from components: with a trailing separator, `lstat` would
        // follow a symlinked project root.
        let path: PathBuf = std::path::absolute(path)
            .map_err(|e| io(path, e))?
            .components()
            .collect();
        if path.is_dir() {
            return Self::folder(&path, None);
        }
        let name = path
            .file_name()
            .ok_or_else(|| invalid("", "", "Expected a source file"))?;
        // Open a single file directly, as the host did before projects: its
        // parent needs search permission only. An input that cannot be
        // opened keeps the host's codes, SOURCE_UNAVAILABLE or INPUT_LIMIT.
        // A symlinked source, including a symlinked manifest, is refused.
        let meta = std::fs::symlink_metadata(&path).map_err(|_| source_unavailable())?;
        if path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("agsdl"))
        {
            if !meta.is_file() {
                return Err(source_unavailable());
            }
            return Self::archived(archive::read(
                open_local(&path).map_err(|_| source_unavailable())?,
            )?);
        }
        // Preserve the exact UTF-8 bytes and 8 MiB limit of the existing JSON input.
        if !meta.is_file() || meta.len() > MAX_JSON_TOTAL {
            return Err(failure(
                "SOURCE_UNAVAILABLE",
                "host",
                "Expected a regular input file of at most 8 MiB",
            ));
        }
        let file = read_local(&path, MAX_JSON_TOTAL).map_err(|e| {
            if e.kind() == std::io::ErrorKind::FileTooLarge {
                failure("INPUT_LIMIT", "host", "Input grew beyond 8 MiB")
            } else {
                source_unavailable()
            }
        })?;
        let authored = std::str::from_utf8(&file.bytes)
            .map_err(|_| failure("PARSE", "source", "Source must be UTF-8"))?;
        if name == MANIFEST
            && serde_json::from_str::<Value>(authored)
                .is_ok_and(|doc| doc.get("projectFormat").is_some())
        {
            return Self::folder(path.parent().unwrap(), Some(file));
        }
        crate::persistence::validate_json(authored)?;
        let authored = authored.to_owned();
        Ok(Self {
            authored,
            origin: Some(path.canonicalize().map_err(|_| source_unavailable())?),
            project: None,
        })
    }

    /// Open untrusted uploaded bytes. References are resolved inside the
    /// archive only. This API never uses a client-supplied local path.
    pub fn open_archive_bytes(bytes: &[u8]) -> Result<Self> {
        Self::archived(archive::read(Cursor::new(bytes))?)
    }

    fn folder(root: &Path, manifest: Option<ProjectFile>) -> Result<Self> {
        // The official tool refuses a project root that is a symbolic link.
        if std::fs::symlink_metadata(root).is_ok_and(|meta| meta.is_symlink()) {
            return Err(invalid(
                MANIFEST,
                "",
                "The project root cannot be a symbolic link",
            ));
        }
        let project = project::assemble_folder(root, manifest)?;
        Self::assembled(project)
    }

    fn archived(files: Files) -> Result<Self> {
        Self::assembled(project::assemble_archive(files)?)
    }

    fn assembled(project: project::Project) -> Result<Self> {
        // A folder accepts only what its archive could list, which also bounds
        // the retained index that a later run reopens.
        if archive::inventory(&project.files)?.len() as u64 > archive::INDEX_LIMIT {
            return Err(invalid(
                MANIFEST,
                "",
                "Project inventory exceeds the 2 MiB archive limit; select fewer or shorter paths",
            ));
        }
        let authored = serde_json::to_string(&project.document)?;
        Ok(Self {
            authored,
            origin: None,
            project: Some(project),
        })
    }

    pub(crate) fn is_document(&self) -> bool {
        self.project.is_none()
    }

    pub(crate) fn without_files(mut self) -> Self {
        if let Some(project) = &mut self.project {
            project.files.clear();
        }
        self
    }

    pub fn authored_json(&self) -> &str {
        &self.authored
    }

    /// Digest expected by the execution compiler for the assembled document.
    pub fn source_sha256(&self) -> String {
        sha256(self.authored.as_bytes())
    }

    /// Declared secret *names* keyed by MCP server. The host supplies values.
    pub fn secrets(&self) -> Option<&BTreeMap<String, Vec<String>>> {
        self.project.as_ref().map(|project| &project.secrets)
    }

    /// Declared project-root-relative assets keyed by local MCP server.
    pub fn mcp_files(&self) -> Option<&BTreeMap<String, Vec<String>>> {
        self.project.as_ref().map(|project| &project.mcp_files)
    }

    /// Borrow the selected files in ascending project-relative path order.
    /// Returns `None` for a plain JSON document.
    pub fn selected_files(&self) -> Option<impl ExactSizeIterator<Item = SelectedFile<'_>> + '_> {
        self.project.as_ref().map(|project| {
            project.files.iter().map(|(path, file)| SelectedFile {
                path,
                bytes: &file.bytes,
                executable: file.executable,
            })
        })
    }

    /// Write a deterministic `agsdl-archive-1` snapshot to an empty sink.
    /// Exports the opened project files without Git history.
    /// Returns a `PROJECT` error for a plain JSON document or an archive limit
    /// violation before writing any bytes. On an I/O error the output may be
    /// incomplete. The output is flushed before success is reported; the
    /// caller still owns syncing and publishing it.
    pub fn write_archive<W: Write + ?Sized>(&self, output: &mut W) -> Result<()> {
        let project = self
            .project
            .as_ref()
            .ok_or_else(|| invalid(MANIFEST, "", "Source is not an AgSDL project"))?;
        archive::write(&project.files, &mut *output)?;
        output.flush().map_err(|error| {
            invalid(
                MANIFEST,
                "",
                &format!("Could not flush archive output: {error}"),
            )
        })
    }

    /// Map an assembled JSON pointer to a project-relative file and a pointer
    /// within that file. Inline fields map to the manifest. This uses the
    /// closest declaring component and does not check whether the value exists.
    /// Returns `None` for a plain JSON document, a non-pointer category or a
    /// malformed JSON pointer.
    pub fn component_origin<'a>(&'a self, pointer: &'a str) -> Option<(&'a str, &'a str)> {
        self.project
            .as_ref()
            .filter(|_| valid_json_pointer(pointer))
            .map(|project| project.origin(pointer))
    }

    /// Translate a compiler or admission diagnostic to its declaring component.
    /// Errors whose pointer is a host category, such as `projectSnapshot`,
    /// name no component and are returned unchanged.
    pub fn locate_error(&self, mut error: EngineFailure) -> EngineFailure {
        if let Some((file, local)) = self
            .component_origin(&error.pointer)
            .filter(|_| error.pointer.starts_with('/'))
        {
            error.message = format!("{file}#{local}: {}", error.message);
        }
        error
    }

    /// Freeze the selected project files in private host storage. Use the
    /// same directory as [crate::host::HostContext::root] when executing a run.
    pub fn retain(&self, host_data: &Path) -> Result<RetainedSource> {
        match &self.project {
            Some(project) => snapshot::retain(host_data, &self.source_sha256(), &project.files),
            None => Ok(RetainedSource {
                location: self.origin.clone().unwrap(),
                id: None,
            }),
        }
    }
}

/// RFC 6901: empty or `/`-prefixed, and every `~` escapes `0` or `1`.
fn valid_json_pointer(pointer: &str) -> bool {
    (pointer.is_empty() || pointer.starts_with('/'))
        && pointer
            .split('~')
            .skip(1)
            .all(|rest| rest.starts_with(['0', '1']))
}

fn invalid(file: &str, pointer: &str, message: &str) -> EngineFailure {
    failure("PROJECT", pointer, &format!("{file}#{pointer}: {message}"))
}

fn source_unavailable() -> EngineFailure {
    failure(
        "SOURCE_UNAVAILABLE",
        "host",
        "Standalone input or storage is unavailable",
    )
}

fn io(path: &Path, error: std::io::Error) -> EngineFailure {
    invalid(&path.display().to_string(), "", &error.to_string())
}

pub(crate) fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn parse_json(bytes: &[u8], file: &str, project_limits: bool) -> Result<Value> {
    let error = |message: &str| invalid(file, "", message);
    let text = std::str::from_utf8(bytes).map_err(|e| error(&e.to_string()))?;
    let value = argentic_core::durable_json::value(text).map_err(|e| error(&e.to_string()))?;
    if project_limits {
        let mut pending = vec![(&value, 0)];
        while let Some((value, depth)) = pending.pop() {
            match value {
                Value::Array(items) if depth < 64 && items.len() <= 1024 => {
                    pending.extend(items.iter().map(|item| (item, depth + 1)));
                }
                Value::Object(items) if depth < 64 => {
                    pending.extend(items.values().map(|item| (item, depth + 1)));
                }
                Value::Array(_) | Value::Object(_) => {
                    return Err(error("Project JSON exceeds nesting or array limits"))
                }
                _ => {}
            }
        }
    }
    Ok(value)
}

fn open_regular(dir: &Dir, path: &Path) -> std::io::Result<cap_std::fs::File> {
    if !dir.symlink_metadata(path)?.is_file() {
        return Err(std::io::Error::other("Expected a regular file"));
    }
    let mut options = cap_std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use cap_std::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW);
    }
    let file = dir.open_with(path, &options)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other("Expected a regular file"));
    }
    Ok(file)
}

/// Open one regular local file without listing its directory.
fn open_local(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other("Expected a regular file"));
    }
    Ok(file)
}

fn read_local(path: &Path, limit: u64) -> std::io::Result<ProjectFile> {
    let file = open_local(path)?;
    let meta = file.metadata()?;
    let mut bytes = Vec::new();
    if meta.len() <= limit {
        (&file).take(limit + 1).read_to_end(&mut bytes)?;
    }
    if meta.len() > limit || bytes.len() as u64 > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::FileTooLarge,
            "File exceeds the read limit",
        ));
    }
    #[cfg(unix)]
    let executable = {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    };
    #[cfg(not(unix))]
    let executable = false;
    Ok(ProjectFile { bytes, executable })
}

fn read_regular(dir: &Dir, path: &Path, limit: u64) -> std::io::Result<ProjectFile> {
    let file = open_regular(dir, path)?;
    let meta = file.metadata()?;
    let mut bytes = Vec::new();
    if meta.len() <= limit {
        file.take(limit + 1).read_to_end(&mut bytes)?;
    }
    if meta.len() > limit || bytes.len() as u64 > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::FileTooLarge,
            "File exceeds the read limit",
        ));
    }
    #[cfg(unix)]
    let executable = {
        use cap_std::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    };
    #[cfg(not(unix))]
    let executable = false;
    Ok(ProjectFile { bytes, executable })
}

fn file_url(path: &Path) -> Result<Url> {
    Url::from_file_path(path).map_err(|_| invalid("source", "", "Cannot form a local document URI"))
}
