//! Local AgSDL input and portable archives. Opening never starts a provider,
//! resolves secret values, executes a command or retrieves an external URI.
mod archive;
#[cfg(test)]
mod archive_writer_tests;
#[cfg(test)]
mod conformance050_tests;
mod member;
mod project;
#[cfg(test)]
mod project_tests;
mod snapshot;

pub use snapshot::RetainedSource;
pub(crate) use snapshot::{McpDeclarations, Snapshot};

use crate::persistence::{
    dto::{EngineFailure, SourceDiagnostic, SourceErrorKind},
    failure, Result,
};
use cap_std::fs::Dir;
use member::open_regular;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
};
use url::Url;

use argentic_core::{SourceBudget, GRAPH_LIMIT_CODE};

const MANIFEST: &str = "agsdl.json";
const MAX_FILES: usize = 8192;
const MAX_TOTAL: u64 = 256 * 1024 * 1024;
const MAX_JSON: u64 = 1024 * 1024;
const MAX_JSON_TOTAL: u64 = 8 * MAX_JSON;

type Files = BTreeMap<String, ProjectFile>;

struct ProjectFile {
    bytes: Vec<u8>,
    executable: bool,
    digest: String,
}

impl ProjectFile {
    fn new(bytes: Vec<u8>, executable: bool) -> Self {
        let digest = sha256(&bytes);
        Self {
            bytes,
            executable,
            digest,
        }
    }
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
        Self::open_path_with_budget(path, SourceBudget::default())
    }

    /// Open a local source using the supplied operation budget.
    pub fn open_path_with_budget(path: &Path, budget: SourceBudget) -> Result<Self> {
        // Rebuild from components: with a trailing separator, `lstat` would
        // follow a symlinked project root.
        let path: PathBuf = std::path::absolute(path)
            .map_err(|e| io(path, e))?
            .components()
            .collect();
        let meta = std::fs::symlink_metadata(&path).map_err(|_| source_unavailable())?;
        if meta.is_dir() || (meta.is_symlink() && path.is_dir()) {
            return Self::assembled(project::assemble_folder_with_budget(
                open_project_root(&path)?,
                None,
                cfg!(unix),
                budget,
            )?);
        }
        let name = path
            .file_name()
            .ok_or_else(|| invalid("", "", "Expected a source file"))?;
        // A symlinked source, including a symlinked manifest, is refused.
        if path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("agsdl"))
        {
            if !meta.is_file() {
                return Err(source_unavailable());
            }
            return Self::open_archive(
                open_local(&path).map_err(|_| source_unavailable())?,
                budget,
            );
        }
        // Preserve the exact UTF-8 bytes and 8 MiB limit of standalone JSON.
        if !meta.is_file() || meta.len() > MAX_JSON_TOTAL {
            return Err(failure(
                "SOURCE_UNAVAILABLE",
                "host",
                "Expected a regular input file of at most 8 MiB",
            ));
        }
        let dir = if name == MANIFEST {
            Some(open_project_root(path.parent().unwrap())?)
        } else {
            None
        };
        let file = match &dir {
            Some(dir) => read_regular(dir, Path::new(MANIFEST), MAX_JSON_TOTAL, &budget),
            None => read_local(&path, MAX_JSON_TOTAL, &budget),
        }
        .map_err(|e| {
            if let Some(error) = read_limit(name.to_str().unwrap_or(""), &e) {
                error
            } else if e.kind() == std::io::ErrorKind::FileTooLarge {
                failure("INPUT_LIMIT", "host", "Input grew beyond 8 MiB")
            } else {
                source_unavailable()
            }
        })?;
        if let Some(dir) = dir {
            let manifest = parse_json(&file.bytes, MANIFEST, false, &budget)?;
            if manifest.get("projectFormat").is_some() {
                return Self::assembled(project::assemble_folder_with_budget(
                    dir,
                    Some(file),
                    cfg!(unix),
                    budget,
                )?);
            }
        }
        let authored = String::from_utf8(file.bytes)
            .map_err(|_| failure("PARSE", "source", "Source must be UTF-8"))?;
        validate_source_syntax(&authored, name.to_str().unwrap_or(""), &budget)?;
        Ok(Self {
            authored,
            origin: Some(path.canonicalize().map_err(|_| source_unavailable())?),
            project: None,
        })
    }

    /// Open untrusted uploaded bytes. References are resolved inside the
    /// archive only. This API never uses a client-supplied local path.
    pub fn open_archive_bytes(bytes: &[u8]) -> Result<Self> {
        Self::open_archive_bytes_with_budget(bytes, SourceBudget::default())
    }

    /// Open uploaded archive bytes using the supplied operation budget.
    pub fn open_archive_bytes_with_budget(bytes: &[u8], budget: SourceBudget) -> Result<Self> {
        Self::open_archive(Cursor::new(bytes), budget)
    }

    #[cfg(test)]
    fn folder(dir: Dir, manifest: Option<ProjectFile>) -> Result<Self> {
        Self::assembled(project::assemble_folder(dir, manifest, cfg!(unix))?)
    }

    fn open_archive(input: impl Read + std::io::Seek, budget: SourceBudget) -> Result<Self> {
        let files = archive::read_with_budget(input, &budget)?;
        Self::assembled(project::assemble_archive(files, budget)?)
    }

    fn assembled(mut project: project::Project) -> Result<Self> {
        // A folder accepts only what its archive could list, which also bounds
        // the retained index that a later run reopens.
        if archive::inventory_size(&project.files, &project.budget)? > archive::INDEX_LIMIT {
            return Err(invalid(
                MANIFEST,
                "",
                "Project inventory exceeds the 2 MiB archive limit; select fewer or shorter paths",
            ));
        }
        let authored = encode_document(&std::mem::take(&mut project.document), &project.budget)?;
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
        if error.source.is_some() {
            return error;
        }
        if let Some((file, local)) = self
            .component_origin(&error.pointer)
            .filter(|_| error.pointer.starts_with('/'))
        {
            error.message = format!("{file}#{local}: {}", error.message);
            error.source = Some(Box::new(SourceDiagnostic {
                path: file.into(),
                pointer: local.into(),
                kind: SourceErrorKind::InvalidContent,
            }));
        }
        error
    }

    /// Freeze the selected project files in private host storage. Use the
    /// same directory as [crate::host::HostContext::root] when executing a run.
    pub fn retain(&self, host_data: &Path) -> Result<RetainedSource> {
        match &self.project {
            Some(project) => {
                let mut retained =
                    snapshot::retain(host_data, &self.source_sha256(), &project.files)?;
                retained.mcp = McpDeclarations {
                    files: project.mcp_files.clone(),
                    secrets: project.secrets.clone(),
                };
                Ok(retained)
            }
            None => Ok(RetainedSource {
                location: self.origin.clone().unwrap(),
                id: None,
                mcp: McpDeclarations::default(),
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

pub(crate) fn mcp_secret_is_reserved(name: &str) -> bool {
    matches!(
        name,
        "PATH" | "ENV" | "BASH_ENV" | "BASHOPTS" | "SHELLOPTS" | "PS4"
    ) || name.starts_with("LD_")
        || name.starts_with("DYLD_")
}

fn invalid(file: &str, pointer: &str, message: &str) -> EngineFailure {
    project_failure(
        "PROJECT",
        file,
        pointer,
        SourceErrorKind::InvalidContent,
        message,
    )
}

fn project_failure(
    code: &str,
    file: &str,
    pointer: &str,
    kind: SourceErrorKind,
    message: &str,
) -> EngineFailure {
    let mut error = failure(code, pointer, &format!("{file}#{pointer}: {message}"));
    error.source = Some(Box::new(SourceDiagnostic {
        path: file.into(),
        pointer: pointer.into(),
        kind,
    }));
    error
}

fn source_unavailable() -> EngineFailure {
    failure(
        "SOURCE_UNAVAILABLE",
        "host",
        "Standalone input or storage is unavailable",
    )
}

fn io(path: &Path, error: std::io::Error) -> EngineFailure {
    project_failure(
        "PROJECT",
        &path.to_string_lossy(),
        "",
        SourceErrorKind::Io,
        &error.to_string(),
    )
}

pub(crate) fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn source_limit(file: &str, error: argentic_core::calculation::Failure) -> EngineFailure {
    project_failure(
        GRAPH_LIMIT_CODE,
        file,
        &error.pointer,
        SourceErrorKind::InvalidContent,
        &error.message,
    )
}

fn validate_source_syntax(authored: &str, file: &str, budget: &SourceBudget) -> Result<()> {
    argentic_core::check_source_json(authored, "", budget).map_err(|error| {
        if error.code == GRAPH_LIMIT_CODE {
            source_limit(file, error)
        } else {
            failure("CORRELATION", "", "Invalid execution data")
        }
    })
}

fn encode_document(value: &Value, budget: &SourceBudget) -> Result<String> {
    struct Writer<'a> {
        budget: &'a SourceBudget,
        bytes: Vec<u8>,
        limit: Option<argentic_core::calculation::Failure>,
    }
    impl Write for Writer<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Err(error) = self
                .budget
                .work(bytes.len().div_ceil(8), "")
                .and_then(|()| reserve_buffer(&mut self.bytes, bytes.len(), self.budget))
            {
                self.limit = Some(error);
                return Err(std::io::ErrorKind::FileTooLarge.into());
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        budget,
        bytes: Vec::new(),
        limit: None,
    };
    let result = serde_json::to_writer(&mut writer, value);
    if let Some(limit) = writer.limit {
        return Err(source_limit(MANIFEST, limit));
    }
    result?;
    Ok(String::from_utf8(writer.bytes).expect("JSON is UTF-8"))
}

fn parse_json(
    bytes: &[u8],
    file: &str,
    project_limits: bool,
    budget: &SourceBudget,
) -> Result<Value> {
    let error = |message: &str| invalid(file, "", message);
    let value = argentic_core::parse_source_json(bytes, budget).map_err(|failure| {
        if failure.code == GRAPH_LIMIT_CODE {
            source_limit(file, failure)
        } else {
            error(&failure.message)
        }
    })?;
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

fn open_project_root(path: &Path) -> Result<Dir> {
    let no_link = || {
        invalid(
            MANIFEST,
            "",
            "The project root cannot be a symbolic link or reparse point",
        )
    };
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let flags = libc::O_DIRECTORY | libc::O_NOFOLLOW;
        #[cfg(target_os = "linux")]
        let flags = flags | libc::O_PATH;
        options.custom_flags(flags);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
            FILE_SHARE_WRITE,
        };
        // cap-std uses paths for some Windows directory operations. Hold the
        // root without delete sharing so its name cannot be replaced.
        options
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE);
    }
    let file = options.open(path).map_err(|error| {
        if std::fs::symlink_metadata(path).is_ok_and(|meta| meta.is_symlink()) {
            no_link()
        } else {
            io(Path::new(MANIFEST), error)
        }
    })?;
    let meta = file
        .metadata()
        .map_err(|error| io(Path::new(MANIFEST), error))?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(no_link());
        }
    }
    if !meta.is_dir() {
        return Err(invalid(MANIFEST, "", "Expected a project directory"));
    }
    Ok(Dir::from_std_file(file))
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
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let file = options.open(path)?;
    let meta = file.metadata()?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Expected a file without a reparse point",
            ));
        }
    }
    if !meta.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Expected a regular file",
        ));
    }
    Ok(file)
}

// Charge the full allocation on growth, as realloc can move the buffer.
fn reserve_buffer(
    bytes: &mut Vec<u8>,
    additional: usize,
    budget: &SourceBudget,
) -> std::result::Result<(), argentic_core::calculation::Failure> {
    let needed = bytes.len() + additional;
    if needed > bytes.capacity() {
        let capacity = needed.max(bytes.capacity() * 2);
        budget.bytes(capacity, "")?;
        bytes.reserve_exact(capacity - bytes.len());
    }
    Ok(())
}

fn read_limit(file: &str, error: &std::io::Error) -> Option<EngineFailure> {
    error
        .get_ref()?
        .downcast_ref::<argentic_core::calculation::Failure>()
        .map(|error| source_limit(file, error.clone()))
}

fn budget_io(error: argentic_core::calculation::Failure) -> std::io::Error {
    std::io::Error::other(error)
}

fn bounded_read(
    mut file: impl Read,
    size: u64,
    limit: u64,
    budget: &SourceBudget,
) -> std::io::Result<Vec<u8>> {
    let too_large = || {
        std::io::Error::new(
            std::io::ErrorKind::FileTooLarge,
            "File exceeds the read limit",
        )
    };
    if size > limit {
        return Err(too_large());
    }
    let mut bytes = Vec::new();
    // The observed size normally gives one allocation. Continue with charged
    // chunks if a concurrently edited file has grown since metadata was read.
    budget
        .work((size as usize).div_ceil(256), "")
        .map_err(budget_io)?;
    reserve_buffer(&mut bytes, size as usize, budget).map_err(budget_io)?;
    let mut chunk = [0; 8192];
    loop {
        let take = chunk.len().min((limit + 1 - bytes.len() as u64) as usize);
        let count = match file.read(&mut chunk[..take]) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            return Ok(bytes);
        }
        let end = bytes.len() + count;
        if end as u64 > limit {
            return Err(too_large());
        }
        let growth = end.saturating_sub((size as usize).max(bytes.len()));
        budget.work(growth.div_ceil(256), "").map_err(budget_io)?;
        reserve_buffer(&mut bytes, count, budget).map_err(budget_io)?;
        bytes.extend_from_slice(&chunk[..count]);
    }
}

fn read_local(path: &Path, limit: u64, budget: &SourceBudget) -> std::io::Result<ProjectFile> {
    let file = open_local(path)?;
    let meta = file.metadata()?;
    let bytes = bounded_read(file, meta.len(), limit, budget)?;
    Ok(ProjectFile::new(bytes, false))
}

fn read_regular(
    dir: &Dir,
    path: &Path,
    limit: u64,
    budget: &SourceBudget,
) -> std::io::Result<ProjectFile> {
    let file = open_regular(dir, path)?;
    let meta = file.metadata()?;
    let bytes = bounded_read(file, meta.len(), limit, budget)?;
    #[cfg(unix)]
    let executable = {
        use cap_std::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    };
    #[cfg(not(unix))]
    let executable = false;
    Ok(ProjectFile::new(bytes, executable))
}

fn file_url(path: &Path) -> Result<Url> {
    Url::from_file_path(path).map_err(|_| invalid("source", "", "Cannot form a local document URI"))
}
