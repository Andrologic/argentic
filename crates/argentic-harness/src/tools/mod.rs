//! Direct, read-only tools. All model paths resolve beneath an admitted root.

mod confined;
mod output;
mod submission;
pub use output::Page;

use std::{
    io::{self, Read},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use cap_std::{
    ambient_authority,
    fs::{Dir, File, OpenOptions},
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    policy::{slash_path, PathGlob, ReadPolicy},
    providers::{ToolCall, ToolDefinition},
};

pub const SUBMIT_RESULT: &str = "agsdl_submit_result";
const MAX_FILE_BYTES: usize = 8 * 1024 * 1024;
const MAX_SCAN_BYTES: usize = 64 * 1024 * 1024;
const MAX_ENTRIES: usize = 20_000;
const MAX_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exposure {
    Direct,
}

struct Root {
    path: PathBuf,
    directory: Dir,
}

pub struct Registry {
    roots: Vec<Root>,
    policy: Vec<ReadPolicy>,
    submission: jsonschema::Validator,
    definitions: Vec<ToolDefinition>,
}

impl Registry {
    /// Root zero is the workspace. Further roots must be authorized and
    /// supplied by the host. The schema covers the submission envelope
    /// (results, choice and instructions for the current step).
    pub fn new(
        roots: &[PathBuf],
        policy: ReadPolicy,
        submission_schema: Value,
    ) -> Result<Self, String> {
        let roots = roots
            .iter()
            .map(|path| {
                let path = path.canonicalize()?;
                let directory = Dir::open_ambient_dir(&path, ambient_authority())?;
                Ok((path, directory))
            })
            .collect::<io::Result<_>>()
            .map_err(|e| e.to_string())?;
        Self::with_open_roots(roots, vec![policy], submission_schema)
    }

    /// Use directory handles already authenticated by the embedding host.
    /// Each independent read ceiling must allow the path.
    pub fn with_open_roots(
        roots: Vec<(PathBuf, Dir)>,
        policy: Vec<ReadPolicy>,
        submission_schema: Value,
    ) -> Result<Self, String> {
        if roots.is_empty() || policy.is_empty() {
            return Err("At least one workspace root and read ceiling are required".into());
        }
        let roots = roots
            .into_iter()
            .map(|(path, directory)| Root { path, directory })
            .collect();
        let submission =
            jsonschema::validator_for(&submission_schema).map_err(|e| e.to_string())?;
        let mut definitions = Vec::new();
        for (name, description, required) in [
            (
                "read",
                "Read a UTF-8 file relative to the selected workspace root.",
                vec!["path"],
            ),
            (
                "list",
                "List the readable entries of a workspace directory.",
                vec![],
            ),
            (
                "glob",
                "Find readable paths matching a glob relative to the selected directory.",
                vec!["pattern"],
            ),
            (
                "grep",
                "Search readable UTF-8 files for a regular expression.",
                vec!["pattern"],
            ),
        ] {
            definitions.push(ToolDefinition {
                name: name.into(),
                description: description.into(),
                parameters: json!({
                    "type": "object", "required": required, "additionalProperties": false,
                    "properties": {
                        "path": {"type":"string"},
                        "root": {"type":"integer","minimum":0},
                        "pattern": {"type":"string"},
                        "cursor": {"type":"string"},
                        "limit": {"type":"integer","minimum":1,"maximum":2000}
                    }
                }),
            });
        }
        definitions.push(ToolDefinition {
            name: SUBMIT_RESULT.into(),
            description:
                "Submit the work result. Correct any validation error in this conversation.".into(),
            parameters: submission_schema,
        });
        Ok(Self {
            roots,
            policy,
            submission,
            definitions,
        })
    }

    fn allows(&self, root: &Root, path: &Path) -> bool {
        !private_project_path(&root.path, path)
            && self.policy.iter().all(|policy| policy.allows(path, true))
    }

    pub fn definitions(&self) -> impl Iterator<Item = (Exposure, &ToolDefinition)> {
        self.definitions.iter().map(|def| (Exposure::Direct, def))
    }

    pub fn validate_submission(&self, arguments: &str) -> Result<Value, String> {
        submission::validate(&self.submission, arguments)
    }

    pub async fn read_tool(
        self: &Arc<Self>,
        call: ToolCall,
        cancel: CancellationToken,
    ) -> Result<Page, String> {
        let tracker = TaskTracker::new();
        let output = self.execute(call, cancel, &tracker).await;
        tracker.close();
        tracker.wait().await;
        output
    }

    pub(crate) async fn execute(
        self: &Arc<Self>,
        call: ToolCall,
        cancel: CancellationToken,
        tracker: &TaskTracker,
    ) -> Result<Page, String> {
        let registry = self.clone();
        tracker
            .spawn_blocking(move || registry.read_sync(&call, &cancel))
            .await
            .map_err(|e| e.to_string())?
    }

    fn read_sync(&self, call: &ToolCall, cancel: &CancellationToken) -> Result<Page, String> {
        let args: Arguments = serde_json::from_str(&call.arguments).map_err(|e| e.to_string())?;
        let root = self.roots.get(args.root).ok_or("Unknown workspace root")?;
        let raw_path = args.path.as_deref().unwrap_or(".");
        let path = relative_path(raw_path).map_err(|e| e.to_string())?;
        if !self.allows(
            root,
            &if path.as_os_str().is_empty() {
                root.path.clone()
            } else {
                root.path.join(&path)
            },
        ) {
            return Err("Read permission denied".into());
        }
        let text = match call.name.as_str() {
            "read" if args.path.is_some() => self
                .read_one(root, &path, cancel)
                .map(|text| text.replace("\r\n", "\n")),
            "list" => self.scan(root, &path, Search::List, cancel),
            "glob" => {
                let pattern = args.pattern.as_deref().ok_or("glob requires pattern")?;
                let glob = PathGlob::compile(pattern).map_err(|e| e.to_string())?;
                self.scan(root, &path, Search::Glob(glob), cancel)
            }
            "grep" => {
                let pattern = args
                    .pattern
                    .as_deref()
                    .filter(|pattern| !pattern.is_empty())
                    .ok_or("grep requires a nonempty pattern")?;
                let pattern = regex::Regex::new(pattern).map_err(|e| e.to_string())?;
                self.scan(root, &path, Search::Grep(pattern), cancel)
            }
            _ => return Err("Unknown read tool or missing arguments".into()),
        }
        .map_err(|e| e.to_string())?;
        let scope = json!([call.name, args.root, raw_path, args.pattern]).to_string();
        check_cancel(cancel).map_err(|e| e.to_string())?;
        output::paginate(&text, &scope, args.cursor.as_deref(), args.limit)
            .map_err(|e| e.to_string())
    }

    fn verify_location(&self, root: &Root, relative: &Path, actual: PathBuf) -> io::Result<()> {
        let expected = if relative.as_os_str().is_empty() {
            root.path.clone()
        } else {
            root.path.join(relative)
        };
        if actual.as_os_str() != expected.as_os_str() || !self.allows(root, &actual) {
            return Err(denied());
        }
        Ok(())
    }

    fn read_one(&self, root: &Root, path: &Path, cancel: &CancellationToken) -> io::Result<String> {
        let file = confined::file(&root.directory, path)?;
        self.read_open_file(root, path, file, cancel)
    }

    fn read_open_file(
        &self,
        root: &Root,
        path: &Path,
        mut file: File,
        cancel: &CancellationToken,
    ) -> io::Result<String> {
        self.verify_location(root, path, confined::location(&file)?)?;
        let text = file_text(&mut file, cancel)?;
        self.verify_location(root, path, confined::location(&file)?)?;
        Ok(text)
    }

    fn scan(
        &self,
        root: &Root,
        path: &Path,
        search: Search,
        cancel: &CancellationToken,
    ) -> io::Result<String> {
        let directory = confined::directory(&root.directory, path)?;
        self.verify_location(root, path, confined::location(&directory)?)?;
        let mut stack = vec![(path.to_path_buf(), directory.entries()?, directory)];
        let mut records = Vec::new();
        let mut entries = 0;
        let mut bytes_read = 0;
        let mut output_bytes = 0;
        loop {
            let depth = stack.len();
            let Some((parent, iter, directory)) = stack.last_mut() else {
                break;
            };
            check_cancel(cancel)?;
            let entry = iter.next();
            self.verify_location(root, parent, confined::location(directory)?)?;
            let Some(entry) = entry else {
                stack.pop();
                continue;
            };
            entries += 1;
            if entries > MAX_ENTRIES {
                return Err(io::Error::other(
                    "Search entry budget exceeded. Select a narrower directory",
                ));
            }
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str().and_then(|name| relative_path(name).ok()) else {
                continue;
            };
            let child = parent.join(&name);
            if !self.allows(root, &root.path.join(&child)) {
                continue;
            }
            let kind = entry.file_type()?;
            self.verify_location(root, parent, confined::location(directory)?)?;
            if !kind.is_file() && !kind.is_dir() {
                continue;
            }
            let Some(display) = slash_path(&child) else {
                continue;
            };
            let relative = child
                .strip_prefix(path)
                .ok()
                .and_then(slash_path)
                .ok_or_else(denied)?;
            match &search {
                Search::List => records.push((display, 0, String::new())),
                Search::Glob(glob) if glob.matches(&relative) => {
                    records.push((display, 0, String::new()))
                }
                Search::Grep(pattern) if kind.is_file() => {
                    let Ok(content) = self.read_one(root, &child, cancel) else {
                        continue;
                    };
                    bytes_read += content.len();
                    if bytes_read > MAX_SCAN_BYTES {
                        return Err(io::Error::other(
                            "Search byte budget exceeded. Select a narrower directory",
                        ));
                    }
                    for (index, line) in content.lines().enumerate() {
                        check_cancel(cancel)?;
                        if pattern.is_match(line) {
                            output_bytes += display.len() + line.len() + 24;
                            if output_bytes > MAX_FILE_BYTES || records.len() >= MAX_ENTRIES {
                                return Err(io::Error::other(
                                    "Search output budget exceeded. Narrow the pattern",
                                ));
                            }
                            records.push((display.clone(), index + 1, line.into()));
                        }
                    }
                }
                _ => {}
            }
            if kind.is_dir() && !matches!(search, Search::List) {
                if depth > MAX_DEPTH {
                    return Err(io::Error::other("Search depth budget exceeded"));
                }
                if let Ok(dir) = confined::directory(&root.directory, &child) {
                    self.verify_location(root, &child, confined::location(&dir)?)?;
                    stack.push((child, dir.entries()?, dir));
                }
            }
        }
        records.sort_unstable();
        let mut text = String::new();
        for (path, line, content) in records {
            check_cancel(cancel)?;
            use std::fmt::Write;
            let formatted = if line == 0 {
                writeln!(text, "{path}")
            } else {
                writeln!(text, "{path}:{line}:{content}")
            };
            formatted.map_err(io::Error::other)?;
        }
        Ok(text)
    }
}

/// Whether a project-relative component names private host metadata.
pub fn private_project_path(root: &Path, path: &Path) -> bool {
    path.strip_prefix(root).is_ok_and(|relative| {
        relative.components().any(|part| {
            part.as_os_str().to_str().is_none_or(|name| {
                matches!(
                    name.to_ascii_lowercase().as_str(),
                    ".macro" | ".git" | ".aws" | ".codex" | "auth.json" | "credentials"
                )
            })
        })
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    #[serde(default)]
    root: usize,
    path: Option<String>,
    pattern: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

enum Search {
    List,
    Glob(PathGlob),
    Grep(regex::Regex),
}

fn denied() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "Path is outside the admitted workspace or uses an unsupported alias",
    )
}

fn relative_path(raw: &str) -> io::Result<PathBuf> {
    if raw.is_empty() || raw.starts_with('/') || raw.contains(['\\', ':', '\0']) {
        return Err(denied());
    }
    let mut path = PathBuf::new();
    for component in Path::new(raw).components() {
        match component {
            Component::Normal(part) => {
                if cfg!(windows) && part.to_string_lossy().ends_with(['.', ' ']) {
                    return Err(denied());
                }
                path.push(part);
            }
            Component::CurDir => {}
            _ => return Err(denied()),
        }
    }
    Ok(path)
}

fn check_cancel(cancel: &CancellationToken) -> io::Result<()> {
    if cancel.is_cancelled() {
        return Err(io::Error::other("Read cancelled"));
    }
    Ok(())
}

fn file_text(file: &mut File, cancel: &CancellationToken) -> io::Result<String> {
    let mut bytes = Vec::new();
    let mut chunk = [0; 16 * 1024];
    loop {
        check_cancel(cancel)?;
        let len = file.read(&mut chunk)?;
        if len == 0 {
            break;
        }
        if bytes.len() + len > MAX_FILE_BYTES {
            return Err(io::Error::other("File exceeds the 8 MiB read budget"));
        }
        bytes.extend_from_slice(&chunk[..len]);
    }
    String::from_utf8(bytes).map_err(io::Error::other)
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos", windows)))]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::policy::Preset;

    #[test]
    fn refuses_an_open_file_moved_after_open() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let relative = Path::new("allowed/memo.txt");
        std::fs::create_dir_all(workspace.join("allowed")).unwrap();
        std::fs::write(workspace.join(relative), "memo").unwrap();
        let workspace = workspace.canonicalize().unwrap();
        let policy =
            ReadPolicy::compile(&workspace, None, Some(Preset::ReadOnly), None, &[]).unwrap();
        let registry = Registry::new(std::slice::from_ref(&workspace), policy, json!({})).unwrap();
        let root = &registry.roots[0];
        let file = confined::file(&root.directory, relative).unwrap();
        std::fs::rename(workspace.join("allowed"), temp.path().join("moved")).unwrap();
        let error = registry
            .read_open_file(root, relative, file, &CancellationToken::new())
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }
}
