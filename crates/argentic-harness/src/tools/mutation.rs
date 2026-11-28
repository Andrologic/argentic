use super::{
    check_cancel, confined, denied, file_text, model_path,
    patch::{
        apply_patch_hunks_to_content, join_text_lines, parse_apply_patch, ParsedPatchOperation,
        MAX_CONTEXT_SEARCH,
    },
    private_project_path, Root, MAX_FILE_BYTES,
};
use crate::policy::Action;
use cap_fs_ext::{FollowSymlinks, MetadataExt, OpenOptionsFollowExt};
use cap_std::fs::{Dir, Metadata, OpenOptions};
#[cfg(any(
    all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ),
    all(target_os = "macos", target_arch = "aarch64")
))]
use ring::digest::{digest, Digest, SHA256};
use serde::Deserialize;
use std::{
    collections::{BTreeSet, HashMap},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock, Mutex, Weak,
    },
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

pub(super) enum Mutation {
    Write {
        path: PathBuf,
        content: String,
    },
    Edit {
        path: PathBuf,
        old: String,
        new: String,
        all: bool,
    },
    Patch(Vec<ParsedPatchOperation>),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteArgs {
    #[serde(default)]
    root: usize,
    path: String,
    content: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditArgs {
    #[serde(default)]
    root: usize,
    path: String,
    old_text: String,
    new_text: String,
    #[serde(default)]
    replace_all: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchArgs {
    #[serde(default)]
    root: usize,
    patch_text: String,
}

fn file_path(raw: &str) -> Result<PathBuf, String> {
    let path = model_path(raw).map_err(|error| error.to_string())?;
    if path.as_os_str().is_empty() {
        return Err("A file path is required".into());
    }
    Ok(path)
}

impl Mutation {
    pub(super) fn parse(name: &str, args: &str) -> Result<(usize, Self), String> {
        let parse_error = |error: serde_json::Error| error.to_string();
        if args.len() > MAX_FILE_BYTES {
            return Err("Mutation exceeds the 8 MiB input budget".into());
        }
        Ok(match name {
            "write" => {
                let args: WriteArgs = serde_json::from_str(args).map_err(parse_error)?;
                (
                    args.root,
                    Self::Write {
                        path: file_path(&args.path)?,
                        content: args.content,
                    },
                )
            }
            "edit" => {
                let args: EditArgs = serde_json::from_str(args).map_err(parse_error)?;
                if args.old_text.is_empty() {
                    return Err("old_text must be nonempty".into());
                }
                (
                    args.root,
                    Self::Edit {
                        path: file_path(&args.path)?,
                        old: args.old_text,
                        new: args.new_text,
                        all: args.replace_all,
                    },
                )
            }
            "apply_patch" => {
                let args: PatchArgs = serde_json::from_str(args).map_err(parse_error)?;
                let ops = parse_apply_patch(&args.patch_text)?;
                if ops.len() > 128 {
                    return Err("A patch may touch at most 128 source files".into());
                }
                (args.root, Self::Patch(ops))
            }
            _ => return Err("Unknown mutation tool".into()),
        })
    }

    pub(super) fn requests(&self) -> Result<Vec<(PathBuf, Action)>, String> {
        let mut actions = Vec::new();
        let mut paths = BTreeSet::new();
        let mut add = |path: PathBuf, read: bool| {
            if !paths.insert(path.clone()) {
                return Err("A patch must touch each path at most once".to_owned());
            }
            if read {
                actions.push((path.clone(), Action::Read));
            }
            actions.push((path, Action::Write));
            Ok(())
        };
        match self {
            Self::Write { path, .. } => add(path.clone(), false)?,
            Self::Edit { path, .. } => add(path.clone(), true)?,
            Self::Patch(operations) => {
                for operation in operations {
                    add(
                        file_path(operation.path())?,
                        matches!(operation, ParsedPatchOperation::Update { .. }),
                    )?;
                    if let ParsedPatchOperation::Update {
                        move_to: Some(path),
                        ..
                    } = operation
                    {
                        add(file_path(path)?, false)?;
                    }
                }
            }
        }
        Ok(actions)
    }

    pub(super) fn run(self, root: &Root, cancel: &CancellationToken) -> Result<String, String> {
        let changes = self.plan(root, cancel).map_err(|e| e.to_string())?;
        for (index, change) in changes.iter().enumerate() {
            if let Err(error) = change.apply(root, cancel) {
                return Err(format!(
                    "Applied {index} of {} file changes. {error}",
                    changes.len()
                ));
            }
        }
        Ok(format!("Applied {} file change(s).", changes.len()))
    }

    fn plan(self, root: &Root, cancel: &CancellationToken) -> io::Result<Vec<Change>> {
        check_cancel(cancel)?;
        let mut changes = Vec::new();
        match self {
            Self::Write { path, content } => {
                changes.push(Target::open(root, path)?.change(Some(content)));
            }
            Self::Edit {
                path,
                old,
                new,
                all,
            } => {
                let target = Target::open(root, path)?;
                let text = target.read(root, cancel)?;
                let count = text.matches(&old).count();
                if count == 0 || (count != 1 && !all) {
                    return Err(io::Error::other(format!("old_text matched {count} locations. Supply one exact match or enable replace_all.")));
                }
                let bytes = text
                    .len()
                    .saturating_sub(count * old.len())
                    .saturating_add(count.saturating_mul(new.len()));
                check_size(bytes)?;
                changes.push(target.change(Some(text.replace(&old, &new))));
            }
            Self::Patch(ops) => {
                let mut total = 0usize;
                let mut remaining = MAX_CONTEXT_SEARCH;
                for op in ops {
                    check_cancel(cancel)?;
                    let target =
                        Target::open(root, file_path(op.path()).map_err(io::Error::other)?)?;
                    match op {
                        ParsedPatchOperation::Add { lines, .. } => {
                            target.require_absent()?;
                            changes.push(
                                target.change(Some(join_text_lines(&lines, !lines.is_empty()))),
                            );
                        }
                        ParsedPatchOperation::Delete { .. } => {
                            target.require_existing()?;
                            changes.push(target.change(None));
                        }
                        ParsedPatchOperation::Update {
                            path,
                            hunks,
                            move_to,
                        } => {
                            let text = target.read(root, cancel)?;
                            let updated = apply_patch_hunks_to_content(
                                &path,
                                &text,
                                &hunks,
                                &mut remaining,
                                cancel,
                            )
                            .map_err(io::Error::other)?;
                            total = total.saturating_add(updated.len());
                            check_size(total)?;
                            if let Some(destination) = move_to {
                                let destination = Target::open(
                                    root,
                                    file_path(&destination).map_err(io::Error::other)?,
                                )?;
                                destination.require_absent()?;
                                let mut change = destination.change(Some(updated));
                                change.mode = target.before.as_ref().map(Metadata::permissions);
                                changes.push(change);
                                changes.push(target.change(None));
                            } else {
                                changes.push(target.change(Some(updated)));
                            }
                        }
                    }
                }
            }
        }
        Ok(changes)
    }
}

type FileLock = AsyncMutex<()>;
static LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<FileLock>>>> = LazyLock::new(Mutex::default);

pub(super) async fn lock_paths(
    paths: BTreeSet<PathBuf>,
    cancel: &CancellationToken,
) -> Result<Vec<OwnedMutexGuard<()>>, String> {
    let locks = {
        let mut map = LOCKS.lock().map_err(|_| "Mutation queue is unavailable")?;
        map.retain(|_, lock| lock.strong_count() > 0);
        paths
            .into_iter()
            .map(|path| {
                let lock = map.entry(path).or_default();
                lock.upgrade().unwrap_or_else(|| {
                    let fresh = Arc::new(FileLock::new(()));
                    *lock = Arc::downgrade(&fresh);
                    fresh
                })
            })
            .collect::<Vec<_>>()
    };
    let mut guards = Vec::with_capacity(locks.len());
    for lock in locks {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err("Mutation cancelled".into()),
            guard = lock.lock_owned() => guards.push(guard),
        }
    }
    Ok(guards)
}

pub(super) struct Target {
    relative: PathBuf,
    parent: Metadata,
    before: Option<Metadata>,
    #[cfg(any(
        all(
            target_os = "linux",
            any(target_arch = "aarch64", target_arch = "x86_64")
        ),
        all(target_os = "macos", target_arch = "aarch64")
    ))]
    digest: Option<Digest>,
}

impl Target {
    pub(super) fn open(root: &Root, relative: PathBuf) -> io::Result<Self> {
        if private_project_path(&root.path, &root.path.join(&relative)) {
            return Err(denied());
        }
        let parent_path = relative.parent().ok_or_else(denied)?;
        let parent = confined::directory(&root.directory, parent_path)?;
        root.verify_location(parent_path, confined::location(&parent)?)?;
        let name = Path::new(relative.file_name().ok_or_else(denied)?);
        let before = metadata(&parent, name)?;
        Ok(Self {
            relative,
            parent: parent.dir_metadata()?,
            before,
            #[cfg(any(
                all(
                    target_os = "linux",
                    any(target_arch = "aarch64", target_arch = "x86_64")
                ),
                all(target_os = "macos", target_arch = "aarch64")
            ))]
            digest: None,
        })
    }

    fn name(&self) -> io::Result<&Path> {
        self.relative.file_name().map(Path::new).ok_or_else(denied)
    }

    pub(super) fn require_absent(&self) -> io::Result<()> {
        if self.before.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Patch destination exists",
            ));
        }
        Ok(())
    }

    fn require_existing(&self) -> io::Result<()> {
        if self.before.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Patch source is absent",
            ));
        }
        Ok(())
    }

    fn read(&self, root: &Root, cancel: &CancellationToken) -> io::Result<String> {
        self.revalidate(root)?;
        let mut file = confined::file(&root.directory, &self.relative)?;
        root.verify_location(&self.relative, confined::location(&file)?)?;
        let before = self.before.as_ref().ok_or_else(denied)?;
        if !same_file(before, &file.metadata()?)? {
            return Err(io::Error::other("Mutation target changed concurrently"));
        }
        let text = file_text(&mut file, cancel)?;
        root.verify_location(&self.relative, confined::location(&file)?)?;
        if !same_file(before, &file.metadata()?)? {
            return Err(io::Error::other("Mutation target changed concurrently"));
        }
        Ok(text)
    }

    #[cfg(any(
        all(
            target_os = "linux",
            any(target_arch = "aarch64", target_arch = "x86_64")
        ),
        all(target_os = "macos", target_arch = "aarch64")
    ))]
    pub(super) fn from_snapshot(
        relative: PathBuf,
        parent: Metadata,
        before: Metadata,
        digest: Digest,
    ) -> Self {
        Self {
            relative,
            parent,
            before: Some(before),
            digest: Some(digest),
        }
    }

    fn parent_path(&self) -> io::Result<&Path> {
        self.relative.parent().ok_or_else(denied)
    }

    fn change(self, content: Option<String>) -> Change {
        self.bytes(content.map(String::into_bytes))
    }

    pub(super) fn bytes(self, content: Option<Vec<u8>>) -> Change {
        let mode = self.before.as_ref().map(Metadata::permissions);
        Change {
            target: self,
            content,
            mode,
        }
    }

    pub(super) fn revalidate(&self, root: &Root) -> io::Result<Dir> {
        let parent_path = self.parent_path()?;
        let current_parent = confined::directory(&root.directory, parent_path)?;
        root.verify_location(parent_path, confined::location(&current_parent)?)?;
        self.check_parent(&current_parent)?;
        Ok(current_parent)
    }

    fn check_parent(&self, current_parent: &Dir) -> io::Result<()> {
        let current = metadata(current_parent, self.name()?)?;
        let unchanged = match (&self.before, &current) {
            (None, None) => true,
            (Some(before), Some(current)) => same_file(before, current)?,
            _ => false,
        };
        if !same_inode(&self.parent, &current_parent.dir_metadata()?) || !unchanged {
            return Err(io::Error::other("Mutation target changed concurrently"));
        }
        #[cfg(any(
            all(
                target_os = "linux",
                any(target_arch = "aarch64", target_arch = "x86_64")
            ),
            all(target_os = "macos", target_arch = "aarch64")
        ))]
        if let (Some(expected), Some(before)) = (&self.digest, &self.before) {
            use std::io::Read;

            let mut file = confined::file(current_parent, self.name()?)?;
            let changed = || io::Error::other("Mutation target changed concurrently");
            if !same_file(before, &file.metadata()?)? {
                return Err(changed());
            }
            let mut content = Vec::with_capacity(before.len() as usize);
            (&mut file)
                .take(before.len() + 1)
                .read_to_end(&mut content)?;
            if digest(&SHA256, &content).as_ref() != expected.as_ref()
                || !same_file(before, &file.metadata()?)?
            {
                return Err(changed());
            }
        }
        Ok(())
    }
}

fn same_inode(a: &Metadata, b: &Metadata) -> bool {
    a.dev() == b.dev() && a.ino() == b.ino()
}

fn same_file(a: &Metadata, b: &Metadata) -> io::Result<bool> {
    #[cfg(unix)]
    {
        use cap_fs_ext::OsMetadataExt;

        if a.ctime() != b.ctime() || a.ctime_nsec() != b.ctime_nsec() {
            return Ok(false);
        }
    }
    Ok(same_inode(a, b) && a.len() == b.len() && a.modified()? == b.modified()?)
}

fn metadata(parent: &Dir, name: &Path) -> io::Result<Option<Metadata>> {
    match parent.symlink_metadata(name) {
        Ok(meta) if meta.is_file() && meta.nlink() == 1 => {
            confined::check_spelling(parent, name)?;
            Ok(Some(meta))
        }
        Ok(_) => Err(denied()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub(super) struct Change {
    target: Target,
    content: Option<Vec<u8>>,
    pub(super) mode: Option<cap_std::fs::Permissions>,
}

fn check_size(len: usize) -> io::Result<()> {
    if len > MAX_FILE_BYTES {
        return Err(io::Error::other("Mutation exceeds the 8 MiB output budget"));
    }
    Ok(())
}

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

impl Change {
    pub(super) fn apply(&self, root: &Root, cancel: &CancellationToken) -> io::Result<()> {
        let target = &self.target;
        let name = target.name()?;
        let parent_path = target.parent_path()?;
        check_cancel(cancel)?;
        let parent = target.revalidate(root)?;
        let Some(content) = &self.content else {
            root.verify_location(parent_path, confined::location(&parent)?)?;
            // Revalidation can hash the target; honour a Stop that arrived meanwhile.
            check_cancel(cancel)?;
            return parent.remove_file(name);
        };
        check_size(content.len())?;
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .follow(FollowSymlinks::No);
        #[cfg(unix)]
        {
            use cap_std::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let (temp, mut file) = loop {
            let seq = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let temp = format!(".argentic-write-{}-{seq}", std::process::id());
            root.verify_location(parent_path, confined::location(&parent)?)?;
            match parent.open_with(&temp, &options) {
                Ok(file) => break (temp, file),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => check_cancel(cancel)?,
                Err(error) => return Err(error),
            }
        };
        let result = (|| {
            let temp_path = parent_path.join(&temp);
            root.verify_location(&temp_path, confined::location(&file)?)?;
            file.write_all(content)?;
            if let Some(mode) = &self.mode {
                let mode = mode.clone();
                #[cfg(unix)]
                let mode = {
                    use cap_std::fs::PermissionsExt;
                    cap_std::fs::Permissions::from_mode(mode.mode() & 0o777)
                };
                file.set_permissions(mode)?;
            }
            file.sync_all()?;
            target.check_parent(&parent)?;
            check_cancel(cancel)?;
            root.verify_location(&temp_path, confined::location(&file)?)?;
            root.verify_location(parent_path, confined::location(&parent)?)?;
            if target.before.is_none() {
                parent.hard_link(&temp, &parent, name)
            } else {
                parent.rename(&temp, &parent, name)
            }
        })();
        drop(file);
        let cleanup = parent.remove_file(&temp);
        result?;
        if cleanup
            .as_ref()
            .is_err_and(|error| error.kind() != io::ErrorKind::NotFound)
        {
            cleanup?;
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use cap_std::ambient_authority;

    #[test]
    fn refuses_publication_when_a_parent_is_renamed_away() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().canonicalize().unwrap().join("workspace");
        std::fs::create_dir_all(path.join("parent")).unwrap();
        std::fs::write(path.join("parent/file"), "old").unwrap();
        let root = Root {
            directory: Dir::open_ambient_dir(&path, ambient_authority()).unwrap(),
            path,
        };
        let target = Target::open(&root, "parent/file".into()).unwrap();
        let moved = tmp.path().join("moved");
        std::fs::rename(root.path.join("parent"), &moved).unwrap();
        std::fs::create_dir(root.path.join("parent")).unwrap();
        let change = target.change(Some("new".into()));
        assert!(change.apply(&root, &CancellationToken::new()).is_err());
        assert_eq!(std::fs::read_to_string(moved.join("file")).unwrap(), "old");
        assert!(!root.path.join("parent/file").exists());
    }
}
