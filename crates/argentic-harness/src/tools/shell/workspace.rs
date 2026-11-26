//! Give Bash fresh inodes. Inode aliases on a live read-write bind mount can
//! mutate or disclose files outside the authorized workspace.
use super::super::{
    check_cancel, confined, denied, mutation::Target, relative_path, Root, MAX_DEPTH, MAX_ENTRIES,
    MAX_FILE_BYTES, MAX_SCAN_BYTES,
};
use cap_fs_ext::{DirExt, FollowSymlinks, MetadataExt, OpenOptionsFollowExt};
use cap_std::{
    ambient_authority,
    fs::{Dir, Metadata, OpenOptions, OpenOptionsExt, PermissionsExt as _},
};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

type Original = BTreeMap<PathBuf, (Target, Vec<u8>, u32)>;

pub(super) struct Snapshot {
    staging: tempfile::TempDir,
    original: Original,
}

impl Snapshot {
    pub(super) fn new(
        root: &Root,
        staging_parent: Option<&Path>,
        cancel: &CancellationToken,
        readable: impl FnMut(&Path) -> bool,
    ) -> io::Result<Self> {
        let staging = if let Some(parent) = staging_parent {
            let parent = parent.canonicalize()?;
            if parent.starts_with(&root.path) {
                return Err(io::Error::other(
                    "Shell staging directory must be outside the workspace",
                ));
            }
            tempfile::tempdir_in(parent)?
        } else {
            tempfile::tempdir()?
        };
        let mut original = BTreeMap::new();
        walk(root, cancel, readable, false, |path, entry| {
            match entry {
                Entry::Directory => fs::create_dir(staging.path().join(path))?,
                Entry::File(bytes, opened, parent) => {
                    let mode = opened.permissions().mode() & 0o777;
                    let target =
                        Target::from_snapshot(path.to_owned(), parent.clone(), opened.clone());
                    let path = path.to_owned();
                    original.insert(path, (target, bytes, mode));
                }
            }
            Ok(())
        })?;
        Self::populate(staging.path(), &original, cancel)?;
        Ok(Self { staging, original })
    }

    fn populate(staging: &Path, original: &Original, cancel: &CancellationToken) -> io::Result<()> {
        let files: Vec<_> = original.iter().collect();
        let workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(4);
        let batch = files.len().div_ceil(workers).max(1);
        std::thread::scope(|scope| {
            let tasks: Vec<_> = files
                .chunks(batch)
                .map(|files| {
                    scope.spawn(move || -> io::Result<()> {
                        for (path, (_, bytes, mode)) in files {
                            check_cancel(cancel)?;
                            let mut dest = fs::OpenOptions::new()
                                .write(true)
                                .create_new(true)
                                .open(staging.join(path))?;
                            dest.write_all(bytes)?;
                            dest.set_permissions(fs::Permissions::from_mode(*mode))?;
                        }
                        Ok(())
                    })
                })
                .collect();
            for task in tasks {
                task.join()
                    .map_err(|_| io::Error::other("Shell workspace copy failed"))??;
            }
            Ok(())
        })
    }

    pub(super) fn path(&self) -> &Path {
        self.staging.path()
    }

    pub(super) fn publish(
        mut self,
        root: &Root,
        cancel: &CancellationToken,
        allowed: impl Fn(&Path) -> bool,
    ) -> io::Result<()> {
        let path = self.staging.path().canonicalize()?;
        let staging = Root {
            directory: Dir::open_ambient_dir(&path, ambient_authority())?,
            path,
        };
        let mut changes = Vec::new();
        walk(
            &staging,
            cancel,
            |_| true,
            true,
            |path, entry| {
                let Entry::File(bytes, opened, _) = entry else {
                    return Ok(());
                };
                let mode = opened.permissions().mode() & 0o777;
                let original = self.original.remove(path);
                if original
                    .as_ref()
                    .is_some_and(|(_, old, old_mode)| old == &bytes && *old_mode == mode)
                {
                    return Ok(());
                }
                if !allowed(path) {
                    return Err(denied());
                }
                let target = match original {
                    Some((target, _, _)) => target,
                    None => {
                        let target = Target::open(root, path.to_owned())?;
                        target.require_absent()?;
                        target
                    }
                };
                target.revalidate(root)?;
                let mut change = target.bytes(Some(bytes));
                change.mode = Some(cap_std::fs::Permissions::from_std(
                    fs::Permissions::from_mode(mode),
                ));
                changes.push(change);
                Ok(())
            },
        )?;
        for (path, (target, _, _)) in self.original {
            if !allowed(&path) {
                return Err(denied());
            }
            target.revalidate(root)?;
            changes.push(target.bytes(None));
        }
        let total = changes.len();
        for (index, change) in changes.into_iter().enumerate() {
            change.apply(root, cancel).map_err(|error| {
                io::Error::other(format!(
                    "Published {index} of {total} file changes: {error}"
                ))
            })?;
        }
        Ok(())
    }
}

/// Sample linked entries through directory handles without following symlinks.
pub(super) struct Budget {
    root: Dir,
    entries: usize,
    bytes: u64,
}

impl Budget {
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            root: Dir::open_ambient_dir(path, ambient_authority())?,
            entries: 0,
            bytes: 0,
        })
    }

    pub(super) fn check(
        &mut self,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> io::Result<()> {
        self.entries = 0;
        self.bytes = 0;
        self.scan(&self.root.try_clone()?, 0, cancel, deadline)
    }

    fn scan(
        &mut self,
        dir: &Dir,
        depth: usize,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> io::Result<()> {
        for entry in dir.entries()? {
            check_cancel(cancel)?;
            if Instant::now() >= deadline {
                return Err(io::Error::other("Shell command timed out"));
            }
            self.entries += 1;
            if self.entries > MAX_ENTRIES || depth > MAX_DEPTH {
                return Err(io::Error::other(
                    "Shell workspace entry or depth budget exceeded during execution",
                ));
            }
            let name = entry?.file_name();
            let meta = match dir.symlink_metadata(&name) {
                Ok(meta) => meta,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let allocated = cap_std::fs::MetadataExt::blocks(&meta).saturating_mul(512);
            let logical = if meta.is_file() { meta.len() } else { 0 };
            self.bytes = self.bytes.saturating_add(allocated.max(logical));
            if self.bytes > MAX_SCAN_BYTES as u64 {
                return Err(io::Error::other(
                    "Shell workspace byte budget exceeded during execution",
                ));
            }
            if meta.is_dir() {
                match dir.open_dir_nofollow(&name) {
                    Ok(child) => self.scan(&child, depth + 1, cancel, deadline)?,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }
}

enum Entry<'a> {
    Directory,
    File(Vec<u8>, &'a Metadata, &'a Metadata),
}

fn walk(
    root: &Root,
    cancel: &CancellationToken,
    mut include: impl FnMut(&Path) -> bool,
    strict: bool,
    mut visit: impl FnMut(&Path, Entry<'_>) -> io::Result<()>,
) -> io::Result<()> {
    let mut entries = 0;
    let mut bytes = 0;
    let mut pending = vec![(PathBuf::new(), root.directory.try_clone()?, 0)];
    while let Some((parent, directory, depth)) = pending.pop() {
        root.verify_location(&parent, confined::location(&directory)?)?;
        let parent_metadata = directory.dir_metadata()?;
        for entry in directory.entries()? {
            check_cancel(cancel)?;
            entries += 1;
            if entries > MAX_ENTRIES || depth > MAX_DEPTH {
                return Err(io::Error::other(
                    "Shell workspace entry or depth budget exceeded",
                ));
            }
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str().and_then(|name| relative_path(name).ok()) else {
                if strict {
                    return Err(io::Error::other(
                        "Cannot publish an unsupported shell workspace filename",
                    ));
                }
                continue;
            };
            let path = parent.join(&name);
            if !include(&path) {
                continue;
            }
            let kind = entry.file_type()?;
            if kind.is_dir() {
                let child = confined::directory(&directory, Path::new(&name))?;
                root.verify_location(&path, confined::location(&child)?)?;
                visit(&path, Entry::Directory)?;
                pending.push((path, child, depth + 1));
            } else if kind.is_file() {
                let meta = directory.symlink_metadata(&name)?;
                if !meta.is_file() || meta.nlink() != 1 {
                    if strict {
                        return Err(io::Error::other("Cannot publish multiplied hard links"));
                    }
                    continue;
                }
                let mut options = OpenOptions::new();
                options
                    .read(true)
                    .follow(FollowSymlinks::No)
                    .custom_flags(libc::O_NONBLOCK);
                let mut file = directory.open_with(&name, &options)?;
                root.verify_location(&path, confined::location(&file)?)?;
                let opened = file.metadata()?;
                if !opened.is_file()
                    || opened.nlink() != 1
                    || opened.dev() != meta.dev()
                    || opened.ino() != meta.ino()
                {
                    return Err(denied());
                }
                let mut content =
                    Vec::with_capacity(opened.len().min(MAX_FILE_BYTES as u64) as usize);
                (&mut file)
                    .take((MAX_FILE_BYTES + 1) as u64)
                    .read_to_end(&mut content)?;
                root.verify_location(&path, confined::location(&file)?)?;
                let current = file.metadata()?;
                if current.nlink() != 1
                    || current.len() != opened.len()
                    || current.modified()? != opened.modified()?
                {
                    return Err(io::Error::other("Shell workspace changed during snapshot"));
                }
                bytes += content.len();
                if content.len() > MAX_FILE_BYTES || bytes > MAX_SCAN_BYTES {
                    return Err(io::Error::other("Shell workspace byte budget exceeded"));
                }
                visit(&path, Entry::File(content, &opened, &parent_metadata))?;
            } else if strict {
                return Err(io::Error::other(
                    "Cannot publish symlinks or special files from shell",
                ));
            }
        }
        root.verify_location(&parent, confined::location(&directory)?)?;
    }
    Ok(())
}
