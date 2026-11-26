//! Give Bash fresh inodes. Inode aliases on a live read-write bind mount can
//! mutate or disclose files outside the authorized workspace.
use super::super::{
    check_cancel, confined, mutation::Target, relative_path, Root, MAX_DEPTH, MAX_ENTRIES,
    MAX_FILE_BYTES, MAX_SCAN_BYTES,
};
use cap_std::{
    ambient_authority,
    fs::{Dir, Metadata, PermissionsExt as _},
};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

pub(super) struct Snapshot {
    staging: tempfile::TempDir,
    original: BTreeMap<PathBuf, (Target, Vec<u8>, u32)>,
}

impl Snapshot {
    pub(super) fn new(
        root: &Root,
        cancel: &CancellationToken,
        readable: impl FnMut(&Path) -> bool,
    ) -> io::Result<Self> {
        let staging = tempfile::tempdir()?;
        let mut original = BTreeMap::new();
        walk(root, cancel, readable, false, |path, entry| {
            match entry {
                Entry::Directory => fs::create_dir(staging.path().join(path))?,
                Entry::File(bytes, opened) => {
                    let target = Target::open(root, path.to_owned())?;
                    if !target.matches(opened)? {
                        return Err(io::Error::other("Shell workspace changed during snapshot"));
                    }
                    let mode = opened.permissions().mode() & 0o777;
                    let destination = staging.path().join(path);
                    fs::write(&destination, &bytes)?;
                    fs::set_permissions(&destination, fs::Permissions::from_mode(mode))?;
                    original.insert(path.to_owned(), (target, bytes, mode));
                }
            }
            Ok(())
        })?;
        Ok(Self { staging, original })
    }

    pub(super) fn path(&self) -> &Path {
        self.staging.path()
    }

    pub(super) fn publish(mut self, root: &Root, cancel: &CancellationToken) -> io::Result<()> {
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
                let Entry::File(bytes, opened) = entry else {
                    return Ok(());
                };
                let mode = opened.permissions().mode() & 0o777;
                let target = match self.original.remove(path) {
                    Some((_, old, old_mode)) if old == bytes && old_mode == mode => return Ok(()),
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
        for (_, (target, _, _)) in self.original {
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

enum Entry<'a> {
    Directory,
    File(Vec<u8>, &'a Metadata),
}

fn walk(
    root: &Root,
    cancel: &CancellationToken,
    mut include: impl FnMut(&Path) -> bool,
    strict: bool,
    mut visit: impl FnMut(&Path, Entry<'_>) -> io::Result<()>,
) -> io::Result<()> {
    use cap_fs_ext::MetadataExt;
    let mut entries = 0;
    let mut bytes = 0;
    let mut pending = vec![(PathBuf::new(), root.directory.try_clone()?, 0)];
    while let Some((parent, directory, depth)) = pending.pop() {
        root.verify_location(&parent, confined::location(&directory)?)?;
        for entry in directory.entries()? {
            check_cancel(cancel)?;
            root.verify_location(&parent, confined::location(&directory)?)?;
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
                let child = confined::directory(&root.directory, &path)?;
                root.verify_location(&path, confined::location(&child)?)?;
                visit(&path, Entry::Directory)?;
                pending.push((path, child, depth + 1));
            } else if kind.is_file() {
                let meta = directory.symlink_metadata(&name)?;
                if meta.nlink() != 1 {
                    if strict {
                        return Err(io::Error::other("Cannot publish multiplied hard links"));
                    }
                    continue;
                }
                let mut file = confined::file(&root.directory, &path)?;
                root.verify_location(&path, confined::location(&file)?)?;
                let opened = file.metadata()?;
                let mut content = Vec::new();
                (&mut file)
                    .take((MAX_FILE_BYTES + 1) as u64)
                    .read_to_end(&mut content)?;
                root.verify_location(&path, confined::location(&file)?)?;
                bytes += content.len();
                if content.len() > MAX_FILE_BYTES || bytes > MAX_SCAN_BYTES {
                    return Err(io::Error::other("Shell workspace byte budget exceeded"));
                }
                visit(&path, Entry::File(content, &opened))?;
            } else if strict {
                return Err(io::Error::other(
                    "Cannot publish symlinks or special files from shell",
                ));
            }
        }
    }
    Ok(())
}
