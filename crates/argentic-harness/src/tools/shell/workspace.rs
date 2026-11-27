//! Give Bash fresh inodes. Inode aliases on a live read-write bind mount can
//! mutate or disclose files outside the authorized workspace.
use super::super::{
    check_cancel, confined, denied,
    mutation::{Change, Target},
    relative_path, Root, MAX_DEPTH, MAX_FILE_BYTES,
};
use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt, OsMetadataExt};
use cap_std::{
    ambient_authority,
    fs::{Dir, File, Metadata, OpenOptions, OpenOptionsExt, PermissionsExt as _},
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

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

use ring::digest::{digest, Digest, SHA256};

const MAX_ENTRIES: usize = 50_000;
const MAX_SCAN_BYTES: u64 = 256 * 1024 * 1024;
type CopyJob = (PathBuf, File, Metadata, Metadata);
type Original = BTreeMap<PathBuf, (Target, Digest, u32)>;

pub(super) struct Snapshot {
    staging: tempfile::TempDir,
    original: Original,
}

impl Snapshot {
    pub(super) fn new(
        root: &Root,
        staging_parent: Option<&Path>,
        cancel: &CancellationToken,
        writable: bool,
        readable: impl FnMut(&Path) -> bool,
    ) -> io::Result<Self> {
        let staging_parent = match staging_parent {
            Some(parent) => parent.to_owned(),
            None => std::env::temp_dir(),
        }
        .canonicalize()?;
        if staging_parent.starts_with(&root.path) {
            return Err(io::Error::other(
                "Shell staging directory must be outside every admitted root",
            ));
        }
        let staging = tempfile::tempdir_in(staging_parent)?;
        let original = std::thread::scope(|scope| {
            let workers = std::thread::available_parallelism()
                .map_or(1, usize::from)
                .min(4);
            let (send, jobs) = std::sync::mpsc::sync_channel::<CopyJob>(64);
            let jobs = std::sync::Arc::new(std::sync::Mutex::new(jobs));
            let tasks: Vec<_> = (0..workers)
                .map(|_| {
                    let jobs = jobs.clone();
                    let staging = staging.path();
                    scope.spawn(move || -> io::Result<Original> {
                        let mut original = BTreeMap::new();
                        let mut copy = FileCopy::default();
                        loop {
                            let job = jobs
                                .lock()
                                .map_err(|_| {
                                    io::Error::other("Shell staging queue is unavailable")
                                })?
                                .recv();
                            let Ok((path, mut file, opened, parent)) = job else {
                                return Ok(original);
                            };
                            check_cancel(cancel)?;
                            let mode = opened.permissions().mode() & 0o777;
                            let hash = copy.stage(
                                &mut file,
                                &opened,
                                &staging.join(&path),
                                mode,
                                writable,
                            )?;
                            verify_file(root, &path, &file, &opened)?;
                            if let Some(hash) = hash {
                                let target =
                                    Target::from_snapshot(path.clone(), parent, opened, hash);
                                original.insert(path, (target, hash, mode));
                            }
                        }
                    })
                })
                .collect();
            drop(jobs);
            let scanned = walk(root, cancel, readable, false, |path, entry| match entry {
                None => fs::create_dir(staging.path().join(path)),
                Some((file, opened, parent)) => send
                    .send((path.to_owned(), file, opened, parent.clone()))
                    .map_err(|_| io::Error::other("Shell workspace copy workers stopped")),
            });
            drop(send);
            let mut original = BTreeMap::new();
            for task in tasks {
                original.append(
                    &mut task
                        .join()
                        .map_err(|_| io::Error::other("Shell workspace copy worker failed"))??,
                );
            }
            scanned?;
            Ok::<_, io::Error>(original)
        })?;
        Ok(Self { staging, original })
    }

    pub(super) fn path(&self) -> &Path {
        self.staging.path()
    }

    pub(super) fn changes(
        mut self,
        root: &Root,
        cancel: &CancellationToken,
        allowed: impl Fn(&Path) -> bool,
    ) -> io::Result<Vec<Change>> {
        let path = self.staging.path().canonicalize()?;
        let staging = Root {
            directory: Dir::open_ambient_dir(&path, ambient_authority())?,
            path,
        };
        let mut changes = Vec::new();
        let mut content = Vec::new();
        walk(
            &staging,
            cancel,
            |_| true,
            true,
            |path, entry| {
                let Some((mut file, opened, _)) = entry else {
                    return Ok(());
                };
                let mode = opened.permissions().mode() & 0o777;
                read_content(&mut file, opened.len(), &mut content)?;
                verify_file(&staging, path, &file, &opened)?;
                let original = self.original.remove(path);
                if original.as_ref().is_some_and(|(_, old, old_mode)| {
                    *old_mode == mode && old.as_ref() == digest(&SHA256, &content).as_ref()
                }) {
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
                let mut change = target.bytes(Some(std::mem::take(&mut content)));
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
        Ok(changes)
    }
}

#[derive(Default)]
struct FileCopy {
    content: Vec<u8>,
    #[cfg(target_os = "linux")]
    unsupported_devices: std::collections::HashSet<u64>,
}

impl FileCopy {
    fn stage(
        &mut self,
        source: &mut File,
        metadata: &Metadata,
        destination: &Path,
        mode: u32,
        writable: bool,
    ) -> io::Result<Option<Digest>> {
        let mut dest = fs::OpenOptions::new()
            .read(writable)
            .write(true)
            .create_new(true)
            .open(destination)?;
        #[cfg(target_os = "linux")]
        if !self.unsupported_devices.contains(&metadata.dev()) {
            // Clone only the data into a fresh inode. Source ACLs and xattrs
            // remain on the original, and a write can never mutate its blocks.
            if unsafe { libc::ioctl(dest.as_raw_fd(), libc::FICLONE, source.as_raw_fd()) } == 0 {
                if dest.metadata()?.len() != metadata.len() {
                    return Err(changed());
                }
                let hash = if writable {
                    read_content(&mut dest, metadata.len(), &mut self.content)?;
                    Some(digest(&SHA256, &self.content))
                } else {
                    None
                };
                dest.set_permissions(fs::Permissions::from_mode(mode))?;
                return Ok(hash);
            }
            let error = io::Error::last_os_error();
            if !matches!(
                error.raw_os_error(),
                Some(libc::EXDEV | libc::EOPNOTSUPP | libc::ENOTTY | libc::EINVAL | libc::ENOSYS)
            ) {
                return Err(error);
            }
            self.unsupported_devices.insert(metadata.dev());
        }
        let hash = if writable {
            read_content(source, metadata.len(), &mut self.content)?;
            dest.write_all(&self.content)?;
            Some(digest(&SHA256, &self.content))
        } else {
            // std can use in-kernel file copies on Linux. Bound the copy even
            // if another process grows the source after the initial stat.
            let source = source.try_clone()?.into_std();
            if io::copy(&mut source.take(metadata.len() + 1), &mut dest)? != metadata.len() {
                return Err(changed());
            }
            None
        };
        dest.set_permissions(fs::Permissions::from_mode(mode))?;
        Ok(hash)
    }
}

fn read_content(file: &mut impl Read, len: u64, content: &mut Vec<u8>) -> io::Result<()> {
    content.clear();
    content.reserve(len as usize);
    file.take(len + 1).read_to_end(content)?;
    if content.len() as u64 != len {
        return Err(changed());
    }
    Ok(())
}

fn changed() -> io::Error {
    io::Error::other("Shell workspace changed during snapshot")
}

fn verify_file(root: &Root, path: &Path, file: &File, opened: &Metadata) -> io::Result<()> {
    root.verify_location(path, confined::location(file)?)?;
    let current = file.metadata()?;
    if current.nlink() != 1
        || current.len() != opened.len()
        || current.modified()? != opened.modified()?
        || current.ctime() != opened.ctime()
        || current.ctime_nsec() != opened.ctime_nsec()
    {
        return Err(changed());
    }
    Ok(())
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
        deadline: &impl Fn() -> Instant,
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
        deadline: &impl Fn() -> Instant,
    ) -> io::Result<()> {
        for entry in dir.entries()? {
            check_cancel(cancel)?;
            if Instant::now() >= deadline() {
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
            if self.bytes > MAX_SCAN_BYTES {
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

type OpenedFile<'a> = (File, Metadata, &'a Metadata);

fn walk(
    root: &Root,
    cancel: &CancellationToken,
    mut include: impl FnMut(&Path) -> bool,
    strict: bool,
    mut visit: impl FnMut(&Path, Option<OpenedFile<'_>>) -> io::Result<()>,
) -> io::Result<()> {
    let mut entries = 0;
    let mut bytes = 0;
    let directory = root.directory.try_clone()?;
    let metadata = directory.dir_metadata()?;
    let mut stack = vec![(PathBuf::new(), directory.entries()?, directory, metadata)];
    while !stack.is_empty() {
        let depth = stack.len() - 1;
        let Some((parent, iter, directory, parent_metadata)) = stack.last_mut() else {
            break;
        };
        check_cancel(cancel)?;
        root.verify_location(parent, confined::location(directory)?)?;
        let Some(entry) = iter.next() else {
            stack.pop();
            continue;
        };
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
            let child = confined::directory(directory, Path::new(&name))?;
            root.verify_location(&path, confined::location(&child)?)?;
            visit(&path, None)?;
            let metadata = child.dir_metadata()?;
            stack.push((path, child.entries()?, child, metadata));
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
            let file = directory.open_with(&name, &options)?;
            root.verify_location(&path, confined::location(&file)?)?;
            let opened = file.metadata()?;
            if !opened.is_file()
                || opened.nlink() != 1
                || opened.dev() != meta.dev()
                || opened.ino() != meta.ino()
            {
                return Err(denied());
            }
            bytes = u64::saturating_add(bytes, opened.len());
            if opened.len() > MAX_FILE_BYTES as u64 || bytes > MAX_SCAN_BYTES {
                return Err(io::Error::other("Shell workspace byte budget exceeded"));
            }
            visit(&path, Some((file, opened, parent_metadata)))?;
        } else if strict {
            return Err(io::Error::other(
                "Cannot publish symlinks or special files from shell",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn shell_snapshot_walk_bounds_open_sibling_directories() {
        const CHILD: &str = "ARGENTIC_TEST_SNAPSHOT_FD_LIMIT";
        if std::env::var_os(CHILD).is_none() {
            use std::os::unix::process::CommandExt;
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "tools::shell::workspace::tests::shell_snapshot_walk_bounds_open_sibling_directories",
                ])
                .env(CHILD, "1");
            // Limit only the child test process. Other tests can open files concurrently.
            unsafe {
                command.pre_exec(|| {
                    let limit = libc::rlimit {
                        rlim_cur: 64,
                        rlim_max: 64,
                    };
                    if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) == -1 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stdout)
            );
            return;
        }

        let fixture = tempfile::tempdir().unwrap();
        let path = fixture.path().canonicalize().unwrap();
        for i in 0..128 {
            fs::create_dir(path.join(format!("child-{i}"))).unwrap();
        }
        let root = Root {
            directory: Dir::open_ambient_dir(&path, ambient_authority()).unwrap(),
            path,
        };
        let mut visited = 0;
        walk(
            &root,
            &CancellationToken::new(),
            |_| true,
            true,
            |_, _| {
                visited += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(visited, 128);
    }

    #[test]
    fn publication_rejects_a_host_edit_with_restored_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().canonicalize().unwrap();
        let file = path.join("file");
        fs::write(&file, "old").unwrap();
        let mtime = fs::metadata(&file).unwrap().modified().unwrap();
        let root = Root {
            directory: Dir::open_ambient_dir(&path, ambient_authority()).unwrap(),
            path,
        };
        let cancel = CancellationToken::new();
        let snapshot = Snapshot::new(&root, None, &cancel, true, |_| true).unwrap();
        fs::write(snapshot.path().join("file"), "new").unwrap();
        let changes = snapshot.changes(&root, &cancel, |_| true).unwrap();
        assert_eq!(changes.len(), 1);

        fs::write(&file, "ext").unwrap();
        fs::File::open(&file).unwrap().set_modified(mtime).unwrap();
        assert_eq!(fs::metadata(&file).unwrap().modified().unwrap(), mtime);
        assert!(changes[0].apply(&root, &cancel).is_err());
        assert_eq!(fs::read_to_string(&file).unwrap(), "ext");

        // Exercise the digest check independently of filesystem timestamp granularity.
        let target = Target::from_snapshot(
            "file".into(),
            root.directory.dir_metadata().unwrap(),
            root.directory.symlink_metadata("file").unwrap(),
            digest(&SHA256, b"old"),
        );
        assert!(target.bytes(None).apply(&root, &cancel).is_err());
        assert_eq!(fs::read_to_string(&file).unwrap(), "ext");
    }

    #[test]
    #[ignore = "explicit performance measurement"]
    fn shell_staging() {
        let fixture = tempfile::tempdir().unwrap();
        let count = std::env::var("ARGENTIC_BENCH_FILES").map_or(20_000, |n| n.parse().unwrap());
        let size = std::env::var("ARGENTIC_BENCH_FILE_SIZE").map_or(10_000, |n| n.parse().unwrap());
        let content = vec![b'x'; size];
        for i in 0..count {
            fs::write(fixture.path().join(format!("file-{i}")), &content).unwrap();
        }
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for source in [fixture.path(), repository.as_path()] {
            let path = source.canonicalize().unwrap();
            let root = Root {
                directory: Dir::open_ambient_dir(&path, ambient_authority()).unwrap(),
                path,
            };
            let mut samples = Vec::new();
            for _ in 0..3 {
                let start = Instant::now();
                let snapshot =
                    Snapshot::new(&root, None, &CancellationToken::new(), true, |relative| {
                        !relative
                            .components()
                            .any(|part| part.as_os_str() == "target")
                            && !super::super::super::private_project_path(
                                &root.path,
                                &root.path.join(relative),
                            )
                    })
                    .unwrap();
                std::hint::black_box(&snapshot);
                samples.push(start.elapsed());
            }
            samples.sort_unstable();
            let median = samples[1];
            println!("shell staging {}: {median:?} median", root.path.display());
            assert!(median.as_secs() < 8, "shell staging exceeded 8 seconds");
        }
    }
}
