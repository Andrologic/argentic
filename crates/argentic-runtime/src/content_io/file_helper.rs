//! Confined reading from a directory handle after native admission.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub const HELPER_ARGUMENT: &str = "--macro-internal-agsdl-content-read";
pub fn qualified_platform() -> bool {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        return super::macos_launcher::available();
    }
    #[cfg(any(target_os = "linux", windows))]
    {
        true
    }
    #[cfg(not(any(
        all(target_os = "macos", target_arch = "aarch64"),
        target_os = "linux",
        windows
    )))]
    {
        false
    }
}
pub const MAX_BYTES: u64 = 10 * 1024 * 1024;
static HOST_EXECUTABLE: OnceLock<PathBuf> = OnceLock::new();

// Only a dedicated native debug QA runner may activate the still-unqualified
// chain. This flag is neither deserialized nor exposed through an IPC command.
#[cfg(all(target_os = "macos", feature = "agsdl-content-qa", debug_assertions))]
static QA_CHAIN_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(all(target_os = "macos", feature = "agsdl-content-qa", debug_assertions))]
pub fn enable_qa_chain() -> std::io::Result<()> {
    if HOST_EXECUTABLE.get().is_none() {
        return Err(std::io::Error::other(
            "Content helper startup is not registered",
        ));
    }
    QA_CHAIN_ENABLED.store(true, std::sync::atomic::Ordering::Release);
    Ok(())
}

/// 09 registers the executable only after wiring `dispatch` into its startup.
/// This native-only registration must never be derived from an IPC request.
fn register_host_executable(path: PathBuf) -> std::io::Result<()> {
    if !path.is_absolute()
        || !path.is_file()
        || path.canonicalize()? != std::env::current_exe()?.canonicalize()?
    {
        return Err(std::io::Error::other(
            "Invalid native content helper executable",
        ));
    }
    HOST_EXECUTABLE
        .set(path)
        .map_err(|_| std::io::Error::other("Content helper already registered"))
}

/// Called by the native executable before starting the host. A helper invocation
/// returns its exit code; a normal invocation registers the same executable.
pub fn initialize() -> std::io::Result<Option<i32>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if let Some(code) = dispatch(&args) {
        return Ok(Some(code));
    }
    register_host_executable(std::env::current_exe()?)?;
    Ok(None)
}

#[cfg(target_os = "macos")]
pub async fn read_confined(
    parent: &cap_std::fs::Dir,
    name: &Path,
    cancel: &tokio_util::sync::CancellationToken,
    owner: Option<super::ContentResourceOwner>,
) -> std::io::Result<Vec<u8>> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    if !content_chain_enabled() {
        return Err(std::io::Error::other(
            "Content helper chain is not yet qualified",
        ));
    }
    let executable = HOST_EXECUTABLE
        .get()
        .cloned()
        .ok_or_else(|| std::io::Error::other("Native content helper is not registered"))?;
    let fd = unsafe { libc::fcntl(parent.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let directory = unsafe { OwnedFd::from_raw_fd(fd) };
    let name = name.to_path_buf();
    let child_cancel = cancel.child_token();
    let guard = child_cancel.clone().drop_guard();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let worker = tokio::task::spawn_blocking(move || {
        super::macos_launcher::read_supervised(&executable, &name, directory, child_cancel, sender)
    });
    if let Some(owner) = owner {
        let cleanup = tokio::spawn(async move {
            worker
                .await
                .map_err(|error| format!("Content reader owner failed: {error}"))
        });
        owner(cleanup).map_err(std::io::Error::other)?;
    }
    let result = receiver
        .await
        .map_err(|_| std::io::Error::other("Content helper worker stopped"))?;
    drop(guard);
    result
}

#[cfg(target_os = "linux")]
pub async fn read_confined(
    parent: &cap_std::fs::Dir,
    name: &Path,
    cancel: &tokio_util::sync::CancellationToken,
    owner: Option<super::ContentResourceOwner>,
) -> std::io::Result<Vec<u8>> {
    if !content_chain_enabled() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Native content helper is not registered",
        ));
    }
    let parent = parent.try_clone()?;
    let name = name.to_owned();
    supervise_read(cancel, owner, move |cancel| {
        read_linux(parent, name, cancel)
    })
    .await
}

#[cfg(target_os = "linux")]
async fn read_linux(
    parent: cap_std::fs::Dir,
    name: PathBuf,
    cancel: tokio_util::sync::CancellationToken,
) -> std::io::Result<Vec<u8>> {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    use std::process::Stdio;
    use tokio::io::AsyncReadExt;

    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    validate_file_name(&name)?;
    let fd = parent.as_raw_fd();
    let owner_pid = unsafe { libc::getpid() };
    let zero: libc::c_ulong = 0;
    // This procfs link identifies the forked child's inherited image until
    // exec. Replacing the executable's installation path cannot change it.
    let mut command = crate::process::background_tokio_command("/proc/self/exe");
    command
        .env_clear()
        .kill_on_drop(true)
        .arg(HELPER_ARGUMENT)
        .arg(fd.to_string())
        .arg(name.as_os_str())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    unsafe {
        command.as_std_mut().pre_exec(move || {
            if libc::prctl(
                libc::PR_SET_PDEATHSIG,
                libc::SIGKILL as libc::c_ulong,
                zero,
                zero,
                zero,
            ) < 0
                || libc::prctl(
                    libc::PR_SET_NO_NEW_PRIVS,
                    1 as libc::c_ulong,
                    zero,
                    zero,
                    zero,
                ) < 0
                || libc::fcntl(fd, libc::F_SETFD, 0) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            if libc::getppid() != owner_pid {
                return Err(std::io::Error::from_raw_os_error(libc::ECANCELED));
            }
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    let result = tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(cancelled()),
        result = async {
            let stdout = child.stdout.take().expect("piped reader output");
            let mut bytes = Vec::new();
            stdout.take(MAX_BYTES + 1).read_to_end(&mut bytes).await?;
            if bytes.len() as u64 > MAX_BYTES {
                return Err(std::io::Error::other("Content exceeds limit"));
            }
            let status = child.wait().await?;
            match status.code() {
                Some(0) => Ok(bytes),
                Some(3) => Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "Content helper refused the file",
                )),
                _ => Err(std::io::Error::other("Content helper failed")),
            }
        } => result,
    };
    if result.is_err() {
        let _ = child.start_kill();
        child.wait().await?;
    }
    result
}

#[cfg(windows)]
pub use super::windows_reader::read_confined;

/// Attach cleanup before starting I/O. If the caller times out or is dropped,
/// the worker receives cancellation and the work owner still joins it.
#[cfg(any(target_os = "linux", windows))]
pub(super) async fn supervise_read<F>(
    cancel: &tokio_util::sync::CancellationToken,
    owner: Option<super::ContentResourceOwner>,
    read: impl FnOnce(tokio_util::sync::CancellationToken) -> F + Send + 'static,
) -> std::io::Result<Vec<u8>>
where
    F: std::future::Future<Output = std::io::Result<Vec<u8>>> + Send + 'static,
{
    use std::time::Duration;
    use tokio::sync::oneshot;

    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    let cancel = cancel.child_token();
    let _cancel_on_drop = cancel.clone().drop_guard();
    let (start, admitted) = oneshot::channel();
    let (done, result) = oneshot::channel();
    let worker_cancel = cancel.clone();
    let worker = tokio::spawn(async move {
        if admitted.await.is_ok() {
            let _ = done.send(read(worker_cancel).await);
        }
        Ok(())
    });
    if let Some(owner) = owner {
        owner(worker).map_err(std::io::Error::other)?;
    }
    start
        .send(())
        .map_err(|_| std::io::Error::other("Content reader stopped"))?;
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(cancelled()),
        result = tokio::time::timeout(Duration::from_secs(30), result) => {
            result
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Content read timed out"))?
                .map_err(|_| std::io::Error::other("Content reader stopped"))?
        }
    }
}

#[cfg(any(target_os = "linux", windows))]
pub(super) fn cancelled() -> std::io::Error {
    std::io::Error::other("Content read cancelled")
}

#[cfg(any(target_os = "macos", target_os = "linux", windows))]
pub(super) fn validate_file_name(name: &Path) -> std::io::Result<()> {
    if name.file_name() == Some(name.as_os_str()) {
        Ok(())
    } else {
        Err(std::io::Error::other("Invalid path component"))
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn content_chain_enabled() -> bool {
    #[cfg(all(target_os = "macos", feature = "agsdl-content-qa", debug_assertions))]
    {
        return QA_CHAIN_ENABLED.load(std::sync::atomic::Ordering::Acquire);
    }
    #[allow(unreachable_code)]
    {
        qualified_platform() && HOST_EXECUTABLE.get().is_some()
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
pub async fn read_confined(
    _parent: &cap_std::fs::Dir,
    _name: &Path,
    _cancel: &tokio_util::sync::CancellationToken,
    _owner: Option<super::ContentResourceOwner>,
) -> std::io::Result<Vec<u8>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Safe file reading is not qualified on this platform",
    ))
}

/// 09 calls this before initializing Tauri and exits with the returned status.
/// The parent must never treat this entry point as permission admission.
pub fn dispatch(args: &[OsString]) -> Option<i32> {
    #[cfg(all(target_os = "macos", feature = "agsdl-content-qa", debug_assertions))]
    if args.first().and_then(|s| s.to_str()) == Some("--macro-internal-agsdl-content-guard-qa") {
        return Some(if args.len() == 1 && guard_qa().is_ok() {
            0
        } else {
            1
        });
    }
    if args.first().and_then(|s| s.to_str()) != Some(HELPER_ARGUMENT) {
        return None;
    }
    Some(match read(args) {
        Ok(()) => 0,
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => 2,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => 3,
        Err(_) => 1,
    })
}

/// Exercises the production guard in a dedicated QA executable only.
#[cfg(all(target_os = "macos", feature = "agsdl-content-qa", debug_assertions))]
fn guard_qa() -> std::io::Result<()> {
    start_control_monitor()?;
    install_guard()?;
    let child = unsafe { libc::fork() };
    if child == 0 {
        unsafe { libc::_exit(42) }
    }
    if child > 0 {
        let mut status = 0;
        unsafe { libc::waitpid(child, &mut status, 0) };
        return Err(std::io::Error::other("Sandbox permitted fork"));
    }
    if !matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM | libc::EACCES)
    ) {
        return Err(std::io::Error::other("Unexpected fork failure"));
    }
    let path = c"/usr/bin/true";
    let argv = [path.as_ptr().cast_mut(), std::ptr::null_mut()];
    let environment = [std::ptr::null_mut()];
    let mut pid = 0;
    let result = unsafe {
        libc::posix_spawn(
            &mut pid,
            path.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            argv.as_ptr(),
            environment.as_ptr(),
        )
    };
    if result == 0 {
        let mut status = 0;
        unsafe { libc::waitpid(pid, &mut status, 0) };
        return Err(std::io::Error::other("Sandbox permitted process launch"));
    }
    if result != libc::EPERM && result != libc::EACCES {
        return Err(std::io::Error::other("Unexpected process launch failure"));
    }
    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn read(args: &[OsString]) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::io::{Read, Write};
    use std::os::fd::{FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;

    if args.len() != 3 {
        return Err(std::io::Error::other("Invalid helper arguments"));
    }
    let parent: i32 = args[1]
        .to_str()
        .and_then(|s| s.parse().ok())
        // The authenticated macOS launcher passes its pinned directory as
        // stdin; other launchers may inherit a descriptor numbered 3 or above.
        .filter(|fd| *fd == 0 || *fd >= 3)
        .ok_or_else(|| std::io::Error::other("Invalid parent descriptor"))?;
    validate_file_name(Path::new(&args[2]))?;
    let name = CString::new(args[2].as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("Invalid path component"))?;
    #[cfg(target_os = "macos")]
    {
        start_control_monitor()?;
        install_guard()?;
    }

    #[cfg(target_os = "macos")]
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    #[cfg(target_os = "linux")]
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let pinned = unsafe { OwnedFd::from_raw_fd(fd) };
    let stat = std::fs::File::from(pinned);
    let metadata = super::single_link_metadata(&stat)?;
    if !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Not a regular file",
        ));
    }
    if metadata.len() > MAX_BYTES {
        return Err(std::io::Error::other("File exceeds limit"));
    }

    #[cfg(target_os = "macos")]
    let mut file = stat;
    #[cfg(target_os = "linux")]
    let mut file = {
        use std::os::fd::AsRawFd;
        // O_PATH pins the checked inode; reopening its proc descriptor cannot
        // follow a replacement of the original directory entry.
        std::fs::File::open(format!("/proc/self/fd/{}", stat.as_raw_fd()))?
    };
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(std::io::Error::other("File exceeds limit"));
    }
    super::single_link_metadata(&file)?;
    std::io::stdout().lock().write_all(&bytes)
}

#[cfg(target_os = "macos")]
fn start_control_monitor() -> std::io::Result<()> {
    use std::os::fd::RawFd;
    use std::time::{Duration, Instant};

    const CONTROL: RawFd = libc::STDERR_FILENO;
    let flags = unsafe { libc::fcntl(CONTROL, libc::F_GETFL) };
    let mut info = std::mem::MaybeUninit::<libc::stat>::uninit();
    if flags < 0
        || flags & libc::O_ACCMODE != libc::O_RDONLY
        || unsafe { libc::fstat(CONTROL, info.as_mut_ptr()) } != 0
    {
        return Err(std::io::Error::other("Missing helper control pipe"));
    }
    let info = unsafe { info.assume_init() };
    if info.st_mode & libc::S_IFMT != libc::S_IFIFO {
        return Err(std::io::Error::other("Invalid helper control pipe"));
    }
    if unsafe { libc::fcntl(CONTROL, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let (ready, started) = std::sync::mpsc::sync_channel(0);
    std::thread::Builder::new()
        .name("agsdl-content-control".into())
        .spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut first = true;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let timeout = if first {
                    0
                } else {
                    remaining.as_millis().min(100) as i32
                };
                let mut event = libc::pollfd {
                    fd: CONTROL,
                    events: libc::POLLIN,
                    revents: 0,
                };
                let result = unsafe { libc::poll(&mut event, 1, timeout) };
                if result < 0 {
                    if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    unsafe { libc::_exit(126) }
                }
                // Darwin can leave poll asleep after the last writer of an
                // unlinked FIFO closes. A nonblocking read distinguishes EOF
                // from a live writer even when no POLLHUP was reported.
                let mut byte = 0u8;
                let count = unsafe { libc::read(CONTROL, (&mut byte as *mut u8).cast(), 1) };
                if count >= 0 {
                    unsafe { libc::_exit(125) }
                }
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::WouldBlock
                    && error.kind() != std::io::ErrorKind::Interrupted
                {
                    unsafe { libc::_exit(126) }
                }
                if first {
                    if ready.send(()).is_err() {
                        unsafe { libc::_exit(126) }
                    }
                    first = false;
                } else if Instant::now() >= deadline {
                    unsafe { libc::_exit(124) }
                }
            }
        })?;
    started
        .recv()
        .map_err(|_| std::io::Error::other("Helper control monitor failed"))
}

#[cfg(target_os = "macos")]
fn install_guard() -> std::io::Result<()> {
    use std::ffi::{c_char, c_int, CString};
    unsafe extern "C" {
        fn sandbox_init(profile: *const c_char, flags: u64, errorbuf: *mut *mut c_char) -> c_int;
        fn sandbox_free_error(errorbuf: *mut c_char);
    }
    let profile = CString::new(include_str!("content_guard.sb")).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Invalid content helper profile",
        )
    })?;
    let mut error = std::ptr::null_mut();
    let result = unsafe { sandbox_init(profile.as_ptr(), 0, &mut error) };
    if !error.is_null() {
        unsafe { sandbox_free_error(error) };
    }
    if result != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Cannot confine content helper",
        ));
    }
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn read(_args: &[OsString]) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Safe file reading is not qualified on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::{dispatch, HELPER_ARGUMENT};
    use std::ffi::OsString;

    #[test]
    fn dispatch_only_accepts_its_internal_command() {
        assert_eq!(dispatch(&[]), None);
        assert_eq!(dispatch(&[OsString::from("--other")]), None);
        let expected = if cfg!(any(target_os = "macos", target_os = "linux")) {
            1
        } else {
            2
        };
        assert_eq!(dispatch(&[OsString::from(HELPER_ARGUMENT)]), Some(expected));
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn dispatch_rejects_unanchored_or_multi_component_paths() {
        for (fd, path) in [
            ("2", "file"),
            ("3", "../file"),
            ("3", "/file"),
            ("3", "a/b"),
        ] {
            assert_eq!(
                dispatch(&[HELPER_ARGUMENT.into(), fd.into(), path.into()]),
                Some(1)
            );
        }
    }

    #[cfg(any(target_os = "linux", windows))]
    #[tokio::test]
    async fn cancelled_read_keeps_its_worker_attached_until_it_stops() {
        use std::sync::{Arc, Mutex};
        use tokio::sync::oneshot;
        use tokio_util::sync::CancellationToken;

        let worker = Arc::new(Mutex::new(None));
        let attached = Arc::clone(&worker);
        let caller_cancel = CancellationToken::new();
        let cancel = caller_cancel.clone();
        let (started, running) = oneshot::channel();
        let (release, stopping) = oneshot::channel();
        let request = tokio::spawn(async move {
            super::supervise_read(
                &cancel,
                Some(Arc::new(move |cleanup| {
                    *attached.lock().unwrap() = Some(cleanup);
                    Ok(())
                })),
                move |cancel| async move {
                    started.send(()).unwrap();
                    cancel.cancelled().await;
                    stopping.await.unwrap();
                    Err(super::cancelled())
                },
            )
            .await
        });
        running.await.unwrap();
        caller_cancel.cancel();
        assert!(request.await.unwrap().is_err());
        let cleanup = worker.lock().unwrap().take().unwrap();
        assert!(!cleanup.is_finished());
        release.send(()).unwrap();
        cleanup.await.unwrap().unwrap();
    }
}
