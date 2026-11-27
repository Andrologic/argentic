//! Native, authenticated launch primitives. Callers must provide physical
//! termination and reap before this can be used for content reads.
use std::ffi::{c_char, CString};
use std::io;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::{Duration, Instant};

#[cfg(all(feature = "agsdl-content-qa", debug_assertions))]
type QaOutputHook = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);
#[cfg(all(feature = "agsdl-content-qa", debug_assertions))]
static QA_OUTPUT_HOOK: std::sync::OnceLock<std::sync::Mutex<Option<QaOutputHook>>> =
    std::sync::OnceLock::new();

#[cfg(all(feature = "agsdl-content-qa", debug_assertions))]
pub fn pause_next_output(
) -> io::Result<(std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>)> {
    let (ready_send, ready_receive) = std::sync::mpsc::channel();
    let (release_send, release_receive) = std::sync::mpsc::channel();
    let mut hook = QA_OUTPUT_HOOK
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| io::Error::other("QA output hook poisoned"))?;
    if hook.is_some() {
        return Err(io::Error::other("QA output hook already installed"));
    }
    *hook = Some((ready_send, release_receive));
    Ok((ready_receive, release_send))
}

unsafe extern "C" {
    fn agsdl_content_macos_available() -> i32;
    fn agsdl_content_macos_launch(
        executable: *const c_char,
        component: *const c_char,
        parent_fd: i32,
        pid: *mut libc::pid_t,
        output_fd: *mut i32,
        control_fd: *mut i32,
    ) -> i32;
}

pub fn available() -> bool {
    unsafe { agsdl_content_macos_available() == 1 }
}

#[cfg(all(feature = "agsdl-content-qa", debug_assertions))]
unsafe extern "C" {
    fn agsdl_content_macos_qa_arm_swap(target: *const c_char, replacement: *const c_char) -> i32;
}

#[cfg(all(feature = "agsdl-content-qa", debug_assertions))]
pub fn arm_image_swap(target: &Path, replacement: &Path) -> io::Result<()> {
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid QA target"))?;
    let replacement = CString::new(replacement.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid QA replacement"))?;
    if unsafe { agsdl_content_macos_qa_arm_swap(target.as_ptr(), replacement.as_ptr()) } != 0 {
        return Err(io::Error::other("Cannot arm QA image substitution"));
    }
    Ok(())
}

pub struct Launch {
    pid: Option<libc::pid_t>,
    pub output: OwnedFd,
    control: Option<OwnedFd>,
}

pub struct Exit {
    pub status: i32,
    pub signal: bool,
}

impl Launch {
    #[cfg(all(feature = "agsdl-content-qa", debug_assertions))]
    pub fn qa_control_open(&self) -> bool {
        self.control.is_some()
    }

    pub fn start(executable: &Path, name: &Path, parent_fd: RawFd) -> io::Result<Self> {
        let executable = CString::new(executable.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "Invalid helper executable")
        })?;
        let name = CString::new(name.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid file component"))?;
        let mut pid = -1;
        let mut output = -1;
        let mut control = -1;
        let code = unsafe {
            agsdl_content_macos_launch(
                executable.as_ptr(),
                name.as_ptr(),
                parent_fd,
                &mut pid,
                &mut output,
                &mut control,
            )
        };
        if code != 0 {
            return Err(io::Error::other(format!(
                "Content helper launch failed ({code})"
            )));
        }
        // Own whatever was returned first so an invalid answer cannot leak a
        // valid descriptor or child.
        let output = (output >= 0).then(|| unsafe { OwnedFd::from_raw_fd(output) });
        let control = (control >= 0).then(|| unsafe { OwnedFd::from_raw_fd(control) });
        match (pid > 0, output) {
            (true, Some(output)) if control.is_some() => Ok(Self {
                pid: Some(pid),
                output,
                control,
            }),
            (valid_pid, _) => {
                if valid_pid {
                    reap(pid);
                }
                Err(io::Error::other(
                    "Content helper returned invalid process or descriptors",
                ))
            }
        }
    }

    /// Closing the only parent writer requests self-termination in the helper.
    pub fn cancel(&mut self) {
        #[cfg(all(feature = "agsdl-content-qa", debug_assertions))]
        let control_fd = {
            use std::os::fd::AsRawFd;
            self.control.as_ref().map(AsRawFd::as_raw_fd)
        };
        self.control.take();
        #[cfg(all(feature = "agsdl-content-qa", debug_assertions))]
        if let Some(fd) = control_fd {
            let after = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            eprintln!("content QA: closed control fd {fd}; fcntl after={after}");
        }
    }

    /// A timeout leaves the child owned by this launch. An exit is reaped once.
    pub fn wait(&mut self, timeout: Duration) -> io::Result<Option<Exit>> {
        let pid = self
            .pid
            .ok_or_else(|| io::Error::other("Helper already reaped"))?;
        let started = Instant::now();
        loop {
            let mut status = 0;
            match unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) } {
                -1 => {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() == Some(libc::ECHILD) {
                        // A host that ignores SIGCHLD lets the kernel reap the
                        // child: it has exited, with an unknown status.
                        self.pid = None;
                        return Ok(Some(Exit {
                            status: -1,
                            signal: false,
                        }));
                    }
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(error);
                    }
                    continue;
                }
                0 => {}
                _ => {
                    self.pid = None;
                    return Ok(Some(Exit {
                        status: if libc::WIFSIGNALED(status) {
                            libc::WTERMSIG(status)
                        } else {
                            libc::WEXITSTATUS(status)
                        },
                        signal: libc::WIFSIGNALED(status),
                    }));
                }
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Ok(None);
            }
            std::thread::sleep(remaining.min(Duration::from_millis(20)));
        }
    }
}

impl Drop for Launch {
    /// Never leave a zombie behind, even if the launch is dropped unwaited.
    fn drop(&mut self) {
        if let Some(pid) = self.pid.take() {
            reap(pid);
        }
    }
}

/// Kill an owned, unreaped child and wait for it.
fn reap(pid: libc::pid_t) {
    unsafe { libc::kill(pid, libc::SIGKILL) };
    let mut status = 0;
    while unsafe { libc::waitpid(pid, &mut status, 0) } == -1
        && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted
    {}
}

/// The worker retains ownership until it reaps the child,
/// including after reporting uncertain termination to the asynchronous caller.
pub fn read_supervised(
    executable: &Path,
    name: &Path,
    directory: OwnedFd,
    cancel: tokio_util::sync::CancellationToken,
    sender: tokio::sync::oneshot::Sender<io::Result<Vec<u8>>>,
) {
    use std::os::fd::AsRawFd;

    if cancel.is_cancelled() {
        let _ = sender.send(Err(io::Error::other("Content read cancelled")));
        return;
    }
    let mut launch = match Launch::start(executable, name, directory.as_raw_fd()) {
        Ok(launch) => launch,
        Err(error) => {
            let _ = sender.send(Err(error));
            return;
        }
    };
    let mut sender = Some(sender);
    let mut bytes = Vec::new();
    let mut failure: Option<io::Error> = None;
    let mut cancelled_at: Option<Instant> = None;
    let mut exited: Option<Exit> = None;
    let mut eof = false;
    let deadline = Instant::now() + Duration::from_secs(30);
    let fd = launch.output.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        failure = Some(io::Error::last_os_error());
    }
    loop {
        let mut made_progress = false;
        if failure.is_none() && (cancel.is_cancelled() || Instant::now() >= deadline) {
            failure = Some(io::Error::other(if cancel.is_cancelled() {
                "Content read cancelled"
            } else {
                "Content helper timed out"
            }));
        }
        if failure.is_some() && cancelled_at.is_none() {
            #[cfg(all(feature = "agsdl-content-qa", debug_assertions))]
            eprintln!("content QA: requesting helper cancellation");
            launch.cancel();
            cancelled_at = Some(Instant::now());
        }
        if exited.is_none() {
            match launch.wait(Duration::ZERO) {
                Ok(Some(status)) => exited = Some(status),
                Ok(None) => {}
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
            }
        }
        if failure.is_none() && !eof {
            let mut buffer = [0u8; 8192];
            let count = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len()) };
            if count > 0 {
                made_progress = true;
                bytes.extend_from_slice(&buffer[..count as usize]);
                #[cfg(all(feature = "agsdl-content-qa", debug_assertions))]
                {
                    let hook = QA_OUTPUT_HOOK
                        .get_or_init(Default::default)
                        .lock()
                        .ok()
                        .and_then(|mut hook| hook.take());
                    if let Some((ready, release)) = hook {
                        if ready.send(()).is_err()
                            || release.recv_timeout(Duration::from_secs(5)).is_err()
                        {
                            failure = Some(io::Error::other("QA output hook failed"));
                        }
                    }
                }
                if bytes.len() as u64 > super::file_helper::MAX_BYTES {
                    failure = Some(io::Error::other("Content exceeds limit"));
                }
            } else if count == 0 {
                eof = true;
            } else {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::WouldBlock
                    && error.kind() != io::ErrorKind::Interrupted
                {
                    failure = Some(error);
                }
            }
        }
        if let Some(status) = &exited {
            if failure.is_some() || eof {
                let result = if let Some(error) = failure.take() {
                    Err(error)
                } else if status.signal || status.status != 0 {
                    Err(match (status.signal, status.status) {
                        (false, 2) => io::Error::new(
                            io::ErrorKind::Unsupported,
                            "Content helper could not enforce its guard",
                        ),
                        (false, 3) => io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "Content helper refused the file",
                        ),
                        _ => io::Error::other(format!(
                            "Content helper exited with status {} (signal: {})",
                            status.status, status.signal
                        )),
                    })
                } else if cancel.is_cancelled() {
                    Err(io::Error::other("Content read cancelled"))
                } else {
                    Ok(bytes)
                };
                if let Some(sender) = sender.take() {
                    let _ = sender.send(result);
                }
                return;
            }
        }
        if cancelled_at.is_some_and(|at| at.elapsed() >= Duration::from_secs(5)) {
            if let Some(sender) = sender.take() {
                #[cfg(all(feature = "agsdl-content-qa", debug_assertions))]
                eprintln!("content QA: helper exit not observed after cancellation");
                let _ = sender.send(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Content helper termination is uncertain",
                )));
            }
        }
        if !made_progress {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
