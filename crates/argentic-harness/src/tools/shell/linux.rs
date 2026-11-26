use std::{
    fs::{self, File},
    io::{self, Read, Write},
    os::fd::{AsRawFd, FromRawFd, RawFd},
    os::unix::process::CommandExt,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

mod seccomp;

const OUTPUT_LIMIT: usize = 256 * 1024;
const SETUP_LIMIT: Duration = Duration::from_secs(5);
const STAGING_CHECK_INTERVAL: Duration = Duration::from_millis(50);

fn binary() -> Result<&'static Path, String> {
    ["/usr/bin/bwrap", "/bin/bwrap", "/usr/local/bin/bwrap"]
        .into_iter()
        .map(Path::new)
        .find(|path| path.is_file())
        .ok_or_else(|| "Argentic shell requires system Bubblewrap (bwrap)".into())
}

pub(super) fn preflight() -> Result<(), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    run(
        ":",
        dir.path(),
        false,
        false,
        5_000,
        &CancellationToken::new(),
    )
    .and_then(|output| {
        if output.starts_with("Exit status: 0\n") {
            Ok(())
        } else {
            Err(output)
        }
    })
    .map_err(|error| format!("Argentic shell sandbox is unavailable: {error}"))
}

fn inheritable(fd: RawFd) -> io::Result<()> {
    if unsafe { libc::fcntl(fd, libc::F_SETFD, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn nonblocking(fd: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags == -1 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn pidfd(pid: i32, supervisor: u32) -> io::Result<File> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    let process = unsafe { File::from_raw_fd(fd as RawFd) };
    // A failed setup can exit before pidfd_open. Verify the reported PID
    // still belongs to the supervisor before using it as a kill target.
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let parent = status
        .lines()
        .find_map(|line| line.strip_prefix("PPid:"))
        .and_then(|ppid| ppid.trim().parse::<u32>().ok());
    let namespace_pid = status
        .lines()
        .find_map(|line| line.strip_prefix("NSpid:"))
        .and_then(|ids| ids.split_whitespace().last());
    if parent != Some(supervisor) || namespace_pid != Some("1") || poll_dead(&process)? {
        return Err(io::Error::other(
            "Bubblewrap namespace process changed during setup",
        ));
    }
    Ok(process)
}

fn poll_dead(process: &File) -> io::Result<bool> {
    let mut poll = libc::pollfd {
        fd: process.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let result = unsafe { libc::poll(&mut poll, 1, 0) };
    if result == -1 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
    Ok(poll.revents & libc::POLLIN != 0)
}

fn kill_namespace(process: &File) -> io::Result<()> {
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            process.as_raw_fd(),
            libc::SIGKILL,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if result == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error);
        }
    }
    Ok(())
}

struct Sandbox {
    supervisor: Child,
    namespace: Option<File>,
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        if let Some(namespace) = &self.namespace {
            let _ = kill_namespace(namespace);
            while matches!(poll_dead(namespace), Ok(false)) {
                thread::sleep(Duration::from_millis(5));
            }
        }
        if !matches!(self.supervisor.try_wait(), Ok(Some(_))) {
            // Before the setup gate opens there is no model process. After it
            // opens the namespace PID 1 has already been joined above.
            unsafe { libc::kill(-(self.supervisor.id() as i32), libc::SIGKILL) };
        }
        let _ = self.supervisor.wait();
    }
}

#[derive(Default)]
struct Capture {
    bytes: Vec<u8>,
    truncated: bool,
}

impl Capture {
    fn pump(&mut self, stream: &mut impl Read) -> io::Result<()> {
        let mut buf = [0; 8192];
        // Yield to the cancellation and deadline checks even when a child
        // continuously fills a pipe.
        for _ in 0..8 {
            let count = match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(count) => count,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            };
            let available = (OUTPUT_LIMIT - self.bytes.len()).min(count);
            self.bytes.extend_from_slice(&buf[..available]);
            self.truncated |= available < count;
        }
        Ok(())
    }

    fn into_text(self) -> String {
        let mut text = String::from_utf8_lossy(&self.bytes).into_owned();
        if self.truncated {
            text.push_str("\n[Shell output capture limit reached]");
        }
        text
    }
}

pub(super) fn run(
    command: &str,
    workspace: &Path,
    write: bool,
    network: bool,
    timeout_ms: u64,
    cancel: &CancellationToken,
) -> Result<String, String> {
    let program = binary()?;
    run_inner(
        program, command, workspace, write, network, timeout_ms, cancel,
    )
    .map_err(|e| e.to_string())
}

fn run_inner(
    program: &Path,
    command: &str,
    workspace: &Path,
    write: bool,
    network: bool,
    timeout_ms: u64,
    cancel: &CancellationToken,
) -> io::Result<String> {
    super::super::check_cancel(cancel)?;
    let mut budget = write
        .then(|| super::workspace::Budget::open(workspace))
        .transpose()?;
    let mut filter = tempfile::tempfile()?;
    seccomp::write_filter(&mut filter, network)?;
    let (mut info, info_writer) = io::pipe()?;
    let (gate_reader, mut gate) = io::pipe()?;
    nonblocking(info.as_raw_fd())?;
    let fds = [
        filter.as_raw_fd(),
        info_writer.as_raw_fd(),
        gate_reader.as_raw_fd(),
    ];
    let mut child = Command::new(program);
    child
        .env_clear()
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    child.args([
        "--unshare-all",
        "--unshare-user",
        "--disable-userns",
        "--as-pid-1",
        "--new-session",
        "--die-with-parent",
        "--cap-drop",
        "ALL",
    ]);
    if network {
        child.arg("--share-net");
    }
    child.args([
        "--dev", "/dev", "--proc", "/proc", "--size", "67108864", "--tmpfs", "/tmp",
    ]);
    // Explicit execution prerequisites. Hosts may not place application data
    // or secrets in these system directories.
    for &system in super::RUNTIME_DIRS.iter().chain(&[super::LOADER_CACHE]) {
        child.args(["--ro-bind-try", system, system]);
    }
    if network {
        for &(system, _) in super::NETWORK_MOUNTS {
            // Resolve distro-managed resolv.conf links without mounting /run.
            if let Ok(source) = fs::canonicalize(system) {
                child.arg("--ro-bind").arg(source).arg(system);
            }
        }
    }
    child
        .arg(if write { "--bind" } else { "--ro-bind" })
        .arg(workspace)
        .arg("/workspace")
        .args([
            "--remount-ro",
            "/",
            "--chdir",
            "/workspace",
            "--clearenv",
            "--setenv",
            "PATH",
            "/usr/bin:/bin",
            "--setenv",
            "HOME",
            "/tmp",
            "--setenv",
            "LANG",
            "C.UTF-8",
        ]);
    for (name, fd) in ["--seccomp", "--info-fd", "--block-fd"]
        .into_iter()
        .zip(fds)
    {
        child.arg(name).arg(fd.to_string());
    }
    child.args(["--", "/bin/bash", "--noprofile", "--norc", "-c",
        "if /bin/bash --noprofile --norc -n -c \"$1\"; then exec /bin/bash --noprofile --norc -c \"$1\"; else exit 2; fi",
        "argentic-shell", command]);
    // Only the execution child gets these descriptors. Ambient host fds stay
    // close-on-exec, and bwrap consumes the three sandbox setup descriptors.
    unsafe {
        child.pre_exec(move || {
            for fd in fds {
                inheritable(fd)?;
            }
            for (resource, ceiling) in [
                (libc::RLIMIT_FSIZE, 16 * 1024 * 1024),
                (libc::RLIMIT_NPROC, 512),
                (libc::RLIMIT_NOFILE, 256),
            ] {
                let mut limit = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                if libc::getrlimit(resource, &mut limit) == -1 {
                    return Err(io::Error::last_os_error());
                }
                limit.rlim_max = limit.rlim_max.min(ceiling);
                limit.rlim_cur = limit.rlim_cur.min(limit.rlim_max);
                if libc::setrlimit(resource, &limit) == -1 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let mut sandbox = Sandbox {
        supervisor: child.spawn()?,
        namespace: None,
    };
    drop(filter);
    drop(info_writer);
    drop(gate_reader);
    let mut stdout = sandbox
        .supervisor
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("Missing Bubblewrap stdout pipe"))?;
    let mut stderr = sandbox
        .supervisor
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("Missing Bubblewrap stderr pipe"))?;
    nonblocking(stdout.as_raw_fd())?;
    nonblocking(stderr.as_raw_fd())?;
    let mut out = Capture::default();
    let mut err = Capture::default();
    let start = Instant::now();
    let deadline = Duration::from_millis(timeout_ms);
    let mut info_bytes = Vec::new();
    loop {
        out.pump(&mut stdout)?;
        err.pump(&mut stderr)?;
        if let Some(status) = sandbox.supervisor.try_wait()? {
            err.pump(&mut stderr)?;
            return Err(io::Error::other(format!(
                "Bubblewrap exited during setup ({status}): {}",
                err.into_text()
            )));
        }
        (&mut info)
            .take(1024)
            .read_to_end(&mut info_bytes)
            .or_else(|error| {
                if error.kind() == io::ErrorKind::WouldBlock {
                    Ok(0)
                } else {
                    Err(error)
                }
            })?;
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&info_bytes) {
            let pid = value["child-pid"]
                .as_i64()
                .and_then(|pid| i32::try_from(pid).ok())
                .filter(|pid| *pid > 1)
                .ok_or_else(|| io::Error::other("Invalid Bubblewrap child PID"))?;
            sandbox.namespace = Some(pidfd(pid, sandbox.supervisor.id())?);
            break;
        }
        super::super::check_cancel(cancel)?;
        if start.elapsed() >= deadline.min(SETUP_LIMIT) || info_bytes.len() >= 1024 {
            return Err(io::Error::other("Bubblewrap setup timed out"));
        }
        thread::sleep(Duration::from_millis(5));
    }
    super::super::check_cancel(cancel)?;
    if start.elapsed() >= deadline {
        return Err(io::Error::other("Shell command timed out"));
    }
    if let Some(budget) = &mut budget {
        budget.check(cancel, start + deadline)?;
    }
    gate.write_all(b"1")?;
    drop(gate);
    let mut next_budget_check = Instant::now() + STAGING_CHECK_INTERVAL;
    let status = loop {
        out.pump(&mut stdout)?;
        err.pump(&mut stderr)?;
        if let Some(status) = sandbox.supervisor.try_wait()? {
            let namespace = sandbox
                .namespace
                .as_ref()
                .ok_or_else(|| io::Error::other("Missing Bubblewrap namespace process"))?;
            if poll_dead(namespace)? {
                break status;
            }
        }
        super::super::check_cancel(cancel)?;
        if start.elapsed() >= deadline {
            return Err(io::Error::other("Shell command timed out"));
        }
        if let Some(budget) = &mut budget {
            if Instant::now() >= next_budget_check {
                // An error returns through Sandbox::drop, which kills PID 1
                // via its pidfd and waits before the staging copy is removed.
                budget.check(cancel, start + deadline)?;
                next_budget_check = Instant::now() + STAGING_CHECK_INTERVAL;
            }
        }
        thread::sleep(Duration::from_millis(5));
    };
    if let Some(budget) = &mut budget {
        budget.check(cancel, start + deadline)?;
    }
    out.pump(&mut stdout)?;
    err.pump(&mut stderr)?;
    let status = status
        .code()
        .map_or_else(|| status.to_string(), |code| code.to_string());
    Ok(format!(
        "Exit status: {status}\nstdout:\n{}\nstderr:\n{}",
        out.into_text(),
        err.into_text()
    ))
}
