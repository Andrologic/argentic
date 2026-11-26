use super::{
    process::{nonblocking, Capture},
    proxy::Proxy,
    workspace::Budget,
};
use crate::seatbelt::{scope, PathScope, BASE_PROFILE, PROCESS_DEFAULTS};
use std::{
    fmt::Write as _,
    fs, io,
    os::{fd::AsRawFd, unix::process::CommandExt},
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const ATTESTATION: &str = "ARGENTIC_SEATBELT_STARTED\n";
const CHECK_INTERVAL: Duration = Duration::from_millis(50);
const REAP_LIMIT: Duration = Duration::from_secs(5);

// Only admit the system tools needed for a bounded shell. In particular this
// excludes interpreters, loaders and launch helpers that can create sessions.
const PROGRAMS: &[&str] = &[
    "/bin/bash",
    "/bin/cat",
    "/bin/chmod",
    "/bin/cp",
    "/bin/date",
    "/bin/echo",
    "/bin/ls",
    "/bin/mkdir",
    "/bin/mv",
    "/bin/pwd",
    "/bin/rm",
    "/bin/rmdir",
    "/bin/sleep",
    "/usr/bin/awk",
    "/usr/bin/cmp",
    "/usr/bin/curl",
    "/usr/bin/cut",
    "/usr/bin/diff",
    "/usr/bin/dirname",
    "/usr/bin/du",
    "/usr/bin/find",
    "/usr/bin/grep",
    "/usr/bin/head",
    "/usr/bin/sed",
    "/usr/bin/sort",
    "/usr/bin/stat",
    "/usr/bin/tail",
    "/usr/bin/tee",
    "/usr/bin/touch",
    "/usr/bin/tr",
    "/usr/bin/uniq",
    "/usr/bin/wc",
];

pub(super) fn preflight() -> Result<(), String> {
    let workspace = tempfile::tempdir().map_err(|e| e.to_string())?;
    let scratch = tempfile::tempdir().map_err(|e| e.to_string())?;
    let network_profile = profile(&[(workspace.path(), false)], scratch.path(), Some(9))?;
    fs::metadata("/usr/bin/sandbox-exec").map_err(|e| e.to_string())?;
    // Compile the network variant here too. Port 9 is never used by this probe.
    execute(
        ":",
        &[(workspace.path(), false)],
        scratch.path(),
        None,
        &network_profile,
        5_000,
        &CancellationToken::new(),
    )
    .and_then(|output| {
        if output.starts_with("Exit status: 0\n") {
            Ok(())
        } else {
            Err(io::Error::other(output))
        }
    })
    .map_err(|error| format!("Argentic Seatbelt sandbox is unavailable: {error}"))
}

fn path_filter(path: &Path, tree: bool) -> Result<String, String> {
    let path = path.canonicalize().map_err(|e| e.to_string())?;
    scope(&if tree {
        PathScope::Subtree(path)
    } else {
        PathScope::Literal(path)
    })
}

fn profile(mounts: &[(&Path, bool)], scratch: &Path, port: Option<u16>) -> Result<String, String> {
    let mut result = String::from(BASE_PROFILE);
    result.push_str("(allow process-fork)\n");
    result.push_str(PROCESS_DEFAULTS);
    for program in PROGRAMS.iter().map(Path::new).filter(|path| path.is_file()) {
        let filter = path_filter(program, false)?;
        let _ = writeln!(result, "(allow process-exec {filter})");
    }
    for directory in super::RUNTIME_DIRS {
        let filter = path_filter(Path::new(directory), true)?;
        let _ = writeln!(
            result,
            "(allow file-read* file-test-existence file-map-executable {filter})"
        );
    }
    for &(file, tree) in super::BASE_RUNTIME_FILES
        .iter()
        .chain(super::NETWORK_MOUNTS.iter().filter(|_| port.is_some()))
    {
        let path = Path::new(file);
        if path.exists() {
            let filter = path_filter(path, tree)?;
            let _ = writeln!(result, "(allow file-read* file-test-existence {filter})");
        }
    }
    result.push_str("(allow file-write-data (literal \"/dev/null\"))\n");
    for (path, write) in mounts.iter().copied().chain([(scratch, true)]) {
        let filter = path_filter(path, true)?;
        let _ = writeln!(result, "(allow file-read* file-test-existence {filter})");
        let _ = writeln!(result, "(deny file-map-executable {filter})");
        if write {
            let _ = writeln!(result, "(allow file-write* {filter})");
        }
    }
    if let Some(port) = port {
        let _ = writeln!(
            result,
            "(allow network-outbound (remote tcp \"localhost:{port}\"))\n\
             (allow system-socket (require-all (socket-domain AF_INET) (socket-type SOCK_STREAM)))"
        );
    }
    Ok(result)
}

pub(super) fn run(
    command: &str,
    mounts: &[(&Path, bool)],
    proxy: Option<&Proxy>,
    timeout_ms: u64,
    cancel: &CancellationToken,
) -> Result<String, String> {
    let dir = tempfile::tempdir_in(
        mounts[0]
            .0
            .parent()
            .ok_or("Shell workspace snapshot has no parent")?,
    )
    .map_err(|error| error.to_string())?;
    let scratch = dir.path().canonicalize().map_err(|e| e.to_string())?;
    let profile = profile(mounts, &scratch, proxy.map(|proxy| proxy.port))?;
    execute(
        command, mounts, &scratch, proxy, &profile, timeout_ms, cancel,
    )
    .map_err(|error| error.to_string())
}

fn execute(
    command: &str,
    mounts: &[(&Path, bool)],
    scratch: &Path,
    proxy: Option<&Proxy>,
    profile: &str,
    timeout_ms: u64,
    cancel: &CancellationToken,
) -> io::Result<String> {
    super::super::check_cancel(cancel)?;
    let mut budgets = mounts
        .iter()
        .copied()
        .chain([(scratch, true)])
        .filter(|(_, write)| *write)
        .map(|(path, _)| Budget::open(path))
        .collect::<io::Result<Vec<_>>>()?;
    let mut process = Command::new("/usr/bin/sandbox-exec");
    process
        .args(["-p", profile, "/bin/bash", "--noprofile", "--norc", "-c"])
        .arg("printf 'ARGENTIC_SEATBELT_STARTED\\n'; exec \"$@\"")
        .arg("argentic-seatbelt")
        .args(super::BASH_ARGS)
        .arg(command)
        .current_dir(mounts[0].0)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", scratch)
        .env("TMPDIR", scratch)
        .env("LANG", "C")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (index, (path, _)) in mounts.iter().enumerate().skip(1) {
        process.env(format!("ARGENTIC_DIRECTORY_{index}"), path);
    }
    if let Some(proxy) = proxy {
        process.envs(proxy.env());
    }
    // New session before Seatbelt applies. Shell job control can make groups
    // within this session. Termination drains the whole session below.
    unsafe {
        process.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            for (resource, ceiling) in [
                (libc::RLIMIT_FSIZE, 16 * 1024 * 1024),
                (libc::RLIMIT_NPROC, 1024),
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
        process: process.spawn()?,
        reaped: false,
    };
    let mut stdout = sandbox
        .process
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("Missing stdout pipe"))?;
    let mut stderr = sandbox
        .process
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("Missing stderr pipe"))?;
    nonblocking(stdout.as_raw_fd())?;
    nonblocking(stderr.as_raw_fd())?;
    let (mut out, mut err) = (Capture::default(), Capture::default());
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let mut next_budget_check = Instant::now();
    let result = loop {
        out.pump(&mut stdout)?;
        err.pump(&mut stderr)?;
        if sandbox.exited()? {
            break Ok(());
        }
        if let Err(error) = super::super::check_cancel(cancel) {
            break Err(error);
        }
        if Instant::now() >= deadline {
            break Err(io::Error::other("Shell command timed out"));
        }
        if Instant::now() >= next_budget_check {
            for budget in &mut budgets {
                budget.check(cancel, deadline)?;
            }
            next_budget_check = Instant::now() + CHECK_INTERVAL;
        }
        thread::sleep(Duration::from_millis(5));
    };
    let status = sandbox.terminate()?;
    result?;
    for budget in &mut budgets {
        budget.check(cancel, deadline)?;
    }
    out.pump(&mut stdout)?;
    err.pump(&mut stderr)?;
    let stderr = err.into_text();
    let stdout = out.into_text();
    let stdout = stdout
        .strip_prefix(ATTESTATION)
        .ok_or_else(|| io::Error::other(format!("Seatbelt startup failed ({status}): {stderr}")))?;
    let code = status
        .code()
        .map_or_else(|| status.to_string(), |code| code.to_string());
    Ok(format!(
        "Exit status: {code}\nstdout:\n{stdout}\nstderr:\n{stderr}"
    ))
}

struct Sandbox {
    process: Child,
    reaped: bool,
}

impl Sandbox {
    fn exited(&self) -> io::Result<bool> {
        let mut info = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                self.process.id(),
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { info.si_pid() } != 0)
    }

    fn terminate(&mut self) -> io::Result<ExitStatus> {
        let group = self.process.id() as libc::pid_t;
        if !self.reaped {
            signal(-group)?;
            // Keep the session leader unreaped to retain its PID while other
            // groups are discovered and signalled.
            let deadline = Instant::now() + REAP_LIMIT;
            loop {
                let mut active = false;
                for pid in session_members(group)? {
                    if pid != group && unsafe { libc::getsid(pid) } == group {
                        active = true;
                        signal(pid)?;
                    }
                }
                if !active && self.exited()? {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::other("Seatbelt shell session did not terminate"));
                }
                thread::sleep(Duration::from_millis(5));
            }
            self.reaped = true;
        }
        self.process.wait()
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        if !self.reaped && self.terminate().is_err() {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
    }
}

fn signal(pid: libc::pid_t) -> io::Result<()> {
    if unsafe { libc::kill(pid, libc::SIGKILL) } == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error);
        }
    }
    Ok(())
}

fn session_members(group: libc::pid_t) -> io::Result<Vec<libc::pid_t>> {
    let capacity = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
    if capacity <= 0 {
        return Err(io::Error::last_os_error());
    }
    let mut pids = vec![0; capacity as usize + 1024];
    let size = i32::try_from(std::mem::size_of_val(pids.as_slice()))
        .map_err(|_| io::Error::other("Oversized Seatbelt process inventory"))?;
    let count = unsafe { libc::proc_listallpids(pids.as_mut_ptr().cast(), size) };
    if count <= 0 || count as usize >= pids.len() {
        return Err(io::Error::other("Cannot enumerate Seatbelt shell session"));
    }
    pids.truncate(count as usize);
    pids.retain(|&pid| pid > 0 && unsafe { libc::getsid(pid) } == group);
    Ok(pids)
}
