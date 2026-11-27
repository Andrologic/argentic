use std::ffi::OsStr;
use std::io;
use std::process::ExitStatus;
use std::time::Duration;

#[cfg(unix)]
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;

#[cfg(unix)]
use std::collections::{HashMap, HashSet};
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
#[cfg(windows)]
use windows_sys::Win32::Foundation::ERROR_PROCESS_ABORTED;
#[cfg(windows)]
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectBasicAccountingInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessLaunchVisibility {
    HiddenBackgroundLauncher,
    VisibleTerminal,
}

#[cfg(target_os = "windows")]
pub const CREATE_NO_WINDOW: u32 = 0x0800_0000;
#[cfg(target_os = "windows")]
const CREATE_SUSPENDED: u32 = 0x0000_0004;

#[cfg(windows)]
#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtResumeProcess(process_handle: windows_sys::Win32::Foundation::HANDLE) -> i32;
}

#[cfg(not(target_os = "windows"))]
pub const CREATE_NO_WINDOW: u32 = 0;

pub fn background_command(program: impl AsRef<OsStr>) -> std::process::Command {
    let mut command = std::process::Command::new(program);
    apply_std_visibility(
        &mut command,
        ProcessLaunchVisibility::HiddenBackgroundLauncher,
    );
    command
}

pub fn visible_terminal_command(program: impl AsRef<OsStr>) -> std::process::Command {
    let mut command = std::process::Command::new(program);
    apply_std_visibility(&mut command, ProcessLaunchVisibility::VisibleTerminal);
    command
}

pub fn background_tokio_command(program: impl AsRef<OsStr>) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(program);
    apply_tokio_visibility(
        &mut command,
        ProcessLaunchVisibility::HiddenBackgroundLauncher,
    );
    command
}

pub fn visible_terminal_tokio_command(program: impl AsRef<OsStr>) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(program);
    apply_tokio_visibility(&mut command, ProcessLaunchVisibility::VisibleTerminal);
    command
}

#[cfg(target_os = "windows")]
fn apply_std_visibility(command: &mut std::process::Command, visibility: ProcessLaunchVisibility) {
    use std::os::windows::process::CommandExt;

    if windows_creation_flags_for_visibility(visibility) != 0 {
        command.creation_flags(windows_creation_flags_for_visibility(visibility));
    }
}

#[cfg(not(target_os = "windows"))]
fn apply_std_visibility(
    _command: &mut std::process::Command,
    _visibility: ProcessLaunchVisibility,
) {
}

#[cfg(target_os = "windows")]
fn apply_tokio_visibility(
    command: &mut tokio::process::Command,
    visibility: ProcessLaunchVisibility,
) {
    if windows_creation_flags_for_visibility(visibility) != 0 {
        command.creation_flags(windows_creation_flags_for_visibility(visibility));
    }
}

#[cfg(not(target_os = "windows"))]
fn apply_tokio_visibility(
    _command: &mut tokio::process::Command,
    _visibility: ProcessLaunchVisibility,
) {
}

pub fn windows_creation_flags_for_visibility(visibility: ProcessLaunchVisibility) -> u32 {
    match visibility {
        ProcessLaunchVisibility::HiddenBackgroundLauncher => CREATE_NO_WINDOW,
        ProcessLaunchVisibility::VisibleTerminal => 0,
    }
}

pub fn is_known_visible_terminal_app_id(app_id: &str) -> bool {
    let app_id = app_id.trim().to_ascii_lowercase();
    matches!(
        app_id.as_str(),
        "windows-terminal"
            | "powershell"
            | "pwsh"
            | "command-prompt"
            | "wezterm"
            | "ghostty"
            | "kitty"
            | "terminal"
            | "gnome-terminal"
            | "konsole"
            | "xfce4-terminal"
            | "tilix"
            | "mate-terminal"
    )
}

pub const DEFAULT_TERMINATION_GRACE_PERIOD: Duration = Duration::from_secs(2);
const HARD_REAP_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(unix)]
const CONTAINMENT_ID_ENV: &str = "MACRO_PROCESS_CONTAINMENT_ID";
#[cfg(unix)]
static NEXT_CONTAINMENT_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(windows)]
type JobObjectHandle = OwnedHandle;

#[cfg(unix)]
#[derive(Debug)]
struct UnixContainmentMarker {
    // Hold the marker descriptor open for the lifetime of the containment marker.
    _file: std::fs::File,
    path: PathBuf,
}

#[cfg(unix)]
impl UnixContainmentMarker {
    fn create(containment_id: &str) -> io::Result<Self> {
        use std::os::fd::AsRawFd;

        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("macro-process-{containment_id}-{nonce}.lock"));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
            let error = io::Error::last_os_error();
            drop(file);
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }
        Ok(Self { _file: file, path })
    }
}

#[cfg(unix)]
impl Drop for UnixContainmentMarker {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub fn background_contained_tokio_command(program: impl AsRef<OsStr>) -> tokio::process::Command {
    let mut command = background_tokio_command(program);
    apply_background_containment(&mut command);
    command
}

pub fn apply_background_containment(command: &mut tokio::process::Command) {
    command.kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
}

#[derive(Debug)]
pub struct ContainedBackgroundProcess {
    #[cfg(target_os = "linux")]
    child: Box<tokio::process::Child>,
    #[cfg(not(target_os = "linux"))]
    child: tokio::process::Child,
    #[cfg(target_os = "linux")]
    command_status: tokio::net::unix::pipe::Receiver,
    #[cfg(target_os = "linux")]
    command_exit: Option<ExitStatus>,
    #[cfg(unix)]
    process_group_id: Option<u32>,
    #[cfg(unix)]
    containment_id: String,
    #[cfg(target_os = "macos")]
    containment_marker: UnixContainmentMarker,
    #[cfg(windows)]
    job_object: JobObjectHandle,
}

impl ContainedBackgroundProcess {
    pub fn spawn(mut command: tokio::process::Command) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        LinuxPidFd::require_support()?;
        apply_background_containment(&mut command);
        #[cfg(unix)]
        let containment_id = next_containment_id();
        #[cfg(unix)]
        command.env(CONTAINMENT_ID_ENV, &containment_id);
        #[cfg(target_os = "macos")]
        let containment_marker = {
            use std::os::fd::AsRawFd;
            use std::os::unix::process::CommandExt;

            let marker = UnixContainmentMarker::create(&containment_id)?;
            let marker_descriptor = marker._file.as_raw_fd();
            unsafe {
                command.as_std_mut().pre_exec(move || {
                    if libc::fcntl(marker_descriptor, libc::F_SETFD, 0) == -1 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            marker
        };
        #[cfg(target_os = "linux")]
        let (status_writer, command_status) = {
            use std::os::fd::AsRawFd;
            use std::os::unix::process::CommandExt;
            let (writer, reader) = tokio::net::unix::pipe::pipe()?;
            let descriptor = writer.as_raw_fd();
            // Drop signals the workload. The reaper must survive long enough
            // to wait for any children it adopts.
            command.kill_on_drop(false);
            unsafe {
                command
                    .as_std_mut()
                    .pre_exec(move || install_linux_process_supervisor(descriptor));
            }
            (writer, reader)
        };
        #[cfg(windows)]
        command.creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED);
        let child = command.spawn()?;
        #[cfg(target_os = "linux")]
        drop(status_writer);
        #[cfg(unix)]
        let process_group_id = child.id();
        #[cfg(windows)]
        let job_object = match attach_child_to_job_object(&child) {
            Ok(job_object) => job_object,
            Err(error) => {
                let mut child = child;
                let _ = child.start_kill();
                return Err(error);
            }
        };
        #[cfg(windows)]
        if let Err(error) = resume_child_process(&child) {
            unsafe { TerminateJobObject(job_object.as_raw_handle() as _, ERROR_PROCESS_ABORTED) };
            return Err(error);
        }
        Ok(Self {
            #[cfg(target_os = "linux")]
            child: Box::new(child),
            #[cfg(not(target_os = "linux"))]
            child,
            #[cfg(target_os = "linux")]
            command_status,
            #[cfg(target_os = "linux")]
            command_exit: None,
            #[cfg(unix)]
            process_group_id,
            #[cfg(unix)]
            containment_id,
            #[cfg(target_os = "macos")]
            containment_marker,
            #[cfg(windows)]
            job_object,
        })
    }

    pub fn spawn_background(program: impl AsRef<OsStr>) -> io::Result<Self> {
        Self::spawn(background_contained_tokio_command(program))
    }

    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn take_stdin(&mut self) -> Option<tokio::process::ChildStdin> {
        self.child.stdin.take()
    }

    pub fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
        self.child.stdout.take()
    }

    pub fn take_stderr(&mut self) -> Option<tokio::process::ChildStderr> {
        self.child.stderr.take()
    }

    #[cfg(unix)]
    pub fn unix_process_group_id(&self) -> Option<u32> {
        self.process_group_id
    }

    #[cfg(unix)]
    fn containment_marker_path(&self) -> Option<&Path> {
        #[cfg(target_os = "macos")]
        {
            Some(&self.containment_marker.path)
        }
        #[cfg(not(target_os = "macos"))]
        {
            None
        }
    }

    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::process::ExitStatusExt;
            use tokio::io::AsyncReadExt;

            // Match Child::wait for callers that did not take stdin.
            drop(self.child.stdin.take());
            if let Some(status) = self.command_exit {
                return Ok(status);
            }
            // The reaper reports one atomic four-byte pipe write. Use a
            // cancellation-safe read because callers select against Stop.
            let mut bytes = [0; std::mem::size_of::<i32>()];
            if self.command_status.read(&mut bytes).await? != bytes.len() {
                return Err(io::Error::other(
                    "Linux reaper did not report the command status",
                ));
            }
            let status = ExitStatus::from_raw(i32::from_ne_bytes(bytes));
            self.command_exit = Some(status);
            Ok(status)
        }
        #[cfg(not(target_os = "linux"))]
        self.child.wait().await
    }

    pub async fn terminate_bounded(&mut self) -> io::Result<ExitStatus> {
        self.terminate_with_grace(DEFAULT_TERMINATION_GRACE_PERIOD)
            .await
    }

    pub async fn terminate_with_grace(&mut self, grace_period: Duration) -> io::Result<ExitStatus> {
        #[cfg(unix)]
        {
            self.terminate_unix(grace_period).await
        }
        #[cfg(windows)]
        {
            self.terminate_windows(grace_period).await
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = grace_period;
            self.child.start_kill()?;
            match tokio::time::timeout(HARD_REAP_TIMEOUT, self.child.wait()).await {
                Ok(status) => status,
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "contained process did not exit after kill",
                )),
            }
        }
    }

    #[cfg(unix)]
    fn stop_unix_workload(&self) -> (Vec<UnixProcessTarget>, Option<io::Error>) {
        #[cfg(target_os = "linux")]
        let supervisor = match self.child.id() {
            Some(pid) => Some(pid),
            // Reaping the supervisor releases the workload group ID.
            None => return (Vec::new(), None),
        };
        #[cfg(not(target_os = "linux"))]
        let supervisor = None;
        terminate_unix_process_tree(
            self.process_group_id,
            &self.containment_id,
            self.containment_marker_path(),
            supervisor,
        )
    }

    #[cfg(unix)]
    async fn terminate_unix(&mut self, grace_period: Duration) -> io::Result<ExitStatus> {
        if self.process_group_id.is_none() {
            return self.wait().await;
        }
        let (targets, inspection_error) = self.stop_unix_workload();
        let reap_timeout = if grace_period.is_zero() {
            HARD_REAP_TIMEOUT
        } else {
            grace_period
        };
        let deadline = tokio::time::Instant::now() + reap_timeout;
        match tokio::time::timeout_at(deadline, self.child.wait()).await {
            Ok(status) => {
                #[cfg(not(target_os = "linux"))]
                if let Some(error) = inspection_error {
                    return Err(io::Error::other(IncompleteProcessTreeInspection(error)));
                }
                let status = status?;
                #[cfg(target_os = "linux")]
                let status = {
                    if !status.success() {
                        return Err(inspection_error.unwrap_or_else(|| {
                            io::Error::other("Linux reaper did not confirm tree exit")
                        }));
                    }
                    self.wait().await?
                };
                // SIGKILL delivery and the leader's reap do not establish
                // that descendants with detached streams have exited.
                wait_for_processes_exit(&targets, deadline).await?;
                // Retire only after all observed processes disappeared.
                // Drop must not scan the machine again or signal a reused PID.
                self.process_group_id = None;
                #[cfg(target_os = "linux")]
                if let Some(error) = inspection_error {
                    let only_inventory_failed = error
                        .get_ref()
                        .and_then(|detail| detail.downcast_ref::<ProcessTreeInspectionDiagnostic>())
                        .is_some_and(|detail| {
                            !detail
                                .failures
                                .0
                                .iter()
                                .any(|(source, _)| *source == ProcessInspector::ProcessHandle)
                        });
                    if !only_inventory_failed {
                        return Err(error);
                    }
                    tracing::debug!(error = %error, "Linux reaper confirmed exit after incomplete process inspection");
                }
                Ok(status)
            }
            Err(_) => {
                #[cfg(target_os = "linux")]
                if let Some(error) = inspection_error {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        IncompleteProcessTreeInspection(error),
                    ));
                }
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "contained process tree did not exit after SIGKILL",
                ))
            }
        }
    }

    #[cfg(windows)]
    async fn terminate_windows(&mut self, grace_period: Duration) -> io::Result<ExitStatus> {
        let reap_timeout = if grace_period.is_zero() {
            HARD_REAP_TIMEOUT
        } else {
            grace_period
        };
        let deadline = tokio::time::Instant::now() + reap_timeout;
        // The leader may already have exited while descendants remain active.
        if unsafe {
            TerminateJobObject(self.job_object.as_raw_handle() as _, ERROR_PROCESS_ABORTED)
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        match tokio::time::timeout_at(deadline, async {
            let status = self.child.wait().await?;
            while windows_job_active_processes(&self.job_object)? != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(status)
        })
        .await
        {
            Ok(status) => status,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "contained process tree did not exit after job termination",
            )),
        }
    }
}

#[cfg(windows)]
fn windows_job_active_processes(job: &JobObjectHandle) -> io::Result<u32> {
    let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
    if unsafe {
        QueryInformationJobObject(
            job.as_raw_handle() as _,
            JobObjectBasicAccountingInformation,
            (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
            std::mem::size_of_val(&info) as u32,
            std::ptr::null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(info.ActiveProcesses)
}

/// Observe the same processes discovered and suspended by the existing tree
/// inspector. Failure or expiry keeps the containment identity unresolved.
#[cfg(unix)]
async fn wait_for_processes_exit(
    targets: &[UnixProcessTarget],
    deadline: tokio::time::Instant,
) -> io::Result<()> {
    loop {
        if process_targets_exited(targets)? {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "contained descendants did not disappear after SIGKILL",
            ));
        }
        tokio::time::sleep_until(
            deadline.min(tokio::time::Instant::now() + Duration::from_millis(10)),
        )
        .await;
    }
}

#[cfg(unix)]
fn process_targets_exited(targets: &[UnixProcessTarget]) -> io::Result<bool> {
    let mut all_exited = true;
    for target in targets {
        #[cfg(target_os = "linux")]
        let exited = target.exited()?;
        #[cfg(not(target_os = "linux"))]
        let exited = {
            if unsafe { libc::kill(*target as libc::pid_t, 0) } == 0 {
                false
            } else {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error);
                }
                true
            }
        };
        all_exited &= exited;
    }
    Ok(all_exited)
}

#[cfg(target_os = "linux")]
fn wait_for_terminal_targets(targets: &[UnixProcessTarget]) -> io::Result<()> {
    let deadline = std::time::Instant::now() + DEFAULT_TERMINATION_GRACE_PERIOD;
    while !process_targets_exited(targets)? {
        if std::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "contained descendants did not disappear after SIGKILL",
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

#[cfg(windows)]
fn resume_child_process(child: &tokio::process::Child) -> io::Result<()> {
    let Some(process_handle) = child.raw_handle() else {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "tokio child did not expose a raw process handle",
        ));
    };
    let status = unsafe { NtResumeProcess(process_handle) };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "failed to resume contained process after job assignment: NTSTATUS {status:#x}"
        )))
    }
}

#[cfg(unix)]
fn signal_process_group(process_group_id: u32, signal: i32) {
    let _ = unsafe { libc::kill(-(process_group_id as libc::pid_t), signal) };
}

#[cfg(all(unix, any(not(target_os = "linux"), test)))]
fn signal_process(process_id: u32, signal: i32) {
    let _ = unsafe { libc::kill(process_id as libc::pid_t, signal) };
}

#[cfg(unix)]
fn next_containment_id() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        NEXT_CONTAINMENT_ID.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(target_os = "linux")]
fn install_linux_process_supervisor(status_descriptor: i32) -> io::Result<()> {
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    // The workload inherits the supervisor's original group. Keep that
    // group's ID tied to the supervisor PID even after the command exits.
    // A short-lived child reserves a separate group for the supervisor.
    let supervisor_group = unsafe { libc::fork() };
    match supervisor_group {
        -1 => return Err(io::Error::last_os_error()),
        0 => unsafe { libc::_exit(0) },
        _ => {}
    }
    match start_linux_workload(supervisor_group, status_descriptor) {
        Ok(None) => Ok(()),
        Ok(Some(command_process_id)) => {
            close_linux_file_descriptors_except(status_descriptor);
            supervise_linux_command(command_process_id, status_descriptor)
        }
        Err(error) => {
            reap_linux_setup_child(supervisor_group);
            Err(error)
        }
    }
}

#[cfg(target_os = "linux")]
fn start_linux_workload(
    supervisor_group: libc::pid_t,
    status_descriptor: i32,
) -> io::Result<Option<libc::pid_t>> {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    // Linux retains the group of an exited child until it is reaped.
    if unsafe { libc::setpgid(supervisor_group, supervisor_group) } == -1 {
        return Err(io::Error::last_os_error());
    }
    let (mut ready_reader, mut ready_writer) = UnixStream::pair()?;
    let pid = unsafe { libc::fork() };
    if pid == -1 {
        return Err(io::Error::last_os_error());
    }
    if pid == 0 {
        drop(ready_writer);
        unsafe { libc::close(status_descriptor) };
        // Do not exec while the supervisor is still in the workload group.
        if ready_reader.read_exact(&mut [0]).is_err() {
            unsafe { libc::_exit(1) };
        }
        return Ok(None);
    }

    drop(ready_reader);
    let ready = if unsafe { libc::setpgid(0, supervisor_group) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        ready_writer.write_all(&[1])
    };
    drop(ready_writer);
    if let Err(error) = ready {
        reap_linux_setup_child(pid);
        return Err(error);
    }
    Ok(Some(pid))
}

#[cfg(target_os = "linux")]
fn reap_linux_setup_child(pid: libc::pid_t) {
    while unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) } == -1
        && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted
    {}
}

#[cfg(target_os = "linux")]
fn supervise_linux_command(command_process_id: libc::pid_t, status_descriptor: i32) -> ! {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    let mut command_status_reported = false;
    loop {
        let mut status = 0;
        let waited = unsafe { libc::waitpid(-1, &mut status, 0) };
        if waited == command_process_id {
            write_linux_command_status(status_descriptor, status);
            unsafe {
                libc::close(status_descriptor);
            }
            command_status_reported = true;
            continue;
        }
        if waited >= 0 {
            continue;
        }

        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::ECHILD) if command_status_reported => unsafe { libc::_exit(0) },
            _ => unsafe { libc::_exit(1) },
        }
    }
}

#[cfg(target_os = "linux")]
fn write_linux_command_status(descriptor: i32, status: i32) {
    let mut written_bytes = 0_usize;
    while written_bytes < std::mem::size_of::<i32>() {
        let result = unsafe {
            libc::write(
                descriptor,
                (&status as *const i32 as *const u8).add(written_bytes) as *const _,
                std::mem::size_of::<i32>() - written_bytes,
            )
        };
        if result > 0 {
            written_bytes += result as usize;
            continue;
        }
        if result == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return;
    }
}

#[cfg(target_os = "linux")]
fn close_linux_file_descriptors_except(preserved_descriptor: i32) {
    let close_range = |first: u32, last: u32| {
        first > last || unsafe { libc::syscall(libc::SYS_close_range, first, last, 0_u32) } == 0
    };
    let lower_closed =
        preserved_descriptor <= 0 || close_range(0, (preserved_descriptor - 1) as u32);
    let upper_closed = close_range((preserved_descriptor + 1) as u32, u32::MAX);
    if lower_closed && upper_closed {
        return;
    }

    for descriptor in 0..65_536 {
        if descriptor != preserved_descriptor {
            let _ = unsafe { libc::close(descriptor) };
        }
    }
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct UnixProcessRecord {
    process_id: u32,
    parent_id: u32,
    process_group_id: u32,
}

#[cfg(unix)]
fn parse_unix_process_table(output: &str) -> Vec<UnixProcessRecord> {
    output
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let process_id = fields.next()?.parse().ok()?;
            let parent_id = fields.next()?.parse().ok()?;
            let process_group_id = fields.next()?.parse().ok()?;
            Some(UnixProcessRecord {
                process_id,
                parent_id,
                process_group_id,
            })
        })
        .collect()
}

#[cfg(unix)]
const PROCESS_INSPECTION_TIMEOUT: Duration = Duration::from_secs(1);
#[cfg(unix)]
const PROCESS_TREE_INSPECTION_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(unix)]
const MAX_PROCESS_INSPECTION_BYTES: usize = 16 * 1024 * 1024;

// Only controlled categories and numeric OS details cross the diagnostic boundary.
// Inspector output, command paths, arguments and environment never do.
pub fn process_error_diagnostic(error: &io::Error) -> String {
    #[cfg(unix)]
    if let Some(detail) = error.get_ref() {
        if let Some(detail) = detail.downcast_ref::<ProcessTreeInspectionDiagnostic>() {
            return detail.to_string();
        }
        if let Some(detail) = detail.downcast_ref::<IncompleteProcessTreeInspection>() {
            return detail.to_string();
        }
        if let Some(detail) = detail.downcast_ref::<InspectionTimeout>() {
            return detail.to_string();
        }
        #[cfg(target_os = "linux")]
        if let Some(detail) = detail.downcast_ref::<LinuxPidFdRequired>() {
            return detail.to_string();
        }
    }
    let message = error.to_string();
    let cause = match message.as_str() {
        "process inspection deadline expired" => "inspection deadline expired",
        "process tree inspection deadline expired" => "tree inspection deadline expired",
        "process inspection timed out" => "inspection timed out",
        "process inspection output exceeded its limit" => "inspection output exceeded its limit",
        "missing inspection stdout" => "missing inspection stdout",
        "ps failed while reading process descendants" => "process table command failed",
        "process containment inspection failed" => "containment command failed",
        "process marker inspection panicked" => "marker inspection panicked",
        "terminal process group ownership could not be verified" => {
            "terminal process group ownership could not be verified"
        }
        "contained process tree did not exit after SIGKILL" => "process tree reap timed out",
        "contained process tree did not exit after job termination" => {
            "process tree reap timed out"
        }
        "contained process did not exit after kill" => "process reap timed out",
        _ => "process operation failed",
    };
    format!(
        "{cause} (kind={:?}, errno={:?})",
        error.kind(),
        error.raw_os_error()
    )
}

#[cfg(unix)]
#[derive(Debug)]
struct InspectionTimeout {
    elapsed_ms: u128,
    bytes: usize,
    eof: bool,
}

#[cfg(unix)]
impl std::fmt::Display for InspectionTimeout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "inspection timed out after {}ms ({} bytes, EOF={})",
            self.elapsed_ms, self.bytes, self.eof
        )
    }
}

#[cfg(unix)]
impl std::error::Error for InspectionTimeout {}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProcessInspector {
    Environment,
    Marker,
    ProcessTable,
    Descendants,
    #[cfg(target_os = "linux")]
    ProcessHandle,
}

#[cfg(unix)]
#[derive(Debug, Default)]
struct ProcessInspectionFailures(Vec<(ProcessInspector, String)>);

#[cfg(unix)]
impl ProcessInspectionFailures {
    fn record(&mut self, inspector: ProcessInspector, error: &io::Error) {
        if !self.0.iter().any(|(source, _)| *source == inspector) {
            let mut cause = process_error_diagnostic(error);
            // All diagnostic fields are controlled ASCII; four sources at most.
            cause.truncate(256);
            self.0.push((inspector, cause));
        }
    }

    fn extend(&mut self, failures: Self) {
        for (inspector, cause) in failures.0 {
            if !self.0.iter().any(|(source, _)| *source == inspector) {
                self.0.push((inspector, cause));
            }
        }
    }
}

#[cfg(unix)]
#[derive(Debug)]
struct ProcessTreeInspectionDiagnostic {
    failures: ProcessInspectionFailures,
    end_reason: &'static str,
}

#[cfg(unix)]
impl std::fmt::Display for ProcessTreeInspectionDiagnostic {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.end_reason)?;
        if let Some((source, cause)) = self.failures.0.first() {
            write!(
                formatter,
                "; first failure: {source:?}: {cause}; failed inspectors:"
            )?;
            for (source, cause) in &self.failures.0 {
                write!(formatter, " {source:?}: {cause};")?;
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
impl std::error::Error for ProcessTreeInspectionDiagnostic {}

#[cfg(unix)]
#[derive(Debug)]
struct IncompleteProcessTreeInspection(io::Error);

#[cfg(unix)]
impl std::fmt::Display for IncompleteProcessTreeInspection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Process tree inspection was incomplete: {}",
            process_error_diagnostic(&self.0)
        )
    }
}

#[cfg(unix)]
impl std::error::Error for IncompleteProcessTreeInspection {}

// This helper must not use ContainedBackgroundProcess: its teardown calls these
// inspections itself. Drain a nonblocking pipe and reap only with try_wait.
#[cfg(unix)]
fn bounded_process_inspection(
    command: &mut std::process::Command,
    deadline: std::time::Instant,
) -> io::Result<std::process::Output> {
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    use std::process::Stdio;
    let started = std::time::Instant::now();
    let deadline = deadline.min(started + PROCESS_INSPECTION_TIMEOUT);
    if std::time::Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "process inspection deadline expired",
        ));
    }
    let mut child = command
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let process_group = child.id();
    let result = (|| {
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("missing inspection stdout"))?;
        let descriptor = stdout.as_raw_fd();
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
        if flags < 0
            || unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut output = Vec::new();
        let mut buffer = [0_u8; 8192];
        let mut eof = false;
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    InspectionTimeout {
                        elapsed_ms: started.elapsed().as_millis(),
                        bytes: output.len(),
                        eof,
                    },
                ));
            }
            match stdout.read(&mut buffer) {
                Ok(0) => eof = true,
                Ok(count) => {
                    if output.len() + count > MAX_PROCESS_INSPECTION_BYTES {
                        return Err(io::Error::other(
                            "process inspection output exceeded its limit",
                        ));
                    }
                    output.extend_from_slice(&buffer[..count]);
                    continue;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
            // On Linux, keep the helper PID reserved while a descendant
            // might still own the pipe and require group cleanup.
            if eof || cfg!(not(target_os = "linux")) {
                if let Some(status) = child.try_wait()? {
                    if eof {
                        return Ok(std::process::Output {
                            status,
                            stdout: output,
                            stderr: Vec::new(),
                        });
                    }
                }
            }
            if eof {
                std::thread::sleep(Duration::from_millis(1));
            } else {
                let mut ready = libc::pollfd {
                    fd: descriptor,
                    events: libc::POLLIN,
                    revents: 0,
                };
                // poll wakes as soon as more output arrives; a fixed sleep per
                // pipe buffer would make a valid large inventory miss its budget.
                unsafe {
                    libc::poll(&mut ready, 1, 5);
                }
            }
        }
    })();
    // Include descendants holding stdout open, including after the helper exits.
    // On Linux a successful inspection already reaped the helper.
    if result.is_err() || cfg!(not(target_os = "linux")) {
        signal_process_group(process_group, libc::SIGKILL);
    }
    let reap_deadline = std::time::Instant::now() + Duration::from_millis(100);
    while matches!(child.try_wait(), Ok(None)) && std::time::Instant::now() < reap_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    result
}

#[cfg(all(test, target_os = "linux"))]
thread_local! {
    static FAIL_PROCESS_TABLE_INSPECTION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(unix)]
fn read_unix_process_table(deadline: std::time::Instant) -> io::Result<Vec<UnixProcessRecord>> {
    #[cfg(all(test, target_os = "linux"))]
    if FAIL_PROCESS_TABLE_INSPECTION.get() {
        return Err(io::Error::from(io::ErrorKind::NotFound));
    }
    let output = bounded_process_inspection(
        std::process::Command::new("/bin/ps").args(["-axo", "pid=,ppid=,pgid="]),
        deadline,
    )?;
    if !output.status.success() {
        return Err(io::Error::other(
            "ps failed while reading process descendants",
        ));
    }
    Ok(parse_unix_process_table(&String::from_utf8_lossy(
        &output.stdout,
    )))
}

#[cfg(unix)]
fn descendant_process_ids(
    roots: impl IntoIterator<Item = u32>,
    table: &[UnixProcessRecord],
    deadline: std::time::Instant,
) -> io::Result<HashSet<u32>> {
    let mut children_by_parent = HashMap::<u32, Vec<u32>>::new();
    for record in table {
        check_process_inspection_deadline(deadline)?;
        children_by_parent
            .entry(record.parent_id)
            .or_default()
            .push(record.process_id);
    }
    let mut descendants = HashSet::new();
    let mut pending = roots.into_iter().collect::<Vec<_>>();
    while let Some(parent_id) = pending.pop() {
        check_process_inspection_deadline(deadline)?;
        if let Some(children) = children_by_parent.get(&parent_id) {
            for &child_id in children {
                check_process_inspection_deadline(deadline)?;
                if descendants.insert(child_id) {
                    pending.push(child_id);
                }
            }
        }
    }
    Ok(descendants)
}

#[cfg(unix)]
fn check_process_inspection_deadline(deadline: std::time::Instant) -> io::Result<()> {
    if std::time::Instant::now() >= deadline {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "process tree inspection deadline expired",
        ))
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn processes_with_containment_id(
    containment_id: &str,
    deadline: std::time::Instant,
) -> io::Result<HashSet<u32>> {
    let expected = format!("{CONTAINMENT_ID_ENV}={containment_id}");
    let own_process_id = std::process::id();
    let processes = std::fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .flatten()
        .take_while(|_| std::time::Instant::now() < deadline)
        .filter_map(|entry| {
            let process_id = entry.file_name().to_str()?.parse::<u32>().ok()?;
            if process_id == own_process_id {
                return None;
            }
            has_linux_containment_env(&entry.path(), expected.as_bytes()).then_some(process_id)
        })
        .collect();
    if std::time::Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "process inspection timed out",
        ));
    }
    Ok(processes)
}

#[cfg(target_os = "macos")]
fn processes_with_containment_marker(
    marker_path: Option<&Path>,
    deadline: std::time::Instant,
) -> io::Result<HashSet<u32>> {
    let Some(marker_path) = marker_path else {
        return Ok(HashSet::new());
    };
    let own_process_id = std::process::id();
    let output = bounded_process_inspection(
        std::process::Command::new("/usr/sbin/lsof")
            .args(["-n", "-P", "-F", "p", "--"])
            .arg(marker_path),
        deadline,
    )?;
    // Macro itself holds the marker open throughout inspection. Even when
    // no child holds it, lsof must report Macro; a nonzero exit is an error.
    if !output.status.success() {
        return Err(io::Error::other("process containment inspection failed"));
    }
    let processes = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.strip_prefix('p')?.parse::<u32>().ok())
        .filter(|process_id| *process_id != own_process_id)
        .collect();
    if std::time::Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "process inspection timed out",
        ));
    }
    Ok(processes)
}

#[cfg(target_os = "linux")]
fn processes_with_containment_marker(
    marker_path: Option<&Path>,
    deadline: std::time::Instant,
) -> io::Result<HashSet<u32>> {
    let Some(marker) = marker_path.and_then(|path| std::fs::metadata(path).ok()) else {
        return Ok(HashSet::new());
    };
    let processes = std::fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .flatten()
        .take_while(|_| std::time::Instant::now() < deadline)
        .filter_map(|entry| {
            let pid = entry.file_name().to_str()?.parse::<u32>().ok()?;
            if pid == std::process::id() {
                return None;
            }
            has_linux_containment_marker(&entry.path(), &marker).then_some(pid)
        })
        .collect();
    if std::time::Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "process inspection timed out",
        ));
    }
    Ok(processes)
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "linux"))))]
fn processes_with_containment_marker(
    _marker_path: Option<&Path>,
    _deadline: std::time::Instant,
) -> io::Result<HashSet<u32>> {
    Ok(HashSet::new())
}

#[cfg(all(unix, not(target_os = "linux")))]
fn has_containment_environment_field(process_description: &str, expected: &str) -> bool {
    process_description
        .split_whitespace()
        .any(|field| field == expected)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn processes_with_containment_id(
    containment_id: &str,
    deadline: std::time::Instant,
) -> io::Result<HashSet<u32>> {
    let expected = format!("{CONTAINMENT_ID_ENV}={containment_id}");
    let own_process_id = std::process::id();
    let output = bounded_process_inspection(
        std::process::Command::new("/bin/ps").args(["-axeww", "-o", "pid=,command="]),
        deadline,
    )?;
    if !output.status.success() {
        return Err(io::Error::other("process containment inspection failed"));
    }
    let processes = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let line = line.trim_start();
            let split_at = line.find(char::is_whitespace)?;
            let process_id = line[..split_at].parse::<u32>().ok()?;
            (process_id != own_process_id
                && has_containment_environment_field(&line[split_at..], &expected))
            .then_some(process_id)
        })
        .collect();
    if std::time::Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "process inspection timed out",
        ));
    }
    Ok(processes)
}

/// Owns the process group and tagged descendants of a PTY child.
#[cfg(unix)]
pub struct TerminalProcessTree {
    pub containment_id: String,
    pub process_group_id: Option<u32>,
    marker: UnixContainmentMarker,
}

#[cfg(unix)]
impl TerminalProcessTree {
    pub fn new() -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        LinuxPidFd::require_support()?;
        let containment_id = next_containment_id();
        let marker = UnixContainmentMarker::create(&containment_id)?;
        Ok(Self {
            containment_id,
            process_group_id: None,
            marker,
        })
    }

    pub fn prepare_command(&self, command: &mut portable_pty::CommandBuilder) {
        command.env(CONTAINMENT_ID_ENV, &self.containment_id);
        // portable-pty closes inherited descriptors before exec. Open the marker
        // inside its child, then preserve the original command as separate argv.
        let prefix: Vec<std::ffi::OsString> = vec![
            "/bin/sh".into(),
            "-c".into(),
            "exec 9<\"$1\"; shift; exec \"$@\"".into(),
            "macro-pty".into(),
            self.marker.path.as_os_str().to_owned(),
        ];
        command.get_argv_mut().splice(0..0, prefix);
    }

    #[cfg(all(any(test, feature = "dev-support"), target_os = "macos"))]
    #[doc(hidden)]
    pub fn marker_path_for_test(&self) -> &Path {
        &self.marker.path
    }

    pub fn terminate(&mut self) {
        if let Err(error) = self.try_terminate() {
            tracing::warn!(action = "terminal_process_tree_termination_incomplete", error = %error,
                containment_id = %self.containment_id,
                "Retaining process tree identity for a subsequent termination attempt");
        }
    }

    pub fn try_terminate(&mut self) -> io::Result<()> {
        if self.process_group_id.is_none() {
            return Ok(());
        }
        let (_targets, error) = terminate_unix_process_tree(
            self.process_group_id,
            &self.containment_id,
            Some(&self.marker.path),
            None,
        );
        if let Some(error) = error {
            // Keep the identity and marker so Drop or the next explicit call
            // can retry discovery of descendants outside the original group.
            return Err(error);
        }
        #[cfg(target_os = "linux")]
        wait_for_terminal_targets(&_targets)?;
        self.process_group_id = None;
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for TerminalProcessTree {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
type UnixProcessTarget = u32;

#[cfg(target_os = "linux")]
type UnixProcessTarget = LinuxPidFd;

#[cfg(unix)]
fn terminate_unix_process_tree(
    root_id: Option<u32>,
    containment_id: &str,
    marker_path: Option<&Path>,
    supervisor: Option<u32>,
) -> (Vec<UnixProcessTarget>, Option<io::Error>) {
    let (targets, error) =
        suspend_unix_process_tree(root_id, containment_id, marker_path, supervisor);
    #[cfg(target_os = "linux")]
    let mut error = error;
    for target in &targets {
        #[cfg(target_os = "linux")]
        if let Err(cause) = target.signal(libc::SIGKILL) {
            error.get_or_insert(cause);
        }
        #[cfg(not(target_os = "linux"))]
        signal_process(*target, libc::SIGKILL);
    }
    if let Some(group) = signalable_unix_group(root_id, supervisor) {
        signal_process_group(group, libc::SIGKILL);
    }
    (targets, error)
}

#[cfg(all(test, target_os = "linux"))]
thread_local! {
    static FAIL_PIDFD_OPEN: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct LinuxPidFdRequired(io::Error);

#[cfg(target_os = "linux")]
impl std::fmt::Display for LinuxPidFdRequired {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Linux pidfd support is required for process containment ({})",
            process_error_diagnostic(&self.0)
        )
    }
}

#[cfg(target_os = "linux")]
impl std::error::Error for LinuxPidFdRequired {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

#[cfg(target_os = "linux")]
struct LinuxPidFd(std::os::fd::OwnedFd);

#[cfg(target_os = "linux")]
impl LinuxPidFd {
    fn require_support() -> io::Result<()> {
        Self::open(std::process::id())
            .and_then(|target| target.signal(0))
            .map_err(|error| io::Error::new(error.kind(), LinuxPidFdRequired(error)))
    }

    fn open(pid: u32) -> io::Result<Self> {
        use std::os::fd::FromRawFd;
        #[cfg(test)]
        if let Some(errno) = FAIL_PIDFD_OPEN.get() {
            return Err(io::Error::from_raw_os_error(errno));
        }
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0_u32) };
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }
        // pidfd_open returns a new close-on-exec descriptor.
        Ok(Self(unsafe {
            std::os::fd::OwnedFd::from_raw_fd(fd as i32)
        }))
    }

    fn signal(&self, signal: i32) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        let result = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.0.as_raw_fd(),
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                0_u32,
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

    fn exited(&self) -> io::Result<bool> {
        use std::os::fd::AsRawFd;
        let mut poll = libc::pollfd {
            fd: self.0.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            if unsafe { libc::poll(&mut poll, 1, 0) } == -1 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            return Ok(poll.revents & (libc::POLLIN | libc::POLLHUP) != 0);
        }
    }
}

#[cfg(target_os = "linux")]
fn linux_process_record(pid: u32, entry: &Path) -> io::Result<UnixProcessRecord> {
    let stat = std::fs::read(entry.join("stat"))?;
    // The command name can contain whitespace and closing parentheses.
    let fields = stat
        .rsplitn(2, |&byte| byte == b')')
        .next()
        .unwrap_or_default();
    let mut fields = std::str::from_utf8(fields)
        .unwrap_or_default()
        .split_whitespace()
        .skip(1)
        .map(str::parse);
    let mut next_id = || {
        fields.next().and_then(Result::ok).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid Linux process identity")
        })
    };
    Ok(UnixProcessRecord {
        process_id: pid,
        parent_id: next_id()?,
        process_group_id: next_id()?,
    })
}

#[cfg(target_os = "linux")]
fn has_linux_containment_env(entry: &Path, expected: &[u8]) -> bool {
    std::fs::read(entry.join("environ"))
        .is_ok_and(|env| env.split(|byte| *byte == 0).any(|value| value == expected))
}

#[cfg(target_os = "linux")]
fn has_linux_containment_marker(entry: &Path, marker: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    std::fs::read_dir(entry.join("fd"))
        .into_iter()
        .flatten()
        .flatten()
        .any(|descriptor| {
            std::fs::metadata(descriptor.path()).is_ok_and(|metadata| {
                metadata.dev() == marker.dev() && metadata.ino() == marker.ino()
            })
        })
}

#[cfg(target_os = "linux")]
fn suspend_linux_candidates(
    candidates: HashSet<u32>,
    targets: &mut HashMap<u32, LinuxPidFd>,
    root_id: Option<u32>,
    supervisor: Option<u32>,
    containment_id: &str,
    marker_path: Option<&Path>,
    deadline: std::time::Instant,
) -> io::Result<bool> {
    let own_pid = std::process::id();
    let mut opened = Vec::new();
    let mut incomplete = None;
    for pid in candidates {
        check_process_inspection_deadline(deadline)?;
        if pid == own_pid {
            continue;
        }
        if let Some(target) = targets.get(&pid) {
            if !target.exited()? {
                target.signal(libc::SIGSTOP)?;
                continue;
            }
            targets.remove(&pid);
        }
        match LinuxPidFd::open(pid) {
            Ok(target) => opened.push((pid, target)),
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {}
            Err(error) => {
                incomplete.get_or_insert(error);
            }
        }
    }

    // The inventory only suggests candidates. Read the containment evidence
    // after opening each pidfd, then check that its process survived the read.
    let expected = format!("{CONTAINMENT_ID_ENV}={containment_id}");
    let marker = marker_path.and_then(|path| std::fs::metadata(path).ok());
    let mut verified = Vec::new();
    for (pid, target) in opened {
        check_process_inspection_deadline(deadline)?;
        let entry = PathBuf::from("/proc").join(pid.to_string());
        let record = match linux_process_record(pid, &entry) {
            Ok(record) => record,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let tagged = has_linux_containment_env(&entry, expected.as_bytes())
            || marker
                .as_ref()
                .is_some_and(|marker| has_linux_containment_marker(&entry, marker));
        verified.push((record, target, tagged));
    }

    // Check each pidfd after every ancestry read. A recycled parent PID
    // cannot admit children through a relationship observed after it died.
    let mut live = Vec::new();
    for (record, target, tagged) in verified {
        if !target.exited()? {
            live.push((record, target, tagged));
        }
    }
    let verified = live;

    // For PTYs, a live, attested group leader anchors its numerical PGID.
    // For background commands that PGID is reserved by the supervisor.
    let mut owned_group = root_id.filter(|_| supervisor.is_some());
    if let Some(group) = root_id.filter(|_| supervisor.is_none()) {
        let leader = targets.get(&group).or_else(|| {
            verified.iter().find_map(|(record, target, tagged)| {
                (*tagged && record.process_id == group && record.process_group_id == group)
                    .then_some(target)
            })
        });
        if let Some(leader) = leader {
            if !leader.exited()? {
                owned_group = Some(group);
            }
        }
    }

    let roots = verified.iter().filter_map(|(record, _, tagged)| {
        (*tagged || owned_group == Some(record.process_group_id)).then_some(record.process_id)
    });
    let mut live_parents = Vec::new();
    for (&pid, target) in targets.iter() {
        if !target.exited()? {
            live_parents.push(pid);
        }
    }
    let records: Vec<_> = verified.iter().map(|(record, _, _)| *record).collect();
    let roots: HashSet<_> = roots.chain(live_parents).chain(supervisor).collect();
    let descendants = descendant_process_ids(roots.iter().copied(), &records, deadline)?;
    let previous_len = targets.len();
    for (record, target, _) in verified {
        check_process_inspection_deadline(deadline)?;
        if roots.contains(&record.process_id) || descendants.contains(&record.process_id) {
            target.signal(libc::SIGSTOP)?;
            targets.insert(record.process_id, target);
        } else if root_id == Some(record.process_group_id) {
            // On Linux a numeric PTY group ID can outlive the leader or be
            // recycled. A live member without ownership evidence leaves Stop
            // incomplete even after all the attested targets are killed.
            incomplete.get_or_insert_with(|| {
                io::Error::other("terminal process group ownership could not be verified")
            });
        }
    }

    incomplete.map_or(Ok(targets.len() != previous_len), Err)
}

#[cfg(unix)]
fn signalable_unix_group(root_id: Option<u32>, _supervisor: Option<u32>) -> Option<u32> {
    // On Linux the unreaped background supervisor pins the workload PGID.
    // A PTY stores only a numeric group ID, which may have been recycled.
    #[cfg(target_os = "linux")]
    _supervisor?;
    root_id
}

#[cfg(unix)]
fn suspend_unix_process_tree(
    root_id: Option<u32>,
    containment_id: &str,
    containment_marker_path: Option<&Path>,
    supervisor: Option<u32>,
) -> (Vec<UnixProcessTarget>, Option<io::Error>) {
    if let Some(group) = signalable_unix_group(root_id, supervisor) {
        signal_process_group(group, libc::SIGSTOP);
    }
    #[cfg(target_os = "linux")]
    let mut targets = HashMap::new();
    #[cfg(not(target_os = "linux"))]
    let mut process_ids = root_id
        .filter(|id| Some(*id) != supervisor)
        .into_iter()
        .collect::<HashSet<_>>();
    let error = suspend_unix_process_tree_using(|deadline| {
        #[cfg(target_os = "linux")]
        let mut process_ids = targets.keys().copied().collect::<HashSet<_>>();
        #[cfg(not(target_os = "linux"))]
        let previous_len = process_ids.len();
        let failures = discover_unix_process_tree(
            &mut process_ids,
            root_id,
            containment_id,
            containment_marker_path,
            supervisor,
            deadline,
        );
        #[cfg(target_os = "linux")]
        {
            let mut failures = failures;
            let changed = match suspend_linux_candidates(
                process_ids,
                &mut targets,
                root_id,
                supervisor,
                containment_id,
                containment_marker_path,
                deadline,
            ) {
                Ok(changed) => changed,
                Err(error) => {
                    failures.record(ProcessInspector::ProcessHandle, &error);
                    false
                }
            };
            (changed, (!failures.0.is_empty()).then_some(failures))
        }
        #[cfg(not(target_os = "linux"))]
        {
            for &process_id in &process_ids {
                if check_process_inspection_deadline(deadline).is_err() {
                    break;
                }
                signal_process(process_id, libc::SIGSTOP);
            }
            (
                process_ids.len() != previous_len,
                (!failures.0.is_empty()).then_some(failures),
            )
        }
    });
    #[cfg(target_os = "linux")]
    let targets = targets.into_values().collect();
    #[cfg(not(target_os = "linux"))]
    let targets = process_ids.into_iter().collect();
    (targets, error)
}

#[cfg(unix)]
fn discover_unix_process_tree(
    process_ids: &mut HashSet<u32>,
    root_id: Option<u32>,
    containment_id: &str,
    containment_marker_path: Option<&Path>,
    supervisor: Option<u32>,
    deadline: std::time::Instant,
) -> ProcessInspectionFailures {
    let mut failures = ProcessInspectionFailures::default();
    // On macOS both searches inventory the machine. They are independent,
    // so share the same budget instead of spending it serially.
    #[cfg(target_os = "macos")]
    let discoveries = std::thread::scope(|scope| {
        let marker = std::thread::Builder::new()
            .name("containment-marker".into())
            .spawn_scoped(scope, || {
                processes_with_containment_marker(containment_marker_path, deadline)
            });
        let environment = processes_with_containment_id(containment_id, deadline);
        let marker = match marker {
            Ok(handle) => handle
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("process marker inspection panicked"))),
            Err(error) => Err(error),
        };
        [environment, marker]
    });
    #[cfg(not(target_os = "macos"))]
    let discoveries = [
        processes_with_containment_id(containment_id, deadline),
        processes_with_containment_marker(containment_marker_path, deadline),
    ];
    for (inspector, discovered) in [ProcessInspector::Environment, ProcessInspector::Marker]
        .into_iter()
        .zip(discoveries)
    {
        match discovered {
            Ok(ids) => process_ids.extend(ids),
            Err(error) => failures.record(inspector, &error),
        }
    }
    match read_unix_process_table(deadline) {
        Ok(table) => {
            if let Some(process_group_id) = root_id {
                process_ids.extend(
                    table
                        .iter()
                        .filter(|record| record.process_group_id == process_group_id)
                        .map(|record| record.process_id),
                );
            }
            let roots = process_ids.iter().copied().chain(supervisor);
            match descendant_process_ids(roots, &table, deadline) {
                Ok(descendants) => process_ids.extend(descendants),
                Err(error) => failures.record(ProcessInspector::Descendants, &error),
            }
        }
        Err(error) => failures.record(ProcessInspector::ProcessTable, &error),
    }
    if let Some(supervisor) = supervisor {
        process_ids.remove(&supervisor);
    }
    failures
}

#[cfg(unix)]
fn suspend_unix_process_tree_using(
    mut inspect: impl FnMut(std::time::Instant) -> (bool, Option<ProcessInspectionFailures>),
) -> Option<io::Error> {
    let deadline = std::time::Instant::now() + PROCESS_TREE_INSPECTION_TIMEOUT;
    let mut failures = ProcessInspectionFailures::default();
    let mut deadline_expired = false;
    for _ in 0..8 {
        if check_process_inspection_deadline(deadline).is_err() {
            deadline_expired = true;
            break;
        }
        // Only a fully successful round can establish convergence. A partial
        // inventory may look unchanged; retry it within the original deadline.
        let (changed, inspection_error) = inspect(deadline);
        let round_complete = inspection_error.is_none();
        if let Some(inspection_error) = inspection_error {
            failures.extend(inspection_error);
        }
        if check_process_inspection_deadline(deadline).is_err() {
            deadline_expired = true;
            break;
        }
        if !changed && round_complete {
            return None;
        }
        std::thread::yield_now();
    }
    let (kind, end_reason) = if deadline_expired {
        (
            io::ErrorKind::TimedOut,
            "process tree inspection deadline expired",
        )
    } else {
        (
            io::ErrorKind::Other,
            "process tree inspection did not converge (pass limit reached)",
        )
    };
    Some(io::Error::new(
        kind,
        ProcessTreeInspectionDiagnostic {
            failures,
            end_reason,
        },
    ))
}

#[cfg(windows)]
fn attach_child_to_job_object(child: &tokio::process::Child) -> io::Result<JobObjectHandle> {
    use std::mem::size_of_val;
    use windows_sys::Win32::System::JobObjects::{
        JobObjectExtendedLimitInformation, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };

    unsafe {
        let raw_job_object = CreateJobObjectW(std::ptr::null(), std::ptr::null());
        if raw_job_object.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job_object = JobObjectHandle::from_raw_handle(raw_job_object as _);
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let configured = SetInformationJobObject(
            job_object.as_raw_handle() as _,
            JobObjectExtendedLimitInformation,
            &limits as *const _ as *const core::ffi::c_void,
            size_of_val(&limits) as u32,
        );
        if configured == 0 {
            return Err(io::Error::last_os_error());
        }
        let Some(process_handle) = child.raw_handle() else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "tokio child did not expose a raw process handle",
            ));
        };
        if AssignProcessToJobObject(job_object.as_raw_handle() as _, process_handle) == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(job_object)
    }
}

impl Drop for ContainedBackgroundProcess {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            if self.process_group_id.is_none() {
                return;
            }
            self.stop_unix_workload();
        }
        #[cfg(windows)]
        {
            unsafe {
                TerminateJobObject(self.job_object.as_raw_handle() as _, ERROR_PROCESS_ABORTED)
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        is_known_visible_terminal_app_id, windows_creation_flags_for_visibility,
        ProcessLaunchVisibility, CREATE_NO_WINDOW,
    };
    #[cfg(unix)]
    use std::fs;

    #[cfg(target_os = "linux")]
    #[test]
    fn terminal_termination_ignores_a_recycled_group_id() {
        use std::os::unix::process::CommandExt;

        // A new child stands in for an unrelated process that acquired the
        // retired PTY leader's numeric PID and process group.
        let mut replacement = std::process::Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let mut tree = super::TerminalProcessTree::new().unwrap();
        tree.process_group_id = Some(replacement.id());
        let (targets, error) = super::suspend_unix_process_tree(
            tree.process_group_id,
            &tree.containment_id,
            Some(&tree.marker.path),
            None,
        );
        replacement.kill().unwrap();
        replacement.wait().unwrap();
        tree.process_group_id = None;
        assert!(error.unwrap().to_string().contains("group ownership"));
        assert!(
            targets.is_empty(),
            "stale group ID selected an unrelated child"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn terminal_stop_reports_incomplete_ownership_after_leader_reap() {
        use std::os::unix::process::CommandExt;
        use std::process::Command;

        let mut tree = super::TerminalProcessTree::new().unwrap();
        let mut leader = Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let group = leader.id();
        tree.process_group_id = Some(group);
        let mut member = Command::new("/bin/sleep")
            .arg("30")
            .process_group(group as i32)
            .spawn()
            .unwrap();
        leader.kill().unwrap();
        leader.wait().unwrap();

        let result = tree.try_terminate();
        let remained = tree.process_group_id;
        let survived = member.try_wait().unwrap().is_none();
        if survived {
            member.kill().unwrap();
            member.wait().unwrap();
        }
        assert!(survived, "signaled a group without proof of ownership");
        assert!(result.unwrap_err().to_string().contains("group ownership"));
        assert_eq!(remained, Some(group));
        tree.try_terminate().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_containment_requires_pidfd_support_before_launch() {
        use super::{background_contained_tokio_command, ContainedBackgroundProcess};

        for errno in [libc::ENOSYS, libc::EPERM] {
            super::FAIL_PIDFD_OPEN.set(Some(errno));
            let terminal = super::TerminalProcessTree::new();
            let background =
                ContainedBackgroundProcess::spawn(background_contained_tokio_command("/bin/true"));
            super::FAIL_PIDFD_OPEN.set(None);
            let background_error = match background {
                Ok(mut process) => {
                    process.terminate_bounded().await.unwrap();
                    panic!("background command launched without pidfd support ({errno})");
                }
                Err(error) => error,
            };
            for error in [terminal.err().unwrap(), background_error] {
                assert!(error
                    .to_string()
                    .contains("Linux pidfd support is required"));
                assert!(super::process_error_diagnostic(&error)
                    .contains(&format!("errno=Some({errno})")));
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn contained_stop_preserves_pidfd_open_failure_with_or_without_reap() {
        use super::{background_contained_tokio_command, ContainedBackgroundProcess};
        use std::time::Duration;

        for errno in [libc::ENOSYS, libc::EPERM] {
            for reaper_suspended in [false, true] {
                let mut command = background_contained_tokio_command("/bin/sleep");
                command.arg("30");
                let mut process = ContainedBackgroundProcess::spawn(command).unwrap();
                let supervisor = super::LinuxPidFd::open(process.id().unwrap()).unwrap();
                if reaper_suspended {
                    supervisor.signal(libc::SIGSTOP).unwrap();
                }
                let grace = if reaper_suspended {
                    Duration::from_millis(50)
                } else {
                    Duration::from_secs(2)
                };
                super::FAIL_PIDFD_OPEN.set(Some(errno));
                let result = process.terminate_with_grace(grace).await;
                super::FAIL_PIDFD_OPEN.set(None);
                if reaper_suspended {
                    supervisor.signal(libc::SIGCONT).unwrap();
                    process.terminate_bounded().await.unwrap();
                }

                let error = result.unwrap_err();
                if reaper_suspended {
                    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
                }
                let diagnostic = super::process_error_diagnostic(&error);
                assert!(diagnostic.contains("ProcessHandle"), "{diagnostic}");
                assert!(
                    diagnostic.contains(&format!("errno=Some({errno})")),
                    "{diagnostic}"
                );
                assert!(process.unix_process_group_id().is_none());
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn tree_inspection_retries_a_partial_round_with_the_original_deadline() {
        let mut calls = 0;
        let mut first_deadline = None;
        let error = super::suspend_unix_process_tree_using(|deadline| {
            calls += 1;
            assert_eq!(*first_deadline.get_or_insert(deadline), deadline);
            (
                false,
                (calls == 1).then(|| {
                    let mut failures = super::ProcessInspectionFailures::default();
                    failures.record(
                        super::ProcessInspector::Environment,
                        &std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "process inspection timed out",
                        ),
                    );
                    failures
                }),
            )
        });
        assert!(error.is_none(), "{error:?}");
        assert_eq!(
            calls, 2,
            "an unchanged partial inventory is not convergence"
        );
    }

    #[cfg(unix)]
    #[test]
    fn tree_inspection_rejects_rounds_that_never_complete() {
        let mut calls = 0;
        let mut first_deadline = None;
        let error = super::suspend_unix_process_tree_using(|deadline| {
            calls += 1;
            assert_eq!(*first_deadline.get_or_insert(deadline), deadline);
            // Success from different sources on different rounds must not be
            // combined into a supposedly complete inventory.
            let mut failures = super::ProcessInspectionFailures::default();
            let source = if calls % 2 == 0 {
                super::ProcessInspector::Environment
            } else {
                super::ProcessInspector::Marker
            };
            failures.record(
                source,
                &std::io::Error::other("process containment inspection failed"),
            );
            (false, Some(failures))
        });
        assert!(error.is_some());
        assert!(calls > 1 && calls <= 8, "retries must be bounded: {calls}");
    }

    #[cfg(unix)]
    #[test]
    fn tree_inspection_preserves_first_cause_and_all_failed_inspectors_across_rounds() {
        let mut calls = 0;
        let error = super::suspend_unix_process_tree_using(|_| {
            calls += 1;
            let mut failures = super::ProcessInspectionFailures::default();
            if calls == 1 {
                failures.record(
                    super::ProcessInspector::Environment,
                    &std::io::Error::from_raw_os_error(libc::EACCES),
                );
                failures.record(
                    super::ProcessInspector::Marker,
                    &std::io::Error::other("process containment inspection failed"),
                );
            } else {
                failures.record(
                    super::ProcessInspector::Environment,
                    &std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "process inspection timed out",
                    ),
                );
                failures.record(
                    super::ProcessInspector::ProcessTable,
                    &std::io::Error::other("ps failed while reading process descendants"),
                );
            }
            (false, Some(failures))
        });
        let message = error.expect("partial rounds must stay failed").to_string();
        assert_eq!(calls, 8);
        assert!(
            message.contains(
                "first failure: Environment: process operation failed (kind=PermissionDenied"
            ),
            "{message}"
        );
        assert!(message.contains(&format!("errno=Some({})", libc::EACCES)));
        assert!(message.contains("Marker: containment command failed"));
        assert!(message.contains("ProcessTable: process table command failed"));
        assert!(message.contains("pass limit reached"));
    }

    #[cfg(unix)]
    #[test]
    fn tree_inspection_deadline_diagnostic_is_bounded_and_excludes_private_text() {
        let mut failures = super::ProcessInspectionFailures::default();
        for _ in 0..8 {
            failures.record(
                super::ProcessInspector::Environment,
                &std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "--token=fixture-secret PRIVATE_ENV=fixture-secret /private/fixture-helper"
                        .repeat(1000),
                ),
            );
            failures.record(
                super::ProcessInspector::Marker,
                &std::io::Error::other("process containment inspection failed"),
            );
            failures.record(
                super::ProcessInspector::ProcessTable,
                &std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    super::InspectionTimeout {
                        elapsed_ms: 1000,
                        bytes: 8192,
                        eof: false,
                    },
                ),
            );
            failures.record(
                super::ProcessInspector::Descendants,
                &std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "process tree inspection deadline expired",
                ),
            );
        }
        assert_eq!(failures.0.len(), 4);
        let error = std::io::Error::other(super::IncompleteProcessTreeInspection(
            std::io::Error::other(super::ProcessTreeInspectionDiagnostic {
                failures,
                end_reason: "process tree inspection deadline expired",
            }),
        ));
        let message = super::process_error_diagnostic(&error);
        assert!(message.contains("first failure: Environment"));
        assert!(message.contains("PermissionDenied"));
        assert!(message.contains("inspection deadline expired"));
        assert!(message.contains("1000ms (8192 bytes, EOF=false)"));
        assert!(!message.contains("fixture-secret"));
        assert!(!message.contains("PRIVATE_ENV"));
        assert!(!message.contains("fixture-helper"));
        assert!(message.len() < 1536, "diagnostic size: {}", message.len());
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn contained_process_retires_identity_only_after_complete_termination() {
        let mut command = super::background_tokio_command("/bin/sleep");
        command.arg("30");
        let mut child = super::ContainedBackgroundProcess::spawn(command).unwrap();
        let group = child.process_group_id;
        fs::remove_file(&child.containment_marker.path).unwrap();
        assert!(child
            .terminate_with_grace(std::time::Duration::ZERO)
            .await
            .is_err());
        assert_eq!(
            child.process_group_id, group,
            "failed inspection must remain retryable"
        );

        child.containment_marker =
            super::UnixContainmentMarker::create(&child.containment_id).unwrap();
        let status = child
            .terminate_with_grace(std::time::Duration::ZERO)
            .await
            .unwrap();
        assert!(child.process_group_id.is_none());
        // A second call and Drop must not re-inventory a verified, reaped tree.
        fs::remove_file(&child.containment_marker.path).unwrap();
        assert_eq!(
            child
                .terminate_with_grace(std::time::Duration::ZERO)
                .await
                .unwrap(),
            status
        );
        drop(child);
    }

    #[cfg(unix)]
    #[test]
    fn bounded_inspection_handles_success_failure_and_output_limit() {
        use std::time::{Duration, Instant};
        let run = |script: &str| {
            super::bounded_process_inspection(
                std::process::Command::new("/bin/sh").args(["-c", script]),
                Instant::now() + Duration::from_secs(2),
            )
        };
        let output = run("printf 'fixture'; exit 0").unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"fixture");
        assert_eq!(run("exit 7").unwrap().status.code(), Some(7));
        let error = run("printf '%017000000d' 0").unwrap_err();
        assert!(error.to_string().contains("output exceeded"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn bounded_inspection_kills_blocked_helper_and_open_pipe_descendant() {
        use std::time::{Duration, Instant};
        for script in [
            "sleep 30 & echo $! > child.pid; wait",
            "sleep 30 & echo $! > child.pid; exit 0",
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            let started = Instant::now();
            let error = super::bounded_process_inspection(
                std::process::Command::new("/bin/sh")
                    .args(["-c", script])
                    .current_dir(temp.path()),
                started + Duration::from_millis(150),
            )
            .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
            assert!(started.elapsed() < Duration::from_secs(2));
            let pid: u32 = fs::read_to_string(temp.path().join("child.pid"))
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            while unsafe { libc::kill(pid as i32, 0) } == 0 && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            assert_eq!(
                unsafe { libc::kill(pid as i32, 0) },
                -1,
                "inspection descendant survived"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn descendant_walk_rejects_an_expired_budget() {
        let table = super::parse_unix_process_table("10 1 10\n11 10 10\n12 11 12\n");
        assert!(super::descendant_process_ids([10], &table, std::time::Instant::now()).is_err());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn terminal_tree_retains_identity_after_marker_inspection_failure() {
        use std::os::unix::process::CommandExt;
        let mut tree = super::TerminalProcessTree::new().unwrap();
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        tree.process_group_id = Some(child.id());
        fs::remove_file(&tree.marker.path).unwrap();
        let error = tree.try_terminate().unwrap_err();
        assert!(error.to_string().contains("inspection"), "{error}");
        assert_eq!(tree.process_group_id, Some(child.id()));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while child.try_wait().unwrap().is_none() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        // Restore the marker that this fixture removed, then verify retry.
        tree.marker = super::UnixContainmentMarker::create(&tree.containment_id).unwrap();
        tree.try_terminate().unwrap();
        assert!(tree.process_group_id.is_none());
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    #[test]
    fn containment_environment_matches_complete_fields_only() {
        let expected = "MACRO_PROCESS_CONTAINMENT_ID=123-1";
        assert!(super::has_containment_environment_field(
            "shell MACRO_PROCESS_CONTAINMENT_ID=123-1",
            expected
        ));
        assert!(super::has_containment_environment_field(
            "shell MACRO_PROCESS_CONTAINMENT_ID=123-1 PATH=/fixture",
            expected
        ));
        assert!(!super::has_containment_environment_field(
            "shell MACRO_PROCESS_CONTAINMENT_ID=123-10",
            expected
        ));
        assert!(!super::has_containment_environment_field(
            "shell OTHER_MACRO_PROCESS_CONTAINMENT_ID=123-1",
            expected
        ));
    }

    #[cfg(target_os = "macos")]
    const MACOS_DOUBLE_FORK_HELPER_ENV: &str = "MACRO_TEST_DOUBLE_FORK_MARKER";
    #[cfg(target_os = "macos")]
    const MACOS_DOUBLE_FORK_ARGV0_PREFIX: &str = "macro-double-fork-marker=";
    #[cfg(target_os = "macos")]
    const MACOS_DOUBLE_FORK_TEST_NAME: &str =
        "process::tests::contained_process_terminates_a_macos_double_fork_with_cleared_environment";

    #[cfg(target_os = "macos")]
    fn macos_double_fork_reexec_marker() -> Option<std::path::PathBuf> {
        std::env::args()
            .next()?
            .strip_prefix(MACOS_DOUBLE_FORK_ARGV0_PREFIX)
            .map(std::path::PathBuf::from)
    }

    #[cfg(target_os = "macos")]
    fn spawn_macos_double_fork_helper_if_requested() -> bool {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let Ok(marker) = std::env::var(MACOS_DOUBLE_FORK_HELPER_ENV) else {
            return false;
        };
        let executable = std::env::current_exe().expect("current test executable");
        let executable = CString::new(executable.as_os_str().as_bytes()).expect("executable path");
        let helper_argv0 = CString::new(format!("{MACOS_DOUBLE_FORK_ARGV0_PREFIX}{marker}"))
            .expect("helper marker argument");
        let test_name = CString::new(MACOS_DOUBLE_FORK_TEST_NAME).expect("test name");
        let exact = CString::new("--exact").expect("exact argument");
        let nocapture = CString::new("--nocapture").expect("nocapture argument");
        let arguments = [
            helper_argv0.as_ptr(),
            test_name.as_ptr(),
            exact.as_ptr(),
            nocapture.as_ptr(),
            std::ptr::null(),
        ];
        let empty_environment = [std::ptr::null()];

        unsafe {
            let session_child = libc::fork();
            if session_child == -1 {
                libc::_exit(111);
            }
            if session_child != 0 {
                return true;
            }
            if libc::setsid() == -1 {
                libc::_exit(112);
            }
            let daemon = libc::fork();
            if daemon == -1 {
                libc::_exit(113);
            }
            if daemon != 0 {
                libc::_exit(0);
            }
            libc::execve(
                executable.as_ptr(),
                arguments.as_ptr(),
                empty_environment.as_ptr(),
            );
            libc::_exit(114);
        }
    }

    #[test]
    fn background_visibility_maps_to_hidden_windows_flag() {
        assert_eq!(
            windows_creation_flags_for_visibility(
                ProcessLaunchVisibility::HiddenBackgroundLauncher
            ),
            CREATE_NO_WINDOW
        );
    }

    #[test]
    fn visible_terminal_visibility_has_no_hidden_windows_flag() {
        assert_eq!(
            windows_creation_flags_for_visibility(ProcessLaunchVisibility::VisibleTerminal),
            0
        );
    }

    #[test]
    fn visible_terminal_app_ids_are_case_insensitive() {
        assert!(is_known_visible_terminal_app_id("PowerShell"));
        assert!(is_known_visible_terminal_app_id("windows-terminal"));
        assert!(!is_known_visible_terminal_app_id("code"));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn contained_process_captures_an_immediate_windows_descendant() {
        use super::{
            background_contained_tokio_command, windows_job_active_processes,
            ContainedBackgroundProcess,
        };
        use std::time::Duration;
        let parent_script = "Start-Process -WindowStyle Hidden -FilePath powershell.exe -ArgumentList '-NoProfile -Command Start-Sleep -Seconds 60'";
        let mut command = background_contained_tokio_command("powershell.exe");
        command.args(["-NoProfile", "-Command", parent_script]);
        let mut process = ContainedBackgroundProcess::spawn(command).expect("spawn parent");
        assert!(process.wait().await.expect("wait parent").success());
        assert!(
            windows_job_active_processes(&process.job_object).unwrap() > 0,
            "descendant must outlive the leader"
        );
        process
            .terminate_with_grace(Duration::ZERO)
            .await
            .expect("terminate job");
        assert_eq!(
            windows_job_active_processes(&process.job_object).unwrap(),
            0,
            "cleanup returned with a live job member"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_cleanup_rejects_a_failed_job_termination() {
        use super::{background_contained_tokio_command, ContainedBackgroundProcess};
        use std::{
            os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
            time::Duration,
        };
        use windows_sys::Win32::{
            Foundation::{DuplicateHandle, ERROR_ACCESS_DENIED},
            System::Threading::GetCurrentProcess,
        };
        let mut command = background_contained_tokio_command("cmd.exe");
        command.args(["/C", "exit", "0"]);
        let mut process = ContainedBackgroundProcess::spawn(command).expect("spawn command");
        assert!(process.wait().await.expect("wait command").success());
        let mut reduced = std::ptr::null_mut();
        let current = unsafe { GetCurrentProcess() };
        // Duplicate with zero access rights to exercise a real Win32 failure
        // after the leader's exit has already been observed.
        assert_ne!(
            unsafe {
                DuplicateHandle(
                    current,
                    process.job_object.as_raw_handle() as _,
                    current,
                    &mut reduced,
                    0,
                    0,
                    0,
                )
            },
            0
        );
        process.job_object = unsafe { OwnedHandle::from_raw_handle(reduced as _) };
        let error = process
            .terminate_with_grace(Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(ERROR_ACCESS_DENIED as i32));
    }

    #[cfg(unix)]
    #[test]
    fn unix_process_table_finds_nested_descendants() {
        let table = super::parse_unix_process_table("10 1 10\n11 10 10\n12 11 12\n20 1 20\n");
        let descendants = super::descendant_process_ids(
            [10],
            &table,
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();

        assert_eq!(descendants, [11, 12].into_iter().collect());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn contained_process_terminates_a_setsid_descendant() {
        use super::{background_contained_tokio_command, ContainedBackgroundProcess};
        use std::process::Stdio;
        use std::time::Duration;

        let setsid_available = std::process::Command::new("sh")
            .args(["-c", "command -v setsid >/dev/null 2>&1"])
            .status()
            .expect("probe setsid")
            .success();
        if !setsid_available {
            return;
        }

        let temp = tempfile::TempDir::new().expect("temp dir");
        let marker = temp.path().join("escaped-descendant.txt");
        let marker_arg = marker.to_string_lossy().replace('\'', "'\\''");
        let script = format!(
            "setsid sh -c 'sleep 1; printf survived > \"$1\"' sh '{marker_arg}' & sleep 30"
        );
        let mut command = background_contained_tokio_command("sh");
        command
            .args(["-c", &script])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut process = ContainedBackgroundProcess::spawn(command).expect("spawn parent");
        tokio::time::sleep(Duration::from_millis(150)).await;
        process
            .terminate_with_grace(Duration::ZERO)
            .await
            .expect("terminate contained process tree");
        tokio::time::sleep(Duration::from_millis(1_200)).await;

        assert!(!marker.exists(), "setsid descendant escaped containment");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn contained_stop_collects_the_linux_reaper_acknowledgement() {
        use super::{background_contained_tokio_command, ContainedBackgroundProcess};
        use std::{process::Stdio, time::Duration};
        use tokio::io::{AsyncBufReadExt, BufReader};

        let mut command = background_contained_tokio_command("/bin/sh");
        command
            .args(["-c", "echo ready; read value"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut process = ContainedBackgroundProcess::spawn(command).unwrap();
        let _stdin = process.take_stdin().unwrap();
        let mut stdout = BufReader::new(process.take_stdout().unwrap());
        let mut ready = String::new();
        tokio::time::timeout(Duration::from_secs(2), stdout.read_line(&mut ready))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ready.trim_end(), "ready");

        let status = process.terminate_with_grace(Duration::ZERO).await.unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        assert!(process.child.wait().await.unwrap().success());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn contained_stop_kills_the_linux_workload_group_when_process_table_fails() {
        use super::{background_contained_tokio_command, ContainedBackgroundProcess};
        use std::{process::Stdio, time::Duration};
        use tokio::io::{AsyncBufReadExt, BufReader};

        let mut command = background_contained_tokio_command("/bin/sh");
        command
            .args([
                "-c",
                "env -i /bin/sh -c 'echo $$; exec /bin/sleep 30' & exit 23",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut process = ContainedBackgroundProcess::spawn(command).unwrap();
        let mut stdout = BufReader::new(process.take_stdout().unwrap());
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(2), stdout.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let descendant: u32 = line.trim().parse().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        assert!(
            !super::processes_with_containment_id(&process.containment_id, deadline)
                .unwrap()
                .contains(&descendant)
        );
        assert_eq!(
            unsafe { libc::getpgid(descendant as libc::pid_t) },
            process.unix_process_group_id().unwrap() as libc::pid_t
        );
        let supervisor = process.child.id().unwrap() as libc::pid_t;
        assert_ne!(unsafe { libc::getpgid(supervisor) }, supervisor);
        assert_eq!(process.wait().await.unwrap().code(), Some(23));

        super::FAIL_PROCESS_TABLE_INSPECTION.set(true);
        let result = process.terminate_with_grace(Duration::from_secs(1)).await;
        super::FAIL_PROCESS_TABLE_INSPECTION.set(false);
        let acknowledgement = process.child.try_wait().unwrap();
        if acknowledgement.is_none() {
            super::signal_process(descendant, libc::SIGKILL);
            process.child.wait().await.unwrap();
        }
        assert_eq!(result.unwrap().code(), Some(23));
        assert!(acknowledgement.unwrap().success());
        assert!(process.unix_process_group_id().is_none());
        assert_eq!(unsafe { libc::kill(descendant as libc::pid_t, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn contained_process_wait_preserves_the_command_exit_status() {
        use super::{background_contained_tokio_command, ContainedBackgroundProcess};
        use std::process::Stdio;
        use std::time::Duration;

        let mut command = background_contained_tokio_command("sh");
        command
            .args(["-c", "cat >/dev/null; exit 23"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut process = ContainedBackgroundProcess::spawn(command).expect("spawn command");
        let status = tokio::time::timeout(Duration::from_secs(1), process.wait())
            .await
            .expect("wait must return after the command exits")
            .expect("wait for command status");

        assert_eq!(status.code(), Some(23));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn contained_process_terminates_a_double_fork_with_cleared_environment() {
        use super::{background_contained_tokio_command, ContainedBackgroundProcess};
        use std::process::Stdio;
        use std::time::Duration;

        let setsid_available = std::process::Command::new("sh")
            .args(["-c", "command -v setsid >/dev/null 2>&1"])
            .status()
            .expect("probe setsid")
            .success();
        if !setsid_available {
            return;
        }

        let temp = tempfile::TempDir::new().expect("temp dir");
        let marker = temp.path().join("escaped-cleared-environment.txt");
        let marker_arg = marker.to_string_lossy().replace('\'', "'\\''");
        let script = format!(
            "setsid -f env -i /bin/sh -c 'sleep 1; printf survived > \"$1\"' sh '{marker_arg}'"
        );
        let mut command = background_contained_tokio_command("sh");
        command
            .args(["-c", &script])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut process = ContainedBackgroundProcess::spawn(command).expect("spawn parent");
        // The shell and setsid launcher both exit before this delay. The
        // per-command subreaper remains alive while the daemon is running.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let status = tokio::time::timeout(Duration::from_secs(1), process.wait())
            .await
            .expect("wait must return after the command exits")
            .expect("wait for command status");
        assert!(status.success(), "the command status must be preserved");
        process
            .terminate_with_grace(Duration::ZERO)
            .await
            .expect("terminate double-forked process tree");
        tokio::time::sleep(Duration::from_millis(1_200)).await;

        assert!(
            !marker.exists(),
            "double-forked descendant with a cleared environment escaped containment"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn contained_process_terminates_a_macos_double_fork_with_cleared_environment() {
        use super::{background_contained_tokio_command, ContainedBackgroundProcess};
        use std::process::Stdio;
        use std::time::Duration;

        if let Some(marker) = macos_double_fork_reexec_marker() {
            std::thread::sleep(Duration::from_secs(1));
            std::fs::write(marker, "survived").expect("write escaped daemon marker");
            return;
        }
        if spawn_macos_double_fork_helper_if_requested() {
            return;
        }

        let temp = tempfile::TempDir::new().expect("temp dir");
        let marker = temp.path().join("escaped-macos-double-fork.txt");
        let mut command = background_contained_tokio_command(
            std::env::current_exe().expect("current test executable"),
        );
        command
            .args([MACOS_DOUBLE_FORK_TEST_NAME, "--exact", "--nocapture"])
            .env(MACOS_DOUBLE_FORK_HELPER_ENV, &marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut process = ContainedBackgroundProcess::spawn(command).expect("spawn helper");
        let status = tokio::time::timeout(Duration::from_secs(1), process.wait())
            .await
            .expect("wait must return after the helper exits")
            .expect("wait for helper status");
        assert!(status.success(), "the helper status must be preserved");
        process
            .terminate_with_grace(Duration::ZERO)
            .await
            .expect("terminate macOS double-forked process tree");
        tokio::time::sleep(Duration::from_millis(1_200)).await;

        assert!(
            !marker.exists(),
            "macOS double-forked descendant with a cleared environment escaped containment"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn descendant_exit_waits_for_actual_absence_and_times_out_conservatively() {
        use std::time::Duration;
        let mut command = super::background_tokio_command("/bin/sleep");
        command
            .arg("30")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap();
        let pid = child.id().unwrap();
        #[cfg(target_os = "linux")]
        let targets = [super::LinuxPidFd::open(pid).unwrap()];
        #[cfg(not(target_os = "linux"))]
        let targets = [pid];
        let expired = super::wait_for_processes_exit(
            &targets,
            tokio::time::Instant::now() + Duration::from_millis(20),
        )
        .await;
        let waiting = super::wait_for_processes_exit(
            &targets,
            tokio::time::Instant::now() + Duration::from_secs(2),
        );
        tokio::pin!(waiting);
        let early = tokio::time::timeout(Duration::from_millis(20), &mut waiting).await;
        child.start_kill().unwrap();
        child.wait().await.unwrap();
        waiting.await.unwrap();
        assert_eq!(expired.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
        assert!(
            early.is_err(),
            "A live detached process was reported joined"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn contained_process_joins_descendant_with_detached_streams() {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut command = super::background_contained_tokio_command("/bin/sh");
        command
            .args([
                "-c",
                "sleep 120 </dev/null >/dev/null 2>&1 & printf '%s\n' \"$!\"; exit 0",
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        let mut process = super::ContainedBackgroundProcess::spawn(command).unwrap();
        let mut output = BufReader::new(process.take_stdout().unwrap()).lines();
        let pid: i32 = output.next_line().await.unwrap().unwrap().parse().unwrap();
        process.wait().await.unwrap();
        process.terminate_bounded().await.unwrap();
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "Detached descendant survived physical completion"
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
        assert!(process.unix_process_group_id().is_none());
    }
}
