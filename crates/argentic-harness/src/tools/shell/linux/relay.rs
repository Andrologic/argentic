//! Standalone Linux helper embedded by build.rs. The host mounts it read-only
//! and runs it as PID 1 so exiting also terminates outstanding proxy streams.
use std::{
    env,
    io::{self, Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    os::{
        fd::{AsRawFd, RawFd},
        unix::{net::UnixStream, process::ExitStatusExt},
    },
    process::{self, Command, ExitStatus},
    sync::Arc,
    thread,
};

const POLLIN: i16 = 0x001;
const POLLOUT: i16 = 0x004;
const POLL_ERROR: i16 = 0x008 | 0x010 | 0x020; // ERR, HUP, NVAL
const POLLRDHUP: i16 = 0x2000;

#[repr(C)]
struct PollFd {
    fd: RawFd,
    events: i16,
    ready: i16,
}

extern "C" {
    fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
    fn poll(fds: *mut PollFd, len: usize, timeout_ms: i32) -> i32;
}

struct Buffer {
    bytes: [u8; 16384],
    start: usize,
    end: usize,
}

impl Buffer {
    fn new() -> Self {
        Self {
            bytes: [0; 16384],
            start: 0,
            end: 0,
        }
    }

    fn empty(&self) -> bool {
        self.start == self.end
    }

    fn read(&mut self, source: &mut impl Read) -> io::Result<bool> {
        match source.read(&mut self.bytes[..]) {
            Ok(size) => {
                self.start = 0;
                self.end = size;
                Ok(size > 0)
            }
            Err(error) if retry(&error) => Ok(true),
            Err(error) => Err(error),
        }
    }

    fn write(&mut self, destination: &mut impl Write) -> io::Result<()> {
        match destination.write(&self.bytes[self.start..self.end]) {
            Ok(0) => Err(io::ErrorKind::WriteZero.into()),
            Ok(size) => {
                self.start += size;
                Ok(())
            }
            Err(error) if retry(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn events(&self, outgoing: &Self) -> i16 {
        (if self.empty() { POLLIN } else { 0 }) | (if outgoing.empty() { 0 } else { POLLOUT })
    }
}

fn retry(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

fn forward(mut client: TcpStream) -> io::Result<()> {
    let mut proxy = UnixStream::connect("/run/argentic-proxy.sock")?;
    client.set_nonblocking(true)?;
    proxy.set_nonblocking(true)?;
    let mut upload = Buffer::new();
    let mut download = Buffer::new();
    let mut proxy_closed = false;
    loop {
        let proxy_events = download.events(&upload);
        let mut fds = [
            PollFd {
                fd: client.as_raw_fd(),
                events: POLLRDHUP
                    | if proxy_closed {
                        upload.events(&download) & POLLOUT
                    } else {
                        upload.events(&download)
                    },
                ready: 0,
            },
            PollFd {
                fd: if proxy_events == 0 {
                    -1
                } else {
                    proxy.as_raw_fd()
                },
                events: proxy_events,
                ready: 0,
            },
        ];
        // Both descriptors stay open and the array is writable for poll.
        if unsafe { poll(fds.as_mut_ptr(), fds.len(), -1) } < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        let [client_fd, proxy_fd] = fds;
        // Disconnects cancel pending proxy authorization even with a full
        // upload buffer. Close both sides to wake the host immediately.
        if client_fd.ready & (POLLRDHUP | POLL_ERROR) != 0 {
            return Ok(());
        }
        if client_fd.ready & POLLOUT != 0 {
            download.write(&mut client)?;
        }
        if proxy_fd.ready & POLL_ERROR != 0 {
            proxy_closed = true;
            upload.start = upload.end;
        }
        if !proxy_closed && proxy_fd.ready & POLLOUT != 0 {
            upload.write(&mut proxy)?;
        }
        if !proxy_closed && client_fd.ready & POLLIN != 0 && !upload.read(&mut client)? {
            return Ok(());
        }
        if proxy_fd.ready & (POLLIN | POLL_ERROR) != 0
            && download.empty()
            && !download.read(&mut proxy)?
        {
            return Ok(());
        }
    }
}

fn wait_for_shell(shell_pid: u32) -> io::Result<ExitStatus> {
    loop {
        let mut status = 0;
        // PID 1 also inherits orphaned grandchildren. Reap them as they exit.
        let pid = unsafe { waitpid(-1, &mut status, 0) };
        if pid == -1 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if pid as u32 == shell_pid {
            return Ok(ExitStatus::from_raw(status));
        }
    }
}

fn main() -> io::Result<()> {
    let mut args = env::args_os().skip(1);
    let port = args
        .next()
        .and_then(|port| port.to_str()?.parse::<u16>().ok())
        .ok_or(io::ErrorKind::InvalidInput)?;
    let program = args.next().ok_or(io::ErrorKind::InvalidInput)?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))?;
    thread::Builder::new().spawn(move || {
        let active = Arc::new(());
        for client in listener.incoming().flatten() {
            if Arc::strong_count(&active) <= 16 {
                let active = active.clone();
                let _ = thread::Builder::new().spawn(move || {
                    let _active = active;
                    let _ = forward(client);
                });
            }
        }
    })?;
    let shell = Command::new(program).args(args).spawn()?;
    let status = wait_for_shell(shell.id())?;
    process::exit(
        status
            .code()
            .unwrap_or_else(|| 128 + status.signal().unwrap_or(0)),
    );
}
