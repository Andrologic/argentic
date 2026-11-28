//! Standalone Linux helper embedded by build.rs. The host mounts it read-only
//! and runs it as PID 1 so exiting also terminates outstanding proxy streams.
#![cfg(target_os = "linux")]

use std::{
    env,
    io::{self, Read, Write},
    net::{Ipv4Addr, Shutdown, TcpListener, TcpStream},
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
const POLL_ERROR: i16 = 0x008 | 0x020; // ERR, NVAL
const POLLHUP: i16 = 0x010;

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

    fn events(&self, incoming_closed: bool, outgoing: &Self) -> i16 {
        (if !incoming_closed && self.empty() {
            POLLIN
        } else {
            0
        }) | (if outgoing.empty() { 0 } else { POLLOUT })
    }
}

fn retry(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

fn forward(client: TcpStream) -> io::Result<()> {
    let proxy = UnixStream::connect("/run/argentic-proxy.sock")?;
    relay(client, proxy)
}

fn relay(mut client: TcpStream, mut proxy: UnixStream) -> io::Result<()> {
    client.set_nonblocking(true)?;
    proxy.set_nonblocking(true)?;
    let mut upload = Buffer::new();
    let mut download = Buffer::new();
    let mut client_eof = false;
    let mut proxy_eof = false;
    while !client_eof || !proxy_eof {
        let proxy_events = download.events(proxy_eof, &upload);
        let mut fds = [
            PollFd {
                fd: client.as_raw_fd(),
                events: upload.events(client_eof, &download),
                ready: 0,
            },
            PollFd {
                fd: if proxy_events == 0 && !proxy_eof {
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
        if client_fd.ready & (POLLHUP | POLL_ERROR) != 0 || proxy_fd.ready & POLL_ERROR != 0 {
            return Ok(());
        }
        if client_fd.ready & POLLOUT != 0 {
            download.write(&mut client)?;
        }
        if proxy_fd.ready & POLLHUP != 0 {
            // The proxy closed both directions. Drain its buffered reply.
            client_eof = true;
            upload.start = upload.end;
        } else if proxy_fd.ready & POLLOUT != 0 {
            upload.write(&mut proxy)?;
        }
        if !client_eof && client_fd.ready & POLLIN != 0 && !upload.read(&mut client)? {
            proxy.shutdown(Shutdown::Write)?;
            client_eof = true;
        }
        if !proxy_eof
            && proxy_fd.ready & (POLLIN | POLLHUP) != 0
            && download.empty()
            && !download.read(&mut proxy)?
        {
            client.shutdown(Shutdown::Write)?;
            proxy_eof = true;
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn relay_drains_after_client_half_closes() -> io::Result<()> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let mut shell = TcpStream::connect(listener.local_addr()?)?;
        let (client, _) = listener.accept()?;
        let (proxy, mut host) = UnixStream::pair()?;
        let timeout = Some(Duration::from_secs(5));
        shell.set_read_timeout(timeout)?;
        host.set_read_timeout(timeout)?;
        let relay = thread::spawn(move || relay(client, proxy));

        shell.write_all(b"request")?;
        shell.shutdown(Shutdown::Write)?;
        let mut request = Vec::new();
        host.read_to_end(&mut request)?;
        assert_eq!(request, b"request");

        host.write_all(b"response")?;
        host.shutdown(Shutdown::Write)?;
        let mut response = Vec::new();
        shell.read_to_end(&mut response)?;
        assert_eq!(response, b"response");
        relay
            .join()
            .map_err(|_| io::Error::other("relay panicked"))?
    }
}
