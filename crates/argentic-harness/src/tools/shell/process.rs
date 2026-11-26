use std::{
    io::{self, Read},
    os::fd::RawFd,
};

const OUTPUT_LIMIT: usize = 256 * 1024;

pub(super) fn nonblocking(fd: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags == -1 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[derive(Default)]
pub(super) struct Capture {
    bytes: Vec<u8>,
    truncated: bool,
}

impl Capture {
    pub(super) fn pump(&mut self, stream: &mut impl Read) -> io::Result<()> {
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

    pub(super) fn into_text(self) -> String {
        let mut text = String::from_utf8_lossy(&self.bytes).into_owned();
        if self.truncated {
            text.push_str("\n[Shell output capture limit reached]");
        }
        text
    }
}
