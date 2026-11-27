use std::io::{self, Seek, Write};

const LD_WORD_ABS: u16 = 0x20;
const JEQ: u16 = 0x15;
const AND: u16 = 0x54;
const JSET: u16 = 0x45;
const RET: u16 = 0x06;
const ALLOW: u32 = 0x7fff0000;
const ERRNO: u32 = 0x00050000 | libc::EPERM as u32;
const KILL_PROCESS: u32 = 0x80000000;

#[cfg(all(target_arch = "x86_64", target_endian = "little"))]
const AUDIT_ARCH: u32 = 0xc000003e;
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
const AUDIT_ARCH: u32 = 0xc00000b7;
#[cfg(all(target_arch = "aarch64", target_endian = "big"))]
const AUDIT_ARCH: u32 = 0x800000b7;

/// bwrap consumes native struct sock_filter records, then installs them after
/// setup. Validate the syscall architecture before interpreting its numbers.
pub(super) fn write_filter(file: &mut std::fs::File, network: bool) -> io::Result<()> {
    let mut emit = |code: u16, jt: u8, jf: u8, value: u32| {
        file.write_all(&code.to_ne_bytes())?;
        file.write_all(&[jt, jf])?;
        file.write_all(&value.to_ne_bytes())
    };
    emit(LD_WORD_ABS, 0, 0, 4)?; // seccomp_data.arch
    emit(JEQ, 1, 0, AUDIT_ARCH)?;
    emit(RET, 0, 0, KILL_PROCESS)?;
    emit(LD_WORD_ABS, 0, 0, 0)?; // seccomp_data.nr
    if cfg!(target_arch = "x86_64") {
        emit(JSET, 0, 1, 0x40000000)?; // x32 ABI tag
        emit(RET, 0, 0, KILL_PROCESS)?;
    }
    for call in [
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_pidfd_getfd,
        libc::SYS_bpf,
        libc::SYS_perf_event_open,
        libc::SYS_io_uring_setup,
        libc::SYS_io_uring_enter,
        libc::SYS_io_uring_register,
        libc::SYS_keyctl,
        libc::SYS_add_key,
        libc::SYS_request_key,
        libc::SYS_open_by_handle_at,
        libc::SYS_setsid,
        libc::SYS_setpgid,
    ] {
        emit(JEQ, 0, 1, call as u32)?;
        emit(RET, 0, 0, ERRNO)?;
    }
    if network {
        // The private namespace has only loopback. Allow IPv4 stream
        // sockets for the loopback bridge and Unix sockets for its one
        // mounted endpoint. socketpair cannot connect to a host socket.
        let argument_offset = if cfg!(target_endian = "little") {
            16
        } else {
            20
        };
        emit(JEQ, 0, 7, libc::SYS_socket as u32)?;
        emit(LD_WORD_ABS, 0, 0, argument_offset)?; // domain
        emit(JEQ, 5, 0, libc::AF_UNIX as u32)?;
        emit(JEQ, 0, 3, libc::AF_INET as u32)?;
        emit(LD_WORD_ABS, 0, 0, argument_offset + 8)?; // type
        emit(AND, 0, 0, 0xf)?; // Ignore CLOEXEC and NONBLOCK
        emit(JEQ, 1, 0, libc::SOCK_STREAM as u32)?;
        emit(RET, 0, 0, ERRNO)?;
    } else {
        for call in [libc::SYS_socket, libc::SYS_socketpair] {
            emit(JEQ, 0, 1, call as u32)?;
            emit(RET, 0, 0, ERRNO)?;
        }
    }
    emit(RET, 0, 0, ALLOW)?;
    file.rewind()
}
