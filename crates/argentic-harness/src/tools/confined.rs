use super::{denied, Dir, File, OpenOptions};
use cap_fs_ext::{DirExt, FollowSymlinks, MetadataExt, OpenOptionsFollowExt};
use std::{
    io,
    path::{Path, PathBuf},
};

pub(super) fn directory(root: &Dir, path: &Path) -> io::Result<Dir> {
    if path.as_os_str().is_empty() {
        return root.try_clone();
    }
    #[cfg(target_os = "linux")]
    if let Some(file) = open_beneath(root, path, libc::O_DIRECTORY)? {
        return Ok(Dir::from_std_file(file));
    }
    walk(root, path)
}

pub(super) fn file(root: &Dir, path: &Path) -> io::Result<File> {
    let parent = path.parent().ok_or_else(denied)?;
    let name = Path::new(path.file_name().ok_or_else(denied)?);
    if !root.symlink_metadata(path)?.is_file() {
        return Err(denied());
    }
    #[cfg(target_os = "linux")]
    if let Some(file) = open_beneath(root, path, libc::O_NONBLOCK)? {
        return check_regular(File::from_std(file));
    }

    let dir = walk(root, parent)?;
    check_spelling(&dir, name)?;
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    #[cfg(unix)]
    {
        use cap_std::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    check_regular(dir.open_with(name, &options)?)
}

fn check_regular(file: File) -> io::Result<File> {
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(denied());
    }
    Ok(file)
}

fn walk(root: &Dir, path: &Path) -> io::Result<Dir> {
    let mut dir = root.try_clone()?;
    for part in path {
        let part = Path::new(part);
        check_spelling(&dir, part)?;
        dir = dir.open_dir_nofollow(part)?;
    }
    Ok(dir)
}

// Case folding and short-name aliases could reach a different read rule.
fn check_spelling(dir: &Dir, name: &Path) -> io::Result<()> {
    if cfg!(any(windows, target_os = "macos")) {
        for entry in dir.entries()? {
            if entry?.file_name() == name.as_os_str() {
                return Ok(());
            }
        }
        return Err(denied());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn open_beneath(root: &Dir, path: &Path, flags: i32) -> io::Result<Option<std::fs::File>> {
    use std::{
        ffi::CString,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::ffi::OsStrExt,
        },
    };
    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| denied())?;
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = (libc::O_RDONLY | libc::O_CLOEXEC | flags) as u64;
    how.resolve = libc::RESOLVE_BENEATH | libc::RESOLVE_NO_SYMLINKS | libc::RESOLVE_NO_MAGICLINKS;
    // The descriptor is owned by the returned File.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            root.as_raw_fd(),
            path.as_ptr(),
            &how,
            size_of::<libc::open_how>(),
        )
    };
    if fd >= 0 {
        return Ok(Some(unsafe { std::fs::File::from_raw_fd(fd as i32) }));
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        // Older kernels and filters can disable openat2. The caller still
        // verifies the location of the handle before accessing its contents.
        Some(libc::ENOSYS | libc::EINVAL | libc::EPERM) => Ok(None),
        _ => Err(error),
    }
}

#[cfg(target_os = "linux")]
pub(super) fn location(handle: &impl std::os::fd::AsRawFd) -> io::Result<PathBuf> {
    std::fs::read_link(format!("/proc/self/fd/{}", handle.as_raw_fd()))
}

#[cfg(target_os = "macos")]
pub(super) fn location(handle: &impl std::os::fd::AsRawFd) -> io::Result<PathBuf> {
    use std::{
        ffi::{CStr, OsStr},
        os::unix::ffi::OsStrExt,
    };
    let mut buffer = [0; libc::MAXPATHLEN as usize];
    if unsafe { libc::fcntl(handle.as_raw_fd(), libc::F_GETPATH, buffer.as_mut_ptr()) } == -1 {
        return Err(io::Error::last_os_error());
    }
    let path = unsafe { CStr::from_ptr(buffer.as_ptr()) };
    Ok(PathBuf::from(OsStr::from_bytes(path.to_bytes())))
}

#[cfg(windows)]
pub(super) fn location(handle: &impl std::os::windows::io::AsRawHandle) -> io::Result<PathBuf> {
    use std::{ffi::OsString, os::windows::ffi::OsStringExt};
    use windows_sys::Win32::Storage::FileSystem::GetFinalPathNameByHandleW;
    let mut buffer = vec![0; 260];
    loop {
        let len = unsafe {
            GetFinalPathNameByHandleW(
                handle.as_raw_handle(),
                buffer.as_mut_ptr(),
                buffer.len() as u32,
                0,
            )
        } as usize;
        if len == 0 {
            return Err(io::Error::last_os_error());
        }
        if len < buffer.len() {
            return Ok(PathBuf::from(OsString::from_wide(&buffer[..len])));
        }
        buffer.resize(len + 1, 0);
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(super) fn location<T>(_handle: &T) -> io::Result<PathBuf> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Cannot verify the open path on this platform",
    ))
}
