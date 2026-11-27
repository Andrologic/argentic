//! Open project members by single components relative to pinned directories.
use cap_std::fs::{Dir, Metadata};
use std::{
    fs::File,
    io,
    path::{Component, Path},
};

fn invalid_member() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Expected a regular project member",
    )
}

#[cfg(unix)]
fn open_relative(parent: &Dir, name: &Path, directory: bool) -> io::Result<std::fs::File> {
    use std::{
        ffi::CString,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::ffi::OsStrExt,
        },
    };
    let name = CString::new(name.as_os_str().as_bytes())?;
    let flags = libc::O_CLOEXEC | libc::O_NOFOLLOW;
    let flags = if directory {
        #[cfg(target_os = "linux")]
        let access = libc::O_PATH;
        #[cfg(not(target_os = "linux"))]
        let access = libc::O_RDONLY;
        flags | access | libc::O_DIRECTORY
    } else {
        flags | libc::O_RDONLY | libc::O_NONBLOCK
    };
    // The argument is a single validated component and the parent outlives openat.
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

#[cfg(windows)]
use crate::content_io::open_relative;

pub(super) fn is_link(metadata: &Metadata) -> bool {
    #[cfg(windows)]
    {
        use cap_std::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        metadata.is_symlink()
    }
}

pub(super) fn with_directory<T>(
    root: &Dir,
    path: &Path,
    access: impl FnOnce(&Dir) -> io::Result<T>,
) -> io::Result<T> {
    // Windows cap-std uses paths for metadata and listing. Keep the ancestors
    // open without delete sharing until those operations have started.
    let mut opened = Vec::new();
    for part in path.components() {
        let Component::Normal(name) = part else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid project member path",
            ));
        };
        let parent = opened.last().unwrap_or(root);
        let child = Dir::from_std_file(open_relative(parent, Path::new(name), true)?);
        let metadata = child.dir_metadata()?;
        if !metadata.is_dir() || is_link(&metadata) {
            return Err(invalid_member());
        }
        opened.push(child);
    }
    access(opened.last().unwrap_or(root))
}

fn with_parent<T>(
    root: &Dir,
    path: &Path,
    access: impl FnOnce(&Dir, &Path) -> io::Result<T>,
) -> io::Result<T> {
    let name = path.file_name().ok_or_else(invalid_member)?;
    with_directory(root, path.parent().unwrap(), |dir| {
        access(dir, Path::new(name))
    })
}

pub(super) fn metadata(root: &Dir, path: &Path) -> io::Result<Metadata> {
    with_parent(root, path, |dir, name| dir.symlink_metadata(name))
}

pub(super) fn open_regular(root: &Dir, path: &Path) -> io::Result<File> {
    with_parent(root, path, |dir, name| {
        let metadata = dir.symlink_metadata(name)?;
        if !metadata.is_file() || is_link(&metadata) {
            return Err(invalid_member());
        }
        let file = open_relative(dir, name, false)?;
        let metadata = crate::content_io::single_link_metadata(&file)?;
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
            if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
                return Err(invalid_member());
            }
        }
        if !metadata.is_file() {
            return Err(invalid_member());
        }
        Ok(file)
    })
}
