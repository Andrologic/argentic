use super::file_helper::{cancelled, supervise_read, validate_file_name, MAX_BYTES};
use super::ContentResourceOwner;
use cap_std::fs::Dir;
use std::fs::File;
use std::io::{self, Read};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::fs::MetadataExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::path::{Path, PathBuf};
use std::ptr;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use windows_sys::{
    Wdk::{
        Foundation::OBJECT_ATTRIBUTES,
        Storage::FileSystem::{
            NtOpenFile, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN_REPARSE_POINT,
            FILE_SYNCHRONOUS_IO_NONALERT,
        },
    },
    Win32::{
        Foundation::{RtlNtStatusToDosError, OBJ_DONT_REPARSE, UNICODE_STRING},
        Storage::FileSystem::{
            GetFileType, FILE_ATTRIBUTE_REPARSE_POINT, FILE_GENERIC_READ, FILE_SHARE_DELETE,
            FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TYPE_DISK,
        },
        System::IO::{CancelSynchronousIo, IO_STATUS_BLOCK},
    },
};

pub async fn read_confined(
    parent: &Dir,
    name: &Path,
    cancel: &CancellationToken,
    owner: Option<ContentResourceOwner>,
) -> io::Result<Vec<u8>> {
    let parent = parent.try_clone()?;
    let name = name.to_owned();
    supervise_read(cancel, owner, move |cancel| read(parent, name, cancel)).await
}

async fn read(parent: Dir, name: PathBuf, cancel: CancellationToken) -> io::Result<Vec<u8>> {
    let (finished, completed) = tokio::sync::oneshot::channel();
    let worker_cancel = cancel.clone();
    let reader = std::thread::Builder::new()
        .name("agsdl-content-reader".into())
        .spawn(move || {
            let result = read_file(parent, &name, &worker_cancel);
            let _ = finished.send(());
            result
        })?;
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            // Cancellation may race with the thread entering a new I/O call.
            // Keep requesting cancellation until the thread actually stops.
            while !reader.is_finished() {
                unsafe { CancelSynchronousIo(reader.as_raw_handle()) };
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        _ = completed => {}
    }
    let result = reader
        .join()
        .map_err(|_| io::Error::other("Content reader panicked"))?;
    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    result
}

fn read_file(parent: Dir, name: &Path, cancel: &CancellationToken) -> io::Result<Vec<u8>> {
    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    let file = open_relative(&parent, name, false)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
        || unsafe { GetFileType(file.as_raw_handle()) } != FILE_TYPE_DISK
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Not a regular file",
        ));
    }
    if metadata.len() > MAX_BYTES {
        return Err(io::Error::other("File exceeds limit"));
    }
    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(io::Error::other("File exceeds limit"));
    }
    Ok(bytes)
}

pub(crate) fn open_relative(parent: &Dir, name: &Path, directory: bool) -> io::Result<File> {
    validate_file_name(name)?;
    let mut name: Vec<u16> = name.as_os_str().encode_wide().collect();
    // A single native filename must not name a stream or contain a terminator.
    if name.iter().any(|unit| matches!(*unit, 0 | 58)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Invalid Windows file name",
        ));
    }
    let length = u16::try_from(name.len() * 2)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "File name is too long"))?;
    let name = UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: name.as_mut_ptr(),
    };
    // The kernel uses the already pinned parent. Reparse points must not
    // redirect the name, even if a file is replaced after policy admission.
    let attributes = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: parent.as_raw_handle(),
        ObjectName: &name,
        Attributes: OBJ_DONT_REPARSE,
        ..Default::default()
    };
    let mut status = IO_STATUS_BLOCK::default();
    let mut handle = ptr::null_mut();
    let (kind, sharing) = if directory {
        (FILE_DIRECTORY_FILE, FILE_SHARE_READ | FILE_SHARE_WRITE)
    } else {
        (
            FILE_NON_DIRECTORY_FILE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        )
    };
    let result = unsafe {
        NtOpenFile(
            &mut handle,
            FILE_GENERIC_READ,
            &attributes,
            &mut status,
            sharing,
            kind | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
        )
    };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(
            unsafe { RtlNtStatusToDosError(result) } as i32,
        ));
    }
    Ok(unsafe { File::from_raw_handle(handle) })
}
