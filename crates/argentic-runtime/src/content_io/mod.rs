//! Native content-reader lifecycle, shared with the host executable.
pub mod file_helper;
#[cfg(all(target_os = "macos", not(feature = "agsdl-content-qa")))]
mod macos_launcher;
#[cfg(all(target_os = "macos", feature = "agsdl-content-qa"))]
#[doc(hidden)]
pub mod macos_launcher;
#[cfg(windows)]
mod windows_reader;
#[cfg(windows)]
pub(crate) use windows_reader::open_relative;

use std::sync::Arc;
pub type ContentResourceOwner =
    Arc<dyn Fn(tokio::task::JoinHandle<Result<(), String>>) -> Result<(), String> + Send + Sync>;

#[doc(hidden)]
pub mod admitted;

/// Read the link count from the open file, never by reopening its name.
pub(crate) fn single_link_metadata(file: &std::fs::File) -> std::io::Result<std::fs::Metadata> {
    let metadata = file.metadata()?;
    #[cfg(unix)]
    let links = std::os::unix::fs::MetadataExt::nlink(&metadata);
    #[cfg(windows)]
    let links = {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };
        let mut info = BY_HANDLE_FILE_INFORMATION::default();
        if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        info.nNumberOfLinks
    };
    if links != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Expected a file with exactly one hard link",
        ));
    }
    Ok(metadata)
}
