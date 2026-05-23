use std::fs::File;
use std::io;

#[cfg(unix)]
pub(crate) fn read_at(file: &File, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    while !buf.is_empty() {
        let read = file.read_at(buf, offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        offset += read as u64;
        buf = &mut buf[read..];
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn read_at(file: &File, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
    use std::os::windows::fs::FileExt;

    while !buf.is_empty() {
        let read = file.seek_read(buf, offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        offset += read as u64;
        buf = &mut buf[read..];
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn write_at(file: &File, mut offset: u64, mut buf: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    while !buf.is_empty() {
        let written = file.write_at(buf, offset)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        offset += written as u64;
        buf = &buf[written..];
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn write_at(file: &File, mut offset: u64, mut buf: &[u8]) -> io::Result<()> {
    use std::os::windows::fs::FileExt;

    while !buf.is_empty() {
        let written = file.seek_write(buf, offset)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        offset += written as u64;
        buf = &buf[written..];
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn preallocate(file: &File, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let len = libc::off_t::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file length exceeds off_t"))?;
    let result = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, len) };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result));
    }
    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
pub(crate) fn preallocate(file: &File, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let len = libc::off_t::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file length exceeds off_t"))?;
    let mut store = libc::fstore_t {
        fst_flags: libc::F_ALLOCATECONTIG,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: len,
        fst_bytesalloc: 0,
    };

    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &store) };
    if result == -1 {
        store.fst_flags = libc::F_ALLOCATEALL;
        let fallback = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &store) };
        if fallback == -1 {
            return Err(io::Error::last_os_error());
        }
    }

    file.set_len(len as u64)
}

#[cfg(windows)]
pub(crate) fn preallocate(file: &File, len: u64) -> io::Result<()> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ALLOCATION_INFO, FileAllocationInfo, SetFileInformationByHandle,
    };

    let allocation_size = i64::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "file length exceeds Windows allocation size",
        )
    })?;
    let allocation = FILE_ALLOCATION_INFO {
        AllocationSize: allocation_size,
    };
    let result = unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle() as _,
            FileAllocationInfo,
            &allocation as *const _ as *const core::ffi::c_void,
            size_of::<FILE_ALLOCATION_INFO>() as u32,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }

    file.set_len(len)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    windows
)))]
pub(crate) fn preallocate(_file: &File, _len: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "physical file preallocation is not supported on this platform",
    ))
}
