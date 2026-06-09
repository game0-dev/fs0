use crate::{Fs0Error, Fs0Result};
use std::time::{SystemTime, UNIX_EPOCH};

#[must_use]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before unix epoch")
        .as_millis() as u64
}

pub fn u64_to_i64(value: u64, name: &str) -> Fs0Result<i64> {
    i64::try_from(value).map_err(|_| Fs0Error::IntegerConversion {
        message: format!("{name} value {value} does not fit in i64"),
    })
}

pub fn i64_to_u64(value: i64, name: &str) -> Fs0Result<u64> {
    u64::try_from(value).map_err(|_| Fs0Error::IntegerConversion {
        message: format!("{name} value {value} is negative"),
    })
}

pub fn split_fs0_path_dir_and_name(path: &str) -> Fs0Result<(String, String)> {
    if !path.starts_with('/') || path.ends_with('/') || path.contains("//") {
        return Err(Fs0Error::InvalidPath {
            path: path.to_owned(),
        });
    }
    let (parent, name) = path.rsplit_once('/').ok_or_else(|| Fs0Error::InvalidPath {
        path: path.to_owned(),
    })?;
    if name.is_empty() || name == "." || name == ".." || name.contains('/') {
        return Err(Fs0Error::InvalidPath {
            path: path.to_owned(),
        });
    }
    if parent
        .split('/')
        .any(|component| component == "." || component == "..")
    {
        return Err(Fs0Error::InvalidPath {
            path: path.to_owned(),
        });
    }
    let parent = if parent.is_empty() { "/" } else { parent };
    Ok((parent.to_owned(), name.to_owned()))
}

pub fn join_fs0_path(dir: &str, name: &str) -> Fs0Result<String> {
    if dir == "/" {
        Ok(format!("/{name}"))
    } else {
        Ok(format!("{dir}/{name}"))
    }
}

pub fn split_fs0_path_components(path: &str) -> Fs0Result<Vec<&str>> {
    if !path.starts_with('/') {
        return Err(Fs0Error::InvalidPath {
            path: path.to_owned(),
        });
    }
    if path == "/" {
        return Ok(Vec::new());
    }
    let components = path[1..].split('/').collect::<Vec<_>>();
    if components
        .iter()
        .any(|component| component.is_empty() || *component == "." || *component == "..")
    {
        return Err(Fs0Error::InvalidPath {
            path: path.to_owned(),
        });
    }

    Ok(components)
}
