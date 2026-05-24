use crate::{Fs0Error, HashId};
use std::time::{SystemTime, UNIX_EPOCH};

#[must_use]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before unix epoch")
        .as_millis() as u64
}

pub fn u64_to_i64(value: u64, name: &str) -> Result<i64, Fs0Error> {
    i64::try_from(value).map_err(|_| Fs0Error::IntegerConversion {
        message: format!("{name} value {value} does not fit in i64"),
    })
}

pub fn i64_to_u64(value: i64, name: &str) -> Result<u64, Fs0Error> {
    u64::try_from(value).map_err(|_| Fs0Error::IntegerConversion {
        message: format!("{name} value {value} is negative"),
    })
}

pub fn split_fs0_path(path: &str) -> Result<(String, String), Fs0Error> {
    if !path.starts_with('/') {
        return Err(Fs0Error::InvalidRequest);
    }
    if path.split('/').any(|component| component == "..") {
        return Err(Fs0Error::InvalidRequest);
    }
    if path == "/" {
        return Err(Fs0Error::InvalidRequest);
    }
    let (parent, name) = path.rsplit_once('/').ok_or(Fs0Error::InvalidRequest)?;
    if name.is_empty() {
        return Err(Fs0Error::InvalidRequest);
    }
    let parent = if parent.is_empty() { "/" } else { parent };
    Ok((parent.to_owned(), name.to_owned()))
}

pub fn join_fs0_path(dir: &str, name: &str) -> Result<String, Fs0Error> {
    if dir == "/" {
        Ok(format!("/{name}"))
    } else {
        Ok(format!("{dir}/{name}"))
    }
}

pub fn hash_id_from_vec(value: Vec<u8>) -> Result<HashId, Fs0Error> {
    let bytes = value
        .try_into()
        .map_err(|_value: Vec<u8>| Fs0Error::InvalidRequest)?;
    Ok(HashId(bytes))
}
