use crate::HashId;
use rusqlite::{Row, types::Type};
use std::io::{Error as IoError, ErrorKind};

pub trait SqliteRowExt {
    fn u64(&self, index: usize, name: &str) -> rusqlite::Result<u64>;
    fn optional_u64(&self, index: usize, name: &str) -> rusqlite::Result<Option<u64>>;
    fn hash_id(&self, index: usize, name: &str) -> rusqlite::Result<HashId>;
}

impl SqliteRowExt for Row<'_> {
    fn u64(&self, index: usize, name: &str) -> rusqlite::Result<u64> {
        let value: i64 = self.get(index)?;
        u64::try_from(value).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                Type::Integer,
                Box::new(IoError::new(
                    ErrorKind::InvalidData,
                    format!("{name} value {value} is negative"),
                )),
            )
        })
    }

    fn optional_u64(&self, index: usize, name: &str) -> rusqlite::Result<Option<u64>> {
        self.get::<_, Option<i64>>(index)?
            .map(|value| {
                u64::try_from(value).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        index,
                        Type::Integer,
                        Box::new(IoError::new(
                            ErrorKind::InvalidData,
                            format!("{name} value {value} is negative"),
                        )),
                    )
                })
            })
            .transpose()
    }

    fn hash_id(&self, index: usize, name: &str) -> rusqlite::Result<HashId> {
        let value: Vec<u8> = self.get(index)?;
        let len = value.len();
        let bytes = value.try_into().map_err(|_value: Vec<u8>| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                Type::Blob,
                Box::new(IoError::new(
                    ErrorKind::InvalidData,
                    format!("{name} must be 32 bytes, got {len}"),
                )),
            )
        })?;

        Ok(HashId(bytes))
    }
}
