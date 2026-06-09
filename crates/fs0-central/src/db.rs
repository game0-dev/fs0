mod bundle_replica;
mod dir;
mod event;
mod file;
mod file_bundle;
mod lease;
mod volume;

pub(crate) use lease::{CreateUpdateLease, LeaseRecord};

use fs0_core::Fs0Result;
use rusqlite::Connection;
use std::path::Path;

#[derive(Debug)]
pub(crate) struct CentralDb {
    conn: Connection,
    dir_cache: dir::DirCache,
}

impl CentralDb {
    pub(crate) fn open(path: impl AsRef<Path>) -> Fs0Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(include_str!("schema.sql"))?;

        Ok(Self {
            conn,
            dir_cache: dir::DirCache::new(),
        })
    }

    pub(crate) fn tx(&mut self) -> Fs0Result<CentralTx<'_>> {
        let Self { conn, dir_cache } = self;
        Ok(CentralTx::new(conn.transaction()?, dir_cache))
    }
}

#[must_use = "transaction must be committed or it will be rolled back on drop"]
pub(crate) struct CentralTx<'conn> {
    pub(super) inner: rusqlite::Transaction<'conn>,
    dir_cache: &'conn dir::DirCache,
}

impl std::fmt::Debug for CentralTx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CentralTx").finish_non_exhaustive()
    }
}

impl<'conn> CentralTx<'conn> {
    fn new(inner: rusqlite::Transaction<'conn>, dir_cache: &'conn dir::DirCache) -> Self {
        Self { inner, dir_cache }
    }

    pub(crate) fn commit(self) -> Fs0Result<()> {
        self.inner.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn rollback(self) -> Fs0Result<()> {
        self.inner.rollback()?;
        Ok(())
    }
}
