use crate::io_platform;
use fs0_core::{
    Fs0Error, Fs0Result, VOLUME_DATA_FILE_IDLE_TTL_MS, VOLUME_DATA_FILE_PREFIX,
    VOLUME_DEFAULT_DATA_FILE_SIZE, now_ms,
};
use parking_lot::{Mutex, RwLock};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::Semaphore;

#[derive(Debug)]
pub(crate) struct DataFileCache {
    root: PathBuf,

    /// Whole-volume read concurrency limit.
    /// Example: 4 for one HDD-backed volume.
    read_sem: Arc<Semaphore>,

    /// Whole-volume write concurrency limit.
    /// Example: 1 for one HDD-backed volume.
    write_sem: Arc<Semaphore>,

    /// index == {VOLUME_DATA_FILE_PREFIX}{index}
    ///
    /// Use Arc<DataFileSlot> so in-flight read/write tasks can keep a slot
    /// after releasing the Vec lock.
    slots: RwLock<Vec<Arc<DataFileSlot>>>,
}

#[derive(Debug)]
struct DataFileSlot {
    /// None means this .data.N file is not currently open.
    ///
    /// This mutex also acts as the per-file open lock:
    /// if many tasks try to open the same .data.N concurrently,
    /// only one task performs the actual open.
    file: Mutex<Option<Arc<File>>>,

    last_access_ms: AtomicU64,
}

impl DataFileSlot {
    fn new() -> Self {
        Self {
            file: Mutex::new(None),
            last_access_ms: AtomicU64::new(0),
        }
    }

    fn touch(&self) {
        self.last_access_ms.store(now_ms(), Ordering::Relaxed);
    }
}

impl DataFileCache {
    pub(crate) fn with_capacity(
        root: PathBuf,
        data_files: usize,
        max_readers: usize,
        max_writers: usize,
    ) -> Self {
        let mut slots = Vec::with_capacity(data_files);

        for _ in 0..data_files {
            slots.push(Arc::new(DataFileSlot::new()));
        }

        Self {
            root,
            read_sem: Arc::new(Semaphore::new(max_readers.max(1))),
            write_sem: Arc::new(Semaphore::new(max_writers.max(1))),
            slots: RwLock::new(slots),
        }
    }

    pub(crate) async fn read_at(&self, index: u64, offset: u64, len: usize) -> Fs0Result<Vec<u8>> {
        let (slot, file) = self.get_or_open(index, false).await?;

        let permit = self
            .read_sem
            .clone()
            .acquire_owned()
            .await
            .expect("read semaphore closed");

        let task_result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            slot.touch();

            let mut bytes = vec![0u8; len];
            io_platform::read_at(&file, offset, &mut bytes)?;

            Ok::<Vec<u8>, std::io::Error>(bytes)
        })
        .await;

        let result = task_result.map_err(|err| Fs0Error::Internal {
            message: format!("read task failed: {err}"),
        })?;

        Ok(result?)
    }

    pub(crate) async fn write_at(&self, index: u64, offset: u64, bytes: Vec<u8>) -> Fs0Result<()> {
        let (slot, file) = self.get_or_open(index, true).await?;

        let permit = self
            .write_sem
            .clone()
            .acquire_owned()
            .await
            .expect("write semaphore closed");

        let task_result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            slot.touch();

            io_platform::write_at(&file, offset, &bytes)?;
            file.sync_data()?;

            Ok::<(), std::io::Error>(())
        })
        .await;

        let result = task_result.map_err(|err| Fs0Error::Internal {
            message: format!("write task failed: {err}"),
        })?;

        Ok(result?)
    }

    /// Call this from a background task, e.g. every 10 seconds.
    ///
    /// Do not call this after every read/write.
    pub(crate) fn reap_idle(&self, now_ms: u64) {
        let slots = self.slots.read();

        for slot in slots.iter() {
            let last_access_ms = slot.last_access_ms.load(Ordering::Relaxed);

            if last_access_ms == 0 {
                continue;
            }

            if now_ms.saturating_sub(last_access_ms) <= VOLUME_DATA_FILE_IDLE_TTL_MS {
                continue;
            }

            let Some(mut guard) = slot.file.try_lock() else {
                continue;
            };

            *guard = None;
        }
    }

    async fn get_or_open(
        &self,
        index: u64,
        create_if_missing: bool,
    ) -> Fs0Result<(Arc<DataFileSlot>, Arc<File>)> {
        let index = usize::try_from(index).map_err(|_| Fs0Error::IntegerConversion {
            message: format!("data file index {index} exceeds usize"),
        })?;

        let slot = {
            let slots = self.slots.read();
            let Some(slot) = slots.get(index) else {
                return Err(Fs0Error::InvalidData {
                    message: format!(
                        "data file index {index} exceeds configured data files {}",
                        slots.len()
                    ),
                });
            };
            slot.clone()
        };

        let root = self.root.clone();
        let open_slot = slot.clone();
        let task_result = tokio::task::spawn_blocking(move || -> Fs0Result<Arc<File>> {
            let mut guard = open_slot.file.lock();

            if let Some(file) = guard.as_ref() {
                return Ok(file.clone());
            }

            let path = root.join(format!("{VOLUME_DATA_FILE_PREFIX}{index}"));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(create_if_missing)
                .truncate(false)
                .open(&path)?;

            if create_if_missing && file.metadata()?.len() < VOLUME_DEFAULT_DATA_FILE_SIZE {
                io_platform::preallocate(&file, VOLUME_DEFAULT_DATA_FILE_SIZE)?;
            }

            let file = Arc::new(file);
            *guard = Some(file.clone());
            Ok(file)
        })
        .await;

        let file = task_result.map_err(|err| Fs0Error::Internal {
            message: format!("open data file task failed: {err}"),
        })??;

        Ok((slot, file))
    }
}
