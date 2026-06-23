pub(crate) mod data;
mod request_scheduler;

use crate::{Fs0Error, Fs0Result, central_session::CentralSession};
use fs0_config::ClientConfig;
use fs0_core::{
    DEFAULT_ZSTD_LEVEL, HashId, TRANSPORT_DATA_ALPN, blake3_hash,
    protocol::{
        DataRequest, DataResponse, DownloadChunkRequest, ProtocolRequest, ProtocolResponse,
        UploadChunkRequest, UploadChunkResponse,
    },
    zstd_compress, zstd_decompress,
};
use fs0_transport::{Connection, Transport};
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::sync::Mutex;
use tracing::{info, warn};

use self::request_scheduler::HashRequestScheduler;

pub(crate) struct StorageSession {
    pub(crate) inner: Arc<StorageSessionInner>,
    upload_scheduler: HashRequestScheduler<UploadChunkJob, UploadChunkResponse>,
    download_scheduler: HashRequestScheduler<DownloadChunkRequest, HashId>,
}

pub(crate) struct UploadChunkJob {
    pub(crate) lease_id: u64,
    pub(crate) file_id: u64,
    pub(crate) volume_id: u64,
    pub(crate) chunk_id: HashId,
    pub(crate) raw_bytes: Vec<u8>,
}

impl fmt::Debug for StorageSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageSession")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct StorageSessionInner {
    config: ClientConfig,
    transport: Transport,
    central: Arc<CentralSession>,
    client_id: u64,
    storage_id: u64,
    connection: Mutex<Option<Connection>>,
}

impl StorageSession {
    pub(crate) fn new(
        config: ClientConfig,
        transport: Transport,
        central: Arc<CentralSession>,
        client_id: u64,
        storage_id: u64,
    ) -> Self {
        let upload_concurrency = config.upload_concurrency;
        let download_concurrency = config.download_concurrency;
        let download_cache_dir = Arc::new(download_cache_dir(&config));
        let inner = Arc::new(StorageSessionInner {
            config,
            transport,
            central,
            client_id,
            storage_id,
            connection: Mutex::new(None),
        });
        let upload_scheduler = HashRequestScheduler::new(upload_concurrency, {
            let inner = Arc::clone(&inner);
            move |job| {
                let inner = Arc::clone(&inner);
                Box::pin(async move { upload_chunk_job(&inner, job).await })
            }
        });
        let download_scheduler = HashRequestScheduler::new(download_concurrency, {
            let inner = Arc::clone(&inner);
            let download_cache_dir = Arc::clone(&download_cache_dir);
            move |job| {
                let inner = Arc::clone(&inner);
                let download_cache_dir = Arc::clone(&download_cache_dir);
                Box::pin(async move {
                    download_chunk_job(&inner, download_cache_dir.as_path(), job).await
                })
            }
        });

        Self {
            inner,
            upload_scheduler,
            download_scheduler,
        }
    }

    pub(crate) async fn enqueue_upload<G>(
        &self,
        job: UploadChunkJob,
        on_complete: G,
    ) -> Fs0Result<()>
    where
        G: FnOnce(Fs0Result<Arc<UploadChunkResponse>>) + Send + 'static,
    {
        let chunk_id = job.chunk_id;
        self.upload_scheduler
            .enqueue_blocking(chunk_id, job, move |result| {
                on_complete(result.map(Arc::new));
            })
            .await
    }

    pub(crate) async fn enqueue_download<G>(
        &self,
        request: DownloadChunkRequest,
        on_complete: G,
    ) -> Fs0Result<()>
    where
        G: FnOnce(Fs0Result<HashId>) + Send + 'static,
    {
        let chunk_id = request.chunk_id;
        self.download_scheduler
            .enqueue_blocking(chunk_id, request, on_complete)
            .await
    }

    pub(crate) async fn close(&self, reason: &[u8]) {
        self.inner.close(reason).await;
    }
}

async fn upload_chunk_job(
    inner: &StorageSessionInner,
    job: UploadChunkJob,
) -> Fs0Result<UploadChunkResponse> {
    let chunk_id = job.chunk_id;
    let raw_len = job.raw_bytes.len() as u64;
    if blake3_hash(&job.raw_bytes) != chunk_id {
        return Err(Fs0Error::HashMismatch { volume_offset: 0 });
    }

    let has_chunk_started_at = Instant::now();
    match inner.has_chunk(job.volume_id, chunk_id).await {
        Ok(Some((existing_raw_len, compressed_len))) => {
            if existing_raw_len != raw_len {
                return Err(Fs0Error::InvalidData {
                    message: "existing chunk raw_len does not match upload job".to_owned(),
                });
            }
            return Ok(UploadChunkResponse {
                chunk_id,
                raw_len,
                compressed_len,
            });
        }
        Ok(None) => {}
        Err(err) => {
            warn!(
                %chunk_id,
                raw_len,
                elapsed_ms = has_chunk_started_at.elapsed().as_millis(),
                error = %err,
                "upload chunk has_chunk failed"
            );
            return Err(err);
        }
    }

    let compress_started_at = Instant::now();
    let compressed_bytes = tokio::task::spawn_blocking(move || {
        zstd_compress(job.raw_bytes.as_slice(), DEFAULT_ZSTD_LEVEL)
    })
    .await
    .map_err(|err| Fs0Error::Internal {
        message: err.to_string(),
    })??;
    let compress_elapsed_ms = compress_started_at.elapsed().as_millis();
    if compress_elapsed_ms > 1_000 {
        info!(
            %chunk_id,
            raw_len,
            compressed_len = compressed_bytes.len(),
            elapsed_ms = compress_elapsed_ms,
            "upload chunk compressed"
        );
    }

    let upload_started_at = Instant::now();
    match inner
        .upload_chunk(UploadChunkRequest {
            lease_id: job.lease_id,
            file_id: job.file_id,
            volume_id: job.volume_id,
            chunk_id,
            raw_len,
            compressed_bytes,
        })
        .await
    {
        Ok(response) => Ok(response),
        Err(err) => {
            warn!(
                %chunk_id,
                raw_len,
                elapsed_ms = upload_started_at.elapsed().as_millis(),
                error = %err,
                "upload chunk rpc failed"
            );
            Err(err)
        }
    }
}

async fn download_chunk_job(
    inner: &StorageSessionInner,
    download_cache_dir: &Path,
    request: DownloadChunkRequest,
) -> Fs0Result<HashId> {
    let chunk_id = request.chunk_id;
    let cache_path = cache_path(download_cache_dir, chunk_id);
    if let Ok(compressed_bytes) = tokio::fs::read(&cache_path).await {
        if compressed_chunk_matches(chunk_id, compressed_bytes.as_slice()).is_ok() {
            return Ok(chunk_id);
        }
        let _ = tokio::fs::remove_file(&cache_path).await;
    }

    let compressed_bytes = inner.download_chunk(request).await?;
    compressed_chunk_matches(chunk_id, compressed_bytes.as_slice())?;
    write_cache_file(&cache_path, compressed_bytes.as_slice()).await?;

    Ok(chunk_id)
}

fn cache_path(cache_dir: &Path, chunk_id: HashId) -> PathBuf {
    cache_dir.join(format!("{}.zst", chunk_id.to_hex()))
}

fn download_cache_dir(config: &ClientConfig) -> PathBuf {
    config
        .download_cache_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("fs0-client-cache"))
}

fn compressed_chunk_matches(chunk_id: HashId, compressed_bytes: &[u8]) -> Fs0Result<()> {
    let max_raw_len = usize::try_from(fs0_core::VOLUME_RAW_CHUNK_SIZE).map_err(|_| {
        Fs0Error::IntegerConversion {
            message: format!("raw_len {} exceeds usize", fs0_core::VOLUME_RAW_CHUNK_SIZE),
        }
    })?;
    let raw = zstd_decompress(compressed_bytes, max_raw_len)?;
    if raw.len() as u64 > fs0_core::VOLUME_RAW_CHUNK_SIZE || blake3_hash(&raw) != chunk_id {
        return Err(Fs0Error::HashMismatch { volume_offset: 0 });
    }

    Ok(())
}

async fn write_cache_file(path: &Path, bytes: &[u8]) -> Fs0Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, bytes).await?;
    Ok(())
}

impl StorageSessionInner {
    pub(crate) async fn ensure_connected(&self) -> Fs0Result<Connection> {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.as_ref()
            && !connection.is_closed()
        {
            return Ok(connection.clone());
        }

        if let Some(closed) = current.take() {
            closed.close(b"fs0 storage reconnect");
        }

        let storage = self
            .central
            .storage_peer(self.storage_id)
            .ok_or(Fs0Error::NotFound)?;
        let data_endpoint = postcard::from_bytes(&storage.iroh_endpoint).map_err(Fs0Error::from)?;
        info!(endpoint = ?data_endpoint, "client connecting to storage");
        let connection = self
            .transport
            .connect(data_endpoint, TRANSPORT_DATA_ALPN)
            .await?;
        self.authenticate(connection.clone()).await?;

        *current = Some(connection.clone());

        Ok(connection)
    }

    async fn authenticate(&self, connection: Connection) -> Fs0Result<()> {
        match connection
            .rpc(ProtocolRequest::Data(DataRequest::Authenticate {
                client_id: self.client_id,
                client_token: self.config.token.clone(),
            }))
            .await
        {
            Ok(ProtocolResponse::Data(DataResponse::Authenticate {
                client_id: authenticated_client_id,
            })) if authenticated_client_id == self.client_id => {
                info!(
                    client_id = self.client_id,
                    "client authenticated storage connection"
                );
            }
            Ok(ProtocolResponse::Error(err)) => {
                warn!(error = %err, "client storage authentication failed");
                connection.close(b"storage authentication failed");
                return Err(err);
            }
            Ok(response) => {
                connection.close(b"storage authentication failed");
                return Err(Fs0Error::InvalidFrame {
                    message: format!("unexpected data response: {response:?}"),
                });
            }
            Err(err) => {
                connection.close(b"storage authentication failed");
                return Err(err);
            }
        }

        Ok(())
    }

    pub(crate) async fn request(&self, request: DataRequest) -> Fs0Result<DataResponse> {
        let connection = self.ensure_connected().await?;
        let response = match connection.rpc(ProtocolRequest::Data(request)).await? {
            ProtocolResponse::Error(err) => Err(err),
            ProtocolResponse::Data(response) => Ok(response),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        };
        if response.is_err() && connection.is_closed() {
            *self.connection.lock().await = None;
        }

        response
    }

    async fn close(&self, reason: &[u8]) {
        if let Some(connection) = self.connection.lock().await.take() {
            connection.close(reason);
        }
    }
}
