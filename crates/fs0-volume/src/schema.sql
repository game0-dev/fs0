CREATE TABLE volume_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  volume_id INTEGER NOT NULL,
  format_version INTEGER NOT NULL,
  max_bytes INTEGER NOT NULL,
  active_volume_offset INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE chunks (
  chunk_id BLOB PRIMARY KEY CHECK (length(chunk_id) = 32),
  volume_offset INTEGER NOT NULL,
  raw_len INTEGER NOT NULL,
  compressed_len INTEGER NOT NULL
);

CREATE INDEX idx_chunks_volume_offset ON chunks(volume_offset);

CREATE TABLE pending_central_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT NOT NULL CHECK (event_type IN ('chunk_stored', 'chunk_deleted')),
  chunk_id BLOB NOT NULL CHECK (length(chunk_id) = 32),
  last_failed_at_ms INTEGER
);

CREATE INDEX idx_pending_central_events_order
  ON pending_central_events(event_id);
