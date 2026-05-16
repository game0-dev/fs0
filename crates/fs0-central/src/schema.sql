CREATE TABLE IF NOT EXISTS volumes (
  volume_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  max_bytes INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
  file_id INTEGER PRIMARY KEY AUTOINCREMENT,
  dir TEXT NOT NULL,
  name TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  compressed_size_bytes INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  UNIQUE (dir, name)
);

CREATE INDEX IF NOT EXISTS idx_files_dir_name
  ON files(dir, name);

CREATE TABLE IF NOT EXISTS chunks (
  chunk_id BLOB PRIMARY KEY CHECK (length(chunk_id) = 32),
  raw_len INTEGER NOT NULL,
  compressed_len INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS file_chunks (
  file_id INTEGER NOT NULL,
  chunk_index INTEGER NOT NULL,
  chunk_id BLOB NOT NULL,
  PRIMARY KEY (file_id, chunk_index),
  FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_file_chunks_chunk
  ON file_chunks(chunk_id);

CREATE TABLE IF NOT EXISTS chunk_replicas (
  chunk_id BLOB NOT NULL,
  volume_id INTEGER NOT NULL,
  PRIMARY KEY (chunk_id, volume_id),
  FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE,
  FOREIGN KEY (volume_id) REFERENCES volumes(volume_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_chunk_replicas_volume
  ON chunk_replicas(volume_id);

CREATE TABLE IF NOT EXISTS append_leases (
  lease_id INTEGER PRIMARY KEY AUTOINCREMENT,
  file_id INTEGER NOT NULL,
  client_id INTEGER NOT NULL,
  volume_id INTEGER NOT NULL,
  base_size_bytes INTEGER NOT NULL,
  prefer_volume_name TEXT,
  state TEXT NOT NULL,
  expires_at_ms INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_append_leases_active_file
  ON append_leases(file_id)
  WHERE state = 'active';

CREATE TABLE IF NOT EXISTS file_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT NOT NULL,
  old_dir TEXT,
  old_name TEXT,
  new_dir TEXT,
  new_name TEXT,
  file_id INTEGER,
  created_at_ms INTEGER NOT NULL
);
