PRAGMA foreign_keys = ON;

CREATE TABLE volumes (
  volume_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  max_bytes INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE files (
  file_id INTEGER PRIMARY KEY AUTOINCREMENT,
  dir TEXT NOT NULL,
  name TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  compressed_size_bytes INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  UNIQUE (dir, name)
);

CREATE INDEX idx_files_dir_name
  ON files(dir, name);

CREATE TABLE bundles (
  bundle_id BLOB PRIMARY KEY CHECK (length(bundle_id) = 32),
  raw_len INTEGER NOT NULL,
  compressed_len INTEGER NOT NULL
);

CREATE TABLE file_bundles (
  file_id INTEGER NOT NULL,
  bundle_index INTEGER NOT NULL,
  bundle_id BLOB NOT NULL,
  PRIMARY KEY (file_id, bundle_index),
  FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  FOREIGN KEY (bundle_id) REFERENCES bundles(bundle_id) ON DELETE RESTRICT
);

CREATE INDEX idx_file_bundles_bundle
  ON file_bundles(bundle_id);

CREATE TABLE bundle_replicas (
  bundle_id BLOB NOT NULL,
  volume_id INTEGER NOT NULL,
  PRIMARY KEY (bundle_id, volume_id),
  FOREIGN KEY (bundle_id) REFERENCES bundles(bundle_id) ON DELETE CASCADE,
  FOREIGN KEY (volume_id) REFERENCES volumes(volume_id) ON DELETE CASCADE
);

CREATE INDEX idx_bundle_replicas_volume
  ON bundle_replicas(volume_id);

CREATE TABLE append_leases (
  lease_id INTEGER PRIMARY KEY AUTOINCREMENT,
  file_id INTEGER NOT NULL,
  client_id INTEGER NOT NULL,
  volume_id INTEGER NOT NULL,
  base_size_bytes INTEGER NOT NULL,
  prefer_volume_name TEXT,
  expires_at_ms INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  FOREIGN KEY (volume_id) REFERENCES volumes(volume_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_append_leases_file
  ON append_leases(file_id);

CREATE TABLE file_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT NOT NULL,
  old_dir TEXT,
  old_name TEXT,
  new_dir TEXT,
  new_name TEXT,
  file_id INTEGER,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX idx_file_events_file
  ON file_events(file_id);
