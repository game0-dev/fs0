PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS volumes (
  volume_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  max_bytes INTEGER NOT NULL,
  max_volume_offset INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dirs (
  dir_id INTEGER PRIMARY KEY,
  parent_dir_id INTEGER,
  name TEXT NOT NULL,
  FOREIGN KEY (parent_dir_id) REFERENCES dirs(dir_id),
  UNIQUE (parent_dir_id, name),
  CHECK (
    (dir_id = 0 AND parent_dir_id IS NULL AND name = '')
    OR
    (dir_id != 0 AND parent_dir_id IS NOT NULL AND name != '')
  )
);

INSERT OR IGNORE INTO dirs (dir_id, parent_dir_id, name)
VALUES (0, NULL, '');

CREATE INDEX IF NOT EXISTS idx_dirs_parent_name
  ON dirs(parent_dir_id, name);

CREATE TABLE IF NOT EXISTS files (
  file_id INTEGER PRIMARY KEY AUTOINCREMENT,
  dir_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  compressed_size_bytes INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  FOREIGN KEY (dir_id) REFERENCES dirs(dir_id),
  UNIQUE (dir_id, name)
);

CREATE INDEX IF NOT EXISTS idx_files_dir_name
  ON files(dir_id, name);

CREATE TABLE IF NOT EXISTS file_bundles (
  file_id INTEGER NOT NULL,
  bundle_index INTEGER NOT NULL,
  bundle_id BLOB NOT NULL CHECK (length(bundle_id) = 32),
  PRIMARY KEY (file_id, bundle_index),
  FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_file_bundles_bundle
  ON file_bundles(bundle_id);

CREATE TABLE IF NOT EXISTS bundle_replicas (
  bundle_id BLOB NOT NULL CHECK (length(bundle_id) = 32),
  volume_id INTEGER NOT NULL,
  raw_len INTEGER NOT NULL,
  compressed_len INTEGER NOT NULL,
  PRIMARY KEY (bundle_id, volume_id),
  FOREIGN KEY (volume_id) REFERENCES volumes(volume_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bundle_replicas_volume
  ON bundle_replicas(volume_id);

CREATE TABLE IF NOT EXISTS update_leases (
  lease_id INTEGER PRIMARY KEY AUTOINCREMENT,
  file_id INTEGER NOT NULL,
  volume_id INTEGER NOT NULL,
  base_size_bytes INTEGER NOT NULL,
  offset_bytes INTEGER NOT NULL,
  prefer_volume_name TEXT,
  expires_at_ms INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  FOREIGN KEY (volume_id) REFERENCES volumes(volume_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_update_leases_file
  ON update_leases(file_id);

CREATE TABLE IF NOT EXISTS file_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT NOT NULL,
  new_dir TEXT,
  new_name TEXT,
  file_id INTEGER,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_file_events_file
  ON file_events(file_id);
