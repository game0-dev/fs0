<p align="center">
  <img src="docs/fs0.png" alt="fs0" width="160">
</p>

<p align="center">
  Experimental append-only distributed storage written in Rust.
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
  <img src="https://img.shields.io/badge/status-experimental-yellow.svg" alt="Status">
  <img src="https://img.shields.io/badge/rust-2024-orange.svg" alt="Rust 2024">
</p>


## What is fs0?

**fs0** is an experimental distributed storage system built around append-only file writes, chunked storage, compressed data blocks, and a small command-line interface.

The current MVP is an append-only distributed file/object store. It is not a full POSIX filesystem yet. Future FUSE support will need a dedicated inode, directory, and attribute model before fs0 should be treated as a mounted filesystem.

It provides:

- a **central server** for metadata and coordination
- **storage nodes** for storing file chunks
- a **CLI client** for file operations
- local **volume** management
- an early foundation for future FUSE support

fs0 is currently intended for development, experimentation, and storage-system research.

> fs0 is not production-ready yet. The storage format, network protocol, and command-line interface may change.

---

## Features

- Append-only file writes
- Upload, append, list, read, download, and delete files
- Chunked file storage
- Zstandard compression
- BLAKE3 content hashing
- Central metadata server
- Storage node process
- Local volume creation and inspection
- Iroh-based transport layer
- JSON output for scripting
- Rust workspace with reusable crates

---

## Installation

### Requirements

- Rust toolchain with Rust 2024 edition support
- Cargo
- Git

### Build from source

```bash
git clone https://github.com/studio-0/fs0.git
cd fs0
cargo build --release -p fs0-cli
```

The binary will be generated at:

```bash
target/release/fs0
```

You can also run it directly with Cargo:

```bash
cargo run -p fs0-cli -- --help
```

Optional: install the CLI locally from the repository checkout:

```bash
cargo install --path crates/fs0-cli
```

Then check:

```bash
fs0 --help
```

---

## Quickstart

This section shows the basic local workflow.

### 1. Create a local config

Create `.local/fs0.local.toml`:

```toml
[central]
db_path = ".local/central.sqlite"
secret_key = "central-secret-key"
bind_port = 7800
replication_factor = 2
auth_tokens = ["dev-token"]

[central.relay]
public_url = "https://1.2.3.4:7801"
token = "relay-token"
https_bind_port = 7801
cert_path = ".local/relay-cert.pem"
key_path = ".local/relay-key.pem"
quic_bind_port = 7802
```

Run the central server:

```bash
fs0 central run --config .local/fs0.local.toml
```

Or with Cargo:

```bash
cargo run -p fs0-cli -- central run --config .local/fs0.local.toml
```

The server prints a central endpoint. Use that endpoint in your client and storage configuration.

---

### 2. Create a central-backed local volume

```bash
mkdir -p .local/volume-1
fs0 --config .local/fs0.local.toml volume create .local/volume-1 \
  --name local-volume-1 \
  --max-bytes 10G
```

Inspect the volume:

```bash
fs0 volume meta .local/volume-1
```

---

### 3. Fill client and storage config

Add the endpoint printed by `fs0 central run`:

```toml
[client]
token = "dev-token"
central_endpoint_id = "central-endpoint-id"
central_addr = "1.2.3.4:7800"

[client.relay]
url = "https://1.2.3.4:7801"
token = "relay-token"
quic_port = 7802
ca_cert = """
-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----
"""

[storage]
name = "local-storage-1"
token = "dev-token"
central_endpoint_id = "central-endpoint-id"
central_addr = "1.2.3.4:7800"
bind_port = 3341

[storage.relay]
url = "https://1.2.3.4:7801"
token = "relay-token"
quic_port = 7802
ca_cert = """
-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----
"""

[[storage.volumes]]
path = ".local/volume-1"
name = "local-volume-1"
```

---

### 4. Run a storage node

```bash
fs0 storage run --config .local/fs0.local.toml
```

---

### 5. Use the client

List files:

```bash
fs0 --config .local/fs0.local.toml ls /
```

Upload a file:

```bash
echo "hello fs0" > hello.txt
fs0 --config .local/fs0.local.toml put /hello.txt hello.txt
```

Print a remote file:

```bash
fs0 --config .local/fs0.local.toml cat /hello.txt
```

Append to a file:

```bash
echo "more data" > more.txt
fs0 --config .local/fs0.local.toml append /hello.txt more.txt
```

Download a file:

```bash
fs0 --config .local/fs0.local.toml get /hello.txt hello.downloaded.txt
```

Delete a file:

```bash
fs0 --config .local/fs0.local.toml rm /hello.txt
```

---

## Command Line Usage

```bash
fs0 [OPTIONS] <COMMAND>
```

### Global options

| Option | Description |
|---|---|
| `--config <PATH>` | Path to the fs0 config file. Defaults to `~/.fs0rc`. |
| `--json` | Print JSON output where supported. |
| `--help` | Print help information. |
| `--version` | Print version information. |

---

## Commands

### `ls`

List files under a remote directory.

```bash
fs0 ls [DIR] [OPTIONS]
```

Examples:

```bash
fs0 --config .local/fs0.local.toml ls /
fs0 --config .local/fs0.local.toml ls /logs --limit 50
fs0 --config .local/fs0.local.toml --json ls /
```

Options:

| Option | Description | Default |
|---|---|---|
| `--limit <LIMIT>` | Maximum number of entries to return. | `100` |
| `--cursor <CURSOR>` | Continue listing from a cursor. | none |

---

### `cat`

Print a remote file to stdout.

```bash
fs0 cat <REMOTE_PATH> [OPTIONS]
```

Examples:

```bash
fs0 --config .local/fs0.local.toml cat /hello.txt
fs0 --config .local/fs0.local.toml cat /hello.txt --offset 1024
fs0 --config .local/fs0.local.toml cat /hello.txt --offset 0 --len 4096
```

Options:

| Option | Description | Default |
|---|---|---|
| `--offset <OFFSET>` | Start reading from byte offset. | `0` |
| `--len <LEN>` | Maximum number of bytes to read. | full file |

---

### `get`

Download a remote file.

```bash
fs0 get <REMOTE_PATH> [LOCAL_PATH] [OPTIONS]
```

Examples:

```bash
fs0 --config .local/fs0.local.toml get /hello.txt
fs0 --config .local/fs0.local.toml get /hello.txt ./hello.txt
fs0 --config .local/fs0.local.toml get /hello.txt - --offset 0 --len 1024
```

If `LOCAL_PATH` is omitted, fs0 uses the file name from the remote path.

Use `-` as `LOCAL_PATH` to write to stdout.

Options:

| Option | Description | Default |
|---|---|---|
| `--offset <OFFSET>` | Start reading from byte offset. | `0` |
| `--len <LEN>` | Maximum number of bytes to read. | full file |

---

### `put`

Upload a local file as a new remote file.

```bash
fs0 put <REMOTE_PATH> <LOCAL_PATH> [OPTIONS]
```

Examples:

```bash
fs0 --config .local/fs0.local.toml put /hello.txt ./hello.txt
fs0 --config .local/fs0.local.toml put /logs/app.log ./app.log --prefer-volume local-volume
fs0 --config .local/fs0.local.toml put /stdin.txt -
```

Use `-` as `LOCAL_PATH` to read from stdin.

Options:

| Option | Description |
|---|---|
| `--prefer-volume <NAME>` | Prefer writing to a specific volume name. |

---

### `append`

Append local data to an existing remote file.

```bash
fs0 append <REMOTE_PATH> <LOCAL_PATH> [OPTIONS]
```

Examples:

```bash
fs0 --config .local/fs0.local.toml append /hello.txt ./more.txt
fs0 --config .local/fs0.local.toml append /hello.txt ./replacement.txt --offset 1024
echo "line" | fs0 --config .local/fs0.local.toml append /hello.txt -
```

Use `-` as `LOCAL_PATH` to read from stdin.

Options:

| Option | Description |
|---|---|
| `--prefer-volume <NAME>` | Prefer writing to a specific volume name. |
| `--offset <BYTES>` | Rewrite from a specific logical offset. Defaults to the current remote file size. |

---

### `rm`

Delete a remote file.

```bash
fs0 rm <REMOTE_PATH>
```

Example:

```bash
fs0 --config .local/fs0.local.toml rm /hello.txt
```

---

### `peers`

Show known storage peers.

```bash
fs0 peers
```

Examples:

```bash
fs0 --config .local/fs0.local.toml peers
fs0 --config .local/fs0.local.toml --json peers
```

---

### `central run`

Run a central metadata server.

```bash
fs0 central run --config <PATH>
```

Example:

```bash
fs0 central run --config .local/fs0.local.toml
```

---

### `central status`

Show central server status.

```bash
fs0 central status
```

Examples:

```bash
fs0 --config .local/fs0.local.toml central status
fs0 --config .local/fs0.local.toml --json central status
```

---

### `storage run`

Run a storage node.

```bash
fs0 storage run --config <PATH>
```

Example:

```bash
fs0 storage run --config .local/fs0.local.toml
```

---

### `volume create`

Create a central-backed local fs0 volume.

```bash
fs0 volume create <PATH> --name <NAME> --max-bytes <SIZE>
```

Examples:

```bash
fs0 --config .local/fs0.local.toml volume create ./data/volume-1 --name local-volume-1 --max-bytes 10G
fs0 --config .local/fs0.local.toml volume create ./data/volume-2 --name local-volume-2 --max-bytes 500M
```

Supported size suffixes:

| Suffix | Meaning |
|---|---|
| `K` / `k` | KiB |
| `M` / `m` | MiB |
| `G` / `g` | GiB |
| `T` / `t` | TiB |

---

### `volume meta`

Inspect local volume metadata.

```bash
fs0 volume meta <PATH>
```

Example:

```bash
fs0 volume meta ./data/volume-1
```

---

## Example Workflow

```bash
# Build
cargo build --release -p fs0-cli

# Start central
target/release/fs0 central run --config .local/fs0.local.toml

# In another terminal, create a central-backed volume
target/release/fs0 --config .local/fs0.local.toml volume create .local/volume-1 --name local-volume-1 --max-bytes 10G

# Run storage node after preparing config
target/release/fs0 storage run --config .local/fs0.local.toml

# Upload and read files
target/release/fs0 --config .local/fs0.local.toml put /hello.txt ./hello.txt
target/release/fs0 --config .local/fs0.local.toml ls /
target/release/fs0 --config .local/fs0.local.toml cat /hello.txt
target/release/fs0 --config .local/fs0.local.toml get /hello.txt ./hello.downloaded.txt
```


## License

MIT
