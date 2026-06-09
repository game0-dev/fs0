<p align="center">
  <img src="docs/fs0.png" alt="fs0" width="160">
</p>

<p align="center">
  Production-ready distributed storage written in Rust.
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
  <img src="https://img.shields.io/badge/status-ready-brightgreen.svg" alt="Status">
  <img src="https://img.shields.io/badge/rust-2024-orange.svg" alt="Rust 2024">
</p>

**fs0** is a distributed storage system with a central metadata service, storage nodes, local volumes, compressed chunks, content hashing, and a single command-line interface for operating the cluster and files.

## Features

- Upload, update, list, read, download, copy, move, and delete files
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

### Build from source

```bash
git clone https://github.com/game0-dev/fs0.git
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

## How to use

### Create a config

```bash
mkdir -p ~/.fs0 .local
```

Create `~/.fs0/config.toml`:

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

`fs0` reads this file by default. Use `--config <PATH>` only when you want to load a different config file.

### Start central

```bash
fs0 central run
```

The server prints a central endpoint. Use that endpoint in `central_endpoint_id`.

### Create a volume

```bash
mkdir -p .local/volume-1
fs0 storage create-volume .local/volume-1 \
  --name local-volume-1 \
  --max-bytes 10G
```

Inspect the volume:

```bash
fs0 storage inspect-volume .local/volume-1
```

### Run storage

```bash
fs0 storage run
```

### Upload and read files

```bash
echo "hello fs0" > hello.txt
fs0 ls /
fs0 put /hello.txt hello.txt
fs0 cat /hello.txt
fs0 get /hello.txt hello.downloaded.txt
```

### Update a file

```bash
echo "new data" > updated.txt
fs0 update /hello.txt updated.txt
fs0 update /hello.txt updated.txt --offset 1024
```

### Copy, move, and delete files

```bash
fs0 cp /hello.txt /copy.txt
fs0 mv /copy.txt /renamed.txt
fs0 rm /renamed.txt
```

### Inspect the cluster

```bash
fs0 peers
fs0 central status
fs0 changes
```

---

## Command-line options

This is the command layout. Run `fs0 -h` for concise help or `fs0 --help` for the full help text.

    Usage: fs0 [OPTIONS] <COMMAND>

    Options:
          --config <PATH>  Path to the fs0 config file; defaults to ~/.fs0/config.toml
          --json             Print JSON output where supported
      -h, --help             Print help
      -V, --version          Print version

    Commands:
      ls          List a remote directory
      stat        Show remote file metadata and read plan
      cat         Print remote file bytes to stdout
      get         Download a remote file
      put         Upload a remote file
      update      Update remote file data
      rm          Delete a remote file
      cp          Copy a remote file
      mv          Move or rename a remote file
      changes     List central file change events
      peers       Show known storage peers
      central     Run or inspect the central server
      storage     Run storage nodes and manage local volumes

### Top-level commands

| Command | Description |
|---|---|
| `fs0 ls` | List a remote directory. |
| `fs0 stat` | Show remote file metadata and read plan. |
| `fs0 cat` | Print remote file bytes to stdout. |
| `fs0 get` | Download a remote file. |
| `fs0 put` | Upload a remote file. |
| `fs0 update` | Update remote file data. |
| `fs0 rm` | Delete a remote file. |
| `fs0 cp` | Copy a remote file. |
| `fs0 mv` | Move or rename a remote file. |
| `fs0 changes` | List central file change events. |
| `fs0 peers` | Show known storage peers. |
| `fs0 central` | Run or inspect the central server. |
| `fs0 storage` | Run storage nodes and manage local volumes. |

### File and cluster commands

| Command | Arguments | Options | Defaults |
|---|---|---|---|
| `fs0 ls` | `[DIR]` | `--limit <N>`, `--cursor <CURSOR>` | `DIR=/`, `limit=100` |
| `fs0 stat` | `<REMOTE_PATH>` | none | none |
| `fs0 cat` | `<REMOTE_PATH>` | `--offset <BYTES>`, `--len <BYTES>` | `offset=0`, `len=full file` |
| `fs0 get` | `<REMOTE_PATH> [LOCAL_PATH]` | `--offset <BYTES>`, `--len <BYTES>` | `LOCAL_PATH=remote file name`, `offset=0`, `len=full file` |
| `fs0 put` | `<REMOTE_PATH> <LOCAL_PATH>` | `--prefer-volume <NAME>` | none |
| `fs0 update` | `<REMOTE_PATH> <LOCAL_PATH>` | `--prefer-volume <NAME>`, `--offset <BYTES>` | `offset=remote file size` |
| `fs0 rm` | `<REMOTE_PATH>` | none | none |
| `fs0 cp` | `<SOURCE_PATH> <TARGET_PATH>` | none | none |
| `fs0 mv` | `<SOURCE_PATH> <TARGET_PATH>` | none | none |
| `fs0 changes` | none | `--cursor <CURSOR>`, `--limit <N>` | `cursor=0`, `limit=100` |
| `fs0 peers` | none | none | none |

### Central commands

| Command | Arguments | Options | Defaults |
|---|---|---|---|
| `fs0 central run` | none | `--config <PATH>` | `~/.fs0/config.toml` |
| `fs0 central status` | none | none | none |

### Storage commands

| Command | Arguments | Options | Defaults |
|---|---|---|---|
| `fs0 storage run` | none | `--config <PATH>` | `~/.fs0/config.toml` |
| `fs0 storage create-volume` | `<PATH>` | `--config <PATH>`, `--name <NAME>`, `--max-bytes <SIZE>` | `config=~/.fs0/config.toml` |
| `fs0 storage inspect-volume` | `<PATH>` | none | none |

`central run`, `storage run`, and `storage create-volume` read `[central]` or `[storage]` from the same config file.

Supported size suffixes for `--max-bytes`: `K`, `M`, `G`, `T`.

## License

MIT
