# SFTP Fetcher

Active SFTP polling service that connects to external SFTP servers and downloads new files to the shared `incoming/` directory.

## Overview

The `sftp_fetcher` is designed for scenarios where you need to **pull** data from external MNO SFTP servers (as opposed to having them push to your `sftp_receiver`).

### Key Features

- **Continuous polling**: Runs indefinitely, checking for new files at configurable intervals
- **Stateful tracking**: Remembers which files have been downloaded (via state file) to avoid duplicates
- **Atomic writes**: Uses atomic file operations to prevent partial writes
- **Flexible authentication**: Supports SSH key or password authentication
- **Post-download cleanup**: Can leave, delete, or move files on the remote server after download

## Architecture

```
External SFTP Server
  sftp.operator.com
  /outgoing/cml/*.csv
         │
         │ (poll every N seconds)
         ▼
  sftp_fetcher service
  ┌─────────────────────────┐
  │ - Connect via paramiko  │
  │ - List remote files     │
  │ - Check state file      │
  │ - Download new files    │
  │ - Write to incoming/    │
  │ - Update state          │
  │ - Cleanup (optional)    │
  └─────────────────────────┘
         │
         │ atomic write
         ▼
  /data/incoming/  ← parser watches this directory
```

## Configuration

### Config File (`config.yml`)

```yaml
sources:
  - name: operator_x
    host: sftp.operator-x.example
    port: 22
    username: gmdi_pull
    private_key_env: OPERATOR_X_SSH_KEY   # Env var containing path to private key
    # password_env: OPERATOR_X_PASSWORD   # Alternative: use password instead
    remote_path: /outgoing/cml
    poll_interval_seconds: 60
    file_glob: "*.csv"
    after_download: leave   # Options: leave | delete | move
```

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `OPERATOR_X_SSH_KEY` | Path to SSH private key file | `/app/ssh_keys/operator_x_key` |
| `OPERATOR_X_PASSWORD` | Password for authentication (if not using key) | `secret123` |
| `STATE_DIR` | Directory for state file persistence | `/app/state` |
| `INCOMING_DIR` | Directory where downloaded files are written | `/app/incoming` |
| `LOG_LEVEL` | Logging verbosity | `INFO`, `DEBUG`, `WARNING` |

### Authentication

**SSH Key (Recommended):**
```bash
# Generate key pair
ssh-keygen -t rsa -b 4096 -f ~/.ssh/operator_x_key

# Share public key with MNO
cat ~/.ssh/operator_x_key.pub

# Set environment variable in docker-compose.yml
environment:
  - OPERATOR_X_SSH_KEY=/app/ssh_keys/operator_x_key
```

**Password:**
```yaml
# In config.yml
password_env: OPERATOR_X_PASSWORD
```
```yaml
# In docker-compose.yml
environment:
  - OPERATOR_X_PASSWORD=your_password
```

### Post-Download Behavior (`after_download`)

| Value | Behavior | Use Case |
|-------|----------|----------|
| `leave` (default) | Files remain on remote server | Safest option; no write permission needed |
| `delete` | Delete file after successful download | When remote storage is limited; no audit trail |
| `move` | Move to subdirectory (default: `"done"`) | **Recommended**: Keeps source clean, preserves audit trail |

**Configuration:**

```yaml
sources:
  - name: operator_x
    # ... other config ...
    
    # What to do after download
    after_download: move  # Options: "leave" | "delete" | "move"
    
    # Only used when after_download="move":
    archive_subdir: "done"  # String: subdirectory name (default: "done")
```

**Example with write permissions:**

```yaml
after_download: move
archive_subdir: "archived"  # Moves to /outgoing/cml/archived/
```

This moves downloaded files to `/outgoing/cml/done/`, preventing the listing from slowing down over time.

## Usage

### Docker Compose

Add to your `docker-compose.yml`:

```yaml
services:
  sftp_fetcher:
    build:
      context: ..
      dockerfile: fetchers/sftp_fetcher/Dockerfile
    volumes:
      - ./fetchers/sftp_fetcher/config.yml:/app/config.yml:ro
      - ./ssh_keys:/app/ssh_keys:ro
      - sftp_fetcher_state:/app/state
      - sftp_fetcher_incoming:/app/incoming
    environment:
      - OPERATOR_X_SSH_KEY=/app/ssh_keys/operator_x_key
      - LOG_LEVEL=INFO
    restart: unless-stopped

volumes:
  sftp_fetcher_state:
  sftp_fetcher_incoming:
```

### Command Line

```bash
python -m fetchers.sftp_fetcher.main \
  --config config.yml \
  --source operator_x
```

## State Management

The fetcher maintains a JSON state file (`{source_name}_state.json`) that tracks:

- Filename and modification time of each downloaded file
- SHA256 content hash for robust deduplication
- File size

This prevents re-downloading already-processed files even after container restarts or if files are modified on the remote server.

**State file location:** `/app/state/{source_name}_state.json`

### Deduplication Strategy

The fetcher uses a **two-key system** for robust tracking:

1. **`(filename, mtime)` key** - Fast check for unchanged files
2. **`(filename, content_hash)` key** - Detects if file content is identical despite different mtime

This handles edge cases:
- **File recreated with same name**: Hash prevents duplicate download
- **File modified but content unchanged**: Hash detects no actual change
- **State file lost**: Will re-download, but won't create duplicates in normal operation

### Backlog Handling

When running continuously without cleanup permissions (`after_download: leave`):

- The remote file list will grow over time
- The fetcher only downloads files it hasn't seen before (based on state)
- State file is automatically pruned to keep last N entries (default: 5000)

**Environment variable:** `MAX_STATE_ENTRIES` - Maximum state entries to retain (default: 5000)

If you need to handle a large backlog after an outage:
1. Start the fetcher - it will download all unseen files
2. Once caught up, it resumes normal polling
3. State pruning ensures memory doesn't grow unbounded

**Automatic pruning:** State file is automatically pruned to keep only the most recent entries.

## Integration with Parser

Files downloaded by `sftp_fetcher` are written directly to the `incoming/` directory, which is monitored by the `parser` service. No additional configuration is needed — the parser will automatically pick up and process new files.

### Volume Sharing

To share the `incoming/` directory with a parser:

```yaml
services:
  sftp_fetcher:
    volumes:
      - shared_incoming:/app/incoming

  parser_operator_x:
    volumes:
      - shared_incoming:/app/data/incoming

volumes:
  shared_incoming:
```

## Error Handling

- **Connection failures**: Exponential backoff (max 5 minutes between retries)
- **Missing remote directory**: Logged as error, retry on next poll
- **Download failures**: File skipped, will retry on next poll
- **Write failures**: Atomic write ensures no partial files; retry on next poll

## Monitoring

Check logs for download activity:

```bash
docker compose logs -f sftp_fetcher
```

Key log messages:
- `Connecting to SFTP user@host:port` - Establishing connection
- `SFTP connection established` - Authentication successful
- `Found N remote files matching '*.csv'` - Files detected on remote server
- `Skipping already-seen file: filename.csv` - File already processed
- `Wrote filename.csv (N bytes)` - New file downloaded successfully
- `Deleting remote file filename.csv` - Post-download cleanup (if configured)
- `Moving /path/file.csv to /path/done/file.csv` - Moving to archive subdirectory
