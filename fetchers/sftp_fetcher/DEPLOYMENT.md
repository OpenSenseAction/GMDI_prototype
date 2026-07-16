# SFTP Fetcher Deployment Guide

This guide explains how to configure and deploy the `sftp_fetcher` service for pulling data from external MNO SFTP servers.

## Prerequisites

- SSH access credentials to the external SFTP server (private key or password)
- Remote path on the SFTP server where files are located
- Docker Compose environment

## Step-by-Step Setup

### 1. Prepare SSH Credentials

**Option A: SSH Key Authentication (Recommended)**

```bash
# Generate a dedicated SSH key pair for this MNO
ssh-keygen -t rsa -b 4096 -f ~/.ssh/gmdi_mno_name_key

# Send the public key to the MNO
cat ~/.ssh/gmdi_mno_name_key.pub

# They should add it to their authorized_keys
```

**Option B: Password Authentication**

If the MNO provides a password instead of SSH key, you'll use password-based authentication (less secure).

### 2. Store Secrets Securely

Create or update your `.env` file (gitignored):

```bash
# For SSH key auth
MNO_NAME_SSH_KEY=/app/ssh_keys/gmdi_mno_name_key

# For password auth (alternative)
MNO_NAME_PASSWORD=your_secure_password_here
```

### 3. Configure the Fetcher

Edit `fetchers/sftp_fetcher/config.yml`:

```yaml
sources:
  - name: mno_name
    host: sftp.mno-example.com
    port: 22
    username: gmdi_pull
    private_key_env: MNO_NAME_SSH_KEY  # References env var
    # password_env: MNO_NAME_PASSWORD  # Alternative to private_key_env
    remote_path: /outgoing/cml
    poll_interval_seconds: 60
    file_glob: "*.csv"
    
    # Recommended: If you have write permissions, move files after download
    after_download: move
    archive_subdir: done  # Files will be moved to /outgoing/cml/done/
```

**Tip:** Using `after_download: move` keeps the source directory clean and prevents performance degradation from accumulating files.

### 4. Add Service to Docker Compose

In your `docker-compose.override.yml` or `docker-compose.yml`:

```yaml
services:
  sftp_fetcher:
    build:
      context: .
      dockerfile: fetchers/sftp_fetcher/Dockerfile
    volumes:
      - ./fetchers/sftp_fetcher/config.yml:/app/config.yml:ro
      - ./ssh_keys:/app/ssh_keys:ro  # Mount SSH keys directory
      - sftp_fetcher_state:/app/state
      - shared_incoming:/app/incoming
    environment:
      - MNO_NAME_SSH_KEY=/app/ssh_keys/gmdi_mno_name_key
      - LOG_LEVEL=INFO
    restart: unless-stopped

volumes:
  sftp_fetcher_state:
  shared_incoming:
```

### 5. Share Incoming Directory with Parser

The parser needs to watch the same `incoming/` directory:

```yaml
services:
  parser_mno_name:
    build: ./parser
    environment:
      - DATABASE_URL=postgresql://mno_name:mno_password@database:5432/mydatabase
      - USER_ID=mno_name
      - PARSER_INCOMING_DIR=/app/data/incoming
      - PARSER_ARCHIVED_DIR=/app/data/archived
      - PARSER_QUARANTINE_DIR=/app/data/quarantine
      - PARSER_ENABLED=true
      - PROCESS_EXISTING_ON_STARTUP=true
    volumes:
      - shared_incoming:/app/data/incoming
      - parser_mno_name_archived:/app/data/archived
      - parser_mno_name_quarantine:/app/data/quarantine
    restart: unless-stopped

volumes:
  shared_incoming:
  parser_mno_name_archived:
  parser_mno_name_quarantine:
```

**Note:** Database roles and migrations are managed separately via the user onboarding workflow (`users.yml` → `generate_config.py`). See the [Multi-User Architecture](../../docs/multi-user-architecture.md) documentation for details.

### 6. Deploy

```bash
docker compose up -d sftp_fetcher parser_mno_name
```

### 7. Monitor

Check logs:

```bash
# Watch fetcher logs
docker compose logs -f sftp_fetcher

# Watch parser logs
docker compose logs -f parser_mno_name
```

Expected log messages:
- `Connecting to SFTP user@host:port`
- `SFTP connection established`
- `Found N remote files matching '*.csv'`
- `Wrote filename.csv (N bytes)`

## Troubleshooting

### Connection Refused

- Check firewall rules allow outbound connections to the MNO's SFTP server
- Verify the hostname and port are correct
- Test manually: `sftp -i path/to/key user@host`

### Permission Denied

- Verify SSH key is correctly mounted and readable
- Check that the public key was added to the MNO's authorized_keys
- For password auth, verify the password is correct

### No Files Found

- Check the `remote_path` exists on the SFTP server
- Verify the `file_glob` pattern matches the files (e.g., `*.csv`)
- Manually list files: `sftp -i key user@host:ls /remote/path`

### State File Issues

If the fetcher re-downloads already-processed files:

- Check that the `sftp_fetcher_state` volume persists across restarts
- Verify state file location: `docker compose exec sftp_fetcher ls -la /app/state/`

## Configuration Reference

### Config File Options

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | Yes | - | Unique identifier for this source |
| `host` | Yes | - | SFTP server hostname |
| `port` | No | `22` | SFTP server port |
| `username` | Yes | - | Username for authentication |
| `private_key_env` | Conditional | - | Env var containing path to private key |
| `password_env` | Conditional | - | Env var containing password (alt. to key) |
| `remote_path` | Yes | - | Remote directory to poll |
| `poll_interval_seconds` | No | `60` | Seconds between polling attempts |
| `file_glob` | No | `*` | Glob pattern for files to download |
| `after_download` | No | `leave` | What to do after download: `leave`, `delete`, `move` |

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `{SOURCE}_SSH_KEY` | Path to SSH private key | `/app/ssh_keys/mno_key` |
| `{SOURCE}_PASSWORD` | Password for auth | `secret123` |
| `STATE_DIR` | State file directory | `/app/state` |
| `INCOMING_DIR` | Incoming files directory | `/app/incoming` |
| `LOG_LEVEL` | Logging verbosity | `INFO`, `DEBUG` |

## Security Considerations

1. **Never commit secrets**: SSH keys and passwords must be in `.env` (gitignored)
2. **Use SSH keys**: Prefer key-based authentication over passwords
3. **Restrict permissions**: The MNO should restrict your SSH key to read-only access
4. **Host key verification**: In production, consider adding known_hosts verification
5. **Network isolation**: Ensure the fetcher can only reach necessary hosts
