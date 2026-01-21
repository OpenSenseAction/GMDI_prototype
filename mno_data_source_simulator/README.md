# MNO Data Source Simulator

Simulates a Mobile Network Operator (MNO) data source by reading CML data from NetCDF files, generating fake real-time data with adjusted timestamps, and uploading via SFTP.

## Features

- Loops through historical NetCDF data with configurable cycle duration
- Writes timestamped CSV files to local directory for inspection
- Uploads files to SFTP server at configurable intervals
- Modular design: separate data generation and upload components

## Architecture

**Modules:**
- `data_generator.py` - Reads NetCDF, generates CSV files with current timestamps
- `sftp_uploader.py` - Uploads files via SFTP, archives after successful upload
- `main.py` - Orchestrates generation and upload

**Data Flow:**
1. Generate CSV → `data_to_upload/`
2. Upload to SFTP server
3. Move to `data_uploaded/` archive

Benefits: Local inspection, resilient to upload failures, manual upload capability.

## Quick Start

**Docker:**
```bash
docker-compose up mno_simulator
```

**Standalone:**
```bash
pip install -r requirements.txt
export SFTP_PASSWORD=your_password
python main.py
```

## Configuration

Edit `config.yml`:
```yaml
data_source:
  loop_duration_seconds: 3600  # How fast to replay historical data

generator:
  generation_frequency_seconds: 60  # How often to generate files

sftp:
  enabled: true
  upload_frequency_seconds: 60  # How often to upload
```

Set `SFTP_PASSWORD` environment variable for SFTP credentials.

## Inspecting Data

```bash
# View generated files
ls data_to_upload/

# With Docker
docker-compose exec mno_simulator ls /app/data_to_upload/
```

## Testing

See `TESTING.md` for complete testing strategy.

```bash
# Unit tests (fast)
pytest tests/ -v -m "not integration"

# All tests including integration (requires Docker)
pytest tests/ -v
```
