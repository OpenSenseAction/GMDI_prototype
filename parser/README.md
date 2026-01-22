````markdown
# Parser Service

Parses CML CSV files uploaded via SFTP and writes results into the Postgres/TimescaleDB schema used by this project.

This document mirrors the concise style used in `mno_data_source_simulator/README.md` and explains what the parser does, how it is organised, and how to run and test it.

## Features

- Watches an incoming directory for uploaded files (CSV, NetCDF placeholder) and processes them automatically
- Plugin-style parsers: raw time series (`cml_data_*.csv`) and metadata (`cml_metadata_*.csv`)
- Writes metadata to `cml_metadata` and timeseries to `cml_data` (idempotent metadata upserts)
- Always ingests raw data files even when metadata is missing; missing metadata IDs are logged as warnings
- Archives processed files under `{ARCHIVED_DIR}/YYYY-MM-DD/` and moves parsing failures to a quarantine directory with `.error.txt` notes
- Robust file moves with cross-device fallback (move → copy)
- Database connection retry with exponential backoff at startup and on-demand
- Environment-driven configuration for paths, DB URL and behaviour

## Architecture

**Modules:**

- `main.py` — service entrypoint and orchestration (wires up registry, watcher, DB writer, file manager)
- `parsers/` — parser implementations (BaseParser, `csv_rawdata_parser.py`, `csv_metadata_parser.py`, `parser_registry.py`)
- `db_writer.py` — database helper for writes and validation
- `file_manager.py` — archive/quarantine helpers with safe move logic
- `file_watcher.py` — filesystem watcher (uses `watchdog`) and stabilization logic

**Data flow:**
1. File is uploaded to the incoming directory (SFTP service)
2. `FileWatcher` detects the file and waits briefly for the upload to finish
3. `ParserRegistry` chooses the appropriate parser
4. Parser returns a pandas DataFrame (or parse error)
5. `DBWriter` writes metadata or raw data (raw data is written regardless of metadata presence)
6. On success the file is archived; on failure it is quarantined and an `.error.txt` file is written

Benefits: small, testable components; plugin-style parsers for future formats; resilient file handling for containerized deployments.

## Quick Start

**Docker (recommended with the provided compose stack):**

```bash
# Start the compose stack (database + sftp + parser + other services)
docker-compose up parser
```

Service name may vary by your `docker-compose.yml`; the repository's compose file includes a `parser` service in this prototype.

**Standalone:**

```bash
cd parser
pip install -r requirements.txt
# Configure env vars as needed, then run
export DATABASE_URL="postgresql://myuser:mypassword@database:5432/mydatabase"
python main.py
```

The service will create the configured incoming/archived/quarantine directories if they do not exist.

## Configuration

All configuration is provided via environment variables. Defaults are useful for local development.

- `DATABASE_URL` — Postgres/TimescaleDB connection string (default: `postgresql://myuser:mypassword@database:5432/mydatabase`)
- `PARSER_INCOMING_DIR` — incoming directory to watch (default: `/app/data/incoming`)
- `PARSER_ARCHIVED_DIR` — archive directory root (default: `/app/data/archived`)
- `PARSER_QUARANTINE_DIR` — quarantine directory (default: `/app/data/quarantine`)
- `PARSER_ENABLED` — `1|true|yes` to enable the service (default: true)
- `PROCESS_EXISTING_ON_STARTUP` — process files already in the incoming directory at startup (default: true)
- `LOG_LEVEL` — logging level (default: `INFO`)

Example (Docker Compose environment block):

```yaml
services:
  parser:
    image: parser:latest
    environment:
      - DATABASE_URL=postgresql://myuser:mypassword@database:5432/mydatabase
      - PARSER_INCOMING_DIR=/app/data/incoming
      - PARSER_ARCHIVED_DIR=/app/data/archived
      - PARSER_QUARANTINE_DIR=/app/data/quarantine
      - LOG_LEVEL=INFO
    volumes:
      - sftp_uploads:/app/data/incoming
      - parser_archived:/app/data/archived
      - parser_quarantine:/app/data/quarantine
```

## Behavior Notes & Edge Cases

- Raw data ingestion: Raw CSV files matching `cml_data_*.csv` are parsed and written to `cml_data` even if corresponding `cml_metadata` entries are missing. The parser calls `DBWriter.validate_rawdata_references()` and logs a warning with a sample of missing CML IDs for operator attention.

- Atomicity: Writes use `psycopg2.extras.execute_values` for batched inserts and transactions; metadata writes use `ON CONFLICT (cml_id) DO UPDATE` to be idempotent.

- File moves: The `FileManager` attempts `shutil.move()` but will fall back to `shutil.copy2()` for cross-device or read-only mount situations. If both fail during quarantine, an orphan note is created.

- Timezones: Quarantine `.error.txt` notes use timezone-aware UTC timestamps.

- DB resilience: `DBWriter` will retry connections a limited number of times with exponential backoff to tolerate DB startup delays.

- Parser extensibility: Add new parser classes by implementing `BaseParser` and registering them in `parsers/parser_registry.py`.

## Testing

Unit tests live next to the package in `parser/tests/`.

```bash
# From repository root
# Run parser unit tests
pytest parser/tests/ -q
```

Notes:
- `DBWriter` unit tests require `psycopg2` to be importable. If `psycopg2` is not installed, those tests are automatically skipped.
- The test suite includes mocks for database operations; integration tests against a running Postgres container are not included here but can be added under `parser/tests/integration/`.

## Logs & Troubleshooting

- Logs are sent to stdout. Set `LOG_LEVEL=DEBUG` for more verbosity.
- If files are not processed check:
  - Incoming directory mount and permissions
  - Parser service logs for parse errors or DB connection errors
  - Quarantine directory for `.error.txt` notes

## Extending the Parser

- Add a new parser: implement `parsers/base_parser.py` interface, add file pattern and parse logic, then register the parser in `parsers/parser_registry.py`.
- Consider adding a `file_processing_log` table or a health HTTP endpoint for production monitoring.

## Inspecting Processed Files

- Archive location example: `/app/data/archived/2026-01-22/cml_data_20260122.csv`
- Quarantine note example: `/app/data/quarantine/cml_data_20260122.csv.error.txt` contains timestamp and error message.

## See also

- `parsers/` — parser implementations
- `db_writer.py` — database write logic
- `file_manager.py` — archive and quarantine helpers
- `file_watcher.py` — incoming file monitoring
- `parser/tests/` — unit tests covering parsers, FileManager, DBWriter (mocked), and registry

````