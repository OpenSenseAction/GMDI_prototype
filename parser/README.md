# Parser Service

Parses CML CSV files uploaded via SFTP and writes to the Postgres/TimescaleDB database.

## Features

- Auto-processes CSV files: `cml_data_*.csv` → `cml_data` table, `cml_metadata_*.csv` → `cml_metadata` table
- Ingests raw data even when metadata is missing (logs warnings for missing IDs)
- Archives successful files to `archived/YYYY-MM-DD/`, quarantines failures with `.error.txt` notes
- Plugin-style parsers for extensibility
- DB connection retry with exponential backoff
- Cross-device file move fallback (move → copy)

## Architecture

**Modules:**
- `main.py` — orchestration (wires registry, watcher, DB writer, file manager)
- `parsers/` — CSV parsers and registry
- `db_writer.py` — database operations with batch inserts
- `file_manager.py` — archive/quarantine with safe moves
- `file_watcher.py` — filesystem monitoring (watchdog)

**Flow:** Upload → Detect → Parse → Write DB → Archive (or Quarantine on error)

## Quick Start

**Docker:**
```bash
docker-compose up parser
```

**Standalone:**
```bash
cd parser
pip install -r requirements.txt
export DATABASE_URL="postgresql://myuser:mypassword@database:5432/mydatabase"
python main.py
```

## Configuration

Environment variables (defaults in parentheses):

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | Postgres connection string | `postgresql://myuser:mypassword@database:5432/mydatabase` |
| `PARSER_INCOMING_DIR` | Watch directory | `/app/data/incoming` |
| `PARSER_ARCHIVED_DIR` | Archive directory | `/app/data/archived` |
| `PARSER_QUARANTINE_DIR` | Quarantine directory | `/app/data/quarantine` |
| `PARSER_ENABLED` | Enable/disable service | `True` |
| `PROCESS_EXISTING_ON_STARTUP` | Process existing files at startup | `True` |
| `LOG_LEVEL` | Logging verbosity | `INFO` |

## Behavior Details

- **Missing metadata:** Raw data is written even when metadata is missing; warnings logged with sample IDs
- **Idempotency:** Metadata writes use `ON CONFLICT DO UPDATE`; safe to reprocess files
- **File moves:** Attempts move, falls back to copy for cross-device mounts
- **DB retry:** 3 connection attempts with exponential backoff
- **Extensibility:** Add parsers by implementing `BaseParser` and registering in `parser_registry.py`

## Testing

```bash
pytest parser/tests/ -v
```

29 unit tests covering parsers, file management, DB operations (mocked), and registry. DBWriter tests auto-skip if `psycopg2` unavailable.

## Troubleshooting

**Check logs:** Sent to stdout; use `LOG_LEVEL=DEBUG` for detail.

**Files not processing:**
- Verify incoming directory mount and permissions
- Check quarantine dir for `.error.txt` notes
- Review logs for DB connection or parse errors

**Archived files:** `/app/data/archived/YYYY-MM-DD/filename.csv`  
**Quarantine notes:** `/app/data/quarantine/filename.csv.error.txt`
