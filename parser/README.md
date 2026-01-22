# Parser Service

Parses CML CSV files uploaded via SFTP and writes to the Postgres/TimescaleDB database.

## Features

- Auto-processes CSV files: cml_data_*.csv to cml_data table, cml_metadata_*.csv to cml_metadata table
- Ingests raw data even when metadata is missing (logs warnings for missing IDs)
- Archives successful files to archived/YYYY-MM-DD/, quarantines failures with .error.txt notes
- Plugin-style parsers for extensibility
- DB connection retry with exponential backoff
- Cross-device file move fallback (move to copy)

## Architecture

**Modules:**
- main.py — orchestration (wires registry, watcher, DB writer, file manager)
- parsers/ — CSV parsers and registry
- db_writer.py — database operations with batch inserts
- file_manager.py — archive/quarantine with safe moves
- file_watcher.py — filesystem monitoring (watchdog)

**Flow:** Upload > Detect > Parse > Write DB > Archive (or Quarantine on error)

## Quick Star## Quick Star## Quick Star## Quick Star## Quick Sta```## Quick Star## Quick Star## Qd p## Quick Star## Quick Star## Quick Star## Quick Star## Quick Sta```## Quick Star## word@database:54## Quick Star## Quick Star## Quick Star## Quick Star## Quick Sta```## Quick Star## Quick Star## Qdes## Quick Star## Quick Star## Quick Star## Quick Star## Quick Sta```## Quick| D## Quick Star## Quick Star## Quick Star## Quick Star## Quick Sta```## Quick Star## Quick Stba## Quick Star## Quick Star## Quick Sirect## Quick Star## Quick Star## Quick Star#IVED_DIR | Archive directory | /app/data/archived |
| PAR| PAR| PAR| NE_DIR | Quarantin| PAR| PAR| PAR| NE_DIR | Quarantin| PAR| PAR| PAR| NE_DIR | Quarantin| PARice | True |
| PROCESS_EXISTING_ON_STARTUP || PROCESS_EXISTING_ON_STARTUP || PROCESS_EXISTING_LEVEL | | PROCESS_EXISTING_ON_STARTUP || PROCESS_EXISTING_ON*Miss| PROCESS_EXISTING_ON_STARTUP || PROCESS_EXISTING_ON_a is missing; warnings logged with sample IDs
- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idemy - **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idhiv- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idempotency:** Metadata writes use- **Idet
