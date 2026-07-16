# Data Fetchers

Two new services to pull data from external sources and feed it into the existing
ingestion pipeline via the shared `data/incoming/` volume.

---

## All ingress paths in one view

The system has four ways data can enter the pipeline. It is worth naming them
together to see where the new fetchers fit:

```
 PUSH (passive — external party initiates)
 ─────────────────────────────────────────────────────────────────────
  MNO via SFTP     →  sftp_receiver (port 2222)  →  incoming/
  User via browser →  webserver /api/upload       →  incoming/

 PULL (active — we initiate, scheduled)
 ─────────────────────────────────────────────────────────────────────
  External SFTP    →  sftp_fetcher  (new)  ─┐
  REST API         →  api_fetcher   (new)  ─┤→  incoming/

                                             ↓
                                          parser  →  TimescaleDB
```

### The incoming SFTP server is infrastructure, not a fetcher

`sftp_receiver` is a passive server (atmoz/sftp image) that MNOs are configured
to push files to. It does not contain polling logic or scheduling. It will stay
as-is; there is no reason to refactor it into a fetcher.

### The web upload is part of the webserver, not a fetcher

`/api/upload` (the drag-and-drop endpoint) is already implemented and functional —
it writes files directly to `incoming/` behind `@login_required`. It makes sense
to keep it in the webserver because it is tightly coupled to the user session and
the browser UI. It is not currently promoted as a primary workflow (MNOs use SFTP
instead), but it remains useful for ad-hoc uploads by administrators.

If it eventually needs to be a first-class workflow, the work is in the UI
(`data_uploads.html`) and access control, not in refactoring it into a separate
service.

**The new `sftp_fetcher` and `api_fetcher` are fetchers** because they contain
active scheduling, state management, and retry logic — none of which belong in a
webserver or a passive SSH daemon.

### Why fetchers write directly to the shared volume, not through the SFTP server

The SFTP server is an ingress point for external parties _pushing_ data in.
For data we actively pull, writing directly to the shared volume avoids an
unnecessary SSH round-trip and extra per-user account management.

---

## Directory layout

```
fetchers/
  shared/                     # Python package shared by both fetchers
    __init__.py
    config.py                 # load_config() with env-var substitution for secrets
    incoming_writer.py        # atomic_write(filename, content) → incoming/
    state.py                  # Persistent state (seen files / last cursor) via JSON file
    polling.py                # run_poll_loop(interval_s, callback) with exponential backoff
  sftp_fetcher/
    fetcher.py                # SFTPFetcher: list remote dir, download new files
    main.py
    config.yml
    Dockerfile
    requirements.txt
  api_fetcher/
    fetcher.py                # APIFetcher: GET endpoint, handle pagination/cursors
    main.py
    config.yml
    Dockerfile
    requirements.txt
```

Both Dockerfiles copy the `fetchers/shared/` package and add it to `PYTHONPATH`.

---

## Shared components (`fetchers/shared/`)

### `config.py`
Loads a YAML config file (same pattern as `mno_data_source_simulator`).
Secrets (passwords, API keys) are never stored in YAML; config values may reference
`$ENV_VAR_NAME` and the loader substitutes them from the environment.

### `incoming_writer.py`
Writes a file atomically to the `incoming/` directory:
1. Write to a `.tmp` file in the same directory.
2. `os.replace()` to final filename.

This prevents the parser's `file_watcher` from picking up a partial write
(the watcher already has a stability check, but atomic writes are a safer
belt-and-suspenders approach).

### `state.py`
Persists fetcher state to a JSON file on the same volume so that a container
restart does not re-fetch everything:
- **SFTP**: set of seen `(filename, mtime)` pairs per remote path.
- **API**: per-endpoint cursor — either a UTC timestamp (`last_fetched_until`) or
  an opaque pagination token, depending on what the API supports. In continuous
  mode this cursor is what determines the `since` parameter of the next request;
  in backfill mode it tracks progress through the requested window.

### `polling.py`
A simple `while True` loop that calls a user-supplied `poll()` callback,
catches exceptions, logs them, and backs off exponentially (max ~5 min) before
retrying. On success it sleeps for the configured `poll_interval_seconds`.

---

## `sftp_fetcher`

Connects to one or more external SFTP servers, lists a configured remote path,
downloads files not yet seen (checked via `state.py`), and writes them to
`incoming/` via `incoming_writer.py`.

Key config fields per source:
```yaml
sources:
  - name: operator_x
    host: sftp.operator-x.example
    port: 22
    username: gmdi_pull
    private_key_env: OPERATOR_X_SSH_KEY   # path to key file, from env
    remote_path: /outgoing/cml
    poll_interval_seconds: 60
    file_glob: "*.csv"
    after_download: leave   # leave | delete | move (see below)
```

Library: `paramiko` (already used by `mno_data_source_simulator`).

### Source cleanup (`after_download`)

What to do with a file on the source SFTP after a successful download is
not yet decided. Three options:

| Value | Behaviour | Requirement |
|---|---|---|
| `leave` | Nothing — file stays on source indefinitely | None (default, safest) |
| `delete` | Remove file from source immediately after download | Write permission on source |
| `move` | Rename to a `done/` subfolder on the source | Write permission on source |

`leave` is the safest default: the MNO manages their own SFTP retention and we
never need write access. The `state.py` seen-file set is what prevents
re-downloading already-processed files.

### Why not rsync?

Rsync is attractive because it handles "what's new" naturally by comparing
source and destination trees. The problem is that the *destination* here is
`incoming/`, which is **transient** — the parser moves files to `archived/`
after processing. Rsync would see those files are gone locally and re-download
them on the next run.

A workaround is to rsync into a **persistent local mirror** directory
(`fetchers/sftp_fetcher/mirror/<source_name>/`) and then copy only new files
from the mirror to `incoming/`. This works, but:
- doubles local storage,
- requires two-step logic (rsync + copy),
- adds an `rsync` binary dependency to the image.

The `state.py` approach achieves the same result with less complexity: the
state file *is* the mirror index, persisted independently of the filesystem.
Rsync to a persistent mirror remains a valid alternative if the state file
feels fragile or if diff-based resume of partial downloads is needed.

---

## `api_fetcher`

Polls one or more REST endpoints and writes the raw JSON response to `incoming/`
as a `.json` file. No conversion is done in the fetcher — the raw payload is
preserved so no information is lost. The parser is responsible for interpreting
the JSON (see [Parser changes](#parser-changes-for-json-support) below).

### Operating modes

**Continuous** (default): fetches recent data on a repeating schedule. On each
poll, the `since` parameter is set to the `last_fetched_until` cursor stored by
`state.py`. After a successful response the cursor is advanced to
`now - overlap_seconds` (a small lookback guards against late-arriving data).
The fetcher runs indefinitely until stopped.

**Backfill**: fetches a fixed historical window once, then exits (or optionally
switches to continuous mode afterwards). Useful for initial ingestion of
historical data. Progress through the window is checkpointed in `state.py` so
a crash can resume from where it left off rather than restarting the whole
period. The window is divided into chunks of `backfill_chunk_hours` to avoid
requests that are too large.

Both modes use the same cursor mechanism in `state.py`; the difference is
whether the end of the window is `now` (continuous) or a fixed timestamp
(backfill).

### What is and isn't standardized

**Auth** (`/login/`, `/refresh/`) is standardized — implemented once in
`fetchers/shared/` and reused for every JWT-based source.

**The query interface is not standardized** — parameter names, date formats,
pagination style, and source-specific filters all vary per API. These are
entirely driven by config, so no code changes are needed when adding a new
source. The config handles:

| Concern | Config key | Example values |
|---|---|---|
| Window param names | `params:` keys | `date_from`/`date_to` vs `since`/`until` |
| Date/time format | `window_format` | `YYYY-MM-DD` vs `YYYY-MM-DDTHH:MM:SSZ` |
| Pagination style | `pagination.type` | `page` (number+size) vs `cursor` vs `offset` |
| Extra fixed params | `params:` literal values | `performance_event: RSL` |
| Multiple fetches per window | `param_variants` | separate RSL and TSL requests |

### `param_variants` — multiple requests per window

Some APIs require separate requests for each measurement type (e.g.
`performance_event: RSL` and `performance_event: TSL`). The `param_variants`
list causes the fetcher to issue one request per variant per window chunk and
write each result to a separate file:

```yaml
sources:
  - name: operator_y
    url: https://api.operator-y.example/cml/
    auth:
      type: jwt
      login_url: https://api.operator-y.example/login/
      refresh_url: https://api.operator-y.example/refresh/
      username_env: OPERATOR_Y_USERNAME
      password_env: OPERATOR_Y_PASSWORD
    mode: continuous         # continuous | backfill
    poll_interval_seconds: 300
    overlap_seconds: 86400   # date-only resolution → 1-day lookback
    backfill_start: "2025-01-01"
    backfill_chunk_hours: 24
    window_format: "YYYY-MM-DD"   # date-only, no time component
    params:
      date_from: "{window_start}"
      date_to: "{window_end}"
      page_size: 1000
    pagination:
      type: page             # page | cursor | offset | none
      page_param: page
      size_param: page_size
    param_variants:          # one file written per variant per window
      - suffix: rsl
        params:
          performance_event: RSL
      - suffix: tsl
        params:
          performance_event: TSL
    output_filename_pattern: "{source_name}_{window_start}_{window_end}_{variant_suffix}_data.json"
```

The fetcher loops over variants × pages. Results for each variant are
collected into a single JSON file per window chunk before being written to
`incoming/`.

### JWT auth flow

The `jwt` auth type implements the standard refresh-token pattern:

1. `POST /login/` `{username, password}` → `{access, refresh}`
2. Add `Authorization: Bearer <access>` to every data request
3. On 401, `POST /refresh/` `{refresh}` → `{access}`, retry once
4. If refresh also fails, re-login from credentials

This pattern (used by DRF Simple JWT, FastAPI, and many others) is
implemented generically in `fetchers/shared/` — the only deployment-specific
config is `login_url`, `refresh_url`, and the credential env vars. No code
changes are needed when switching from the mock to a real API that follows
this flow.

The **development mock** (a small Flask or FastAPI app committed to this repo)
implements the same three endpoints with dummy tokens and synthetic JSON data.
This means the full fetcher → parser → DB pipeline can be exercised locally
without any real credentials.

Library: `httpx` (supports sync and async, has timeout/retry primitives).

---

## Parser changes for JSON support

The current parser dispatch in `service_logic.py` is hardcoded:
`"meta" in filename → parse_metadata_csv`, everything else →
`parse_rawdata_csv`. Adding JSON support is the natural first step of the
planned modular parser refactor.

**Proposed dispatch approach:** route by file extension rather than filename
substring, with a registry of parser modules:

```
parser/parsers/
  demo_csv_data/          # existing
    parse_raw.py
    parse_metadata.py
  api_json/               # new
    parse_raw.py          # JSON → DataFrame with the same schema
    parse_metadata.py     # optional: if the API also returns link metadata
    field_map.yml         # per-source field mapping (source_name → column names)
```

`service_logic.py` would select the parser module based on extension
(`.csv` → `demo_csv_data`, `.json` → `api_json`) and then apply the same
meta/data filename convention within each module.

JSON files are archived exactly like CSV files (gzip-compressed to
`archived/YYYY-MM-DD/`) so the raw API payloads are retained alongside the
processed data.

---

## Docker Compose integration

Add both services to `docker-compose.override.yml`:

```yaml
services:
  sftp_fetcher:
    build: ./fetchers/sftp_fetcher
    volumes:
      - ./fetchers/sftp_fetcher/config.yml:/app/config.yml:ro
      - ./ssh_keys:/app/ssh_keys:ro
      - sftp_fetcher_state:/app/state
      - shared_incoming:/app/incoming
    environment:
      - MNO_NAME_SSH_KEY=/app/ssh_keys/mno_key
      - LOG_LEVEL=INFO
    restart: unless-stopped

  api_fetcher:
    build: ./fetchers/api_fetcher
    volumes:
      - ./fetchers/api_fetcher/config.yml:/app/config.yml:ro
      - api_fetcher_state:/app/state
      - shared_incoming:/app/incoming
    env_file: .env
    restart: unless-stopped

volumes:
  sftp_fetcher_state:
  api_fetcher_state:
  shared_incoming:
```

Both services write to the shared `incoming/` volume, which is then watched by
the appropriate parser service.

---

## Open questions

- **JSON metadata**: For the initial implementation, link metadata is supplied
  as a one-off CSV file (same as today). Two upgrade paths for later:
  (a) metadata embedded in the raw data JSON — the `api_json` parser extracts
  and upserts it alongside measurements; (b) metadata available at a separate
  API endpoint — `api_fetcher` fetches it once at startup (or on a slow
  schedule) and writes it to `incoming/` as a metadata JSON file.
- **File naming for JSON**: Filename is free to define. Suggested convention:
  `{mno_username}_{window_start}_{window_end}_data.json`
  e.g. `operator_y_20260101T000000Z_20260102T000000Z_data.json`.
  This embeds source identity and query window, is sortable, and leaves room
  for a future `_meta.json` sibling. The meta/data distinction by filename
  substring (currently `"meta"` vs `"data"/"raw"`) will be revisited as part
  of the modular parser work — extension-based dispatch makes it redundant for
  CSV too.
- **Credentials management**: For now, env vars via `.env` / Docker secrets.
  If the number of sources grows, a secrets manager (Vault, AWS SSM) may be
  worth considering.
- **Shared package versioning**: `fetchers/shared/` is copied into both
  images at build time. A change to shared code requires rebuilding both
  images — acceptable for now, worth noting.

---

## Implementation order

API pull is priority 1. The full modular parser refactor is deferred until a
second format makes it worthwhile.

**PR 1 — `feat/api-fetcher`** *(self-contained end-to-end, no existing code broken)*
1. `fetchers/shared/` package (config, incoming_writer, state, polling)
2. `fetchers/api_fetcher/` service (auth, fetcher, main, Dockerfile)
3. `fetchers/api_fetcher/mock_server/` — Flask app for local dev and integration tests
4. `parser/parsers/api_json/` — JSON parser + `example_field_map.yml`
5. Minimal JSON branch in `parser/service_logic.py` (single `elif` on `.json` extension)
6. Integration test: mock server fixture, assert records land in DB

**PR 2 — `feat/sftp-fetcher`** ✅ *IMPLEMENTED*
1. `fetchers/sftp_fetcher/` service - **DONE**
2. Integration test: mock SFTP server, assert files land in `incoming/` - *TODO*

**PR 3 — `feat/parser-modular`** *(cleanup; defer until a second format arrives)*
1. Extension-based registry replacing `"meta"/"data"` filename substring checks
2. Revisit meta/data naming convention across all formats
