# Multi-User Architecture

## Status

The core multi-user changes have been merged to the repository (GitHub PRs #32, #33, #35, #40, #41).
The database schema, RLS/isolation model, parser wiring, SFTP multi-user ingestion,
webserver authentication, and Grafana auth proxy are in place. Remaining implementation
work: `feat/web-api-upload` and `feat/user-onboarding` (see PR6 / PR7 below).

| PR | Branch | Status | Scope |
|----|--------|--------|-------|
| 1 | `feat/db-add-user-id` | merged | `user_id` columns, updated aggregate + compression |
| 2 | `feat/db-roles-rls` | merged | Roles, RLS, security-barrier views |
| 3 | `feat/parser-user-id` | merged | Parser injects `user_id`; removes compat defaults |
| 4 | `feat/sftp-multi-user` | merged | Per-user SFTP dirs, volumes, parser instances |
| 5 | `feat/webserver-auth` | merged | Login, session, DB role switching — go-live milestone |
| 6 | `feat/web-api-upload` | not started | HTTP API upload + drag-and-drop |
| 7 | `feat/user-onboarding` | not started | `add_user.sh`, docs |
| 8 | `feat/grafana-auth-proxy` | merged | Per-user Grafana datasources + auth proxy header |

### Recent merged GitHub PRs

- #32 — `feat/db-add-user-id`: added `user_id` columns, back-compatible defaults, and updated primary keys/indexes (migration `001_add_user_id.sql`).
- #33 — `feat/db-roles-rls`: created per-user roles and Row-Level Security, security-barrier views, and grants (migration `004_add_roles_rls.sql`).
- #35 — `feat/sftp-multi-user` (includes parser wiring): per-user SFTP directories, per-user parser services, onboarding `demo_orange_cameroun` (`006_add_user2.sql`), and parser now stamps `user_id` on writes.
- #40 — `feat/webserver-auth`: webserver login/session management and per-request `SET ROLE` DB impersonation (uses `webserver_role` credentials).
- #41 — `feat/grafana-auth-proxy`: Grafana auth proxy + per-org/postgres datasources and bootstrap (`grafana/init_grafana.py` and provisioning).

These PRs are merged; the rest of this document describes architecture and remaining tasks.

---

## Architecture

```
External
  https://company.com   (port 443)
  sftp://company.com    (port 2222)
         |
  Nginx reverse proxy
    :443 → webserver:5000
    :2222 → sftp_receiver:22
         |
  ┌─────────────────────────────────────────┐
  │ SFTP (shared, one instance)             │
  │  /home/user1/uploads/  ← SSH key user1  │
  │  /home/user2/uploads/  ← SSH key user2  │
  └────────────────┬────────────────────────┘
                   │ per-user Docker volumes
  ┌────────────────▼────────────────────────┐
  │ Parser instances (one per user)         │
  │  parser_user1 → watches user1 volume    │
  │  parser_user2 → watches user2 volume    │
  └────────────────┬────────────────────────┘
                   │
  ┌────────────────▼────────────────────────┐
  │ TimescaleDB                             │
  │  RLS on cml_metadata + cml_stats        │
  │  security-barrier views for cml_data    │
  └────────────────┬────────────────────────┘
                   │
  ┌────────────────▼────────────────────────┐
  │ Webserver (shared, one instance)        │
  │  connects as webserver_role             │
  │  SET ROLE <user> per request            │
  └─────────────────────────────────────────┘
```

| Component | Count (10 users) | RAM |
|-----------|-----------------|-----|
| SFTP | 1 | 17 MB |
| Webserver | 1 | 250 MB |
| Parsers | 10 | 630 MB |
| Database | 1 | 2–3 GB (compression + aggregates) |
| Grafana | 1 | 200 MB |
| **Total** | **14** | **~3–4 GB** |

---

## Isolation Model (as implemented)

### Role conventions

- **`user1`** — PostgreSQL LOGIN role. Role name equals the `user_id` value stored in data rows.
  This lets a single generic `current_user`-based RLS policy cover every user; onboarding a new
  user only requires `CREATE ROLE <name> LOGIN PASSWORD '...'` and granting it to `webserver_role`.
- **`webserver_role`** — PostgreSQL LOGIN role used by the webserver process.
  Has read-all RLS policies for admin/cross-tenant queries.
  Impersonates a user role via `SET ROLE` for user-scoped requests.
- **`myuser`** — superuser, used by parser until PR3 is deployed. Bypasses RLS.

### Where isolation is enforced

| Table / View | Isolation mechanism |
|---|---|
| `cml_metadata` | RLS, generic `current_user` policy |
| `cml_stats` | RLS, generic `current_user` policy |
| `cml_data` | **No RLS** — TimescaleDB compressed hypertable; RLS and compression are mutually exclusive |
| `cml_data_secure` | `security_barrier` view: `WHERE user_id = current_user` |
| `cml_data_1h` | **No RLS** — materialized view; same constraint |
| `cml_data_1h_secure` | `security_barrier` view: `WHERE user_id = current_user` |

**Rule:** user roles must never query `cml_data` or `cml_data_1h` directly. All user-facing
read paths use `cml_data_secure` and `cml_data_1h_secure`. The webserver queries the raw tables
only when connected as `webserver_role` (for admin/cross-user aggregates).

### Pending DB hardening (do before PR3)

Two one-line changes in a new migration `006_harden_roles.sql`:

```sql
-- Prevent webserver_role from inheriting user1's write privileges automatically.
-- SET ROLE must be called explicitly; nothing is inherited at login time.
ALTER ROLE webserver_role NOINHERIT;

-- Users must access cml_data only through cml_data_secure (security-barrier view).
-- cml_data has no RLS (compressed hypertable constraint), so direct grants of any
-- kind bypass the WITH CHECK OPTION isolation boundary on the view.
REVOKE SELECT, INSERT, UPDATE ON cml_data FROM user1;
```

Apply with:
```bash
docker exec -i gmdi_prototype-database-1 psql -U myuser -d mydatabase \
  < database/migrations/006_harden_roles.sql
```

Also update `database/init.sql` and `database/migrations/004_add_roles_rls.sql` to include both
changes so fresh deployments and the migration history stay consistent.

---

## PR3 — `feat/parser-user-id`

**Goal:** parser injects `user_id` from env; removes single-user compatibility defaults.

### Changes

**`parser/db_writer.py`**

Add `user_id` parameter to `__init__` and stamp it onto every write:

```python
class DBWriter:
    def __init__(self, db_url: str, user_id: str, connect_timeout: int = 10):
        self.user_id = user_id
        ...

    def write_metadata(self, df: pd.DataFrame) -> int:
        df = df.copy()
        df["user_id"] = self.user_id
        ...

    def write_rawdata(self, df: pd.DataFrame) -> int:
        df = df.copy()
        df["user_id"] = self.user_id
        ...
```

The `ON CONFLICT` clause in `write_metadata` must be updated from
`ON CONFLICT (cml_id, sublink_id)` to `ON CONFLICT (cml_id, sublink_id, user_id)` once the
backward-compat `UNIQUE (cml_id, sublink_id)` constraint is dropped (see migration below).

**`parser/main.py`**

```python
class Config:
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://myuser:mypassword@database:5432/mydatabase")
    USER_ID = os.getenv("USER_ID", "demo_openmrg")  # currently defaults to 'demo_openmrg' for convenience; set explicitly in production
    ...

def main():
    ...
    db_writer = DBWriter(Config.DATABASE_URL, user_id=Config.USER_ID)
    ...
```

**`docker-compose.yml`** — add `USER_ID=user1` to the existing `parser` service.

**`database/migrations/007_remove_compat_defaults.sql`**

```sql
-- Drop the single-user compat UNIQUE constraint (kept through PR2 for parser backward compat).
ALTER TABLE cml_metadata DROP CONSTRAINT cml_metadata_cml_id_sublink_id_key;

-- Remove fail-open DEFAULT 'user1' now that the parser always supplies user_id explicitly.
ALTER TABLE cml_data     ALTER COLUMN user_id DROP DEFAULT;
ALTER TABLE cml_metadata ALTER COLUMN user_id DROP DEFAULT;
ALTER TABLE cml_stats    ALTER COLUMN user_id DROP DEFAULT;

-- Remove the default from update_cml_stats() signature.
CREATE OR REPLACE FUNCTION update_cml_stats(
    target_cml_id  TEXT,
    target_user_id TEXT          -- no default; caller must supply it
) RETURNS VOID AS $$
...
$$ LANGUAGE plpgsql;
```

Apply migration 007 only after the parser is redeployed with `USER_ID` set.

---

## PR4 — `feat/sftp-multi-user`

**Goal:** multiple SFTP users with isolated directories; per-user parser instances.

### SFTP entrypoint

```bash
# sftp_receiver/entrypoint.sh
#!/bin/bash
set -e
mkdir -p /home/user1/uploads /home/user2/uploads
chown -R 1001:1001 /home/user1/uploads
chown -R 1002:1002 /home/user2/uploads
exec /entrypoint \
    user1::1001:1001:uploads \
    user2::1002:1002:uploads
```

### SSH keys

```bash
for user in user1 user2; do
    mkdir -p ssh_keys/${user}
    ssh-keygen -t ed25519 -f ssh_keys/${user}/id_ed25519 -N "" -C "${user}@gmdi"
    cp ssh_keys/${user}/id_ed25519.pub ssh_keys/${user}/authorized_keys
done
```

### `docker-compose.yml` changes

Replace the single `parser` service with per-user parser services.
Each parser connects as the matching PostgreSQL role (not `myuser`).

```yaml
services:
  sftp_receiver:
    ...
    volumes:
      - ./ssh_keys/user1/authorized_keys:/home/user1/.ssh/keys/authorized_keys:ro
      - ./ssh_keys/user2/authorized_keys:/home/user2/.ssh/keys/authorized_keys:ro
      - sftp_user1_uploads:/home/user1/uploads
      - sftp_user2_uploads:/home/user2/uploads

  parser_user1:
    build: ./parser
    depends_on:
      database:
        condition: service_healthy
    environment:
      - DATABASE_URL=postgresql://user1:user1password@database:5432/mydatabase
      - USER_ID=user1
      - PARSER_INCOMING_DIR=/app/data/incoming
      - PARSER_ARCHIVED_DIR=/app/data/archived
      - PARSER_QUARANTINE_DIR=/app/data/quarantine
      - PARSER_ENABLED=true
      - PROCESS_EXISTING_ON_STARTUP=true
    volumes:
      - sftp_user1_uploads:/app/data/incoming
      - parser_user1_archived:/app/data/archived
      - parser_user1_quarantine:/app/data/quarantine

  parser_user2:
    build: ./parser
    depends_on:
      database:
        condition: service_healthy
    environment:
      - DATABASE_URL=postgresql://user2:user2password@database:5432/mydatabase
      - USER_ID=user2
      - PARSER_INCOMING_DIR=/app/data/incoming
      - PARSER_ARCHIVED_DIR=/app/data/archived
      - PARSER_QUARANTINE_DIR=/app/data/quarantine
      - PARSER_ENABLED=true
      - PROCESS_EXISTING_ON_STARTUP=true
    volumes:
      - sftp_user2_uploads:/app/data/incoming
      - parser_user2_archived:/app/data/archived
      - parser_user2_quarantine:/app/data/quarantine


volumes:
  sftp_user1_uploads:
  sftp_user2_uploads:
  parser_user1_archived:
  parser_user1_quarantine:
  parser_user2_archived:
  parser_user2_quarantine:
```

Note: the current `docker-compose.yml` and migrations rename the example `user1`/`user2`
to more descriptive dataset users (`demo_openmrg` / `demo_orange_cameroun`) via
`database/migrations/007_rename_users_add_orange_cameroun.sql`.  The compose services
in this repository are named `parser_openmrg` and `parser_orange_cameroun` (see
`docker-compose.yml`) and the migrations/tests reference the `demo_*` role names.

---

## PR5 — `feat/webserver-auth` (go-live milestone)

**Goal:** login, session management, per-request DB role switching. All routes require login.

### DB access pattern

The webserver holds **one** DB connection credential (`webserver_role`). It does not store
per-user DB passwords. For user-scoped requests it switches role within a transaction using
`SET LOCAL ROLE`, which PostgreSQL automatically reverts at transaction end — preventing role
bleed if the connection is ever reused.

```python
from contextlib import contextmanager
from psycopg2 import sql
import psycopg2

@contextmanager
def user_db_scope(user_id: str):
    """Context manager: connection scoped to user_id for the duration of one request.

    Safety notes:
    - user_id is allowlisted against USERS before reaching SQL composition.
      USERS is loaded from the trusted config file at startup; it is never
      derived from request input.
    - The role name is composed with sql.Identifier, not value binding (%s).
      %s is for SQL values (string literals); role names are identifiers and
      must be quoted as such to be both correct and injection-safe.
    - SET LOCAL ROLE reverts automatically at transaction end.
    """
    if user_id not in USERS:
        raise ValueError(f"Unknown user_id: {user_id!r}")

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))   # connects as webserver_role
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(user_id))
            )
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def get_admin_db_connection():
    """Unscoped connection as webserver_role (cross-tenant admin queries)."""
    return psycopg2.connect(os.getenv("DATABASE_URL"))
```

Usage in route handlers:

```python
@app.route("/")
@login_required
def overview():
    with user_db_scope(current_user.id) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT cml_id) FROM cml_metadata")
        ...
```

`DATABASE_URL` in `docker-compose.yml` must be updated to use `webserver_role` credentials:
```
DATABASE_URL=postgresql://webserver_role:webserverpassword@database:5432/mydatabase
```

### User store

Users are stored in a file mounted into the container (not in the DB). Passwords are hashed
with `werkzeug.security.generate_password_hash` (scrypt/pbkdf2).

```json
// configs/users.json  (mounted read-only at /app/users.json)
{
  "user1": {
    "password_hash": "<scrypt hash>",
    "display_name": "User 1"
  }
}
```

No `db_password` field — role switching is done via `SET ROLE`, not a second connection string.

### `webserver/main.py` key additions

```python
from flask import Flask, session, redirect, url_for, request, flash, render_template
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash
import json, os, psycopg2

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY")   # must be set in docker-compose / secrets

login_manager = LoginManager(app)
login_manager.login_view = "login"

with open(os.getenv("USERS_CONFIG_PATH", "/app/users.json")) as f:
    USERS = json.load(f)

class User(UserMixin):
    def __init__(self, user_id):
        self.id = user_id
        self.display_name = USERS[user_id].get("display_name", user_id)

@login_manager.user_loader
def load_user(user_id):
    return User(user_id) if user_id in USERS else None

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if username in USERS and check_password_hash(USERS[username]["password_hash"], password):
            login_user(User(username))
            return redirect(request.args.get("next") or url_for("overview"))
        flash("Invalid credentials")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))
```

All existing routes gain `@login_required`. Calls to `get_db_connection()` are replaced with
`get_user_db_connection(current_user.id)` for user-scoped data and `get_admin_db_connection()`
for admin-only cross-tenant operations.

### `docker-compose.yml` webserver environment

```yaml
  webserver:
    environment:
      - DATABASE_URL=postgresql://webserver_role:webserverpassword@database:5432/mydatabase
      - SECRET_KEY=<random 32-byte hex, set via .env or Docker secret>
      - USERS_CONFIG_PATH=/app/users.json
    volumes:
      - ./configs/users.json:/app/users.json:ro
```

### New dependencies (`webserver/requirements.txt`)

```
flask-login>=0.6.3
```

### Security notes

- `SECRET_KEY` must be set to a cryptographically random value and not committed to the repo.
- Session cookies should be `HttpOnly` and `Secure` (set `SESSION_COOKIE_SECURE=True` behind HTTPS).
- Grafana access via `/grafana/` should be restricted until a Grafana auth integration is added.

---

## PR6 — `feat/web-api-upload`

**Goal:** HTTP API (`/api/upload` with API key) and drag-and-drop web UI (`/data-uploads`).

Both upload paths save files directly into the user's SFTP incoming volume — the same directory
each per-user parser already watches. No separate ingestion coordinator service is needed.

```python
@app.route("/api/upload", methods=["POST"])
def api_upload():
    """API key authenticated file upload."""
    api_key = request.headers.get("X-API-Key", "")
    user_id = API_KEYS.get(api_key)          # dict loaded from env / config at startup
    if not user_id:
        return jsonify({"error": "Unauthorized"}), 401

    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"error": "No file"}), 400

    ext = Path(secure_filename(file.filename)).suffix.lower()
    if ext not in {".csv", ".nc"}:
        return jsonify({"error": "Unsupported file type"}), 400

    dest = Path(f"/app/data/{user_id}/incoming") / secure_filename(file.filename)
    dest.parent.mkdir(parents=True, exist_ok=True)
    file.save(dest)
    return jsonify({"ok": True, "filename": dest.name}), 201

@app.route("/upload", methods=["POST"])
@login_required
def web_upload():
    """Session-authenticated drag-and-drop upload."""
    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"error": "No file"}), 400

    ext = Path(secure_filename(file.filename)).suffix.lower()
    if ext not in {".csv", ".nc"}:
        return jsonify({"error": "Unsupported file type"}), 400

    dest = Path(f"/app/data/{current_user.id}/incoming") / secure_filename(file.filename)
    dest.parent.mkdir(parents=True, exist_ok=True)
    file.save(dest)
    return jsonify({"ok": True, "filename": dest.name}), 201
```

API keys are stored outside the repo (env vars or Docker secrets), not in `users.json`.

---

## PR7 — `feat/user-onboarding`

**Goal:** `scripts/add_user.sh` automates all steps for adding a new user.

### What the script does

1. Generate ED25519 SSH key pair → `ssh_keys/<name>/`
2. `CREATE ROLE <name> LOGIN PASSWORD '<generated>';`
3. `GRANT <name> TO webserver_role;` — enables `SET ROLE` from webserver
4. Grant `SELECT, INSERT, UPDATE` on `cml_metadata`, `cml_stats` to new role
5. Grant `INSERT, UPDATE` on `cml_data_secure` (parser writes) and `SELECT` on `cml_data_secure`, `cml_data_1h_secure` (reads) to new role — no direct grants on raw `cml_data`
6. Add user entry to `configs/users.json` (hashed password via `scripts/hash_password.py`)
7. Print docker-compose snippet for sftp_receiver + parser service + volumes

### Onboarding SQL template

```sql
CREATE ROLE :user LOGIN PASSWORD :'password';
GRANT :user TO webserver_role;
GRANT SELECT, INSERT, UPDATE ON cml_metadata, cml_stats TO :user;
-- All cml_data access goes through the security-barrier view, never the raw table.
-- cml_data has no RLS (compressed hypertable), so any direct grant — SELECT, INSERT,
-- or UPDATE — bypasses the WITH CHECK OPTION isolation boundary on cml_data_secure.
GRANT SELECT, INSERT, UPDATE ON cml_data_secure TO :user;
GRANT SELECT ON cml_data_1h_secure TO :user;
```

Note: no per-user RLS policy is needed. The generic `current_user` policy already installed on
`cml_metadata` and `cml_stats` covers every role whose name matches their `user_id` value.
`cml_data_secure` enforces isolation on reads **and** writes via `WHERE user_id = current_user
WITH CHECK OPTION`; `cml_data_1h_secure` handles the aggregate. Any direct grant on raw
`cml_data` — including `INSERT` or `UPDATE` — would allow a tenant to write rows with an
arbitrary `user_id`, overwriting or injecting another tenant's data.

---

## Credentials and secrets

| Credential | Current (dev) | Production |
|---|---|---|
| DB superuser | `myuser` / `mypassword` in compose | Replace via `.env` or Docker secrets |
| `user1` role | `user1password` in SQL | Rotate before first additional user goes live |
| `webserver_role` | `webserverpassword` in SQL | Rotate before PR5 go-live |
| Flask `SECRET_KEY` | not yet set | Set via Docker secret; never commit |

SQL-embedded passwords in `init.sql` and migration `004` are documented as dev defaults. Rotate
them before deploying a second user. `006_harden_roles.sql` is the right place to also document
the rotation procedure.

---

## Processor service — required decision before onboarding a second user

`processor/main.py` currently runs `SELECT * FROM cml_data` as the `myuser` superuser. It is a
stub (no HTTP server, `process_data()` is empty), but it will fetch all tenants' data once a
second user exists.

Before PR7 is executed, one of these must be decided:

- **Remove it** if it has no planned function.
- **Make it per-user** (like parsers) — each instance connects as the matching tenant role and
  queries `cml_data_secure` with `WHERE user_id = current_user` automatically enforced.
- **Make it an admin service** — connects as `webserver_role` and uses explicit `WHERE user_id`
  filters; must never expose cross-tenant results through a user-facing path.

No second user should be onboarded until this is resolved.

---

## PR8 — `feat/grafana-auth-proxy`

**Goal:** Grafana dashboards are scoped to the logged-in user without a separate Grafana login.
Users retain full interactive dashboard access and can build their own panels.

### Known gap (until this PR)

Grafana currently connects as `myuser` (superuser) and sees all tenants' data regardless of
which user is logged in to the webserver. The `/grafana/` proxy is gated by `@login_required`
(PR5), so unauthenticated access is blocked, but data isolation within Grafana is not enforced.

### Approach — Grafana auth proxy + per-user datasources

Grafana's [Auth Proxy](https://grafana.com/docs/grafana/latest/setup-grafana/configure-security/configure-authentication/auth-proxy/)
mode trusts an upstream header (`X-WEBAUTH-USER`) set by a reverse proxy or, in our case, the
Flask `/grafana/` proxy. Grafana auto-provisions a Grafana user on first login and maps them to
an org/team. Combined with per-user PostgreSQL datasources (each connecting as the matching PG
role), queries are automatically scoped to that user's data via RLS and security-barrier views.

**Data isolation chain:**
```
Flask session → X-WEBAUTH-USER header → Grafana user → per-user datasource → PG role → RLS
```

### Changes

**`grafana/provisioning/datasources/postgres.yml`** — replace single `myuser` datasource with
one datasource per user:

```yaml
apiVersion: 1
datasources:
  - name: demo_openmrg
    uid: ds_demo_openmrg
    type: grafana-postgresql-datasource
    access: proxy
    url: database:5432
    database: mydatabase
    user: demo_openmrg
    secureJsonData:
      password: <demo_openmrg password>
    jsonData:
      sslmode: disable

  - name: demo_orange_cameroun
    uid: ds_demo_orange_cameroun
    type: grafana-postgresql-datasource
    access: proxy
    url: database:5432
    database: mydatabase
    user: demo_orange_cameroun
    secureJsonData:
      password: <demo_orange_cameroun password>
    jsonData:
      sslmode: disable
```

**`grafana/provisioning/datasources/postgres.yml`** — also keep an admin datasource connecting
as `webserver_role` for cross-tenant dashboards used by operators.

**`docker-compose.yml` — Grafana environment:**

```yaml
  grafana:
    environment:
      - GF_AUTH_PROXY_ENABLED=true
      - GF_AUTH_PROXY_HEADER_NAME=X-WEBAUTH-USER
      - GF_AUTH_PROXY_HEADER_PROPERTY=username
      - GF_AUTH_PROXY_AUTO_SIGN_UP=true
      - GF_AUTH_PROXY_WHITELIST=webserver  # only accept header from the webserver container
      - GF_AUTH_DISABLE_LOGIN_FORM=true
      - GF_AUTH_ANONYMOUS_ENABLED=false
```

**`webserver/main.py` — inject header in Grafana proxy:**

```python
@app.route("/grafana/", defaults={"path": ""}, methods=[...])
@app.route("/grafana/<path:path>", methods=[...])
@login_required
def grafana_proxy(path):
    headers = {k: v for k, v in request.headers if k.lower() != "host"}
    headers["X-WEBAUTH-USER"] = current_user.id   # inject identity
    ...
```

### Onboarding impact

`scripts/add_user.sh` (PR7) must also provision the Grafana datasource and add a `GF_`
environment variable or Grafana API call to create the user's org/team mapping.

### Security notes

- `GF_AUTH_PROXY_WHITELIST` must restrict the trusted header to the webserver container IP/name
  so external clients cannot forge `X-WEBAUTH-USER`.
- Grafana datasource passwords are dev defaults; rotate before production.
- Per-user datasources connecting as PG login roles provide the same DB-level isolation as the
  webserver (RLS on `cml_metadata`/`cml_stats`, security-barrier views for `cml_data`).

---

## Success criteria

- Each user's `cml_metadata` and `cml_stats` rows are invisible to other user roles (RLS).
- Each user role cannot read `cml_data` directly; only `cml_data_secure` is accessible.
- `webserver_role` without `SET ROLE` can read all tenants' metadata and stats (admin path).
- After `SET ROLE user1`, all queries on `cml_data_secure` and `cml_data_1h_secure` return only `user_id = 'user1'` rows.
- The webserver requires login on all routes (PR5). ✓
- A second user can be fully onboarded without touching the running DB schema (PR7).
- Grafana dashboards are scoped to the logged-in user; no cross-tenant data visible (PR8).
- Database RAM stays ≤ 3 GB for 10 users (compression + aggregate already in place).

