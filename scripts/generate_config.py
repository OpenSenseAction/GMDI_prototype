#!/usr/bin/env python3
"""
generate_config.py — regenerate all user-specific derived artifacts from users.yml.

Usage:
    python scripts/generate_config.py [--users-file PATH] [--repo-root PATH]
                                       [--ssh-keys-dir PATH]

Run this script after editing users.yml, then commit all changed files.
The deployed system never runs this generator; it only reads the outputs.

Outputs (all paths relative to the repository root):
  docker-compose.override.yml
  sftp_receiver/entrypoint.sh
  webserver/configs/users.json          (existing password hashes preserved)
  grafana/provisioning/datasources/postgres.yml
  grafana/init_grafana.py               (ORGS/USERS lists replaced)
  database/migrations/NNN_add_<id>.sql  (only emitted for genuinely new users)
  ssh_keys/<id>/                        (ED25519 key pair generated if absent)
"""

import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_users(users_file: Path) -> list[dict]:
    with open(users_file) as f:
        data = yaml.safe_load(f)
    users = data.get("users", [])
    _validate(users)
    return users


def _validate(users: list[dict]) -> None:
    seen_ids = set()
    seen_uids = set()
    seen_grafana_orgs = set()
    for u in users:
        uid = u["id"]
        if uid in seen_ids:
            raise ValueError(f"Duplicate user id: {uid!r}")
        seen_ids.add(uid)

        linux_uid = u["uid"]
        if linux_uid in seen_uids:
            raise ValueError(f"Duplicate uid {linux_uid} (user {uid!r})")
        seen_uids.add(linux_uid)

        grafana_org = u["grafana_org_id"]
        if grafana_org in seen_grafana_orgs:
            raise ValueError(f"Duplicate grafana_org_id {grafana_org} (user {uid!r})")
        seen_grafana_orgs.add(grafana_org)

        for src in u.get("sources", []):
            if not src.get("id"):
                raise ValueError(f"Source missing 'id' for user {uid!r}")
            parser = src.get("parser")
            if not parser:
                raise ValueError(
                    f"Source {src['id']!r} missing 'parser' for user {uid!r}"
                )
            # Valid parsers include legacy aliases that map to demo_csv_data
            valid_parsers = {
                "demo_csv_data",
                "csv_generic",
                "openmrg",
                "orange_cameroun",
                "other_mno_csv",
                "api_json",
            }
            if parser not in valid_parsers:
                raise ValueError(
                    f"Source {src['id']!r} for user {uid!r} has unknown parser "
                    f"{parser!r}. Valid values: {sorted(valid_parsers)}"
                )
            if parser == "csv_generic" and not src.get("csv_config"):
                raise ValueError(
                    f"Source {src['id']!r} for user {uid!r} uses parser "
                    f"'csv_generic' but has no 'csv_config' block"
                )
            # Validate entrypoint if specified
            entrypoint = src.get("entrypoint", "sftp_push")
            valid_entrypoints = {"sftp_push", "sftp_pull"}
            if entrypoint not in valid_entrypoints:
                raise ValueError(
                    f"Source {src['id']!r} for user {uid!r} has unknown entrypoint "
                    f"{entrypoint!r}. Valid values: {sorted(valid_entrypoints)}"
                )


# ---------------------------------------------------------------------------
# 1. docker-compose.override.yml
# ---------------------------------------------------------------------------


def _sftp_volumes_entry(user: dict, ssh_keys_dir: str) -> list[str]:
    """Named-volume and host-key-mount entries for one user."""
    lines = []
    for src in user["sources"]:
        vol = f"sftp_{user['id']}_{src['id']}"
        lines.append(f"      - {vol}:/home/{user['id']}/uploads/{src['id']}")
    lines.append(
        f"      - {ssh_keys_dir}/{user['id']}/authorized_keys"
        f":/home/{user['id']}/.ssh/keys/authorized_keys:ro"
    )
    return lines


def _sftp_command_args(users: list[dict]) -> str:
    """atmoz/sftp command line entries for all users."""
    parts = []
    for u in users:
        for src in u["sources"]:
            parts.append(f'"{u["id"]}::{u["uid"]}:{u["uid"]}:uploads/{src["id"]}"')
    return ", ".join(parts)


def _parser_service(user: dict, src: dict) -> str:
    svc = f"parser_{user['id']}_{src['id']}"
    sftp_vol = f"sftp_{user['id']}_{src['id']}"
    arch_vol = f"parser_{user['id']}_{src['id']}_archived"
    quar_vol = f"parser_{user['id']}_{src['id']}_quarantine"
    db_url = (
        f"postgresql://{user['id']}:{user['id']}_password" "@database:5432/mydatabase"
    )
    entrypoint = src.get("entrypoint", "sftp_push")
    env_lines = [
        f"      - DATABASE_URL={db_url}",
        f"      - USER_ID={user['id']}",
        f"      - PARSER_TYPE={src['parser']}",
        f"      - PARSER_INCOMING_DIR=/app/data/incoming",
        f"      - PARSER_ARCHIVED_DIR=/app/data/archived",
        f"      - PARSER_QUARANTINE_DIR=/app/data/quarantine",
        f"      - PARSER_ENABLED=true",
        f"      - PROCESS_EXISTING_ON_STARTUP=true",
        f"      - LOG_LEVEL=INFO",
    ]
    if src.get("csv_config"):
        csv_config_json = json.dumps(src["csv_config"], separators=(",", ":"))
        env_lines.append(f"      - PARSER_CSV_CONFIG={csv_config_json}")
    return "\n".join(
        [
            f"  {svc}:",
            f"    build: ./parser",
            f"    depends_on:",
            f"      database:",
            f"        condition: service_healthy",
            f"    environment:",
        ]
        + env_lines
        + [
            f"    volumes:",
            f"      - {sftp_vol}:/app/data/incoming",
            f"      - {arch_vol}:/app/data/archived",
            f"      - {quar_vol}:/app/data/quarantine",
            f"    restart: unless-stopped",
            f"    command: python -m parser.entrypoints.{entrypoint}",
            "",
        ]
    )


def generate_compose_override(
    users: list[dict], ssh_keys_dir: str = "./ssh_keys"
) -> str:
    lines = [
        "# docker-compose.override.yml",
        "# AUTO-GENERATED by scripts/generate_config.py — do not edit by hand.",
        "# Edit users.yml then re-run the generator.",
        "#",
        "# Adds per-user parser services and wires the SFTP receiver with",
        "# per-user upload subdirectories and authorised-key mounts.",
        "",
        "services:",
    ]

    # sftp_receiver override — command + per-user volumes/key mounts
    lines.append("  sftp_receiver:")
    lines.append(f"    command: [{_sftp_command_args(users)}]")
    lines.append("    volumes:")
    for user in users:
        lines.extend(_sftp_volumes_entry(user, ssh_keys_dir))

    lines.append("")

    # per-source parser services
    for user in users:
        for src in user["sources"]:
            lines.append(_parser_service(user, src))

    # named volumes section
    lines.append("volumes:")
    for user in users:
        for src in user["sources"]:
            lines.append(f"  sftp_{user['id']}_{src['id']}:")
            lines.append(f"  parser_{user['id']}_{src['id']}_archived:")
            lines.append(f"  parser_{user['id']}_{src['id']}_quarantine:")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# 2. sftp_receiver/entrypoint.sh
# ---------------------------------------------------------------------------


def generate_entrypoint_sh(users: list[dict]) -> str:
    lines = [
        "#!/bin/bash",
        "# AUTO-GENERATED by scripts/generate_config.py — do not edit by hand.",
        "set -e",
        "",
        "# Fix ownership of upload subdirectories for each SFTP user",
    ]
    for user in users:
        for src in user["sources"]:
            lines.append(f"mkdir -p /home/{user['id']}/uploads/{src['id']}")
            lines.append(
                f"chown -R {user['uid']}:{user['uid']} "
                f"/home/{user['id']}/uploads/{src['id']}"
            )
    lines += [
        "",
        "# Execute the original atmoz/sftp entrypoint",
        'exec /entrypoint "$@"',
        "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 3. webserver/configs/users.json
# ---------------------------------------------------------------------------


def generate_users_json(users: list[dict], existing_json: dict) -> dict:
    """
    Merge users.yml into the existing users.json.
    - Existing password hashes and grafana_org_id are preserved.
    - New users get a skeleton entry (password must be set via set_password.py).
    - Users present in users.json but absent from users.yml are left intact
      (so manual removals require an explicit edit of users.json).
    """
    result = dict(existing_json)
    for u in users:
        uid = u["id"]
        if uid in result:
            # Keep existing hash; update display_name and grafana_org_id
            result[uid]["display_name"] = u["display_name"]
            result[uid]["grafana_org_id"] = u["grafana_org_id"]
        else:
            result[uid] = {
                "password_hash": "",
                "display_name": u["display_name"],
                "grafana_org_id": u["grafana_org_id"],
            }
            print(
                f"  [users.json] Added skeleton entry for new user {uid!r}. "
                "Run scripts/set_password.py to set their password."
            )
    return result


# ---------------------------------------------------------------------------
# 4. database/migrations/NNN_add_<id>.sql
# ---------------------------------------------------------------------------

_SQL_TEMPLATE = """\
-- AUTO-GENERATED by scripts/generate_config.py — do not edit by hand.
-- Migration: add user role for {user_id}
--
-- Apply with:
--   docker compose exec -T database psql -U myuser -d mydatabase \\
--     < {migration_path}
--
-- Rollback: REVOKE grants, DROP ROLE {user_id}.

-- ---------------------------------------------------------------------------
-- Step 1: Create login role (idempotent)
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{user_id}') THEN
        CREATE ROLE {user_id} LOGIN PASSWORD '{user_id}_password';
    END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- Step 2: Schema access
-- ---------------------------------------------------------------------------

GRANT USAGE ON SCHEMA public TO {user_id};

-- ---------------------------------------------------------------------------
-- Step 3: Table / view access
-- ---------------------------------------------------------------------------

-- RLS on cml_metadata and cml_stats is enforced via the generic
-- current_user policy already installed on those tables.
GRANT SELECT, INSERT, UPDATE ON cml_metadata, cml_stats TO {user_id};

-- cml_data has no RLS (compressed TimescaleDB hypertable).
-- Parser writes (write_rawdata) and stats updates (update_cml_stats) go
-- directly to cml_data.  Read isolation for webserver/Grafana is provided
-- by the security-barrier views cml_data_secure / cml_data_1h_secure.
GRANT SELECT, INSERT, UPDATE ON cml_data TO {user_id};
GRANT SELECT ON cml_data_secure TO {user_id};
GRANT SELECT ON cml_data_1h_secure TO {user_id};
GRANT EXECUTE ON FUNCTION update_cml_stats(TEXT, TEXT) TO {user_id};
GRANT EXECUTE ON FUNCTION update_cml_stats_windowed(TEXT, TEXT) TO {user_id};

-- file_processing_log: parser INSERTs a row for every processed file;
-- webserver_role only needs SELECT.
GRANT SELECT, INSERT ON file_processing_log TO {user_id};
GRANT USAGE ON SEQUENCE file_processing_log_id_seq TO {user_id};

-- ---------------------------------------------------------------------------
-- Step 4: Allow webserver_role to impersonate this user
-- ---------------------------------------------------------------------------

GRANT {user_id} TO webserver_role;
"""


def _existing_migrated_users(migrations_dir: Path) -> set[str]:
    """Return set of user_ids already handled in any migration file.

    Scans both the filename pattern (*_add_<id>.sql) and the SQL content
    for ``CREATE ROLE <id>`` / ``rolname = '<id>'`` so that hand-written
    migrations (006_add_user2.sql, 007_rename_users_add_orange_cameroun.sql)
    are also detected.
    """
    migrated: set[str] = set()
    # Pattern 1: filename-based (generated migrations)
    for path in migrations_dir.glob("*_add_*.sql"):
        m = re.search(r"_add_(.+)\.sql$", path.name)
        if m:
            migrated.add(m.group(1))
    # Pattern 2: content-based (hand-written migrations)
    for path in sorted(migrations_dir.glob("*.sql")):
        content = path.read_text()
        for m in re.finditer(r"CREATE ROLE\s+(\w+)", content, re.IGNORECASE):
            migrated.add(m.group(1))
        for m in re.finditer(r"rolname\s*=\s*'(\w+)'", content, re.IGNORECASE):
            migrated.add(m.group(1))
        # ALTER ROLE user1 RENAME TO demo_openmrg — also counts the new name
        for m in re.finditer(r"RENAME TO\s+(\w+)", content, re.IGNORECASE):
            migrated.add(m.group(1))
    return migrated


def _next_migration_number(migrations_dir: Path) -> str:
    """Return the next zero-padded migration number (e.g. '009')."""
    existing = sorted(migrations_dir.glob("[0-9][0-9][0-9]_*.sql"))
    if not existing:
        return "001"
    last = existing[-1].name
    n = int(last[:3]) + 1
    return f"{n:03d}"


def generate_user_migrations(users: list[dict], migrations_dir: Path) -> list[Path]:
    """Write a migration SQL file for each user not already migrated."""
    already_done = _existing_migrated_users(migrations_dir)
    new_files = []
    for u in users:
        uid = u["id"]
        if uid in already_done:
            print(f"  [migrations] Skipping {uid!r} — migration already exists.")
            continue
        num = _next_migration_number(migrations_dir)
        filename = f"{num}_add_{uid}.sql"
        path = migrations_dir / filename
        path.write_text(
            _SQL_TEMPLATE.format(
                user_id=uid, migration_path=f"database/migrations/{filename}"
            )
        )
        print(
            f"  [migrations] Created {path.relative_to(migrations_dir.parent.parent)}"
        )
        new_files.append(path)
    return new_files


# ---------------------------------------------------------------------------
# 5. grafana/provisioning/datasources/postgres.yml
# ---------------------------------------------------------------------------


def generate_grafana_datasources(users: list[dict]) -> str:
    lines = [
        "# AUTO-GENERATED by scripts/generate_config.py — do not edit by hand.",
        "apiVersion: 1",
        "",
        "datasources:",
        "  # Each Grafana organisation has exactly ONE PostgreSQL datasource",
        "  # connecting as the matching PG login role.  RLS on",
        "  # cml_metadata/cml_stats and security-barrier views",
        "  # (cml_data_secure, cml_data_1h_secure) scope all queries to that",
        "  # role's data automatically.",
        "  #",
        "  # Dashboards use a ${datasource} template variable of type",
        "  # 'datasource' filtered to grafana-postgresql-datasource.  Because",
        "  # each org has only one such datasource the variable auto-selects",
        "  # the correct one — no user interaction required.",
        "",
    ]
    # Only org 1 (the Grafana default org) is provisioned here.  Grafana reads
    # this file at startup, before init_grafana.py has had a chance to create
    # orgs 2+.  Provisioning a non-existent org causes Grafana to exit with
    # "org.notFound".  Additional orgs are created by init_grafana.py and their
    # datasources are registered there via the Grafana API.
    for u in users[:1]:
        uid = u["id"]
        org_id = u["grafana_org_id"]
        lines += [
            f"  # Org {org_id} — {uid}",
            f"  - name: PostgreSQL",
            f"    uid: ds_{uid}",
            f"    type: grafana-postgresql-datasource",
            f"    access: proxy",
            f"    orgId: {org_id}",
            f"    url: database:5432",
            f"    database: mydatabase",
            f"    user: {uid}",
            f"    secureJsonData:",
            f"      password: {uid}_password",
            f"    jsonData:",
            f"      sslmode: disable",
            f"    isDefault: true",
            f"    editable: false",
            "",
        ]
    lines += [
        "# Note: only org 1 is listed above, intentionally.  Grafana reads this",
        "# file at startup, before init_grafana.py has created orgs 2+.  Listing",
        "# a non-existent org here causes Grafana to exit with 'org.notFound'.",
        "# Datasources for additional orgs are registered by init_grafana.py via",
        "# the Grafana API after those orgs have been created.",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# 6. grafana/init_grafana.py — replace ORGS / USERS lists
# ---------------------------------------------------------------------------


def _orgs_list_literal(users: list[dict]) -> str:
    """Render the ORGS list as a Python literal."""
    lines = ["ORGS = ["]
    for u in users:
        lines.append(
            f'    {{"id": {u["grafana_org_id"]}, "name": "{u["id"]}"}},',
        )
    lines.append("]")
    return "\n".join(lines)


def _users_list_literal(users: list[dict]) -> str:
    """Render the USERS list as a Python literal."""
    lines = ["USERS = ["]
    for u in users:
        lines.append(
            f'    {{"login": "{u["id"]}", "org_id": {u["grafana_org_id"]}, "role": "Viewer"}},',
        )
    lines.append("]")
    return "\n".join(lines)


def update_init_grafana(users: list[dict], init_grafana_path: Path) -> None:
    """
    Replace the ORGS = [...] and USERS = [...] blocks in init_grafana.py with
    versions derived from users.yml.  Everything else in the file is preserved.
    """
    src = init_grafana_path.read_text()

    # Replace ORGS block
    orgs_pattern = re.compile(
        r"^ORGS\s*=\s*\[.*?\]",
        re.MULTILINE | re.DOTALL,
    )
    new_orgs = _orgs_list_literal(users)
    if orgs_pattern.search(src):
        src = orgs_pattern.sub(new_orgs, src, count=1)
    else:
        raise RuntimeError("Could not locate ORGS = [...] in init_grafana.py")

    # Replace USERS block
    users_pattern = re.compile(
        r"^USERS\s*=\s*\[.*?\]",
        re.MULTILINE | re.DOTALL,
    )
    new_users = _users_list_literal(users)
    if users_pattern.search(src):
        src = users_pattern.sub(new_users, src, count=1)
    else:
        raise RuntimeError("Could not locate USERS = [...] in init_grafana.py")

    init_grafana_path.write_text(src)


# ---------------------------------------------------------------------------
# 7. SSH key generation
# ---------------------------------------------------------------------------


def ensure_ssh_keys(users: list[dict], ssh_keys_dir: Path) -> None:
    keygen_available = shutil.which("ssh-keygen") is not None
    if not keygen_available:
        print(
            "  [ssh_keys] WARNING: ssh-keygen not found — skipping key generation.\n"
            "             Generate keys manually with:\n"
            "               ssh-keygen -t ed25519 -f ssh_keys/<user_id>/id_ed25519 -N ''"
        )

    for u in users:
        uid = u["id"]
        key_dir = ssh_keys_dir / uid
        key_dir.mkdir(parents=True, exist_ok=True)
        priv_key = key_dir / "id_ed25519"
        auth_keys = key_dir / "authorized_keys"

        if priv_key.exists():
            print(f"  [ssh_keys] {uid}: key already exists, skipping.")
        elif keygen_available:
            subprocess.run(
                [
                    "ssh-keygen",
                    "-t",
                    "ed25519",
                    "-f",
                    str(priv_key),
                    "-N",
                    "",
                    "-C",
                    f"{uid}@gmdi",
                ],
                check=True,
                capture_output=True,
            )
            print(f"  [ssh_keys] {uid}: generated new ED25519 key pair.")
        else:
            print(f"  [ssh_keys] {uid}: skipped (ssh-keygen unavailable).")

        # authorized_keys must exist for the SFTP receiver volume mount
        if not auth_keys.exists():
            pub_key = key_dir / "id_ed25519.pub"
            if pub_key.exists():
                auth_keys.write_bytes(pub_key.read_bytes())
                print(f"  [ssh_keys] {uid}: created authorized_keys from public key.")
            elif keygen_available:
                print(
                    f"  [ssh_keys] WARNING: {uid}: no authorized_keys and no public key found."
                )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--users-file",
        default=None,
        help="Path to users.yml (default: <repo-root>/users.yml)",
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root directory (default: directory of this script's parent)",
    )
    parser.add_argument(
        "--ssh-keys-dir",
        default=None,
        help=(
            "Directory that holds per-user SSH key subdirectories "
            "(default: <repo-root>/ssh_keys).  The path is written verbatim "
            "into docker-compose.override.yml as the host side of the "
            "authorized_keys bind-mount, so a relative path like ../ssh_keys "
            "is resolved by Docker Compose relative to the override file."
        ),
    )
    args = parser.parse_args(argv)

    script_dir = Path(__file__).resolve().parent
    repo_root = Path(args.repo_root) if args.repo_root else script_dir.parent
    users_file = Path(args.users_file) if args.users_file else repo_root / "users.yml"
    # ssh_keys_dir for key generation is always an absolute/real path;
    # ssh_keys_mount is the string written into the compose file (may be relative).
    if args.ssh_keys_dir:
        ssh_keys_mount = args.ssh_keys_dir.rstrip("/")
        ssh_keys_dir = (repo_root / ssh_keys_mount).resolve()
    else:
        ssh_keys_mount = "./ssh_keys"
        ssh_keys_dir = repo_root / "ssh_keys"

    print(f"Repository root : {repo_root}")
    print(f"Users file      : {users_file}")
    print(f"SSH keys dir    : {ssh_keys_dir}  (mount path: {ssh_keys_mount})")
    print()

    users = load_users(users_file)
    print(f"Loaded {len(users)} user(s): {', '.join(u['id'] for u in users)}")
    print()

    # 1. docker-compose.override.yml
    override_path = repo_root / "docker-compose.override.yml"
    override_path.write_text(generate_compose_override(users, ssh_keys_mount))
    print(f"  [compose]    Written {override_path.relative_to(repo_root)}")

    # 2. sftp_receiver/entrypoint.sh
    entrypoint_path = repo_root / "sftp_receiver" / "entrypoint.sh"
    entrypoint_path.write_text(generate_entrypoint_sh(users))
    entrypoint_path.chmod(0o755)
    print(f"  [sftp]       Written {entrypoint_path.relative_to(repo_root)}")

    # 3. webserver/configs/users.json
    users_json_path = repo_root / "webserver" / "configs" / "users.json"
    existing_json: dict = {}
    if users_json_path.exists():
        existing_json = json.loads(users_json_path.read_text())
    new_json = generate_users_json(users, existing_json)
    users_json_path.write_text(json.dumps(new_json, indent=4) + "\n")
    print(f"  [webserver]  Written {users_json_path.relative_to(repo_root)}")

    # 4. Database migrations (only for new users)
    migrations_dir = repo_root / "database" / "migrations"
    generate_user_migrations(users, migrations_dir)  # prints its own status lines

    # 5. Grafana datasources provisioning file
    ds_path = repo_root / "grafana" / "provisioning" / "datasources" / "postgres.yml"
    ds_path.write_text(generate_grafana_datasources(users))
    print(f"  [grafana]    Written {ds_path.relative_to(repo_root)}")

    # 6. Update init_grafana.py ORGS/USERS lists
    init_grafana_path = repo_root / "grafana" / "init_grafana.py"
    update_init_grafana(users, init_grafana_path)
    print(f"  [grafana]    Updated {init_grafana_path.relative_to(repo_root)}")

    # 7. SSH keys
    ensure_ssh_keys(users, ssh_keys_dir)

    print()
    print("Done. Commit the regenerated files:")
    print(
        "  git add users.yml docker-compose.override.yml "
        "sftp_receiver/entrypoint.sh \\\n"
        "      webserver/configs/users.json database/migrations/ \\\n"
        "      grafana/provisioning/datasources/postgres.yml "
        "grafana/init_grafana.py \\\n"
        "      ssh_keys/"
    )


if __name__ == "__main__":
    main()
