"""Tests for scripts/generate_config.py — focused on the --ssh-keys-dir option."""

from pathlib import Path

import pytest
import yaml

# Import helpers directly from the script.
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from generate_config import (
    generate_compose_override,
    ensure_ssh_keys,
    main,
)


# ---------------------------------------------------------------------------
# Minimal users fixture
# ---------------------------------------------------------------------------

USERS = [
    {
        "id": "alice",
        "uid": 2001,
        "display_name": "Alice",
        "grafana_org_id": 1,
        "sources": [{"id": "main", "parser": "openmrg"}],
    }
]


def _make_users_yml(repo_root: Path, users: list[dict] | None = None) -> Path:
    data = {"users": users or USERS}
    p = repo_root / "users.yml"
    p.write_text(yaml.dump(data))
    return p


# ---------------------------------------------------------------------------
# generate_compose_override — ssh_keys_dir param
# ---------------------------------------------------------------------------


def test_default_ssh_keys_dir_uses_dot_slash():
    """Without --ssh-keys-dir the mount path is ./ssh_keys/<user>/..."""
    output = generate_compose_override(USERS)
    assert "./ssh_keys/alice/authorized_keys" in output


def test_custom_ssh_keys_dir_written_verbatim():
    """A relative path like ../ssh_keys is written as-is into the compose file."""
    output = generate_compose_override(USERS, ssh_keys_dir="../ssh_keys")
    assert "../ssh_keys/alice/authorized_keys" in output
    assert "./ssh_keys" not in output


def test_absolute_ssh_keys_dir_written_verbatim(tmp_path):
    """An absolute path is also written verbatim."""
    abs_path = str(tmp_path / "keys")
    output = generate_compose_override(USERS, ssh_keys_dir=abs_path)
    assert f"{abs_path}/alice/authorized_keys" in output


# ---------------------------------------------------------------------------
# ensure_ssh_keys — uses the resolved absolute path for key generation
# ---------------------------------------------------------------------------


def test_ensure_ssh_keys_creates_keys_in_given_dir(tmp_path):
    """Keys are generated under the supplied directory, not under repo_root."""
    keys_dir = tmp_path / "external_keys"
    ensure_ssh_keys(USERS, keys_dir)

    priv = keys_dir / "alice" / "id_ed25519"
    pub = keys_dir / "alice" / "id_ed25519.pub"
    auth = keys_dir / "alice" / "authorized_keys"

    assert priv.exists(), "private key not generated"
    assert pub.exists(), "public key not generated"
    assert auth.exists(), "authorized_keys not created"


def test_ensure_ssh_keys_skips_existing_key(tmp_path):
    """Existing private key is left untouched (no overwrite)."""
    keys_dir = tmp_path / "keys"
    user_dir = keys_dir / "alice"
    user_dir.mkdir(parents=True)
    priv = user_dir / "id_ed25519"
    priv.write_text("EXISTING")

    ensure_ssh_keys(USERS, keys_dir)
    assert priv.read_text() == "EXISTING"


# ---------------------------------------------------------------------------
# main() — end-to-end with --ssh-keys-dir
# ---------------------------------------------------------------------------


def test_main_ssh_keys_dir_override_uses_external_dir(tmp_path, monkeypatch):
    """
    Running main() with --ssh-keys-dir writes ../ssh_keys paths into the
    generated compose override and places keys in the external directory.
    """
    # Build a minimal repo layout inside tmp_path
    repo_root = tmp_path / "repo"
    for subdir in ("sftp_receiver", "webserver/configs", "database/migrations",
                   "grafana/provisioning/datasources", "grafana", "ssh_keys"):
        (repo_root / subdir).mkdir(parents=True)

    # Minimal users.yml (deployment-level, one real user)
    _make_users_yml(tmp_path)  # tmp_path/users.yml

    # Stub out files that main() reads/updates
    (repo_root / "webserver" / "configs" / "users.json").write_text("{}")
    (repo_root / "grafana" / "init_grafana.py").write_text(
        "ORGS = []\nUSERS = []\n"
    )

    # External ssh_keys dir lives next to the repo (simulating deployment layout)
    ext_keys_dir = tmp_path / "ssh_keys"
    ext_keys_dir.mkdir()

    main([
        "--users-file", str(tmp_path / "users.yml"),
        "--repo-root", str(repo_root),
        "--ssh-keys-dir", str(ext_keys_dir),
    ])

    # 1. Compose override uses the external path
    override = (repo_root / "docker-compose.override.yml").read_text()
    assert str(ext_keys_dir) + "/alice/authorized_keys" in override

    # 2. Keys were generated in the external directory
    assert (ext_keys_dir / "alice" / "id_ed25519").exists()

    # 3. Default ssh_keys inside repo_root was NOT used
    assert not (repo_root / "ssh_keys" / "alice").exists()


def test_main_default_ssh_keys_dir_stays_inside_repo(tmp_path):
    """Without --ssh-keys-dir, keys go into <repo-root>/ssh_keys as before."""
    repo_root = tmp_path / "repo"
    for subdir in ("sftp_receiver", "webserver/configs", "database/migrations",
                   "grafana/provisioning/datasources", "grafana", "ssh_keys"):
        (repo_root / subdir).mkdir(parents=True)

    _make_users_yml(repo_root)
    (repo_root / "webserver" / "configs" / "users.json").write_text("{}")
    (repo_root / "grafana" / "init_grafana.py").write_text(
        "ORGS = []\nUSERS = []\n"
    )

    main(["--repo-root", str(repo_root)])

    override = (repo_root / "docker-compose.override.yml").read_text()
    assert "./ssh_keys/alice/authorized_keys" in override
    assert (repo_root / "ssh_keys" / "alice" / "id_ed25519").exists()
