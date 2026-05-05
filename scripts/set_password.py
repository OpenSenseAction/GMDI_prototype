#!/usr/bin/env python3
"""
set_password.py — set or update the webserver login password for a user.

Usage:
    python scripts/set_password.py <user_id> [--users-json PATH]

The script reads the existing users.json, prompts for the new password
(with confirmation), hashes it with scrypt via werkzeug, and writes the
result back.  The password is never echoed or stored in plain text.

Requires:
    pip install werkzeug
"""

import argparse
import getpass
import json
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("user_id", help="User ID to set password for")
    parser.add_argument(
        "--users-json",
        default=None,
        help=(
            "Path to users.json "
            "(default: <repo-root>/webserver/configs/users.json)"
        ),
    )
    args = parser.parse_args(argv)

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    users_json_path = (
        Path(args.users_json)
        if args.users_json
        else repo_root / "webserver" / "configs" / "users.json"
    )

    if not users_json_path.exists():
        print(f"Error: {users_json_path} not found.", file=sys.stderr)
        sys.exit(1)

    users = json.loads(users_json_path.read_text())
    if args.user_id not in users:
        print(
            f"Error: user {args.user_id!r} not found in {users_json_path}.\n"
            "Add the user to users.yml and re-run scripts/generate_config.py first.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        from werkzeug.security import generate_password_hash
    except ImportError:
        print(
            "Error: werkzeug is not installed. Run: pip install werkzeug",
            file=sys.stderr,
        )
        sys.exit(1)

    password = getpass.getpass(f"New password for {args.user_id!r}: ")
    if not password:
        print("Error: password must not be empty.", file=sys.stderr)
        sys.exit(1)

    confirm = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Error: passwords do not match.", file=sys.stderr)
        sys.exit(1)

    users[args.user_id]["password_hash"] = generate_password_hash(password)
    users_json_path.write_text(json.dumps(users, indent=4) + "\n")
    print(f"Password updated for {args.user_id!r} in {users_json_path}.")


if __name__ == "__main__":
    main()
