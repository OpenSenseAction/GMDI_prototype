"""Mock REST API server for api_fetcher development and testing.

Implements the DRF Simple JWT + CML data endpoints that the api_fetcher
expects.  Never use in production.

Endpoints:
  POST /login/             → {access, refresh}
  POST /refresh/           → {access} or 401
  GET  /cml/               → {count, next, previous, results: [...]}

Query params for GET /cml/:
  date_from          YYYY-MM-DD
  date_to            YYYY-MM-DD
  performance_event  RSL or TSL
  page               1-based page number
  page_size          records per page (default 100)
"""

import secrets
from datetime import datetime, timedelta

from flask import Flask, jsonify, request

app = Flask(__name__)

# ── In-memory token store ─────────────────────────────────────────────────────
# Maps token_hex → {"type": "access"|"refresh", "username": str}
_TOKENS: dict[str, dict] = {}

_VALID_USERS = {"testuser": "testpass"}


def _new_token(username: str, token_type: str) -> str:
    tok = secrets.token_hex(24)
    _TOKENS[tok] = {"type": token_type, "username": username}
    return tok


def _require_bearer() -> tuple[bool, str]:
    """Return (ok, username_or_error_msg)."""
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return False, "Missing or malformed Authorization header"
    token = header.split(" ", 1)[1]
    entry = _TOKENS.get(token)
    if not entry or entry["type"] != "access":
        return False, "Invalid or expired access token"
    return True, entry["username"]


# ── Auth endpoints ────────────────────────────────────────────────────────────


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.post("/login/")
def login():
    body = request.get_json(silent=True) or {}
    username = body.get("username", "")
    password = body.get("password", "")
    if _VALID_USERS.get(username) != password:
        return jsonify({"detail": "No active account found with the given credentials"}), 401
    access = _new_token(username, "access")
    refresh = _new_token(username, "refresh")
    return jsonify({"access": access, "refresh": refresh})


@app.post("/refresh/")
def refresh():
    body = request.get_json(silent=True) or {}
    tok = body.get("refresh", "")
    entry = _TOKENS.get(tok)
    if not entry or entry["type"] != "refresh":
        return jsonify({"detail": "Token is invalid or expired"}), 401
    new_access = _new_token(entry["username"], "access")
    return jsonify({"access": new_access})


# ── Data endpoint ─────────────────────────────────────────────────────────────


def _generate_records(
    date_from: str,
    date_to: str,
    performance_event: str,
    link_ids: list[str],
) -> list[dict]:
    """Synthesise hourly CML records for the requested window."""
    try:
        start = datetime.strptime(date_from, "%Y-%m-%d")
        end = datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        return []

    records = []
    current = start
    while current < end:
        for link_id in link_ids:
            records.append(
                {
                    "timestamp": current.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "link_id": link_id,
                    "sublink_id": "1",
                    "performance_event": performance_event,
                    # Synthetic measurement value
                    "value": round(-40.0 - (hash((current, link_id)) % 20), 1),
                }
            )
        current += timedelta(hours=1)
    return records


@app.get("/cml/")
def cml_data():
    ok, username_or_err = _require_bearer()
    if not ok:
        return jsonify({"detail": username_or_err}), 401

    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")
    performance_event = request.args.get("performance_event", "RSL").upper()
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 100))

    if not date_from or not date_to:
        return jsonify({"detail": "date_from and date_to are required"}), 400
    if performance_event not in ("RSL", "TSL"):
        return jsonify({"detail": "performance_event must be RSL or TSL"}), 400

    link_ids = ["10001", "10002", "10003"]
    all_records = _generate_records(date_from, date_to, performance_event, link_ids)

    total = len(all_records)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    page_records = all_records[start_idx:end_idx]

    return jsonify(
        {
            "count": total,
            "next": None if end_idx >= total else f"?page={page + 1}",
            "previous": None if page == 1 else f"?page={page - 1}",
            "results": page_records,
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
