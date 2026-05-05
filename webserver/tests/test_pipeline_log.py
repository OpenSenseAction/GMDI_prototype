import os
import sys
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import Mock, call

import pytest

sys.modules.setdefault("folium", Mock())
sys.modules.setdefault("requests", Mock())

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import main as wm  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

_LOG_ROW = (1, "cml_data_20260505.csv", "archived", 756, None, datetime(2026, 5, 5, 9, 0))
_LOG_ROW_Q = (2, "bad_file.csv", "quarantined", None, "Parse error", datetime(2026, 5, 5, 8, 0))
_STATS = (10, 2, 5000)  # archived, quarantined, total_rows


@pytest.fixture
def auth_client(monkeypatch):
    """Client with login bypassed and user_db_scope mocked."""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor

    mock_user = Mock()
    mock_user.id = "demo_openmrg"

    @contextmanager
    def mock_scope(user_id):
        yield mock_conn

    monkeypatch.setattr(wm, "user_db_scope", mock_scope)
    monkeypatch.setattr(wm, "current_user", mock_user)
    monkeypatch.setitem(wm.app.config, "LOGIN_DISABLED", True)

    wm.app.config["TESTING"] = True
    return wm.app.test_client(), mock_cursor


@pytest.fixture
def client():
    wm.app.config["TESTING"] = True
    return wm.app.test_client()


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def test_pipeline_log_redirects_unauthenticated(client):
    resp = client.get("/pipeline-log")
    assert resp.status_code == 302
    assert "login" in resp.headers["Location"]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_pipeline_log_returns_200(auth_client):
    client, cursor = auth_client
    cursor.fetchone.side_effect = [_STATS, (12,)]  # stats, total count
    cursor.fetchall.return_value = [_LOG_ROW, _LOG_ROW_Q]

    resp = client.get("/pipeline-log")
    assert resp.status_code == 200


def test_pipeline_log_shows_rows(auth_client):
    client, cursor = auth_client
    cursor.fetchone.side_effect = [_STATS, (1,)]
    cursor.fetchall.return_value = [_LOG_ROW]

    resp = client.get("/pipeline-log")
    body = resp.data.decode()
    assert "cml_data_20260505.csv" in body


def test_pipeline_log_shows_summary_stats(auth_client):
    client, cursor = auth_client
    cursor.fetchone.side_effect = [_STATS, (12,)]
    cursor.fetchall.return_value = [_LOG_ROW, _LOG_ROW_Q]

    resp = client.get("/pipeline-log")
    body = resp.data.decode()
    # archived=10, quarantined=2, total_rows=5000 (rendered as "5,000" by the template)
    assert "10" in body
    assert "2" in body
    assert "5,000" in body


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


def test_pipeline_log_status_filter_archived(auth_client):
    client, cursor = auth_client
    cursor.fetchone.side_effect = [_STATS, (1,)]
    cursor.fetchall.return_value = [_LOG_ROW]

    resp = client.get("/pipeline-log?status=archived")
    assert resp.status_code == 200
    # The status param must appear in the WHERE clause — check cursor was
    # called with 'archived' in the params at some point
    all_calls = [str(c) for c in cursor.execute.call_args_list]
    assert any("archived" in c for c in all_calls)


def test_pipeline_log_invalid_status_ignored(auth_client):
    """Invalid status values are silently dropped (no WHERE clause injected)."""
    client, cursor = auth_client
    cursor.fetchone.side_effect = [_STATS, (1,)]
    cursor.fetchall.return_value = [_LOG_ROW]

    resp = client.get("/pipeline-log?status=evil'; DROP TABLE--")
    assert resp.status_code == 200
    all_calls = [str(c) for c in cursor.execute.call_args_list]
    assert not any("DROP" in c for c in all_calls)


def test_pipeline_log_search_filter(auth_client):
    client, cursor = auth_client
    cursor.fetchone.side_effect = [_STATS, (1,)]
    cursor.fetchall.return_value = [_LOG_ROW]

    resp = client.get("/pipeline-log?search=cml_data")
    assert resp.status_code == 200
    all_calls = [str(c) for c in cursor.execute.call_args_list]
    assert any("cml_data" in c for c in all_calls)


def test_pipeline_log_combined_filters(auth_client):
    client, cursor = auth_client
    cursor.fetchone.side_effect = [_STATS, (1,)]
    cursor.fetchall.return_value = [_LOG_ROW]

    resp = client.get("/pipeline-log?status=archived&search=douala")
    assert resp.status_code == 200
    all_calls = [str(c) for c in cursor.execute.call_args_list]
    assert any("archived" in c for c in all_calls)
    assert any("douala" in c for c in all_calls)


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


def test_pipeline_log_page_defaults_to_1(auth_client):
    client, cursor = auth_client
    cursor.fetchone.side_effect = [_STATS, (0,)]
    cursor.fetchall.return_value = []

    resp = client.get("/pipeline-log")
    assert resp.status_code == 200
    # OFFSET 0 should appear (page 1)
    all_calls = [str(c) for c in cursor.execute.call_args_list]
    assert any("0" in c for c in all_calls)


def test_pipeline_log_invalid_page_falls_back_to_1(auth_client):
    client, cursor = auth_client
    cursor.fetchone.side_effect = [_STATS, (0,)]
    cursor.fetchall.return_value = []

    resp = client.get("/pipeline-log?page=notanumber")
    assert resp.status_code == 200


def test_pipeline_log_page_2_uses_correct_offset(auth_client):
    client, cursor = auth_client
    cursor.fetchone.side_effect = [_STATS, (60,)]  # 60 total → 2 pages of 50
    cursor.fetchall.return_value = [_LOG_ROW]

    resp = client.get("/pipeline-log?page=2")
    assert resp.status_code == 200
    # OFFSET 50 should appear in the paginated query
    all_calls = [str(c) for c in cursor.execute.call_args_list]
    assert any("50" in c for c in all_calls)


# ---------------------------------------------------------------------------
# DB error handling
# ---------------------------------------------------------------------------


def test_pipeline_log_db_error_returns_200_with_flash(auth_client):
    """A DB failure must not crash the page — it should return 200 with empty rows."""
    client, cursor = auth_client
    cursor.fetchone.side_effect = Exception("connection refused")

    resp = client.get("/pipeline-log")
    assert resp.status_code == 200
    # rows table should be empty, not a 500
    body = resp.data.decode()
    assert "pipeline-log" in body or "Pipeline" in body
