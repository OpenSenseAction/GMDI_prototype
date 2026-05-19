import sys
from contextlib import contextmanager
from unittest.mock import Mock

import pytest


# Ensure optional heavy imports won't fail at import time
sys.modules.setdefault("folium", Mock())
sys.modules.setdefault("requests", Mock())


def test_api_cml_stats_returns_cached_stats(monkeypatch):
    # Make the webserver package modules importable (tests add webserver/ to sys.path)
    import os

    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

    # Import main module from webserver package directory
    import main as wm

    # Prepare mock DB connection and cursor
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor

    # Row fields: cml_id, completeness_percent_6h, total_records_6h, valid_records_6h,
    # mean_rsl_6h, stddev_rsl_6h, completeness_percent_1h, stddev_rsl_1h, last_rsl
    mock_cursor.fetchall.return_value = [
        (
            "10001",
            94.2,   # completeness_percent_6h
            2160,   # total_records_6h
            2031,   # valid_records_6h
            -50.0,  # mean_rsl_6h
            3.0,    # stddev_rsl_6h
            90.0,   # completeness_percent_1h
            1.3,    # stddev_rsl_1h
            -45.0,  # last_rsl
        )
    ]

    # Ensure cursor.close and conn.close exist
    mock_cursor.close = Mock()
    mock_conn.close = Mock()

    # The route now uses user_db_scope(current_user.id) instead of get_db_connection().
    # Mock user_db_scope to yield the mock connection, and disable login enforcement.
    @contextmanager
    def mock_user_db_scope(user_id):
        yield mock_conn

    mock_user = Mock()
    mock_user.id = "demo_openmrg"

    monkeypatch.setattr(wm, "user_db_scope", mock_user_db_scope)
    monkeypatch.setattr(wm, "current_user", mock_user)
    monkeypatch.setitem(wm.app.config, "LOGIN_DISABLED", True)

    client = wm.app.test_client()
    resp = client.get("/api/cml-stats")
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)
    assert len(data) == 1
    row = data[0]
    assert row["cml_id"] == "10001"
    assert row["completeness_percent"] == 94.2
    assert row["completeness_percent_1h"] == 90.0
    assert row["stddev_last_60min"] == 1.3
    assert row["last_rsl"] == -45.0
