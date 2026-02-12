import sys
from unittest.mock import Mock

import pytest


# Ensure optional heavy imports won't fail at import time
sys.modules.setdefault("folium", Mock())
sys.modules.setdefault("altair", Mock())
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

    # Row fields: cml_id, total_records, valid_records, null_records, completeness_percent,
    # min_rsl, max_rsl, mean_rsl, stddev_rsl, last_rsl, stddev_last_60min
    mock_cursor.fetchall.return_value = [
        (
            "10001",
            10,
            9,
            1,
            90.0,
            -60.0,
            -40.0,
            -50.0,
            3.0,
            -45.0,
            1.5,
        )
    ]

    # Ensure cursor.close and conn.close exist
    mock_cursor.close = Mock()
    mock_conn.close = Mock()

    monkeypatch.setattr(wm, "get_db_connection", lambda: mock_conn)

    client = wm.app.test_client()
    resp = client.get("/api/cml-stats")
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)
    assert len(data) == 1
    row = data[0]
    assert row["cml_id"] == "10001"
    assert row["completeness_percent"] == 90.0
    assert row["last_rsl"] == -45.0
