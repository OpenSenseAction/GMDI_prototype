"""Tests for historical time slider API endpoints."""

import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest


# Ensure optional heavy imports won't fail at import time
sys.modules.setdefault("folium", Mock())
sys.modules.setdefault("requests", Mock())


def test_api_cml_stats_historical_with_at_parameter(monkeypatch):
    """Test /api/cml-stats with ?at= parameter queries cml_stats_history."""
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    import main as wm
    
    # Prepare mock DB connection and cursor
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Historical query returns 10 columns including is_provisional
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
            False,  # is_provisional
        )
    ]
    
    mock_cursor.close = Mock()
    mock_conn.close = Mock()
    
    @contextmanager
    def mock_user_db_scope(user_id):
        yield mock_conn
    
    mock_user = Mock()
    mock_user.id = "demo_openmrg"
    
    monkeypatch.setattr(wm, "user_db_scope", mock_user_db_scope)
    monkeypatch.setattr(wm, "current_user", mock_user)
    monkeypatch.setitem(wm.app.config, "LOGIN_DISABLED", True)
    
    client = wm.app.test_client()
    
    # Test with historical timestamp
    resp = client.get("/api/cml-stats?at=2026-07-25T14:00:00Z")
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)
    assert len(data) == 1
    row = data[0]
    assert row["cml_id"] == "10001"
    assert row["is_provisional"] == False
    
    # Verify get_cml_stats_at function was called
    cursor = mock_conn.cursor.return_value
    sql = cursor.execute.call_args[0][0]
    assert "get_cml_stats_at" in sql


def test_api_cml_stats_invalid_at_parameter(monkeypatch):
    """Test /api/cml-stats with invalid ?at= parameter returns 400."""
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    import main as wm
    
    mock_user = Mock()
    mock_user.id = "demo_openmrg"
    
    monkeypatch.setattr(wm, "current_user", mock_user)
    monkeypatch.setitem(wm.app.config, "LOGIN_DISABLED", True)
    
    client = wm.app.test_client()
    
    # Invalid timestamp format
    resp = client.get("/api/cml-stats?at=invalid-timestamp")
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_api_cml_stats_time_range_endpoint(monkeypatch):
    """Test /api/cml-stats-time-range endpoint."""
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    import main as wm
    
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Return minimum snapshot time from history
    min_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    mock_cursor.fetchone.return_value = (min_time,)
    mock_cursor.close = Mock()
    mock_conn.close = Mock()
    
    @contextmanager
    def mock_user_db_scope(user_id):
        yield mock_conn
    
    mock_user = Mock()
    mock_user.id = "demo_openmrg"
    
    monkeypatch.setattr(wm, "user_db_scope", mock_user_db_scope)
    monkeypatch.setattr(wm, "current_user", mock_user)
    monkeypatch.setitem(wm.app.config, "LOGIN_DISABLED", True)
    
    client = wm.app.test_client()
    resp = client.get("/api/cml-stats-time-range")
    
    assert resp.status_code == 200
    data = resp.get_json()
    assert "min" in data
    assert "max" in data
    assert data["min"] is not None
    
    # Verify query uses cml_stats_history
    cursor = mock_conn.cursor.return_value
    sql = cursor.execute.call_args[0][0]
    assert "cml_stats_history" in sql
    assert "MIN(snapshot_time)" in sql


def test_api_cml_stats_time_range_no_history(monkeypatch):
    """Test /api/cml-stats-time-range when no history exists."""
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    import main as wm
    
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    
    # No history available
    mock_cursor.fetchone.return_value = (None,)
    mock_cursor.close = Mock()
    
    @contextmanager
    def mock_user_db_scope(user_id):
        yield mock_conn
    
    mock_user = Mock()
    mock_user.id = "demo_openmrg"
    
    monkeypatch.setattr(wm, "user_db_scope", mock_user_db_scope)
    monkeypatch.setattr(wm, "current_user", mock_user)
    monkeypatch.setitem(wm.app.config, "LOGIN_DISABLED", True)
    
    client = wm.app.test_client()
    resp = client.get("/api/cml-stats-time-range")
    
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["min"] is None
    # max should still be current hour
    assert data["max"] is not None
