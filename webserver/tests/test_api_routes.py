import os
import sys
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import Mock

import pytest

sys.modules.setdefault("folium", Mock())
sys.modules.setdefault("requests", Mock())

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import main as wm  # noqa: E402


@pytest.fixture
def auth_client(monkeypatch):
    """Test client with login bypassed and user_db_scope mocked."""
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


def test_api_cml_metadata_returns_cmls(auth_client):
    client, cursor = auth_client
    cursor.fetchall.return_value = [("CML01", 10.0, 5.0, 10.5, 5.5)]
    resp = client.get("/api/cml-metadata")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["cmls"]) == 1
    assert data["cmls"][0]["id"] == "CML01"
    assert data["cmls"][0]["site_0_lon"] == 10.0


def test_api_cml_metadata_empty(auth_client):
    client, cursor = auth_client
    cursor.fetchall.return_value = []
    resp = client.get("/api/cml-metadata")
    assert resp.status_code == 200
    assert resp.get_json() == {"cmls": []}


def test_api_cml_map_returns_list(auth_client):
    client, cursor = auth_client
    cursor.fetchall.return_value = [("CML01", 10.0, 5.0, 10.5, 5.5)]
    resp = client.get("/api/cml-map")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data[0]["cml_id"] == "CML01"
    assert data[0]["site_0"] == {"lon": 10.0, "lat": 5.0}
    assert data[0]["site_1"] == {"lon": 10.5, "lat": 5.5}


def test_api_data_time_range_with_data(auth_client):
    client, cursor = auth_client
    dt1 = datetime(2025, 1, 1, 0, 0)
    dt2 = datetime(2025, 12, 31, 0, 0)
    cursor.fetchone.return_value = (dt1, dt2)
    resp = client.get("/api/data-time-range")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["earliest"] == dt1.isoformat()
    assert data["latest"] == dt2.isoformat()


def test_api_data_time_range_no_data(auth_client):
    client, cursor = auth_client
    cursor.fetchone.return_value = (None, None)
    resp = client.get("/api/data-time-range")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["earliest"] is None
    assert data["latest"] is None
