import os
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
from werkzeug.security import generate_password_hash

sys.modules.setdefault("folium", Mock())
sys.modules.setdefault("requests", Mock())

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import main as wm  # noqa: E402

_ADMIN_PAGES = ["/admin/realtime", "/admin/archive"]
_ADMIN_APIS = [
    "/api/admin/cml-map",
    "/api/admin/cml-stats",
    "/api/admin/archive-timeseries",
]


@pytest.fixture
def cursor(monkeypatch):
    """Mock cursor behind admin_db_scope."""
    cur = Mock()
    conn = Mock()
    conn.cursor.return_value = cur

    @contextmanager
    def scope():
        yield conn

    monkeypatch.setattr(wm, "admin_db_scope", scope)
    return cur


@pytest.fixture
def users(monkeypatch):
    monkeypatch.setattr(
        wm,
        "USERS",
        {
            "tenant_a": {
                "password_hash": generate_password_hash("pw"),
                "display_name": "Tenant A",
            },
            "tenant_b": {
                "password_hash": generate_password_hash("pw"),
                "display_name": "Tenant B",
            },
            "boss": {
                "password_hash": generate_password_hash("pw"),
                "display_name": "Boss",
                "is_admin": True,
            },
        },
    )


def _login(username):
    """Authenticated client; sets the session directly to bypass the login rate limit."""
    wm.app.config["TESTING"] = True
    client = wm.app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = username
        sess["_fresh"] = True
    return client


@pytest.mark.parametrize("path", _ADMIN_PAGES + _ADMIN_APIS)
def test_admin_routes_redirect_unauthenticated(path):
    wm.app.config["TESTING"] = True
    resp = wm.app.test_client().get(path)
    assert resp.status_code == 302
    assert "login" in resp.headers["Location"]


@pytest.mark.parametrize("path", _ADMIN_PAGES + _ADMIN_APIS)
def test_admin_routes_forbidden_for_tenant(users, cursor, path):
    resp = _login("tenant_a").get(path)
    assert resp.status_code == 403
    cursor.execute.assert_not_called()


def test_admin_overview_redirects_to_admin_map(users):
    resp = _login("boss").get("/")
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/admin/realtime")


def test_user_db_scope_rejects_admin_account(users, monkeypatch):
    mock_connect = Mock()
    monkeypatch.setattr(wm.psycopg2, "connect", mock_connect)
    with pytest.raises(ValueError, match="no tenant data role"):
        with wm.user_db_scope("boss"):
            pass  # pragma: no cover
    mock_connect.assert_not_called()


def test_admin_realtime_page_lists_providers_not_admins(users):
    html = _login("boss").get("/admin/realtime").get_data(as_text=True)
    assert "Tenant A" in html
    assert "Tenant B" in html
    assert '"tenant_a"' in html
    assert '"boss"' not in html


def test_admin_realtime_embeds_grafana_when_org_assigned(users, monkeypatch):
    monkeypatch.setitem(wm.USERS["boss"], "grafana_org_id", 3)
    html = _login("boss").get("/admin/realtime").get_data(as_text=True)
    assert "/grafana/d/cml-admin-realtime/" in html
    assert "orgId=3" in html


def test_admin_realtime_without_grafana_org_has_map_only(users):
    html = _login("boss").get("/admin/realtime").get_data(as_text=True)
    assert 'id="grafana-panel"' not in html


def test_admin_cml_map_returns_all_providers(users, cursor):
    sublinks = [{"frequency": 28000.0, "polarization": "v"}]
    cursor.fetchall.return_value = [
        ("tenant_a", "C1", 10.0, 50.0, 10.1, 50.1, 691.4, sublinks),
        ("tenant_b", "C1", 11.0, 51.0, 11.1, 51.1, None, None),
    ]
    resp = _login("boss").get("/api/admin/cml-map")
    assert resp.status_code == 200
    data = resp.get_json()
    assert [d["user_id"] for d in data] == ["tenant_a", "tenant_b"]
    assert data[0]["site_0"] == {"lon": 10.0, "lat": 50.0}
    assert data[0]["length"] == 691.4
    assert data[0]["sublinks"] == sublinks
    assert data[1]["length"] is None
    assert data[1]["sublinks"] == []


def test_admin_cml_stats_returns_all_providers(users, cursor):
    cursor.fetchall.return_value = [
        ("tenant_a", "C1", 95.0, 360, 342, 1.5, -42.0),
        ("tenant_b", "C1", None, None, None, float("nan"), None),
    ]
    resp = _login("boss").get("/api/admin/cml-stats")
    assert resp.status_code == 200
    data = resp.get_json()
    assert [d["user_id"] for d in data] == ["tenant_a", "tenant_b"]
    assert data[0]["stddev_last_60min"] == 1.5
    assert data[1]["stddev_last_60min"] is None
    assert data[1]["total_records"] == 0


def test_admin_cml_map_db_error(users, monkeypatch):
    @contextmanager
    def broken():
        raise RuntimeError("down")
        yield  # pragma: no cover

    monkeypatch.setattr(wm, "admin_db_scope", broken)
    assert _login("boss").get("/api/admin/cml-map").status_code == 500


def test_admin_timeseries_groups_by_provider(users, cursor):
    day = datetime(2026, 1, 2, tzinfo=timezone.utc)
    cursor.fetchall.return_value = [
        ("tenant_a", day, 100, -50.5),
        ("tenant_a", datetime(2026, 1, 3, tzinfo=timezone.utc), 90, None),
        ("tenant_b", day, 7, float("nan")),
    ]
    resp = _login("boss").get("/api/admin/archive-timeseries?days=7")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["tenant_a"] == {
        "days": ["2026-01-02", "2026-01-03"],
        "records": [100, 90],
        "mean_rsl": [-50.5, None],
    }
    assert data["tenant_b"]["mean_rsl"] == [None]
    assert cursor.execute.call_args[0][1] == (7,)


@pytest.mark.parametrize("value", ["0", "9999", "abc", "-1"])
def test_admin_timeseries_invalid_days_falls_back(users, cursor, value):
    cursor.fetchall.return_value = []
    resp = _login("boss").get(f"/api/admin/archive-timeseries?days={value}")
    assert resp.status_code == 200
    assert cursor.execute.call_args[0][1] == (30,)


def test_admin_archive_page_merges_summary(users, cursor):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 5, tzinfo=timezone.utc)
    cursor.fetchall.side_effect = [
        [("tenant_a", 3)],
        [("tenant_a", 12345)],
        [("tenant_a", start, end)],
    ]
    html = _login("boss").get("/admin/archive").get_data(as_text=True)
    assert "12,345" in html
    assert "2026-01-01" in html
    assert "no data" in html  # tenant_b has no data
