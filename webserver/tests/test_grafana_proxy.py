import os
import sys
from unittest.mock import Mock

import pytest
from werkzeug.security import generate_password_hash

sys.modules.setdefault("folium", Mock())
sys.modules.setdefault("requests", Mock())

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import main as wm  # noqa: E402


def _make_grafana_response(status=200, content=b"ok", headers=None):
    """Build a minimal requests.Response-like mock."""
    resp = Mock()
    resp.status_code = status
    resp.content = content
    resp.headers = headers or {"Content-Type": "text/plain"}
    return resp


@pytest.fixture
def logged_in_client(monkeypatch):
    """Test client with demo_openmrg actually logged in via the login route.

    Uses a real session so flask_login's current_user proxy resolves correctly
    inside the grafana_proxy route handler.
    """
    monkeypatch.setitem(
        wm.USERS,
        "demo_openmrg",
        {
            "password_hash": generate_password_hash("testpass"),
            "display_name": "OpenMRG",
        },
    )
    wm.app.config["TESTING"] = True
    client = wm.app.test_client()
    client.post("/login", data={"username": "demo_openmrg", "password": "testpass"})
    return client


def test_grafana_proxy_injects_webauth_user_header(logged_in_client, monkeypatch):
    """X-WEBAUTH-USER must be set to the logged-in user's id."""
    mock_requests = Mock()
    mock_requests.request.return_value = _make_grafana_response()
    monkeypatch.setattr(wm, "requests", mock_requests)

    logged_in_client.get("/grafana/")

    _, kwargs = mock_requests.request.call_args
    assert kwargs["headers"]["X-WEBAUTH-USER"] == "demo_openmrg"


def test_grafana_proxy_strips_client_webauth_user_header(logged_in_client, monkeypatch):
    """A browser-supplied X-WEBAUTH-USER must be removed before forwarding."""
    mock_requests = Mock()
    mock_requests.request.return_value = _make_grafana_response()
    monkeypatch.setattr(wm, "requests", mock_requests)

    logged_in_client.get("/grafana/", headers={"X-WEBAUTH-USER": "attacker"})

    _, kwargs = mock_requests.request.call_args
    # The injected value must be the server-controlled user id, not the
    # attacker-supplied value.  The case-insensitive strip must have fired.
    assert kwargs["headers"]["X-WEBAUTH-USER"] == "demo_openmrg"


def test_grafana_proxy_unauthenticated_redirects_to_login():
    """Unauthenticated requests must not reach Grafana at all."""
    wm.app.config["TESTING"] = True
    client = wm.app.test_client()
    resp = client.get("/grafana/")
    assert resp.status_code == 302
    assert "login" in resp.headers["Location"]


def test_grafana_proxy_forwards_path(logged_in_client, monkeypatch):
    """The full subpath must be forwarded to the Grafana container URL."""
    mock_requests = Mock()
    mock_requests.request.return_value = _make_grafana_response()
    monkeypatch.setattr(wm, "requests", mock_requests)

    logged_in_client.get("/grafana/d/abc123/my-dashboard")

    args, _ = mock_requests.request.call_args
    assert args[1] == "http://grafana:3000/grafana/d/abc123/my-dashboard"
