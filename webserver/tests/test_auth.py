import os
import sys
from unittest.mock import Mock

import pytest
from werkzeug.security import generate_password_hash

# Stub optional heavy imports before main.py is loaded
sys.modules.setdefault("folium", Mock())
sys.modules.setdefault("requests", Mock())

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import main as wm  # noqa: E402

_PROTECTED_ROUTES = ["/", "/realtime", "/api/cml-stats", "/api/cml-metadata"]


@pytest.fixture
def client():
    wm.app.config["TESTING"] = True
    return wm.app.test_client()


@pytest.fixture
def test_user(monkeypatch):
    monkeypatch.setitem(
        wm.USERS,
        "testuser",
        {"password_hash": generate_password_hash("testpass"), "display_name": "Test"},
    )
    return "testuser", "testpass"


def test_login_page_accessible(client):
    assert client.get("/login").status_code == 200


@pytest.mark.parametrize("path", _PROTECTED_ROUTES)
def test_protected_routes_redirect_unauthenticated(client, path):
    resp = client.get(path)
    assert resp.status_code == 302
    assert "login" in resp.headers["Location"]


def test_login_valid_credentials_redirects(client, test_user):
    username, password = test_user
    resp = client.post("/login", data={"username": username, "password": password})
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/")


def test_login_wrong_password_stays_on_login(client, test_user):
    username, _ = test_user
    resp = client.post("/login", data={"username": username, "password": "wrong"})
    assert resp.status_code == 200


def test_login_unknown_user_stays_on_login(client):
    resp = client.post("/login", data={"username": "nobody", "password": "x"})
    assert resp.status_code == 200


def test_login_open_redirect_blocked(client, test_user):
    username, password = test_user
    resp = client.post(
        "/login?next=https://evil.com",
        data={"username": username, "password": password},
    )
    assert resp.status_code == 302
    assert "evil.com" not in resp.headers["Location"]


def test_logout_redirects_to_login(client):
    resp = client.get("/logout")
    assert resp.status_code == 302
    assert "login" in resp.headers["Location"]


def test_logout_when_authenticated_clears_session(client, test_user):
    username, password = test_user
    client.post("/login", data={"username": username, "password": password})
    resp = client.get("/logout")
    assert resp.status_code == 302
    assert "login" in resp.headers["Location"]
    # Session is cleared: next protected request redirects again
    assert client.get("/").status_code == 302
