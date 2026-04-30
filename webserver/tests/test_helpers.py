import os
import sys
from unittest.mock import Mock

sys.modules.setdefault("folium", Mock())
sys.modules.setdefault("requests", Mock())

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import main as wm  # noqa: E402


# ── safe_float ────────────────────────────────────────────────────────────────


def test_safe_float_none_returns_none():
    assert wm.safe_float(None) is None


def test_safe_float_valid_number():
    assert wm.safe_float(3.14) == 3.14
    assert wm.safe_float("2.5") == 2.5


def test_safe_float_non_numeric_returns_none():
    assert wm.safe_float("abc") is None


def test_safe_float_nan_returns_none():
    assert wm.safe_float(float("nan")) is None


def test_safe_float_inf_returns_none():
    assert wm.safe_float(float("inf")) is None


# ── load_user ─────────────────────────────────────────────────────────────────


def test_load_user_known(monkeypatch):
    monkeypatch.setitem(wm.USERS, "alice", {"display_name": "Alice"})
    user = wm.load_user("alice")
    assert user is not None
    assert user.id == "alice"
    assert user.display_name == "Alice"


def test_load_user_unknown(monkeypatch):
    monkeypatch.setattr(wm, "USERS", {"alice": {}})
    assert wm.load_user("nobody") is None
