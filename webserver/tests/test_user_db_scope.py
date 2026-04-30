import os
import sys
from unittest.mock import Mock, MagicMock

import pytest
from psycopg2 import sql as pgsql

# Stub optional heavy imports before main.py is loaded
sys.modules.setdefault("folium", Mock())
sys.modules.setdefault("requests", Mock())

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import main as wm  # noqa: E402


def test_unknown_user_raises_before_connecting(monkeypatch):
    mock_connect = Mock()
    monkeypatch.setattr(wm.psycopg2, "connect", mock_connect)
    monkeypatch.setattr(wm, "USERS", {"known": {}})

    with pytest.raises(ValueError, match="Unknown user_id"):
        with wm.user_db_scope("injected_role"):
            pass  # pragma: no cover

    mock_connect.assert_not_called()


def test_exception_inside_scope_triggers_rollback(monkeypatch):
    mock_conn = MagicMock()
    monkeypatch.setattr(wm.psycopg2, "connect", Mock(return_value=mock_conn))
    monkeypatch.setattr(wm, "USERS", {"myuser": {}})

    with pytest.raises(RuntimeError):
        with wm.user_db_scope("myuser"):
            raise RuntimeError("boom")

    mock_conn.rollback.assert_called_once()
    mock_conn.commit.assert_not_called()


def test_set_local_role_uses_sql_identifier(monkeypatch):
    """SET LOCAL ROLE must use pgsql.Identifier, not string interpolation."""
    mock_conn = MagicMock()
    monkeypatch.setattr(wm.psycopg2, "connect", Mock(return_value=mock_conn))
    monkeypatch.setattr(wm, "USERS", {"myuser": {}})

    with wm.user_db_scope("myuser"):
        pass

    cur = mock_conn.cursor.return_value.__enter__.return_value
    call_arg = cur.execute.call_args[0][0]
    assert isinstance(call_arg, pgsql.Composable)
