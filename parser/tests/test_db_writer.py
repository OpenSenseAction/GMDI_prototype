"""Tests for DBWriter class."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import sys

# Skip all tests if psycopg2 not available
psycopg2 = pytest.importorskip("psycopg2", reason="psycopg2 not installed")

from ..db_writer import DBWriter


@pytest.fixture
def mock_connection():
    """Mock psycopg2 connection."""
    conn = Mock()
    conn.closed = False
    cursor = Mock()
    conn.cursor.return_value = cursor
    cursor.__enter__ = Mock(return_value=cursor)
    cursor.__exit__ = Mock(return_value=False)
    return conn


def test_dbwriter_connect_success():
    """Test successful database connection."""
    with patch("parser.db_writer.psycopg2.connect") as mock_connect:
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        writer = DBWriter("postgresql://test")
        writer.connect()

        assert writer.is_connected()
        mock_connect.assert_called_once()


def test_dbwriter_connect_retry_then_success():
    """Test connection retry logic succeeds on second attempt."""
    with patch("parser.db_writer.psycopg2.connect") as mock_connect:
        mock_connect.side_effect = [
            Exception("Connection failed"),
            Mock(),  # Success on second attempt
        ]

        with patch("parser.db_writer.time.sleep"):  # Skip actual sleep
            writer = DBWriter("postgresql://test")
            writer.connect()

            assert writer.is_connected()
            assert mock_connect.call_count == 2


def test_dbwriter_connect_all_retries_fail():
    """Test connection fails after max retries."""
    with patch("parser.db_writer.psycopg2.connect") as mock_connect:
        mock_connect.side_effect = Exception("Connection failed")

        with patch("parser.db_writer.time.sleep"):
            writer = DBWriter("postgresql://test")

            with pytest.raises(Exception, match="Connection failed"):
                writer.connect()

            assert mock_connect.call_count == 3  # max_retries


def test_dbwriter_already_connected_skips_reconnect():
    """Test that connect() does nothing if already connected."""
    with patch("parser.db_writer.psycopg2.connect") as mock_connect:
        mock_connect.return_value = Mock()

        writer = DBWriter("postgresql://test")
        writer.connect()
        writer.connect()  # Second call

        mock_connect.assert_called_once()


def test_write_metadata_empty_dataframe(mock_connection):
    """Test write_metadata with empty DataFrame returns 0."""
    writer = DBWriter("postgresql://test")
    writer.conn = mock_connection

    result = writer.write_metadata(pd.DataFrame())
    assert result == 0

    result = writer.write_metadata(None)
    assert result == 0


def test_write_metadata_not_connected():
    """Test write_metadata raises error when not connected."""
    writer = DBWriter("postgresql://test")
    df = pd.DataFrame({"cml_id": ["123"], "site_0_lon": [13.4]})

    with pytest.raises(RuntimeError, match="Not connected"):
        writer.write_metadata(df)


def test_write_metadata_success(mock_connection):
    """Test successful metadata write."""
    writer = DBWriter("postgresql://test")
    writer.conn = mock_connection

    df = pd.DataFrame(
        {
            "cml_id": ["123", "456"],
            "sublink_id": ["sublink_1", "sublink_2"],
            "site_0_lon": [13.4, 13.5],
            "site_0_lat": [52.5, 52.6],
            "site_1_lon": [13.6, 13.7],
            "site_1_lat": [52.7, 52.8],
            "frequency": [38.0, 38.5],
            "polarization": ["H", "V"],
        }
    )

    with patch("parser.db_writer.psycopg2.extras.execute_values") as mock_exec:
        result = writer.write_metadata(df)

        assert result == 2
        mock_exec.assert_called_once()
        mock_connection.commit.assert_called_once()


def test_write_rawdata_success(mock_connection):
    """Test successful raw data write."""
    writer = DBWriter("postgresql://test")
    writer.conn = mock_connection

    df = pd.DataFrame(
        {
            "time": pd.to_datetime(["2026-01-22 10:00:00", "2026-01-22 10:01:00"]),
            "cml_id": ["123", "456"],
            "sublink_id": ["A", "B"],
            "rsl": [-45.0, -46.0],
            "tsl": [1.0, 2.0],
        }
    )

    with patch("parser.db_writer.psycopg2.extras.execute_values") as mock_exec:
        result = writer.write_rawdata(df)

        assert result == 2
        mock_exec.assert_called_once()
        mock_connection.commit.assert_called_once()


def test_write_rawdata_with_nan_sublink(mock_connection):
    """Test raw data write handles NaN in sublink_id."""
    writer = DBWriter("postgresql://test")
    writer.conn = mock_connection

    df = pd.DataFrame(
        {
            "time": pd.to_datetime(["2026-01-22 10:00:00"]),
            "cml_id": ["123"],
            "sublink_id": [float("nan")],
            "rsl": [-45.0],
            "tsl": [1.0],
        }
    )

    with patch("parser.db_writer.psycopg2.extras.execute_values") as mock_exec:
        result = writer.write_rawdata(df)
        assert result == 1


def test_validate_rawdata_references_empty():
    """Test validation with empty DataFrame."""
    writer = DBWriter("postgresql://test")
    ok, missing = writer.validate_rawdata_references(pd.DataFrame())
    assert ok is True
    assert missing == []


def test_validate_rawdata_references_with_missing(mock_connection):
    """Test validation detects missing metadata IDs."""
    writer = DBWriter("postgresql://test")
    writer.conn = mock_connection

    # Mock database has only ("123", "sublink_1")
    cursor = mock_connection.cursor.return_value
    cursor.fetchall.return_value = [("123", "sublink_1")]

    df = pd.DataFrame(
        {
            "cml_id": ["123", "123", "456", "789"],
            "sublink_id": ["sublink_1", "sublink_2", "sublink_1", "sublink_1"],
        }
    )

    ok, missing = writer.validate_rawdata_references(df)

    assert ok is False
    assert set(missing) == {
        ("123", "sublink_2"),
        ("456", "sublink_1"),
        ("789", "sublink_1"),
    }


def test_close_connection(mock_connection):
    """Test closing database connection."""
    writer = DBWriter("postgresql://test")
    writer.conn = mock_connection

    writer.close()

    mock_connection.close.assert_called_once()
    assert writer.conn is None


def test_close_already_closed():
    """Test closing when connection is None."""
    writer = DBWriter("postgresql://test")
    writer.conn = None

    writer.close()  # Should not raise
    assert writer.conn is None
