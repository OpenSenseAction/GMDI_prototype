"""Tests for cml_stats_history snapshot functionality."""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timezone


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
    cursor.fetchone.return_value = (42,)  # Mock row count
    cursor.rowcount = 42
    return conn


def test_write_stats_snapshot_success(mock_connection):
    """Test write_stats_snapshot materializes snapshot correctly."""
    writer = DBWriter("postgresql://test", user_id="demo_openmrg")
    writer.conn = mock_connection
    
    at_time = datetime(2026, 7, 25, 14, 30, 0, tzinfo=timezone.utc)
    rows = writer.write_stats_snapshot(at_time)
    
    assert rows == 42
    mock_connection.cursor.assert_called_once()
    cursor = mock_connection.cursor.return_value
    cursor.execute.assert_called_once()
    
    # Verify the call uses hour-truncated time
    call_args = cursor.execute.call_args[0]
    sql = call_args[0]
    params = call_args[1]
    
    assert "materialize_cml_stats_snapshot" in sql
    assert params[1] == "demo_openmrg"
    # Check that time is truncated to hour
    assert params[0].minute == 0
    assert params[0].second == 0
    assert params[0].microsecond == 0


def test_write_stats_snapshot_rollback_on_error(mock_connection):
    """Test write_stats_snapshot rolls back on error."""
    writer = DBWriter("postgresql://test", user_id="demo_openmrg")
    writer.conn = mock_connection
    
    mock_connection.cursor.return_value.execute.side_effect = Exception("DB error")
    
    with pytest.raises(Exception, match="DB error"):
        writer.write_stats_snapshot(datetime.now(tz=timezone.utc))
    
    mock_connection.rollback.assert_called_once()


def test_write_provisional_snapshot_success(mock_connection):
    """Test write_provisional_snapshot writes current hour snapshot."""
    writer = DBWriter("postgresql://test", user_id="demo_openmrg")
    writer.conn = mock_connection
    
    with patch('parser.db_writer.datetime') as mock_datetime:
        mock_now = datetime(2026, 7, 25, 14, 30, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_now
        mock_datetime.timezone = timezone
        
        rows = writer.write_provisional_snapshot()
    
    assert rows == 42
    cursor = mock_connection.cursor.return_value
    cursor.execute.assert_called_once()
    
    call_args = cursor.execute.call_args[0]
    sql = call_args[0]
    params = call_args[1]
    
    assert "INSERT INTO cml_stats_history" in sql
    assert "is_provisional" in sql
    assert params[1] == "demo_openmrg"
    # Check that snapshot_time is hour-truncated
    assert params[0].minute == 0
    assert params[0].second == 0


def test_write_provisional_snapshot_uses_cml_stats_table(mock_connection):
    """Test provisional snapshot copies from cml_stats table."""
    writer = DBWriter("postgresql://test", user_id="demo_openmrg")
    writer.conn = mock_connection
    
    writer.write_provisional_snapshot()
    
    cursor = mock_connection.cursor.return_value
    sql = cursor.execute.call_args[0][0]
    
    # Should select from cml_stats
    assert "FROM cml_stats" in sql
    # Should have ON CONFLICT clause
    assert "ON CONFLICT" in sql
    # Should set is_provisional=TRUE
    assert "is_provisional          = TRUE" in sql


def test_write_stats_snapshot_with_naive_datetime(mock_connection):
    """Test write_stats_snapshot handles naive datetime by making it UTC."""
    writer = DBWriter("postgresql://test", user_id="demo_openmrg")
    writer.conn = mock_connection
    
    # Naive datetime (no timezone)
    at_time = datetime(2026, 7, 25, 14, 30, 0)
    rows = writer.write_stats_snapshot(at_time)
    
    assert rows == 42
    cursor = mock_connection.cursor.return_value
    params = cursor.execute.call_args[0][1]
    # Should be converted to UTC
    assert params[0].tzinfo == timezone.utc
