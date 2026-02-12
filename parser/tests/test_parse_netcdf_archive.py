import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np


@patch("parser.parse_netcdf_archive.psycopg2.connect")
@patch("parser.parse_netcdf_archive.xr.open_dataset")
@patch("parser.parse_netcdf_archive.os.path.exists")
def test_main_clears_and_loads_data(mock_exists, mock_open_dataset, mock_connect):
    """Test main() truncates tables and loads new archive data."""
    from parser.parse_netcdf_archive import main

    mock_exists.return_value = True

    # Mock minimal NetCDF dataset
    mock_ds = MagicMock()
    mock_ds.cml_id.values = np.array([101, 102])
    mock_ds.site_0_lon.values = np.array([10.0, 11.0])
    mock_ds.site_0_lat.values = np.array([50.0, 51.0])
    mock_ds.site_1_lon.values = np.array([10.1, 11.1])
    mock_ds.site_1_lat.values = np.array([50.1, 51.1])
    mock_ds.frequency.values = np.array([[20, 21], [22, 23]])
    mock_ds.polarization.values = np.array([["H", "V"], ["V", "H"]])
    mock_ds.time.values = pd.date_range("2024-01-01", periods=10, freq="10s")
    mock_ds.sizes = {"sublink_id": 2, "cml_id": 2}
    mock_ds.tsl.isel.return_value.values = np.random.rand(10, 2, 2)
    mock_ds.rsl.isel.return_value.values = np.random.rand(10, 2, 2)
    mock_open_dataset.return_value = mock_ds

    # Mock database
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (
        pd.Timestamp("2024-01-01"),
        pd.Timestamp("2024-01-02"),
        1000,
    )
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    main()

    # Verify truncate is called (critical for demo setup)
    mock_cursor.execute.assert_any_call("TRUNCATE TABLE cml_data")
    mock_cursor.execute.assert_any_call("TRUNCATE TABLE cml_metadata")


@patch("parser.parse_netcdf_archive.psycopg2.connect")
def test_main_fails_on_db_error(mock_connect):
    """Test main() handles database connection errors."""
    from parser.parse_netcdf_archive import main

    mock_connect.side_effect = Exception("Connection refused")

    with patch("parser.parse_netcdf_archive.os.path.exists", return_value=True):
        with patch("parser.parse_netcdf_archive.xr.open_dataset"):
            with pytest.raises(SystemExit):
                main()
