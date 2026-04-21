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

    # Build proper DataArray mocks for RSL and TSL.
    # The refactored code uses ds["rsl"] (item access) and inspects .dims / .ndim / .values,
    # so we need explicit mocks rather than relying on MagicMock attribute auto-creation.
    # Dimension order: (sublink_id, cml_id, time) — the OpenMRG layout.
    rsl_values = np.random.rand(2, 2, 10)  # (sublink_id, cml_id, time)
    mock_rsl = MagicMock()
    mock_rsl.dims = ("sublink_id", "cml_id", "time")
    mock_rsl.ndim = 3
    mock_rsl.values = rsl_values
    mock_rsl.isel.return_value.values = rsl_values  # isel returns same-shaped slice

    tsl_values = np.random.rand(2, 2, 10)
    mock_tsl = MagicMock()
    mock_tsl.dims = ("sublink_id", "cml_id", "time")
    mock_tsl.ndim = 3
    mock_tsl.values = tsl_values
    mock_tsl.isel.return_value.values = tsl_values

    # Mock minimal NetCDF dataset
    mock_ds = MagicMock()
    mock_ds.cml_id.values = np.array([101, 102])
    mock_ds.sublink_id.values = np.array([0, 1])
    mock_ds.site_0_lon.values = np.array([10.0, 11.0])
    mock_ds.site_0_lat.values = np.array([50.0, 51.0])
    mock_ds.site_1_lon.values = np.array([10.1, 11.1])
    mock_ds.site_1_lat.values = np.array([50.1, 51.1])
    mock_ds.frequency.values = np.array([[20, 21], [22, 23]])
    mock_ds.frequency.dims = ("cml_id", "sublink_id")
    mock_ds.time.values = pd.date_range("2024-01-01", periods=10, freq="10s")
    mock_ds.sizes = {"sublink_id": 2, "cml_id": 2}
    mock_ds.__getitem__ = MagicMock(
        side_effect=lambda key: mock_rsl if key == "rsl" else mock_tsl
    )
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


@patch("parser.parse_netcdf_archive.os.path.exists")
def test_download_netcdf_skips_when_file_exists(mock_exists):
    """download_netcdf() does nothing when the file is already present."""
    from parser.parse_netcdf_archive import download_netcdf
    import urllib.request

    mock_exists.return_value = True
    with patch.object(urllib.request, "urlretrieve") as mock_retrieve:
        download_netcdf("http://example.com/file.nc", "/tmp/file.nc")
        mock_retrieve.assert_not_called()


@patch("parser.parse_netcdf_archive.os.path.exists")
def test_download_netcdf_downloads_when_missing(mock_exists):
    """download_netcdf() calls urlretrieve when file is absent."""
    from parser.parse_netcdf_archive import download_netcdf
    import urllib.request

    mock_exists.return_value = False
    with patch.object(urllib.request, "urlretrieve") as mock_retrieve:
        download_netcdf("http://example.com/file.nc", "/tmp/file.nc")
        mock_retrieve.assert_called_once()
        args = mock_retrieve.call_args[0]
        assert args[0] == "http://example.com/file.nc"
        assert args[1] == "/tmp/file.nc"


@patch("parser.parse_netcdf_archive.NETCDF_URL", "")
@patch("parser.parse_netcdf_archive.os.path.exists")
def test_main_exits_when_file_missing_and_no_url(mock_exists):
    """main() calls sys.exit when NetCDF file is absent and no download URL is set."""
    from parser.parse_netcdf_archive import main

    mock_exists.return_value = False
    with pytest.raises(SystemExit):
        main()


@patch("parser.parse_netcdf_archive.xr.open_dataset")
@patch("parser.parse_netcdf_archive.os.path.exists")
def test_main_exits_on_open_dataset_error(mock_exists, mock_open_dataset):
    """main() calls sys.exit when xr.open_dataset raises an exception."""
    from parser.parse_netcdf_archive import main

    mock_exists.return_value = True
    mock_open_dataset.side_effect = Exception("corrupt file")
    with pytest.raises(SystemExit):
        main()
