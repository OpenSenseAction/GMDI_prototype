import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))


@patch("generate_archive.Path.mkdir")
@patch("generate_archive.Path.exists")
@patch("generate_archive.CMLDataGenerator")
@patch("generate_archive.gzip.open", new_callable=mock_open)
def test_generate_archive_creates_gzipped_files(
    mock_gzip, mock_generator_class, mock_exists, mock_mkdir
):
    """Test generate_archive_data() creates compressed metadata and data files."""
    from generate_archive import generate_archive_data

    mock_exists.return_value = True

    mock_generator = MagicMock()
    mock_generator_class.return_value = mock_generator
    mock_generator.get_metadata_dataframe.return_value = pd.DataFrame(
        {
            "cml_id": ["101", "102"],
            "sublink_id": ["sublink_1", "sublink_1"],
        }
    )
    mock_generator.generate_data.return_value = pd.DataFrame(
        {
            "time": pd.date_range("2024-01-01", periods=2, freq="5min"),
            "cml_id": ["101", "101"],
            "tsl": [50.0, 51.0],
            "rsl": [-60.0, -61.0],
        }
    )

    with patch("generate_archive.Path.stat") as mock_stat:
        mock_stat.return_value.st_size = 1024
        generate_archive_data()

    # Verify gzipped files created (critical for demo setup)
    assert mock_gzip.call_count == 2
    mock_generator.close.assert_called_once()


@patch("generate_archive.Path.exists")
def test_generate_archive_fails_if_netcdf_missing(mock_exists):
    """Test generate_archive_data() fails when NetCDF file missing."""
    from generate_archive import generate_archive_data

    mock_exists.return_value = False

    with pytest.raises(SystemExit):
        generate_archive_data()
