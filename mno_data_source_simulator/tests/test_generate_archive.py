import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import numpy as np
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))


@patch("generate_archive.Path.mkdir")
@patch("generate_archive.Path.exists")
@patch("generate_archive.CMLDataGenerator")
@patch("builtins.open", new_callable=mock_open)
def test_generate_archive_creates_files(
    mock_open_fn, mock_generator_class, mock_exists, mock_mkdir
):
    """Test generate_archive_data() creates metadata and data CSV files."""
    from generate_archive import generate_archive_data

    mock_exists.return_value = True

    # Minimal per-link DataFrame returned by dataset.isel().to_dataframe().reset_index()
    slice_df = pd.DataFrame(
        {
            "cml_id": ["101", "102"],
            "sublink_id": ["sublink_1", "sublink_1"],
            "tsl": [50.0, 51.0],
            "rsl": [-60.0, -61.0],
        }
    )

    mock_generator = MagicMock()
    mock_generator_class.return_value = mock_generator
    mock_generator.get_metadata_dataframe.return_value = pd.DataFrame(
        {
            "cml_id": ["101", "102"],
            "sublink_id": ["sublink_1", "sublink_1"],
        }
    )
    # Internal attributes used by the numpy-cached generation path
    mock_generator.original_time_points = list(range(720))
    mock_generator._get_netcdf_index_for_timestamp.return_value = 0
    mock_generator.dataset.isel.return_value.to_dataframe.return_value.reset_index.return_value = (
        slice_df
    )

    with patch("generate_archive.Path.stat") as mock_stat, \
         patch("pathlib.Path.is_dir", return_value=True):
        mock_stat.return_value.st_size = 1024
        generate_archive_data(
            archive_days=1,
            output_dir="/tmp/test_archive",
            netcdf_file="/fake/file.nc",
            interval_seconds=300,
        )

    mock_generator.close.assert_called_once()


@patch("generate_archive.Path.exists")
def test_generate_archive_fails_if_netcdf_missing(mock_exists):
    """Test generate_archive_data() fails when NetCDF file missing."""
    from generate_archive import generate_archive_data

    mock_exists.return_value = False

    with pytest.raises(SystemExit):
        generate_archive_data(
            archive_days=1,
            output_dir="/tmp/test_archive",
            netcdf_file="/fake/file.nc",
            interval_seconds=300,
        )
