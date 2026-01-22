"""Unit tests for the MNO data source simulator."""

import tempfile
import shutil
import time
from pathlib import Path
import sys

import pytest
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from data_generator import CMLDataGenerator


# Use relative path that works both locally and in CI
NETCDF_FILE = str(
    Path(__file__).parent.parent.parent
    / "parser"
    / "example_data"
    / "openMRG_cmls_20150827_12hours.nc"
)


@pytest.fixture
def test_dir():
    """Create a temporary directory for tests."""
    tmp_dir = tempfile.mkdtemp()
    yield tmp_dir
    shutil.rmtree(tmp_dir)


def test_generator_initialization(test_dir):
    """Test that generator initializes correctly."""
    generator = CMLDataGenerator(
        netcdf_file=NETCDF_FILE,
        loop_duration_seconds=3600,
        output_dir=test_dir,
    )

    assert generator.dataset is not None
    assert Path(test_dir).exists()

    generator.close()


def test_csv_file_generation(test_dir):
    """Test that CSV files are generated correctly."""
    generator = CMLDataGenerator(
        netcdf_file=NETCDF_FILE,
        output_dir=test_dir,
    )

    # Generate a CSV file
    csv_files = generator.generate_data_and_write_csv()

    # Should return a list with one file
    assert isinstance(csv_files, list)
    assert len(csv_files) == 1
    csv_file = csv_files[0]

    # Check file exists
    assert Path(csv_file).exists()

    # Check file is in correct directory
    assert csv_file.startswith(test_dir)

    # Load and validate CSV content
    df = pd.read_csv(csv_file)

    # Check required columns exist
    required_columns = ["time", "cml_id", "sublink_id", "tsl", "rsl"]
    for col in required_columns:
        assert col in df.columns

    # Check data is not empty
    assert len(df) > 0

    # Check data types
    assert df["cml_id"].iloc[0] is not None
    assert df["tsl"].iloc[0] is not None
    assert df["rsl"].iloc[0] is not None

    generator.close()


def test_multiple_csv_generation(test_dir):
    """Test generating multiple CSV files."""
    generator = CMLDataGenerator(
        netcdf_file=NETCDF_FILE,
        output_dir=test_dir,
    )

    # Generate 3 files with small delays to ensure unique timestamps
    files = []
    for i in range(3):
        csv_files = generator.generate_data_and_write_csv()
        files.extend(csv_files)
        if i < 2:  # Don't sleep after last iteration
            time.sleep(1)

    # Check all files exist
    for f in files:
        assert Path(f).exists()

    # Check files have unique names
    assert len(files) == len(set(files))

    generator.close()


def test_metadata_csv_generation(test_dir):
    """Test that metadata CSV file is generated correctly."""
    generator = CMLDataGenerator(
        netcdf_file=NETCDF_FILE,
        output_dir=test_dir,
    )

    # Generate metadata CSV using the class method
    filepath = generator.write_metadata_csv()

    # Check file exists
    assert Path(filepath).exists()

    # Load and validate CSV content
    loaded_df = pd.read_csv(filepath)

    # Check required columns exist (matching database schema)
    required_columns = [
        "cml_id",
        "sublink_id",
        "site_0_lon",
        "site_0_lat",
        "site_1_lon",
        "site_1_lat",
        "frequency",
        "polarization",
        "length",
    ]
    for col in required_columns:
        assert col in loaded_df.columns

    # Check column order is correct
    assert list(loaded_df.columns) == required_columns

    # Check data is not empty
    assert len(loaded_df) > 0
    assert len(loaded_df) == 728  # Expected number of CMLs (including both sublinks)

    # Check specific hardcoded values from known CMLs in the NetCDF file
    # Use query to find specific CML/sublink combinations instead of hardcoded row indices
    # This makes the test robust to different iteration orders across platforms

    # First CML (cml_id 10001, sublink_1)
    row_10001_1 = loaded_df[
        (loaded_df["cml_id"] == 10001) & (loaded_df["sublink_id"] == "sublink_1")
    ]
    assert len(row_10001_1) == 1
    row_10001_1 = row_10001_1.iloc[0]
    assert row_10001_1["site_0_lat"] == pytest.approx(57.70368)
    assert row_10001_1["site_0_lon"] == pytest.approx(11.99507)
    assert row_10001_1["site_1_lat"] == pytest.approx(57.69785)
    assert row_10001_1["site_1_lon"] == pytest.approx(11.99110)
    assert row_10001_1["frequency"] == pytest.approx(28206.5)
    assert row_10001_1["polarization"] == "v"
    assert row_10001_1["length"] == pytest.approx(691.44)

    # Second CML (cml_id 10002, sublink_1)
    row_10002_1 = loaded_df[
        (loaded_df["cml_id"] == 10002) & (loaded_df["sublink_id"] == "sublink_1")
    ]
    assert len(row_10002_1) == 1
    row_10002_1 = row_10002_1.iloc[0]
    assert row_10002_1["site_0_lat"] == pytest.approx(57.72539)
    assert row_10002_1["site_0_lon"] == pytest.approx(11.98181)
    assert row_10002_1["site_1_lat"] == pytest.approx(57.72285)
    assert row_10002_1["site_1_lon"] == pytest.approx(11.97265)
    assert row_10002_1["frequency"] == pytest.approx(38528.0)
    assert row_10002_1["polarization"] == "v"
    assert row_10002_1["length"] == pytest.approx(614.55)

    generator.close()


def test_generate_fake_data_for_timestamps_with_list(test_dir):
    """Test generating fake data for a list of timestamps."""
    generator = CMLDataGenerator(
        netcdf_file=NETCDF_FILE,
        loop_duration_seconds=3600,
        output_dir=test_dir,
    )

    # Create a list of timestamps
    timestamps = [
        pd.Timestamp("2026-01-21 10:00:00"),
        pd.Timestamp("2026-01-21 10:05:00"),
        pd.Timestamp("2026-01-21 10:10:00"),
    ]

    # Generate fake data
    df = generator.generate_data(timestamps)

    # Check structure
    assert len(df) > 0
    assert "time" in df.columns
    assert "cml_id" in df.columns
    assert "tsl" in df.columns
    assert "rsl" in df.columns

    # Check we have data for all timestamps
    unique_times = df["time"].unique()
    assert len(unique_times) == 3

    # Check timestamps match input
    for ts in timestamps:
        assert ts in df["time"].values

    generator.close()


def test_generate_fake_data_for_timestamps_with_daterange(test_dir):
    """Test generating fake data with pandas DatetimeIndex."""
    generator = CMLDataGenerator(
        netcdf_file=NETCDF_FILE,
        loop_duration_seconds=3600,
        output_dir=test_dir,
    )

    # Create a date range
    timestamps = pd.date_range(start="2026-01-21 10:00:00", periods=12, freq="5min")

    # Generate fake data
    df = generator.generate_data(timestamps)

    # Check structure
    assert len(df) > 0
    required_columns = ["time", "cml_id", "sublink_id", "tsl", "rsl"]
    for col in required_columns:
        assert col in df.columns

    # Check we have data for all timestamps
    unique_times = df["time"].unique()
    assert len(unique_times) == 12

    # Check data cycles through NetCDF (should use different indices)
    # Due to looping, some values should differ
    tsl_values = df.groupby("cml_id")["tsl"].apply(list)
    # Check that at least one CML has varying TSL values
    has_variation = any(len(set(vals)) > 1 for vals in tsl_values if len(vals) > 1)
    assert has_variation

    generator.close()


def test_generate_fake_data_for_timestamps_with_array(test_dir):
    """Test generating fake data with numpy array."""
    generator = CMLDataGenerator(
        netcdf_file=NETCDF_FILE,
        loop_duration_seconds=3600,
        output_dir=test_dir,
    )

    # Create timestamps as numpy array
    import numpy as np

    timestamps = np.array(
        [
            pd.Timestamp("2026-01-21 10:00:00"),
            pd.Timestamp("2026-01-21 10:30:00"),
            pd.Timestamp("2026-01-21 11:00:00"),
        ]
    )

    # Generate fake data
    df = generator.generate_data(timestamps)

    # Check basic structure
    assert len(df) > 0
    assert len(df["time"].unique()) == 3

    generator.close()


def test_generate_data_current_time(test_dir):
    """Test generating data for current time (no timestamps provided)."""
    generator = CMLDataGenerator(
        netcdf_file=NETCDF_FILE,
        loop_duration_seconds=3600,
        output_dir=test_dir,
    )

    # Generate data for current time
    df = generator.generate_data()

    # Check structure
    assert len(df) > 0
    assert "time" in df.columns
    assert "cml_id" in df.columns
    assert "tsl" in df.columns
    assert "rsl" in df.columns

    # Should have exactly one timestamp
    assert len(df["time"].unique()) == 1

    generator.close()


def test_get_metadata_dataframe(test_dir):
    """Test getting metadata as DataFrame."""
    generator = CMLDataGenerator(
        netcdf_file=NETCDF_FILE,
        output_dir=test_dir,
    )

    # Get metadata DataFrame
    metadata_df = generator.get_metadata_dataframe()

    # Check structure
    assert isinstance(metadata_df, pd.DataFrame)
    assert len(metadata_df) == 728

    # Check expected columns
    expected_columns = [
        "site_0_lat",
        "site_0_lon",
        "site_1_lat",
        "site_1_lon",
        "frequency",
        "polarization",
        "length",
    ]
    for col in expected_columns:
        assert col in metadata_df.columns

    generator.close()


def test_generate_data_and_write_csv_with_timestamps(test_dir):
    """Test generating and writing CSV with custom timestamps."""
    generator = CMLDataGenerator(
        netcdf_file=NETCDF_FILE,
        output_dir=test_dir,
    )

    # Create timestamps
    timestamps = pd.date_range(start="2026-01-21 10:00:00", periods=5, freq="5min")

    # Generate CSV files
    csv_files = generator.generate_data_and_write_csv(timestamps=timestamps)

    # Should return one file
    assert isinstance(csv_files, list)
    assert len(csv_files) == 1

    # Load and check content
    df = pd.read_csv(csv_files[0])
    assert len(df["time"].unique()) == 5

    generator.close()


def test_generate_data_and_write_csv_with_split_freq(test_dir):
    """Test generating and writing CSV with frequency splitting."""
    generator = CMLDataGenerator(
        netcdf_file=NETCDF_FILE,
        output_dir=test_dir,
    )

    # Create timestamps spanning 3 hours
    timestamps = pd.date_range(start="2026-01-21 10:00:00", periods=12, freq="15min")

    # Split by hour
    csv_files = generator.generate_data_and_write_csv(
        timestamps=timestamps, split_freq="1h"
    )

    # Should return 3 files (one per hour)
    assert isinstance(csv_files, list)
    assert len(csv_files) == 3

    # Check all files exist
    for filepath in csv_files:
        assert Path(filepath).exists()

    # Load first file and check it has 4 timestamps (15min intervals in 1 hour)
    df = pd.read_csv(csv_files[0])
    assert len(df["time"].unique()) == 4

    generator.close()
