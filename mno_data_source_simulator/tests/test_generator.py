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


NETCDF_FILE = "/Users/chwala-c/code/gmdi_prototype/parser/example_data/openMRG_cmls_20150827_12hours.nc"


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
    csv_file = generator.generate_and_write_csv()

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
        csv_file = generator.generate_and_write_csv()
        files.append(csv_file)
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

    # Generate a data point to get metadata
    data_point = generator.get_current_data_point()

    # Convert metadata to DataFrame
    metadata_df = generator.get_metadata_as_dataframe(data_point)

    # Generate filename
    timestamp = data_point["timestamp"].strftime("%Y%m%d_%H%M%S")
    filename = f"cml_metadata_{timestamp}.csv"
    filepath = Path(test_dir) / filename

    # Write metadata to CSV
    metadata_df.to_csv(filepath, index=False)

    # Check file exists
    assert filepath.exists()

    # Load and validate CSV content
    loaded_df = pd.read_csv(filepath)

    # Check required columns exist (based on actual OpenMRG dataset)
    required_columns = [
        "site_0_lat",
        "site_0_lon",
        "site_1_lat",
        "site_1_lon",
        "frequency",
        "polarization",
        "length",
    ]
    for col in required_columns:
        assert col in loaded_df.columns

    # Check data is not empty
    assert len(loaded_df) > 0
    assert len(loaded_df) == 728  # Expected number of CMLs

    # Check specific hardcoded values from the first 2 entries in the NetCDF file
    # First entry (cml_id 10001)
    assert loaded_df.iloc[0]["site_0_lat"] == pytest.approx(57.70368)
    assert loaded_df.iloc[0]["site_0_lon"] == pytest.approx(11.99507)
    assert loaded_df.iloc[0]["site_1_lat"] == pytest.approx(57.69785)
    assert loaded_df.iloc[0]["site_1_lon"] == pytest.approx(11.99110)
    assert loaded_df.iloc[0]["frequency"] == pytest.approx(28206.5)
    assert loaded_df.iloc[0]["polarization"] == "v"
    assert loaded_df.iloc[0]["length"] == pytest.approx(691.44)

    # Second entry (cml_id 10002)
    assert loaded_df.iloc[1]["site_0_lat"] == pytest.approx(57.72539)
    assert loaded_df.iloc[1]["site_0_lon"] == pytest.approx(11.98181)
    assert loaded_df.iloc[1]["site_1_lat"] == pytest.approx(57.72285)
    assert loaded_df.iloc[1]["site_1_lon"] == pytest.approx(11.97265)
    assert loaded_df.iloc[1]["frequency"] == pytest.approx(38528.0)
    assert loaded_df.iloc[1]["polarization"] == "v"
    assert loaded_df.iloc[1]["length"] == pytest.approx(614.55)

    generator.close()
