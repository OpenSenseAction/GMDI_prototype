"""Extended tests for CSV parsers edge cases."""

import pandas as pd
from pathlib import Path
import pytest
from ..parsers.csv_rawdata_parser import CSVRawDataParser
from ..parsers.csv_metadata_parser import CSVMetadataParser


def test_rawdata_parser_can_parse():
    """Test can_parse logic for raw data files."""
    parser = CSVRawDataParser()

    assert parser.can_parse(Path("cml_data_test.csv"))
    assert parser.can_parse(Path("cml_data_20260122.csv"))
    assert parser.can_parse(Path("CML_DATA_test.CSV"))  # Case insensitive
    assert not parser.can_parse(Path("cml_metadata_test.csv"))
    assert not parser.can_parse(Path("other_file.csv"))


def test_metadata_parser_can_parse():
    """Test can_parse logic for metadata files."""
    parser = CSVMetadataParser()

    assert parser.can_parse(Path("cml_metadata_test.csv"))
    assert parser.can_parse(Path("cml_metadata_20260122.csv"))
    assert parser.can_parse(Path("CML_METADATA_test.CSV"))
    assert not parser.can_parse(Path("cml_data_test.csv"))
    assert not parser.can_parse(Path("other_file.csv"))


def test_rawdata_parser_invalid_timestamps(tmp_path):
    """Test raw data parser rejects invalid timestamps."""
    content = """time,cml_id,sublink_id,tsl,rsl
invalid_timestamp,10001,sublink_1,1.0,-46.0
"""
    p = tmp_path / "cml_data_bad_time.csv"
    p.write_text(content)

    parser = CSVRawDataParser()
    df, err = parser.parse(p)

    assert df is None
    assert "Invalid timestamps" in err


def test_rawdata_parser_missing_cml_id(tmp_path):
    """Test raw data parser converts empty cml_id to 'nan' string (actual behavior)."""
    content = """time,cml_id,sublink_id,tsl,rsl
2026-01-22 10:00:00,,sublink_1,1.0,-46.0
"""
    p = tmp_path / "cml_data_no_id.csv"
    p.write_text(content)

    parser = CSVRawDataParser()
    df, err = parser.parse(p)

    # Empty string becomes 'nan' when converted to str, which is allowed
    assert err is None
    assert df is not None
    assert df.iloc[0]["cml_id"] == "nan"


def test_rawdata_parser_with_nan_values(tmp_path):
    """Test raw data parser handles NaN in numeric columns."""
    content = """time,cml_id,sublink_id,tsl,rsl
2026-01-22 10:00:00,10001,sublink_1,,
2026-01-22 10:01:00,10002,sublink_2,1.0,-46.0
"""
    p = tmp_path / "cml_data_with_nan.csv"
    p.write_text(content)

    parser = CSVRawDataParser()
    df, err = parser.parse(p)

    # Should succeed - NaN is allowed in rsl/tsl
    assert err is None
    assert len(df) == 2
    assert pd.isna(df.iloc[0]["tsl"])
    assert pd.isna(df.iloc[0]["rsl"])


def test_rawdata_parser_file_not_found(tmp_path):
    """Test raw data parser handles file not found."""
    parser = CSVRawDataParser()
    df, err = parser.parse(tmp_path / "nonexistent.csv")

    assert df is None
    assert "Failed to read CSV" in err


def test_rawdata_parser_get_file_type():
    """Test raw data parser returns correct file type."""
    parser = CSVRawDataParser()
    assert parser.get_file_type() == "rawdata"


def test_metadata_parser_get_file_type():
    """Test metadata parser returns correct file type."""
    parser = CSVMetadataParser()
    assert parser.get_file_type() == "metadata"


def test_metadata_parser_invalid_latitude(tmp_path):
    """Test metadata parser rejects invalid latitude."""
    content = """cml_id,site_0_lon,site_0_lat,site_1_lon,site_1_lat
10001,13.4,100.0,13.5,52.5
"""
    p = tmp_path / "meta_bad_lat.csv"
    p.write_text(content)

    parser = CSVMetadataParser()
    df, err = parser.parse(p)

    assert df is None
    assert "Invalid latitude" in err


def test_metadata_parser_with_nan_coords(tmp_path):
    """Test metadata parser validation behavior with NaN coordinates."""
    content = """cml_id,site_0_lon,site_0_lat,site_1_lon,site_1_lat
10001,13.4,52.5,,
10002,,,13.5,52.6
"""
    p = tmp_path / "meta_with_nan.csv"
    p.write_text(content)

    parser = CSVMetadataParser()
    df, err = parser.parse(p)

    # NaN values fail .between() validation, so error is expected
    assert df is None
    assert "Invalid longitude" in err


def test_metadata_parser_column_order_preserved(tmp_path):
    """Test metadata parser returns columns in expected order."""
    content = """cml_id,site_0_lon,site_0_lat,site_1_lon,site_1_lat
10001,13.4,52.5,13.5,52.6
"""
    p = tmp_path / "meta_test.csv"
    p.write_text(content)

    parser = CSVMetadataParser()
    df, err = parser.parse(p)

    assert err is None
    expected_cols = ["cml_id", "site_0_lon", "site_0_lat", "site_1_lon", "site_1_lat"]
    assert list(df.columns) == expected_cols


def test_rawdata_parser_extra_columns_preserved(tmp_path):
    """Test raw data parser preserves extra columns in DataFrame."""
    content = """time,cml_id,sublink_id,tsl,rsl,extra_col
2026-01-22 10:00:00,10001,sublink_1,1.0,-46.0,extra_value
"""
    p = tmp_path / "cml_data_extra.csv"
    p.write_text(content)

    parser = CSVRawDataParser()
    df, err = parser.parse(p)

    assert err is None
    assert "extra_col" in df.columns
