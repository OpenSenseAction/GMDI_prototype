import pandas as pd
from pathlib import Path

from ..parsers.csv_rawdata_parser import CSVRawDataParser
from ..parsers.csv_metadata_parser import CSVMetadataParser


def test_csv_rawdata_parser_valid(tmp_path):
    content = """time,cml_id,sublink_id,tsl,rsl
2026-01-22 10:00:00,10001,sublink_1,1.0,-46.0
2026-01-22 10:01:00,10002,sublink_1,0.0,-41.0
"""
    p = tmp_path / "cml_data_test.csv"
    p.write_text(content)

    parser = CSVRawDataParser()
    df, err = parser.parse(p)
    assert err is None
    assert df is not None
    assert len(df) == 2
    assert list(df.columns) == ["time", "cml_id", "sublink_id", "tsl", "rsl"]


def test_csv_rawdata_parser_missing_columns(tmp_path):
    content = """time,cml_id
2026-01-22 10:00:00,10001
"""
    p = tmp_path / "cml_data_bad.csv"
    p.write_text(content)
    parser = CSVRawDataParser()
    df, err = parser.parse(p)
    assert df is None
    assert "Missing required columns" in err


def test_csv_metadata_parser_valid(tmp_path):
    content = """cml_id,site_0_lon,site_0_lat,site_1_lon,site_1_lat
10001,13.3888,52.5170,13.4050,52.5200
10002,13.3500,52.5100,13.3600,52.5150
"""
    p = tmp_path / "cml_metadata_test.csv"
    p.write_text(content)
    parser = CSVMetadataParser()
    df, err = parser.parse(p)
    assert err is None
    assert df is not None
    assert len(df) == 2


def test_csv_metadata_parser_invalid_coords(tmp_path):
    content = """cml_id,site_0_lon,site_0_lat,site_1_lon,site_1_lat
10001,200.0,52.5170,13.4050,52.5200
"""
    p = tmp_path / "cml_meta_bad.csv"
    p.write_text(content)
    parser = CSVMetadataParser()
    df, err = parser.parse(p)
    assert df is None
    assert "Invalid longitude" in err
