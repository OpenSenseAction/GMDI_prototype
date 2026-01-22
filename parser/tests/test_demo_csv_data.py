"""Tests for demo_csv_data parser functions and validation."""

import pandas as pd
from pathlib import Path
from ..demo_csv_data.parse_raw import parse_rawdata_csv
from ..demo_csv_data.parse_metadata import parse_metadata_csv
from ..validate_dataframe import validate_dataframe


def test_parse_rawdata_csv(tmp_path):
    csv = tmp_path / "raw.csv"
    csv.write_text(
        "time,cml_id,sublink_id,tsl,rsl\n2026-01-22 10:00:00,10001,sublink_1,1.0,-46.0\n2026-01-22 10:01:00,,sublink_2,1.2,-45.5\n"
    )
    df = parse_rawdata_csv(csv)
    assert isinstance(df, pd.DataFrame)
    assert "time" in df.columns
    assert df.shape[0] == 2
    assert validate_dataframe(df, "rawdata")


def test_parse_metadata_csv(tmp_path):
    csv = tmp_path / "meta.csv"
    csv.write_text(
        "cml_id,sublink_id,site_0_lon,site_0_lat,site_1_lon,site_1_lat,frequency,polarization,length\n"
        "10001,sublink_1,13.4,52.5,13.5,52.6,18.0,H,2.1\n"
    )
    df = parse_metadata_csv(csv)
    assert isinstance(df, pd.DataFrame)
    for col in [
        "cml_id",
        "sublink_id",
        "site_0_lon",
        "site_0_lat",
        "site_1_lon",
        "site_1_lat",
        "frequency",
        "polarization",
        "length",
    ]:
        assert col in df.columns
    assert df.shape[0] == 1
    assert validate_dataframe(df, "metadata")


def test_validate_dataframe_invalid():
    df = pd.DataFrame({"foo": [1, 2]})
    assert not validate_dataframe(df, "rawdata")
    assert not validate_dataframe(df, "metadata")
