"""Tests for the csv_generic configurable parser."""

import pandas as pd
import pytest
from pathlib import Path

from ..parsers.csv_generic.parse_raw import parse_rawdata_csv
from ..parsers.csv_generic.parse_metadata import parse_metadata_csv
from ..validate_dataframe import validate_dataframe


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_csv(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# parse_rawdata_csv — no config (canonical column names already present)
# ---------------------------------------------------------------------------


def test_rawdata_no_config(tmp_path):
    """Files already in canonical format work with an empty config."""
    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "time,cml_id,sublink_id,tsl,rsl\n"
        "2026-01-22 10:00:00,CML1,A,1.0,-46.0\n"
        "2026-01-22 10:01:00,CML1,A,1.1,-45.5\n",
    )
    df = parse_rawdata_csv(csv, {})
    assert list(df.columns[:5]) == ["time", "cml_id", "sublink_id", "tsl", "rsl"]
    assert len(df) == 2
    assert validate_dataframe(df, "rawdata")


# ---------------------------------------------------------------------------
# parse_rawdata_csv — column rename
# ---------------------------------------------------------------------------


def test_rawdata_column_rename(tmp_path):
    """rawdata_columns renames source columns to canonical names."""
    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "timestamp,link_id,sub,tx,rx\n"
        "2026-01-22 10:00:00,CML1,A,1.0,-46.0\n",
    )
    config = {
        "rawdata_columns": {
            "timestamp": "time",
            "link_id": "cml_id",
            "sub": "sublink_id",
            "tx": "tsl",
            "rx": "rsl",
        }
    }
    df = parse_rawdata_csv(csv, config)
    assert validate_dataframe(df, "rawdata")
    assert df["cml_id"].iloc[0] == "CML1"
    assert df["tsl"].iloc[0] == pytest.approx(1.0)
    assert df["rsl"].iloc[0] == pytest.approx(-46.0)


# ---------------------------------------------------------------------------
# parse_rawdata_csv — read_csv_kwargs (semicolon separator)
# ---------------------------------------------------------------------------


def test_rawdata_semicolon_separator(tmp_path):
    """read_csv_kwargs are forwarded to pd.read_csv."""
    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "time;cml_id;sublink_id;tsl;rsl\n"
        "2026-01-22 10:00:00;CML1;A;1.0;-46.0\n",
    )
    df = parse_rawdata_csv(csv, {"read_csv_kwargs": {"sep": ";"}})
    assert validate_dataframe(df, "rawdata")
    assert len(df) == 1


# ---------------------------------------------------------------------------
# parse_rawdata_csv — timezone conversion
# ---------------------------------------------------------------------------


def test_rawdata_timezone_conversion(tmp_path):
    """Naive timestamps are localised then converted to UTC."""
    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "time,cml_id,sublink_id,tsl,rsl\n"
        # Africa/Douala is UTC+1 year-round (no DST)
        "2026-01-22 11:00:00,CML1,A,1.0,-46.0\n",
    )
    df = parse_rawdata_csv(csv, {"timezone": "Africa/Douala"})
    assert df["time"].dt.tz is not None
    # 11:00 local (UTC+1) → 10:00 UTC
    assert df["time"].iloc[0].hour == 10
    assert str(df["time"].dt.tz) == "UTC"


def test_rawdata_already_tz_aware(tmp_path):
    """Already-aware timestamps are just converted to UTC, not double-localised."""
    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "time,cml_id,sublink_id,tsl,rsl\n"
        "2026-01-22 10:00:00+02:00,CML1,A,1.0,-46.0\n",
    )
    # Passing a timezone should not raise even though the timestamp is already aware
    df = parse_rawdata_csv(csv, {"timezone": "Africa/Douala"})
    assert df["time"].dt.tz is not None
    assert df["time"].iloc[0].hour == 8  # UTC+2 → UTC


def test_rawdata_no_timezone_leaves_naive(tmp_path):
    """Without timezone config, naive timestamps are left as-is."""
    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "time,cml_id,sublink_id,tsl,rsl\n"
        "2026-01-22 10:00:00,CML1,A,1.0,-46.0\n",
    )
    df = parse_rawdata_csv(csv, {})
    # validate_dataframe only cares that the column exists and is not all-NaT
    assert validate_dataframe(df, "rawdata")


# ---------------------------------------------------------------------------
# parse_rawdata_csv — misconfigured rename map (missing column → NaN, no crash)
# ---------------------------------------------------------------------------


def test_rawdata_missing_rename_target_gives_nan(tmp_path):
    """A wrong key in rawdata_columns produces NaN columns, not a KeyError."""
    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "time,cml_id,sublink_id,tsl,rsl\n"
        "2026-01-22 10:00:00,CML1,A,1.0,-46.0\n",
    )
    # Map tsl/rsl to wrong names — canonical tsl/rsl will be missing after rename
    config = {"rawdata_columns": {"tsl": "tx_power", "rsl": "rx_power"}}
    # Should not raise; tsl/rsl columns will be all-NaN
    df = parse_rawdata_csv(csv, config)
    assert "tsl" in df.columns
    assert "rsl" in df.columns
    assert df["tsl"].isna().all()
    assert df["rsl"].isna().all()


# ---------------------------------------------------------------------------
# parse_metadata_csv — no config
# ---------------------------------------------------------------------------


def test_metadata_no_config(tmp_path):
    """Files already in canonical format work with an empty config."""
    csv = _write_csv(
        tmp_path,
        "meta.csv",
        "cml_id,sublink_id,site_0_lon,site_0_lat,site_1_lon,site_1_lat,"
        "frequency,polarization,length\n"
        "CML1,A,13.4,52.5,13.5,52.6,18.0,H,2.1\n",
    )
    df = parse_metadata_csv(csv, {})
    assert validate_dataframe(df, "metadata")
    assert len(df) == 1


# ---------------------------------------------------------------------------
# parse_metadata_csv — column rename
# ---------------------------------------------------------------------------


def test_metadata_column_rename(tmp_path):
    """metadata_columns renames source columns to canonical names."""
    csv = _write_csv(
        tmp_path,
        "meta.csv",
        "link_id,sub,lon_a,lat_a,lon_b,lat_b,freq,pol,len\n"
        "CML1,A,13.4,52.5,13.5,52.6,18.0,H,2.1\n",
    )
    config = {
        "metadata_columns": {
            "link_id": "cml_id",
            "sub": "sublink_id",
            "lon_a": "site_0_lon",
            "lat_a": "site_0_lat",
            "lon_b": "site_1_lon",
            "lat_b": "site_1_lat",
            "freq": "frequency",
            "pol": "polarization",
            "len": "length",
        }
    }
    df = parse_metadata_csv(csv, config)
    assert validate_dataframe(df, "metadata")
    assert df["site_0_lon"].iloc[0] == pytest.approx(13.4)


# ---------------------------------------------------------------------------
# parse_metadata_csv — read_csv_kwargs
# ---------------------------------------------------------------------------


def test_metadata_semicolon_separator(tmp_path):
    """read_csv_kwargs are forwarded to pd.read_csv for metadata too."""
    csv = _write_csv(
        tmp_path,
        "meta.csv",
        "cml_id;sublink_id;site_0_lon;site_0_lat;site_1_lon;site_1_lat;"
        "frequency;polarization;length\n"
        "CML1;A;13.4;52.5;13.5;52.6;18.0;H;2.1\n",
    )
    df = parse_metadata_csv(csv, {"read_csv_kwargs": {"sep": ";"}})
    assert validate_dataframe(df, "metadata")


# ---------------------------------------------------------------------------
# load_parser integration — round-trip through service_logic dispatch
# ---------------------------------------------------------------------------


def test_load_parser_csv_generic(tmp_path):
    """load_parser('csv_generic', config) returns a working ParserBundle."""
    from ..service_logic import load_parser

    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "ts,id,sub,tx,rx\n"
        "2026-01-22 10:00:00,CML1,A,1.0,-46.0\n",
    )
    config = {
        "rawdata_columns": {
            "ts": "time",
            "id": "cml_id",
            "sub": "sublink_id",
            "tx": "tsl",
            "rx": "rsl",
        }
    }
    bundle = load_parser("csv_generic", config)
    assert not bundle.is_metadata_file("raw_data.csv")
    assert bundle.is_metadata_file("metadata.csv")
    df = bundle.parse_rawdata(csv)
    assert validate_dataframe(df, "rawdata")


def test_load_parser_csv_generic_custom_meta_keyword(tmp_path):
    """metadata_filename_keyword overrides the default 'meta' substring match."""
    from ..service_logic import load_parser

    config = {"metadata_filename_keyword": "station_info"}
    bundle = load_parser("csv_generic", config)
    assert bundle.is_metadata_file("station_info_2026.csv")
    assert not bundle.is_metadata_file("meta_something.csv")
