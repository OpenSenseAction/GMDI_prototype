"""Tests for the other_mno_csv parser."""

import pandas as pd
import pytest
from pathlib import Path

from ..parsers.other_mno_csv.parse_raw import parse_rawdata_csv
from ..parsers.other_mno_csv.parse_metadata import parse_metadata_csv
from ..validate_dataframe import validate_dataframe


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_csv(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# parse_rawdata_csv — OtherMNO format (semicolon, different columns)
# ---------------------------------------------------------------------------


def test_rawdata_other_mno_format(tmp_path):
    """OtherMNO raw data with semicolon separator and custom column names."""
    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "timestamp;link_id;sublink;tx_power;rx_power\n"
        "2026-01-22 10:00:00+01:00;CML1;A;1.0;-46.0\n"
        "2026-01-22 10:01:00+01:00;CML1;A;1.1;-45.5\n",
    )
    df = parse_rawdata_csv(csv)
    assert validate_dataframe(df, "rawdata")
    assert len(df) == 2
    assert df["cml_id"].iloc[0] == "CML1"
    assert df["tsl"].iloc[0] == pytest.approx(1.0)
    assert df["rsl"].iloc[0] == pytest.approx(-46.0)


def test_rawdata_missing_columns_give_nan(tmp_path):
    """Missing tsl/rsl columns produce NaN, not KeyError."""
    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "timestamp;link_id;sublink\n"
        "2026-01-22 10:00:00+01:00;CML1;A\n",
    )
    df = parse_rawdata_csv(csv)
    assert "tsl" in df.columns
    assert "rsl" in df.columns
    assert df["tsl"].isna().all()
    assert df["rsl"].isna().all()


def test_rawdata_timezone_aware_timestamps(tmp_path):
    """Timestamps with timezone offset are parsed correctly."""
    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "timestamp;link_id;sublink;tx_power;rx_power\n"
        "2026-01-22 11:00:00+01:00;CML1;A;1.0;-46.0\n",
    )
    df = parse_rawdata_csv(csv)
    # Timestamps keep their timezone info (not converted to UTC by this parser)
    assert df["time"].dt.tz is not None


# ---------------------------------------------------------------------------
# parse_metadata_csv — OtherMNO format
# ---------------------------------------------------------------------------


def test_metadata_other_mno_format(tmp_path):
    """OtherMNO metadata with semicolon separator and custom column names."""
    csv = _write_csv(
        tmp_path,
        "meta.csv",
        "link_id;sublink;lon_a;lat_a;lon_b;lat_b;freq_mhz;pol;length_km\n"
        "CML1;A;13.4;52.5;13.5;52.6;18.0;H;2.1\n",
    )
    df = parse_metadata_csv(csv)
    assert validate_dataframe(df, "metadata")
    assert df["site_0_lon"].iloc[0] == pytest.approx(13.4)
    assert df["frequency"].iloc[0] == pytest.approx(18.0)


def test_metadata_missing_optional_columns(tmp_path):
    """Metadata without optional coordinate columns still works."""
    csv = _write_csv(
        tmp_path,
        "meta.csv",
        "link_id;sublink;freq_mhz;pol;length_km\n"
        "CML1;A;18.0;H;2.1\n",
    )
    df = parse_metadata_csv(csv)
    # Should not crash; just missing those columns
    assert "cml_id" in df.columns
    assert "frequency" in df.columns


# ---------------------------------------------------------------------------
# load_parser integration
# ---------------------------------------------------------------------------


def test_load_parser_other_mno_csv(tmp_path):
    """load_parser('other_mno_csv') returns a working ParserBundle."""
    from ..service_logic import load_parser

    csv = _write_csv(
        tmp_path,
        "raw.csv",
        "timestamp;link_id;sublink;tx_power;rx_power\n"
        "2026-01-22 10:00:00+01:00;CML1;A;1.0;-46.0\n",
    )
    bundle = load_parser("other_mno_csv")
    assert not bundle.is_metadata_file("raw_data.csv")
    assert bundle.is_metadata_file("metadata_links.csv")
    df = bundle.parse_rawdata(csv)
    assert validate_dataframe(df, "rawdata")
