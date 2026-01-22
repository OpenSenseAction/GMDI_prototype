"""Tests for ParserRegistry."""

from pathlib import Path
import pytest
from ..parsers.parser_registry import ParserRegistry
from ..parsers.csv_rawdata_parser import CSVRawDataParser
from ..parsers.csv_metadata_parser import CSVMetadataParser


def test_registry_finds_rawdata_parser():
    """Test registry returns correct parser for raw data files."""
    registry = ParserRegistry()

    parser = registry.get_parser(Path("cml_data_20260122.csv"))
    assert parser is not None
    assert isinstance(parser, CSVRawDataParser)


def test_registry_finds_metadata_parser():
    """Test registry returns correct parser for metadata files."""
    registry = ParserRegistry()

    parser = registry.get_parser(Path("cml_metadata_20260122.csv"))
    assert parser is not None
    assert isinstance(parser, CSVMetadataParser)


def test_registry_returns_none_for_unknown_file():
    """Test registry returns None for unsupported files."""
    registry = ParserRegistry()

    parser = registry.get_parser(Path("unknown_file.txt"))
    assert parser is None

    parser = registry.get_parser(Path("random.csv"))
    assert parser is None


def test_registry_case_insensitive():
    """Test file matching is case-insensitive."""
    registry = ParserRegistry()

    parser = registry.get_parser(Path("CML_DATA_test.CSV"))
    assert parser is not None
    assert isinstance(parser, CSVRawDataParser)

    parser = registry.get_parser(Path("CML_METADATA_test.CSV"))
    assert parser is not None
    assert isinstance(parser, CSVMetadataParser)


def test_get_supported_extensions():
    """Test supported extensions list."""
    registry = ParserRegistry()
    exts = registry.get_supported_extensions()

    assert ".csv" in exts
    assert ".nc" in exts
    assert ".h5" in exts
