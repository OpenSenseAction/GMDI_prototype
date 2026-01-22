"""Simple registry mapping files to parser implementations."""

from pathlib import Path
from typing import Optional, List

from .base_parser import BaseParser
from .csv_rawdata_parser import CSVRawDataParser
from .csv_metadata_parser import CSVMetadataParser


class ParserRegistry:
    def __init__(self):
        # Instantiate parser classes here; future design may load plugins dynamically
        self.parsers: List[BaseParser] = [CSVRawDataParser(), CSVMetadataParser()]

    def get_parser(self, filepath: Path) -> Optional[BaseParser]:
        for p in self.parsers:
            try:
                if p.can_parse(filepath):
                    return p
            except Exception:
                # Defensive: a parser's can_parse should never crash the registry
                continue
        return None

    def get_supported_extensions(self) -> List[str]:
        # For now return common ones; could be dynamic
        return [".csv", ".nc", ".h5", ".hdf5"]
