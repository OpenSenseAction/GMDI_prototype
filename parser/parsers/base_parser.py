"""Abstract base class for parsers."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Tuple
import pandas as pd


class BaseParser(ABC):
    @abstractmethod
    def can_parse(self, filepath: Path) -> bool:
        """Return True if this parser can handle the given file path."""

    @abstractmethod
    def parse(self, filepath: Path) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Parse a file and return (df, error). On success error is None."""

    @abstractmethod
    def get_file_type(self) -> str:
        """Return logical file type, e.g. 'rawdata' or 'metadata'."""

    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Optional common validation hook for DataFrame contents."""
        if df is None:
            return False, "No dataframe"
        if df.empty:
            return False, "Empty dataframe"
        return True, None
