"""Test configuration fixtures.

Add project root to sys.path so tests can import local packages during CI/local runs.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
