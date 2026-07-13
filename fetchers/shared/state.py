import json
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class FetcherState:
    """Persist fetcher progress to a JSON file so container restarts resume
    from where they left off rather than re-fetching everything.

    API fetchers use the cursor methods (``get_cursor`` / ``set_cursor``).
    SFTP fetchers use the seen-files methods (``is_seen`` / ``mark_seen``).
    """

    def __init__(self, state_path: Path):
        self.path = Path(state_path)
        self._data: dict = self._load()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> dict:
        if self.path.exists():
            try:
                with open(self.path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning(
                    "Could not load state from %s (%s); starting fresh",
                    self.path,
                    exc,
                )
        return {"sources": {}}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        try:
            with open(tmp, "w") as f:
                json.dump(self._data, f, indent=2)
            os.replace(tmp, self.path)
        except Exception:
            if tmp.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass
            raise

    def _source(self, name: str) -> dict:
        return self._data["sources"].setdefault(name, {})

    # ------------------------------------------------------------------
    # API cursor
    # ------------------------------------------------------------------

    def get_cursor(self, source_name: str) -> Optional[str]:
        """Return the ``last_fetched_until`` ISO timestamp for an API source,
        or ``None`` if this source has never been fetched."""
        return self._source(source_name).get("last_fetched_until")

    def set_cursor(self, source_name: str, value: str) -> None:
        """Advance the cursor for an API source and persist to disk."""
        self._source(source_name)["last_fetched_until"] = value
        self._save()

    # ------------------------------------------------------------------
    # SFTP seen-files
    # ------------------------------------------------------------------

    def is_seen(self, source_name: str, filename: str, mtime: str) -> bool:
        """Return ``True`` if this ``(filename, mtime)`` pair was already
        downloaded from the named SFTP source."""
        seen = self._source(source_name).get("seen_files", {})
        return seen.get(filename) == mtime

    def mark_seen(self, source_name: str, filename: str, mtime: str) -> None:
        """Record that a file has been downloaded and persist."""
        self._source(source_name).setdefault("seen_files", {})[filename] = mtime
        self._save()
