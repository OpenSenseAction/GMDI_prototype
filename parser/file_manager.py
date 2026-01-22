"""File management utilities for the parser service.

Handles archiving successful files and quarantining failed files with
an accompanying `.error.txt` that contains failure details.
"""

from pathlib import Path
import shutil
import datetime
import logging

logger = logging.getLogger(__name__)


class FileManager:
    def __init__(self, incoming_dir: str, archived_dir: str, quarantine_dir: str):
        self.incoming_dir = Path(incoming_dir)
        self.archived_dir = Path(archived_dir)
        self.quarantine_dir = Path(quarantine_dir)

        for d in (self.incoming_dir, self.archived_dir, self.quarantine_dir):
            d.mkdir(parents=True, exist_ok=True)

    def _archive_subdir(self) -> Path:
        today = datetime.date.today().isoformat()
        subdir = self.archived_dir / today
        subdir.mkdir(parents=True, exist_ok=True)
        return subdir

    def archive_file(self, filepath: Path) -> Path:
        """Move `filepath` to archive/YYYY-MM-DD/ and return destination path."""
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        dest_dir = self._archive_subdir()
        dest = dest_dir / filepath.name
        shutil.move(str(filepath), str(dest))
        logger.info(f"Archived file {filepath} → {dest}")
        return dest

    def quarantine_file(self, filepath: Path, error: str) -> Path:
        """Move file to quarantine and write an error metadata file next to it."""
        filepath = Path(filepath)
        if not filepath.exists():
            # If file doesn't exist, we still write an error note in quarantine
            self.quarantine_dir.mkdir(parents=True, exist_ok=True)
            note_path = self.quarantine_dir / (filepath.name + ".error.txt")
            note_path.write_text(
                f"Original file not found: {filepath}\nError: {error}\n"
            )
            return note_path

        dest = self.quarantine_dir / filepath.name
        shutil.move(str(filepath), str(dest))
        # Create an error metadata file containing the reason
        note_path = self.quarantine_dir / (dest.name + ".error.txt")
        note_contents = f"Quarantined at: {datetime.datetime.utcnow().isoformat()}Z\nError: {error}\n"
        try:
            note_path.write_text(note_contents)
        except Exception:
            logger.exception("Failed to write quarantine error file")

        logger.warning(f"Quarantined file {dest} with error: {error}")
        return dest

    def get_archived_path(self, filepath: Path) -> Path:
        """Return the destination archive path for a given filepath (without moving)."""
        subdir = self._archive_subdir()
        return subdir / Path(filepath).name

    def is_valid_file(
        self, filepath: Path, allowed_exts=None, max_size_bytes: int = None
    ) -> bool:
        """Basic checks whether a file should be processed.

        - allowed_exts: list of extensions like ['.csv', '.nc'] or None
        - max_size_bytes: maximum allowed file size or None
        """
        filepath = Path(filepath)
        if not filepath.exists() or not filepath.is_file():
            return False

        if allowed_exts and filepath.suffix.lower() not in allowed_exts:
            return False

        if max_size_bytes is not None:
            try:
                if filepath.stat().st_size > max_size_bytes:
                    return False
            except OSError:
                return False

        return True
