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

    def _safe_move(self, filepath: Path, dest: Path) -> bool:
        """Attempt to move file; fall back to copy if move fails. Returns True if successful."""
        try:
            shutil.move(str(filepath), str(dest))
            logger.info(f"Moved file {filepath} → {dest}")
            return True
        except Exception:
            try:
                shutil.copy2(str(filepath), str(dest))
                logger.info(f"Copied file {filepath} → {dest}")
                return True
            except Exception:
                logger.exception("Failed to move or copy file")
                return False

    def archive_file(self, filepath: Path) -> Path:
        """Move `filepath` to archive/YYYY-MM-DD/ and return destination path."""
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        dest_dir = self._archive_subdir()
        dest = dest_dir / filepath.name
        if not self._safe_move(filepath, dest):
            raise RuntimeError(f"Failed to archive file {filepath}")
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
        if not self._safe_move(filepath, dest):
            # As a last resort, write an error note mentioning original path
            dest = self.quarantine_dir / (filepath.name + ".orphan")

        # Create an error metadata file containing the reason
        note_path = self.quarantine_dir / (dest.name + ".error.txt")
        # Use timezone-aware UTC timestamp instead of deprecated utcnow()
        try:
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        except Exception:
            # Fallback to naive UTC if timezone is unavailable
            now = datetime.datetime.utcnow().isoformat() + "Z"

        note_contents = (
            f"Quarantined at: {now}\nError: {error}\nOriginalPath: {filepath}\n"
        )
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
