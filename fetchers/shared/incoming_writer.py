import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def atomic_write(incoming_dir: Path, filename: str, content: bytes) -> Path:
    """Write *content* to ``incoming_dir/filename`` atomically.

    Writes to a ``.tmp`` sibling first, then uses ``os.replace()`` so the
    parser's file-watcher never observes a partial file.  The watcher already
    has its own stability check, but atomic writes are a belt-and-suspenders
    guarantee.
    """
    incoming_dir = Path(incoming_dir)
    incoming_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = incoming_dir / (filename + ".tmp")
    final_path = incoming_dir / filename
    try:
        tmp_path.write_bytes(content)
        os.replace(tmp_path, final_path)
        logger.info("Wrote %s (%d bytes)", final_path.name, len(content))
        return final_path
    except Exception:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise
