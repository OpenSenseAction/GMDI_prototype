"""Watch for new files in the incoming directory and invoke a callback."""

import time
import logging
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent

logger = logging.getLogger(__name__)


class FileUploadHandler(FileSystemEventHandler):
    def __init__(self, callback, supported_extensions):
        super().__init__()
        self.callback = callback
        self.supported_extensions = supported_extensions
        self.processing = set()

    def on_created(self, event: FileCreatedEvent):
        if event.is_directory:
            return

        filepath = Path(event.src_path)
        if (
            self.supported_extensions
            and filepath.suffix.lower() not in self.supported_extensions
        ):
            logger.debug(f"Ignoring unsupported file: {filepath.name}")
            return

        # Wait for file to stabilize
        self._wait_for_file_ready(filepath)

        if str(filepath) in self.processing:
            return

        self.processing.add(str(filepath))
        try:
            logger.info(f"Detected new file: {filepath}")
            self.callback(filepath)
        except Exception:
            logger.exception(f"Error processing file: {filepath}")
        finally:
            self.processing.discard(str(filepath))

    def _wait_for_file_ready(self, filepath: Path, timeout: int = 10):
        if not filepath.exists():
            return

        start = time.time()
        last_size = -1
        while time.time() - start < timeout:
            try:
                current = filepath.stat().st_size
                if current == last_size and current > 0:
                    return
                last_size = current
            except OSError:
                pass
            time.sleep(0.5)
        logger.warning(f"Timeout waiting for file to stabilize: {filepath}")


class FileWatcher:
    def __init__(self, watch_dir: str, callback, supported_extensions):
        self.watch_dir = Path(watch_dir)
        self.callback = callback
        self.supported_extensions = (
            [e.lower() for e in supported_extensions] if supported_extensions else []
        )
        self.observer = None

    def start(self):
        if not self.watch_dir.exists():
            raise ValueError(f"Watch directory does not exist: {self.watch_dir}")
        handler = FileUploadHandler(self.callback, self.supported_extensions)
        self.observer = Observer()
        self.observer.schedule(handler, str(self.watch_dir), recursive=False)
        self.observer.start()
        logger.info(f"Started watching {self.watch_dir}")

    def stop(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()
            logger.info("Stopped file watcher")
