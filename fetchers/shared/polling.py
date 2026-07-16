import logging
import signal
import time
from typing import Callable

logger = logging.getLogger(__name__)

_MAX_BACKOFF_S = 300  # 5 minutes


def run_poll_loop(
    poll_fn: Callable,
    interval_s: float,
    log: logging.Logger = None,
) -> None:
    """Call *poll_fn* repeatedly, sleeping *interval_s* between successful calls.

    On exception: logs the error, backs off exponentially (doubles each
    failure up to ``_MAX_BACKOFF_S``), then retries.  The backoff resets to
    *interval_s* after a successful call.

    Runs until the process is killed (``SIGTERM`` / ``SIGINT``).
    """
    if log is None:
        log = logger
    
    # Handle shutdown signals gracefully
    shutdown_requested = False
    
    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        log.info(f"Received signal {signum}, shutting down gracefully...")
        shutdown_requested = True
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    backoff = interval_s
    while not shutdown_requested:
        try:
            poll_fn()
            backoff = interval_s  # reset on success
            time.sleep(interval_s)
        except Exception:
            log.exception("Poll failed, retrying in %.0fs", backoff)
            if not shutdown_requested:
                time.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF_S)
