"""
State manager for rain processing service.
Manages persistent state (last processed timestamps) in JSON file with file locking.
"""

import json
import fcntl
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages persistent state for rain processing service.
    Uses file locking to ensure atomic updates.
    """

    def __init__(self, state_path: str = "/app/data/state/rain_processing_state.json"):
        """
        Args:
            state_path: Path to JSON state file
        """
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize empty state file if it doesn't exist
        if not self.state_path.exists():
            self._write_state({})
            logger.info(f"Initialized empty state file at {self.state_path}")

    def get_last_processed_time(self, user_id: str) -> Optional[datetime]:
        """
        Get the last processed timestamp for a user.

        Args:
            user_id: User identifier

        Returns:
            Last processed datetime (UTC), or None if user has no state
        """
        try:
            state = self._read_state()
            user_state = state.get(user_id)

            if user_state is None:
                return None

            timestamp_str = user_state.get("last_processed_time")
            if timestamp_str is None:
                return None

            # Parse ISO 8601 timestamp
            # Handle both 'Z' suffix and '+00:00' format
            timestamp_str = timestamp_str.replace("Z", "+00:00")
            return datetime.fromisoformat(timestamp_str)

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Error reading state for user {user_id}: {e}")
            return None

    def update_last_processed_time(self, user_id: str, timestamp: datetime) -> None:
        """
        Update the last processed timestamp for a user.
        Uses atomic write pattern: write to temp file, then rename.

        Args:
            user_id: User identifier
            timestamp: New last processed time (should be UTC)
        """
        try:
            # Ensure timestamp is timezone-aware
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            # Read current state with exclusive lock
            state = self._read_state()

            # Update user's timestamp
            state[user_id] = {
                "last_processed_time": timestamp.isoformat().replace("+00:00", "Z")
            }

            # Write atomically
            self._write_state(state)
            logger.debug(f"Updated state for user {user_id}: {timestamp}")

        except Exception as e:
            logger.error(f"Error updating state for user {user_id}: {e}")
            raise

    def initialize_user(
        self, user_id: str, timestamp: Optional[datetime] = None
    ) -> None:
        """
        Initialize state for a user if not already present.
        Defaults to current time (UTC) to avoid backfill.

        Args:
            user_id: User identifier
            timestamp: Initial timestamp (defaults to now)
        """
        if self.get_last_processed_time(user_id) is None:
            if timestamp is None:
                timestamp = datetime.now(timezone.utc)

            self.update_last_processed_time(user_id, timestamp)
            logger.info(f"Initialized state for new user: {user_id}")

    def _read_state(self) -> Dict:
        """Read state file with file locking."""
        if not self.state_path.exists():
            return {}

        try:
            with open(self.state_path, "r") as f:
                # Use shared lock for reading
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    content = f.read()
                    if not content.strip():
                        return {}
                    return json.loads(content)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error reading state file: {e}")
            return {}

    def _write_state(self, state: Dict) -> None:
        """Write state file atomically with file locking."""
        try:
            # Create temp file path
            temp_path = self.state_path.with_suffix(".tmp")

            # Write to temp file with exclusive lock
            with open(temp_path, "w") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    json.dump(state, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)

            # Atomic rename
            os.rename(temp_path, self.state_path)

        except Exception as e:
            logger.error(f"Error writing state file: {e}")
            # Clean up temp file if it exists
            temp_path = self.state_path.with_suffix(".tmp")
            if temp_path.exists():
                temp_path.unlink()
            raise
