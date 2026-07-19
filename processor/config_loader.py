"""
Configuration loader for rain processing service.
Loads and validates YAML configuration with support for hot-reloading.
"""

import yaml
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class RainProcessingConfig:
    """
    Loads and manages rain processing configuration from YAML.
    Supports hot-reloading without service restart.
    """

    def __init__(self, config_path: str = "/app/config/rain_processing.yml"):
        """
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self._last_loaded: Optional[datetime] = None
        self.load()

    def load(self) -> None:
        """
        Load configuration from YAML file.
        Validates structure and raises ValueError if invalid.

        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If YAML is malformed
            ValueError: If required fields are missing or invalid
        """
        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)

            # Validate structure
            self._validate_config(config)

            self._config = config
            self._last_loaded = datetime.now(timezone.utc)
            logger.info(f"Configuration loaded from {self.config_path}")

        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"YAML parsing error: {e}")
            raise
        except ValueError as e:
            logger.error(f"Configuration validation error: {e}")
            raise

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate configuration structure.

        Args:
            config: Configuration dictionary to validate

        Raises:
            ValueError: If configuration is invalid
        """
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")

        # Validate global section
        if "global" not in config:
            raise ValueError("Missing 'global' section in configuration")

        global_cfg = config["global"]
        if not isinstance(global_cfg, dict):
            raise ValueError("'global' section must be a dictionary")

        # Validate users section
        if "users" not in config:
            raise ValueError("Missing 'users' section in configuration")

        users_cfg = config["users"]
        if not isinstance(users_cfg, dict):
            raise ValueError("'users' section must be a dictionary")

        # Validate each user entry
        for user_id, user_config in users_cfg.items():
            if not isinstance(user_config, dict):
                raise ValueError(f"User '{user_id}' configuration must be a dictionary")

            # Check required fields
            if "enabled" not in user_config:
                raise ValueError(f"User '{user_id}' missing 'enabled' field")

            if not isinstance(user_config["enabled"], bool):
                raise ValueError(f"User '{user_id}' 'enabled' field must be boolean")

            if "processing_variant" not in user_config:
                raise ValueError(f"User '{user_id}' missing 'processing_variant' field")

            # Validate optional fields if present
            if "poll_interval_seconds" in user_config:
                if (
                    not isinstance(user_config["poll_interval_seconds"], int)
                    or user_config["poll_interval_seconds"] <= 0
                ):
                    raise ValueError(
                        f"User '{user_id}' 'poll_interval_seconds' must be a positive integer"
                    )

            if "data_window_minutes" in user_config:
                if (
                    not isinstance(user_config["data_window_minutes"], int)
                    or user_config["data_window_minutes"] <= 0
                ):
                    raise ValueError(
                        f"User '{user_id}' 'data_window_minutes' must be a positive integer"
                    )

    def should_reload(self, reload_interval_seconds: int) -> bool:
        """
        Check if enough time has passed to reload config.

        Args:
            reload_interval_seconds: Time between reloads

        Returns:
            True if config should be reloaded
        """
        if self._last_loaded is None:
            return True

        now = datetime.now(timezone.utc)
        elapsed = (now - self._last_loaded).total_seconds()
        return elapsed >= reload_interval_seconds

    def get_global_config(self) -> Dict[str, Any]:
        """Get global configuration section."""
        return self._config.get("global", {})

    def get_user_config(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific user.

        Args:
            user_id: User identifier

        Returns:
            User config dict, or None if user not in config
        """
        return self._config.get("users", {}).get(user_id)

    def get_enabled_users(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all users where enabled=true.

        Returns:
            Dict mapping user_id to user config
        """
        users = self._config.get("users", {})
        return {uid: cfg for uid, cfg in users.items() if cfg.get("enabled", False)}
