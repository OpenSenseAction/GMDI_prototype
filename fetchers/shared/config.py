import logging
import os

import yaml

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yml") -> dict:
    """Load a YAML config file.

    Secrets are never stored inline in YAML. Fields ending in ``_env``
    (e.g. ``username_env``) hold the *name* of an environment variable;
    call :func:`resolve_env` to read the actual value at runtime.
    """
    logger.info("Loading config from %s", config_path)
    with open(config_path) as f:
        return yaml.safe_load(f)


def resolve_env(var_name: str) -> str:
    """Read a required environment variable.

    Raises ``ValueError`` if the variable is not set, so misconfiguration
    fails loudly at startup rather than silently later.
    """
    value = os.environ.get(var_name)
    if value is None:
        raise ValueError(
            f"Required environment variable {var_name!r} is not set"
        )
    return value
