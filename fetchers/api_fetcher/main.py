"""Entry point for the API fetcher service.

Reads ``config.yml`` (or the path set by ``CONFIG_PATH``), creates one
:class:`~fetcher.APIFetcher` per source, and drives them in a round-robin
poll loop.
"""

import logging
import os
import sys
from pathlib import Path

import httpx

from shared.config import load_config, resolve_env
from shared.polling import run_poll_loop
from shared.state import FetcherState
from auth import JWTAuth
from fetcher import APIFetcher


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s  %(name)-30s  %(levelname)-8s  %(message)s",
    )


def build_auth(auth_cfg: dict, client: httpx.Client):
    auth_type = auth_cfg.get("type", "jwt")
    if auth_type == "jwt":
        username = resolve_env(auth_cfg["username_env"])
        password = resolve_env(auth_cfg["password_env"])
        return JWTAuth(
            login_url=auth_cfg["login_url"],
            refresh_url=auth_cfg["refresh_url"],
            username=username,
            password=password,
        )
    if auth_type == "bearer":
        token = resolve_env(auth_cfg["token_env"])

        class _BearerAuth:
            def get(self, c, url, params=None):
                resp = c.get(
                    url, params=params, headers={"Authorization": f"Bearer {token}"}
                )
                resp.raise_for_status()
                return resp

        return _BearerAuth()
    if auth_type == "api_key":
        key = resolve_env(auth_cfg["key_env"])
        header = auth_cfg.get("header", "X-Api-Key")

        class _APIKeyAuth:
            def get(self, c, url, params=None):
                resp = c.get(url, params=params, headers={header: key})
                resp.raise_for_status()
                return resp

        return _APIKeyAuth()
    raise ValueError(f"Unsupported auth type: {auth_type!r}")


def main() -> None:
    config_path = os.environ.get("CONFIG_PATH", "config.yml")
    cfg = load_config(config_path)

    log_level = cfg.get("log_level", os.environ.get("LOG_LEVEL", "INFO"))
    setup_logging(log_level)
    log = logging.getLogger("api_fetcher")

    incoming_dir = Path(os.environ.get("INCOMING_DIR", cfg.get("incoming_dir", "/app/data/incoming")))
    state_path = Path(os.environ.get("STATE_PATH", cfg.get("state_path", "/app/data/state/api_fetcher_state.json")))
    poll_interval = float(cfg.get("poll_interval_seconds", 60))

    state = FetcherState(state_path)

    sources: list[dict] = cfg.get("sources", [])
    if not sources:
        log.error("No sources defined in config; exiting")
        sys.exit(1)

    # Shared HTTP client (connection pooling across all sources)
    with httpx.Client(timeout=30) as client:
        fetchers: list[APIFetcher] = []
        for src in sources:
            auth = build_auth(src["auth"], client)
            fetcher = APIFetcher(
                name=src["name"],
                cfg=src,
                incoming_dir=incoming_dir,
                state=state,
                auth=auth,
                client=client,
            )
            fetchers.append(fetcher)
            log.info("Registered source %r (mode=%s)", src["name"], src.get("mode", "continuous"))

        def poll():
            for f in fetchers:
                f.fetch()

        log.info(
            "Starting poll loop: %d source(s), interval=%.0fs",
            len(fetchers),
            poll_interval,
        )
        run_poll_loop(poll, poll_interval, log)


if __name__ == "__main__":
    main()
