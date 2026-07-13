"""APIFetcher: polls a paginated REST API and writes raw JSON files.

One APIFetcher is created per *source* entry in ``config.yml``.  Each fetch
cycle:

1. Computes the ``[window_start, window_end)`` time window.
2. For every variant in ``param_variants`` (e.g. RSL vs TSL), fetches all
   pages from the API.
3. Writes the combined page results as a single JSON file to
   ``incoming_dir/``.
4. Advances the state cursor so the next cycle continues from ``window_end``.
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from shared.incoming_writer import atomic_write
from shared.state import FetcherState

logger = logging.getLogger(__name__)

_DT_FMT = "%Y-%m-%dT%H:%M:%SZ"  # datetime window_format
_DATE_FMT = "%Y-%m-%d"  # date window_format


class APIFetcher:
    """Fetch one configured API source.

    :param name: Source name; used as the file-name prefix and state key.
    :param cfg: The ``source`` dict from ``config.yml``.
    :param incoming_dir: ``Path`` to the shared ``data/incoming/`` directory.
    :param state: Shared :class:`~shared.state.FetcherState` instance.
    :param auth: A :class:`~auth.JWTAuth` (or compatible) auth object.
    :param client: An open ``httpx.Client`` session.
    """

    def __init__(
        self,
        name: str,
        cfg: dict,
        incoming_dir: Path,
        state: FetcherState,
        auth,
        client,
    ):
        self.name = name
        self.cfg = cfg
        self.incoming_dir = incoming_dir
        self.state = state
        self.auth = auth
        self.client = client

        self._window_format: str = cfg.get("window_format", "datetime")
        self._chunk_hours: int = cfg.get("chunk_hours", 24)
        self._overlap_seconds: int = cfg.get("overlap_seconds", 60)
        self._mode: str = cfg.get("mode", "continuous")
        self._endpoint: str = cfg["endpoint"]
        self._base_params: dict = cfg.get("base_params", {})
        self._param_variants: list[dict] = cfg.get("param_variants", [{}])
        self._page_key: str = cfg.get("page_key", "page")
        self._page_size_key: str = cfg.get("page_size_key", "page_size")
        self._page_size: int = cfg.get("page_size", 100)
        self._results_key: Optional[str] = cfg.get("results_key")
        self._date_from_key: str = cfg.get("date_from_key", "date_from")
        self._date_to_key: str = cfg.get("date_to_key", "date_to")

        # Backfill mode: fixed range
        self._backfill_start: Optional[str] = cfg.get("backfill_start")
        self._backfill_end: Optional[str] = cfg.get("backfill_end")

    # ------------------------------------------------------------------
    # Window computation
    # ------------------------------------------------------------------

    def _fmt(self, dt: datetime) -> str:
        if self._window_format == "date":
            return dt.strftime(_DATE_FMT)
        return dt.strftime(_DT_FMT)

    def _parse(self, s: str) -> datetime:
        for fmt in (_DT_FMT, _DATE_FMT):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        raise ValueError(f"Cannot parse datetime string: {s!r}")

    def _next_window(self) -> Optional[tuple[str, str]]:
        now = datetime.now(tz=timezone.utc)

        if self._mode == "backfill":
            if not self._backfill_start or not self._backfill_end:
                raise ValueError(
                    f"Source {self.name!r}: backfill mode requires "
                    "'backfill_start' and 'backfill_end'"
                )
            cursor = self.state.get_cursor(self.name)
            start = self._parse(cursor) if cursor else self._parse(self._backfill_start)
            end_limit = self._parse(self._backfill_end)

            if start >= end_limit:
                logger.info(
                    "Source %s: backfill complete (cursor >= backfill_end)", self.name
                )
                return None

            end = min(start + timedelta(hours=self._chunk_hours), end_limit)
            return self._fmt(start), self._fmt(end)

        # continuous mode
        cursor = self.state.get_cursor(self.name)
        if cursor:
            cursor_dt = self._parse(cursor)
            # Shift the left edge back by overlap to catch late-arriving records,
            # but always advance the right edge beyond the cursor.
            start = cursor_dt - timedelta(seconds=self._overlap_seconds)
            end = min(cursor_dt + timedelta(hours=self._chunk_hours), now)
        else:
            start = now - timedelta(hours=self._chunk_hours)
            end = now

        if end <= start:
            return None

        return self._fmt(start), self._fmt(end)

    # ------------------------------------------------------------------
    # Fetching
    # ------------------------------------------------------------------

    def _fetch_variant(self, date_from: str, date_to: str, variant: dict) -> list:
        """Fetch all pages for one variant, return combined records list."""
        params = {
            **self._base_params,
            self._date_from_key: date_from,
            self._date_to_key: date_to,
            **variant,
        }
        if self._page_size:
            params[self._page_size_key] = self._page_size

        records: list = []
        page = 1
        while True:
            params[self._page_key] = page
            resp = self.auth.get(self.client, self._endpoint, params=params)
            data = resp.json()

            if self._results_key:
                page_records = data.get(self._results_key, [])
            elif isinstance(data, list):
                page_records = data
            else:
                # Assume a mapping with a natural results container
                page_records = data.get("results", data.get("data", []))

            records.extend(page_records)

            total_count = data.get("count") if isinstance(data, dict) else None
            if total_count is not None and len(records) >= total_count:
                break
            if len(page_records) < self._page_size:
                break
            page += 1

        logger.debug(
            "Source %s variant %s: fetched %d records for %s→%s",
            self.name,
            variant,
            len(records),
            date_from,
            date_to,
        )
        return records

    def _make_filename(self, date_from: str, date_to: str, variant: dict) -> str:
        # Use the "suffix" key of the variant as a file tag, or empty.
        suffix = variant.get("suffix", "")
        safe_from = date_from.replace(":", "").replace("-", "")
        safe_to = date_to.replace(":", "").replace("-", "")
        parts = [self.name, safe_from, safe_to]
        if suffix:
            parts.append(suffix)
        parts.append("data")
        return "_".join(parts) + ".json"

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def fetch(self) -> bool:
        """Run one fetch cycle.  Returns ``True`` if any data was written."""
        window = self._next_window()
        if window is None:
            return False

        date_from, date_to = window
        wrote_any = False

        for variant in self._param_variants:
            # Remove the internal 'suffix' key before sending to the API
            api_variant = {k: v for k, v in variant.items() if k != "suffix"}
            try:
                records = self._fetch_variant(date_from, date_to, api_variant)
            except Exception:
                logger.exception(
                    "Source %s: failed fetching variant %s", self.name, variant
                )
                continue

            if not records:
                logger.info(
                    "Source %s: no records for %s→%s variant %s",
                    self.name,
                    date_from,
                    date_to,
                    variant,
                )
            else:
                filename = self._make_filename(date_from, date_to, variant)
                content = json.dumps(records, ensure_ascii=False).encode()
                atomic_write(self.incoming_dir, filename, content)
                wrote_any = True

        # Advance cursor to window end regardless of whether we got records
        # (so we don't repeatedly re-fetch empty windows).
        self.state.set_cursor(self.name, date_to)
        return wrote_any
