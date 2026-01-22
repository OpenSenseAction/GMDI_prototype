"""Database writer utilities for the parser service.

Provides a DBWriter class that handles connections and writes for
`cml_metadata` and `cml_data` tables. Uses psycopg2 and
psycopg2.extras.execute_values for batch inserts.

This module is intentionally minimal and logs errors rather than
exiting the process so the caller can decide how to handle failures.
"""

from typing import List, Tuple, Optional, Set
import time
import psycopg2
import psycopg2.extras
import logging

logger = logging.getLogger(__name__)


class DBWriter:
    """Simple database writer helper.

    Usage:
        db = DBWriter(os.getenv('DATABASE_URL'))
        db.connect()
        db.write_metadata(df)
        db.write_rawdata(df)
        db.close()
    """

    def __init__(self, db_url: str, connect_timeout: int = 10):
        self.db_url = db_url
        self.connect_timeout = connect_timeout
        self.conn: Optional[psycopg2.extensions.connection] = None

        # Retry configuration
        self.max_retries = 3
        self.retry_backoff_seconds = 2

    def _attempt_connect(self) -> psycopg2.extensions.connection:
        """Attempt a single database connection."""
        return psycopg2.connect(self.db_url, connect_timeout=self.connect_timeout)

    def connect(self) -> None:
        if self.conn:
            return

        logger.debug("Connecting to database with retries")
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self.conn = self._attempt_connect()
                logger.debug("Database connection established")
                return
            except Exception as e:
                last_exc = e
                logger.warning(
                    "Database connection attempt %d/%d failed: %s",
                    attempt,
                    self.max_retries,
                    e,
                )
                if attempt < self.max_retries:
                    sleep_time = self.retry_backoff_seconds * (2 ** (attempt - 1))
                    logger.debug("Sleeping %s seconds before retry", sleep_time)
                    time.sleep(sleep_time)

        logger.exception("All database connection attempts failed")
        raise last_exc

    def is_connected(self) -> bool:
        if self.conn is None:
            return False

        # psycopg2 connection uses `.closed` with integer 0 when open.
        # Tests may supply Mock objects where `.closed` is a Mock (truthy).
        # Be permissive: if `.closed` is an int/bool, treat 0/False as connected.
        closed = getattr(self.conn, "closed", None)
        if isinstance(closed, (int, bool)):
            return closed == 0 or closed is False

        # Unknown `.closed` type (e.g. Mock); assume connection is present.
        return True

    def close(self) -> None:
        if self.conn and not self.conn.closed:
            try:
                self.conn.close()
            except Exception:
                logger.exception("Error closing DB connection")
        self.conn = None

    def get_existing_metadata_ids(self) -> Set[str]:
        """Return set of cml_id values present in cml_metadata."""
        if not self.is_connected():
            raise RuntimeError("Not connected to database")

        cur = self.conn.cursor()
        try:
            cur.execute("SELECT cml_id FROM cml_metadata")
            rows = cur.fetchall()
            return {str(r[0]) for r in rows}
        finally:
            cur.close()

    def validate_rawdata_references(self, df) -> Tuple[bool, List[str]]:
        """Check that all cml_id values in df exist in cml_metadata.

        Returns (True, []) if all present, otherwise (False, missing_ids).
        """
        if df is None or df.empty:
            return True, []

        cml_ids = set(df["cml_id"].astype(str).unique())
        existing = self.get_existing_metadata_ids()
        missing = sorted(list(cml_ids - existing))
        return (len(missing) == 0, missing)

    def write_metadata(self, df) -> int:
        """Write metadata DataFrame to `cml_metadata`.

        Uses `ON CONFLICT (cml_id) DO UPDATE` to be idempotent.
        Returns number of rows written (or updated).
        """
        if df is None or df.empty:
            return 0

        if not self.is_connected():
            raise RuntimeError("Not connected to database")

        # Convert DataFrame to list of tuples
        cols = ["cml_id", "site_0_lon", "site_0_lat", "site_1_lon", "site_1_lat"]
        df_subset = df[cols].copy()
        df_subset["cml_id"] = df_subset["cml_id"].astype(str)
        records = [tuple(x) for x in df_subset.to_numpy()]

        sql = (
            "INSERT INTO cml_metadata (cml_id, site_0_lon, site_0_lat, site_1_lon, site_1_lat) "
            "VALUES %s "
            "ON CONFLICT (cml_id) DO UPDATE SET "
            "site_0_lon = EXCLUDED.site_0_lon, "
            "site_0_lat = EXCLUDED.site_0_lat, "
            "site_1_lon = EXCLUDED.site_1_lon, "
            "site_1_lat = EXCLUDED.site_1_lat"
        )

        cur = self.conn.cursor()
        try:
            psycopg2.extras.execute_values(
                cur, sql, records, template=None, page_size=1000
            )
            self.conn.commit()
            return len(records)
        except Exception:
            self.conn.rollback()
            logger.exception("Failed to write metadata to database")
            raise
        finally:
            cur.close()

    def write_rawdata(self, df) -> int:
        """Write raw time series DataFrame to `cml_data`.

        Expects df to have columns: time, cml_id, sublink_id, rsl, tsl
        Returns number of rows written.
        """
        if df is None or df.empty:
            return 0

        if not self.is_connected():
            raise RuntimeError("Not connected to database")

        # Convert DataFrame to list of tuples
        cols = ["time", "cml_id", "sublink_id", "rsl", "tsl"]
        df_subset = df[cols].copy()
        df_subset["cml_id"] = df_subset["cml_id"].astype(str)
        df_subset["sublink_id"] = (
            df_subset["sublink_id"].astype(str).replace("nan", None)
        )
        records = [tuple(x) for x in df_subset.to_numpy()]

        sql = "INSERT INTO cml_data (time, cml_id, sublink_id, rsl, tsl) VALUES %s"

        cur = self.conn.cursor()
        try:
            psycopg2.extras.execute_values(
                cur, sql, records, template=None, page_size=1000
            )
            self.conn.commit()
            return len(records)
        except Exception:
            self.conn.rollback()
            logger.exception("Failed to write raw data to database")
            raise
        finally:
            cur.close()
