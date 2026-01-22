"""Database writer utilities for the parser service.

Provides a DBWriter class that handles connections and writes for
`cml_metadata` and `cml_data` tables. Uses psycopg2 and
psycopg2.extras.execute_values for batch inserts.

This module is intentionally minimal and logs errors rather than
exiting the process so the caller can decide how to handle failures.
"""

from typing import List, Tuple, Optional, Set
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

    def connect(self) -> None:
        if self.conn:
            return

        logger.debug("Connecting to database with retries")
        attempt = 0
        last_exc = None
        while attempt < self.max_retries:
            try:
                self.conn = psycopg2.connect(self.db_url, connect_timeout=self.connect_timeout)
                logger.debug("Database connection established")
                return
            except Exception as e:
                last_exc = e
                attempt += 1
                logger.warning("Database connection attempt %d/%d failed: %s", attempt, self.max_retries, e)
                if attempt < self.max_retries:
                    sleep_time = self.retry_backoff_seconds * (2 ** (attempt - 1))
                    logger.debug("Sleeping %s seconds before retry", sleep_time)
                    time_to_sleep = sleep_time
                    import time

                    time.sleep(time_to_sleep)

        logger.exception("All database connection attempts failed")
        # re-raise the last exception so callers can handle it
        raise last_exc

    def is_connected(self) -> bool:
        return self.conn is not None and not self.conn.closed

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

        records = []
        for _, row in df.iterrows():
            records.append(
                (
                    str(row.get("cml_id")),
                    (
                        float(row.get("site_0_lon"))
                        if row.get("site_0_lon") is not None
                        else None
                    ),
                    (
                        float(row.get("site_0_lat"))
                        if row.get("site_0_lat") is not None
                        else None
                    ),
                    (
                        float(row.get("site_1_lon"))
                        if row.get("site_1_lon") is not None
                        else None
                    ),
                    (
                        float(row.get("site_1_lat"))
                        if row.get("site_1_lat") is not None
                        else None
                    ),
                )
            )

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

        records = []
        for _, row in df.iterrows():
            # psycopg2 will accept Python datetimes or ISO strings
            records.append(
                (
                    row.get("time"),
                    str(row.get("cml_id")),
                    (
                        str(row.get("sublink_id"))
                        if row.get("sublink_id") is not None
                        else None
                    ),
                    (
                        float(row.get("rsl"))
                        if row.get("rsl") is not None
                        and not (str(row.get("rsl")) == "nan")
                        else None
                    ),
                    (
                        float(row.get("tsl"))
                        if row.get("tsl") is not None
                        and not (str(row.get("tsl")) == "nan")
                        else None
                    ),
                )
            )

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
