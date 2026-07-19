"""
Database interface for CML data operations.
Provides fixed API for workflows to read/write data.
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from typing import List, Optional
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class CMLDataInterface:
    """
    Database interface for CML data operations.
    Provides fixed API for workflows to read/write data.
    """

    def __init__(self, database_url: str):
        """
        Args:
            database_url: PostgreSQL connection string
        """
        self.database_url = database_url
        self._connection: Optional[psycopg2.extensions.connection] = None

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(self.database_url)
            yield conn
        finally:
            if conn is not None:
                conn.close()

    def fetch_raw_cml_data_rows(
        self, user_id: str, start_time: datetime, end_time: datetime
    ) -> pd.DataFrame:
        """
        Fetch raw CML data (RSL, TSL) for a time window as tabular rows.

        Args:
            user_id: User identifier
            start_time: Window start (inclusive)
            end_time: Window end (inclusive)

        Returns:
            DataFrame with columns: time, cml_id, sublink_id, user_id, rsl, tsl
            Sorted by time ascending
            Empty DataFrame if no data found
        """
        # Ensure timestamps are timezone-aware
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        query = """
            SELECT time, cml_id, sublink_id, user_id, rsl, tsl
            FROM cml_data
            WHERE user_id = %s 
              AND time >= %s 
              AND time <= %s
            ORDER BY time ASC
        """

        try:
            with self._get_connection() as conn:
                df = pd.read_sql_query(
                    query, conn, params=(user_id, start_time, end_time), index_col=None
                )

                if df.empty:
                    logger.debug(f"No raw data found for user {user_id} in time window")
                    return df

                # Ensure time column is timezone-aware
                if df["time"].dt.tz is None:
                    df["time"] = df["time"].dt.tz_localize("UTC")

                logger.debug(f"Fetched {len(df)} raw data rows for user {user_id}")
                return df

        except psycopg2.Error as e:
            logger.error(f"Database error fetching raw data: {e}")
            raise

    def fetch_cml_metadata_rows(
        self, user_id: str, cml_ids: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Fetch CML metadata (coordinates, frequency, length, polarization) as tabular rows.

        Args:
            user_id: User identifier
            cml_ids: List of CML IDs to fetch (None = fetch all for user)

        Returns:
            DataFrame with columns: cml_id, sublink_id, site_0_lon, site_0_lat,
                                   site_1_lon, site_1_lat, frequency, polarization, length
            Empty DataFrame if no metadata found
        """
        query = """
            SELECT cml_id, sublink_id, site_0_lon, site_0_lat, 
                   site_1_lon, site_1_lat, frequency, polarization, length
            FROM cml_metadata
            WHERE user_id = %s
        """

        params = [user_id]

        if cml_ids is not None:
            placeholders = ",".join(["%s"] * len(cml_ids))
            query += f" AND cml_id IN ({placeholders})"
            params.extend(cml_ids)

        try:
            with self._get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params, index_col=None)

                if df.empty:
                    logger.debug(f"No metadata found for user {user_id}")
                    return df

                logger.debug(f"Fetched {len(df)} metadata rows for user {user_id}")
                return df

        except psycopg2.Error as e:
            logger.error(f"Database error fetching metadata: {e}")
            raise

    def write_rain_data(self, rain_df: pd.DataFrame) -> int:
        """
        Write processed rain data to cml_rain_data table.

        Args:
            rain_df: DataFrame with columns: time, cml_id, sublink_id, user_id,
                     tl, wet, baseline, waa, a_rain, r

        Returns:
            Number of rows written

        Raises:
            ValueError: If required columns are missing
            psycopg2.Error: On database errors
        """
        # Validate required columns
        required_columns = [
            "time",
            "cml_id",
            "sublink_id",
            "user_id",
            "tl",
            "wet",
            "baseline",
            "waa",
            "a_rain",
            "r",
        ]

        missing_columns = [
            col for col in required_columns if col not in rain_df.columns
        ]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        if rain_df.empty:
            logger.debug("No rain data to write")
            return 0

        # Prepare data for insertion
        # Convert wet column to boolean-compatible format
        df_copy = rain_df.copy()

        # Ensure time is timezone-aware and convert to UTC
        if df_copy["time"].dt.tz is None:
            df_copy["time"] = df_copy["time"].dt.tz_localize("UTC")
        else:
            df_copy["time"] = df_copy["time"].dt.tz_convert("UTC")

        # Convert to list of tuples for execute_values
        records = []
        for _, row in df_copy.iterrows():
            record = (
                row["time"],
                row["cml_id"],
                row["sublink_id"],
                row["user_id"],
                float(row["tl"]) if pd.notna(row["tl"]) else None,
                bool(row["wet"]) if pd.notna(row["wet"]) else None,
                float(row["baseline"]) if pd.notna(row["baseline"]) else None,
                float(row["waa"]) if pd.notna(row["waa"]) else None,
                float(row["a_rain"]) if pd.notna(row["a_rain"]) else None,
                float(row["r"]) if pd.notna(row["r"]) else None,
            )
            records.append(record)

        query = """
            INSERT INTO cml_rain_data 
            (time, cml_id, sublink_id, user_id, tl, wet, baseline, waa, a_rain, r)
            VALUES %s
            ON CONFLICT (time, cml_id, sublink_id, user_id) DO NOTHING
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, query, records)
                    conn.commit()

                    rows_written = cur.rowcount
                    logger.info(f"Wrote {rows_written} rain data rows to database")
                    return rows_written

        except psycopg2.Error as e:
            logger.error(f"Database error writing rain data: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.debug("Database connection closed")
