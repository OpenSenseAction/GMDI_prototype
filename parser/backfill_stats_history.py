#!/usr/bin/env python3
"""Backfill cml_stats_history from existing cml_data_1h data.

Run once after applying migration 015 to populate historical snapshots.

Usage:
    docker compose exec parser python -m parser.backfill_stats_history

Or with environment variables:
    DATABASE_URL=postgresql://... USER_ID=demo_openmrg python backfill_stats_history.py
"""

import os
import sys
from datetime import datetime, timezone

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_writer import DBWriter


def backfill_stats_history():
    """Backfill cml_stats_history for all available hours."""
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://myuser:mypassword@database:5432/mydatabase"
    )
    user_id = os.getenv("USER_ID", "demo_openmrg")
    
    print(f"Starting backfill for user_id={user_id}")
    
    db = DBWriter(database_url, user_id=user_id)
    db.connect()
    
    try:
        # Get all distinct hours from cml_data_1h
        cur = db.conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT date_trunc('hour', bucket) as hour
            FROM cml_data_1h
            WHERE user_id = %s
            ORDER BY hour ASC
            """,
            (user_id,)
        )
        hours = [row[0] for row in cur.fetchall()]
        cur.close()
        
        if not hours:
            print("No historical data found in cml_data_1h")
            return
        
        print(f"Found {len(hours)} hours to backfill")
        
        # Backfill each hour
        processed = 0
        for i, hour in enumerate(hours):
            try:
                rows = db.write_stats_snapshot(hour)
                processed += 1
                
                if processed % 100 == 0 or i == len(hours) - 1:
                    print(f"Progress: {processed}/{len(hours)} hours processed ({100.0*processed/len(hours):.1f}%)")
            except Exception as e:
                print(f"Error processing hour {hour}: {e}")
                continue
        
        print(f"Backfill complete: {processed}/{len(hours)} hours successfully processed")
        
    finally:
        db.close()


if __name__ == "__main__":
    backfill_stats_history()
