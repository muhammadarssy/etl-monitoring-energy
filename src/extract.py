"""Extract: watermark, batas waktu, dan hitung readings di window."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from src.db import Database


def get_watermark(datamart: Database, job_name: str) -> Optional[datetime]:
    with datamart.cursor() as cur:
        cur.execute(
            """
            SELECT last_processed_until
            FROM datamart.etl_state
            WHERE job_name = %s
            """,
            (job_name,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_earliest_reading_time(source: Database) -> Optional[datetime]:
    with source.cursor() as cur:
        cur.execute("SELECT MIN(time) FROM public.meter_readings")
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else None


def get_upper_bound(source: Database, lag_minutes: int) -> datetime:
    with source.cursor() as cur:
        cur.execute("SELECT NOW()")
        now = cur.fetchone()[0]
    return now - timedelta(minutes=lag_minutes)


def default_watermark(backfill_days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=backfill_days)


def count_readings_in_window(
    source: Database,
    window_start: datetime,
    window_end: datetime,
) -> int:
    with source.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM public.meter_readings
            WHERE time >= %s AND time < %s
            """,
            (window_start, window_end),
        )
        return int(cur.fetchone()[0])
