"""Extract: watermark, batas waktu, dan hitung readings di window."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from src.db import Database


def as_aware_utc(ts: datetime) -> datetime:
    """Normalisasi timestamp dari DB agar selalu aware UTC.

    Sesi koneksi di-pin ke UTC. Jika driver mengembalikan naive datetime,
    anggap UTC — jangan interpretasikan sebagai WIB, supaya tidak geser 7 jam
    saat OS/Airflow UTC dan cluster PostgreSQL GMT+7.
    """
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


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
        return as_aware_utc(row[0]) if row and row[0] is not None else None


def get_earliest_reading_time(source: Database) -> Optional[datetime]:
    with source.cursor() as cur:
        cur.execute("SELECT MIN(time) FROM public.meter_readings")
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return as_aware_utc(row[0])


def get_upper_bound(source: Database, lag_minutes: int) -> datetime:
    with source.cursor() as cur:
        cur.execute("SELECT NOW()")
        now = cur.fetchone()[0]
    return as_aware_utc(now) - timedelta(minutes=lag_minutes)


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
