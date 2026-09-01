"""Load: UPSERT agregasi & update watermark (tanpa hapus raw)."""
from __future__ import annotations

from datetime import datetime

import structlog

from src.config import Settings
from src.db import Database
from src.transform import (
    AGG_VALUE_COLS_5MIN_1H,
    build_select_1h,
    build_select_5min,
    build_select_counter_daily,
    build_upsert_1h,
    build_upsert_5min,
    build_upsert_counter_daily,
    build_upsert_daily_from_1h,
    build_upsert_monthly_from_daily,
)

log = structlog.get_logger(__name__)

COUNTER_DAILY_COLS = [
    "bucket_start",
    "meter_id",
    "impep_start",
    "impep_end",
    "expep_start",
    "expep_end",
]


def update_watermark(datamart: Database, job_name: str, processed_until: datetime) -> None:
    with datamart.cursor() as cur:
        cur.execute(
            """
            INSERT INTO datamart.etl_state (job_name, last_processed_until, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (job_name) DO UPDATE SET
                last_processed_until = EXCLUDED.last_processed_until,
                updated_at = NOW()
            """,
            (job_name, processed_until),
        )


def _upsert_rows(datamart: Database, table: str, colnames: list[str], rows: list) -> int:
    if not rows:
        return 0
    insert_cols = ", ".join(colnames)
    placeholders = ", ".join(["%s"] * len(colnames))
    value_cols = [c for c in colnames if c not in ("bucket_start", "meter_id")]
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in value_cols)
    sql = f"""
        INSERT INTO {table} ({insert_cols})
        VALUES ({placeholders})
        ON CONFLICT (bucket_start, meter_id) DO UPDATE SET
            {updates}
    """
    with datamart.cursor() as cur:
        for row in rows:
            cur.execute(sql, row)
    return len(rows)


def _rollup_marts(
    conn,
    settings: Settings,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    tz = settings.timezone
    tol = settings.etl_boundary_tolerance_minutes

    with conn.cursor() as cur:
        cur.execute(build_upsert_daily_from_1h(tz), (window_end, window_start))
        counts["daily"] = cur.rowcount

    with conn.cursor() as cur:
        cur.execute(
            build_upsert_counter_daily(tz, tol),
            (window_start, window_end, tol, tol),
        )
        counts["counter_daily"] = cur.rowcount

    with conn.cursor() as cur:
        cur.execute(build_upsert_monthly_from_daily(tz), (window_end, window_start))
        counts["monthly"] = cur.rowcount

    return counts


def aggregate_window_same_db(
    conn,
    settings: Settings,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    tol = settings.etl_boundary_tolerance_minutes
    tz = settings.timezone
    wbp_s = settings.wbp_start_hour
    wbp_e = settings.wbp_end_hour

    sql_5 = build_upsert_5min(tz, wbp_s, wbp_e)
    with conn.cursor() as cur:
        cur.execute(sql_5, (window_start, window_end, window_end))
        counts["5min"] = cur.rowcount

    sql_1h = build_upsert_1h(tz, wbp_s, wbp_e, tol)
    with conn.cursor() as cur:
        cur.execute(
            sql_1h,
            (window_start, window_end, window_end, tol, tol, window_start, window_end),
        )
        counts["1h"] = cur.rowcount

    counts.update(_rollup_marts(conn, settings, window_start, window_end))
    return counts


def aggregate_window_cross_db(
    source: Database,
    datamart: Database,
    settings: Settings,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    tol = settings.etl_boundary_tolerance_minutes
    tz = settings.timezone
    wbp_s = settings.wbp_start_hour
    wbp_e = settings.wbp_end_hour

    colnames = ["bucket_start", "meter_id", *AGG_VALUE_COLS_5MIN_1H]

    with source.cursor() as cur:
        cur.execute(
            build_select_5min(tz, wbp_s, wbp_e),
            (window_start, window_end, window_end),
        )
        rows_5 = cur.fetchall()
    counts["5min"] = _upsert_rows(datamart, "datamart.meter_agg_5min", colnames, rows_5)

    with source.cursor() as cur:
        cur.execute(
            build_select_1h(tz, wbp_s, wbp_e, tol),
            (window_start, window_end, window_end, tol, tol, window_start, window_end),
        )
        rows_1h = cur.fetchall()
    counts["1h"] = _upsert_rows(datamart, "datamart.meter_agg_1h", colnames, rows_1h)

    with datamart.cursor() as cur:
        cur.execute(build_upsert_daily_from_1h(tz), (window_end, window_start))
        counts["daily"] = cur.rowcount

    with source.cursor() as cur:
        cur.execute(
            build_select_counter_daily(tz, tol),
            (window_start, window_end, tol, tol),
        )
        rows_counter = cur.fetchall()
    counts["counter_daily"] = _upsert_rows(
        datamart, "datamart.meter_counter_daily", COUNTER_DAILY_COLS, rows_counter
    )

    with datamart.cursor() as cur:
        cur.execute(build_upsert_monthly_from_daily(tz), (window_end, window_start))
        counts["monthly"] = cur.rowcount

    return counts
