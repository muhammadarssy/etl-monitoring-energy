"""Job harian: sesi nyala/mati mesin dari Pt (energy) dan running time (utils).

Jadwalkan terpisah dari ETL 5 menit/1 jam, 1x per hari setelah hari WIB selesai.

PT residual saat mesin mati sering tidak persis 0; nilai <= PT_ON_THRESHOLD
dianggap noise/mati (default 5).

  python runtime_daily.py --init-schema
  python runtime_daily.py
  python runtime_daily.py --day 2026-09-01
  python runtime_daily.py --pt-on-threshold 5
  python runtime_daily.py --reset
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import structlog

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.config import Settings, get_settings
from src.db import Database
from src.extract import (
    default_watermark,
    get_earliest_reading_time,
    get_upper_bound,
    get_watermark,
)
from src.load import update_watermark

log = structlog.get_logger(__name__)

RUNTIME_COLS = [
    "bucket_start",
    "meter_id",
    "device_type",
    "session_no",
    "start_time",
    "stop_time",
    "running_seconds",
    "still_running",
    "sample_count",
    "pt_max",
]


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


def _tzinfo(tz_name: str):
    """Asia/Jakarta UTC+7 (tanpa DST). ZoneInfo tidak diandalkan di Windows tanpa tzdata."""
    if tz_name in ("Asia/Jakarta", "Asia/Bangkok", "Asia/Ho_Chi_Minh"):
        return timezone(timedelta(hours=7))
    if tz_name in ("UTC", "Etc/UTC"):
        return timezone.utc
    return timezone(timedelta(hours=7))


def day_start_of(ts: datetime, tz_name: str) -> datetime:
    tz = _tzinfo(tz_name)
    local = ts.astimezone(tz)
    return local.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_day(value: str, tz_name: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=_tzinfo(tz_name))


def build_select_runtime(tz: str) -> str:
    """Energy: 1 baris per sesi Pt > threshold. Utils: 1 baris per hari, jumlah durasi nyala.

    Threshold memotong noise residual: PT <= pt_on dianggap mati.
    """
    return f"""
    WITH params AS (
        SELECT
            %s::timestamptz AS day_start,
            %s::timestamptz AS day_end,
            %s::double precision AS pt_on
    ),
    energy_s AS (
        SELECT
            r.meter_id,
            r.device_type,
            r.time,
            r.pt,
            p.day_start,
            p.day_end,
            (COALESCE(r.pt, 0) > p.pt_on) AS is_on,
            LAG(COALESCE(r.pt, 0) > p.pt_on) OVER (
                PARTITION BY r.meter_id ORDER BY r.time
            ) AS prev_on,
            LEAD(COALESCE(r.pt, 0) > p.pt_on) OVER (
                PARTITION BY r.meter_id ORDER BY r.time
            ) AS next_on,
            LEAD(r.time) OVER (
                PARTITION BY r.meter_id ORDER BY r.time
            ) AS next_time
        FROM public.meter_readings r
        CROSS JOIN params p
        WHERE r.time >= p.day_start
          AND r.time < p.day_end
          AND r.device_type = 'energy'
    ),
    energy_marked AS (
        SELECT
            *,
            SUM(
                CASE WHEN is_on AND NOT COALESCE(prev_on, FALSE) THEN 1 ELSE 0 END
            ) OVER (PARTITION BY meter_id ORDER BY time) AS session_no
        FROM energy_s
    ),
    energy_sessions AS (
        SELECT
            day_start AS bucket_start,
            meter_id,
            MAX(device_type) AS device_type,
            session_no,
            MIN(time) AS start_time,
            MAX(
                CASE
                    WHEN is_on AND next_on IS FALSE THEN next_time
                    WHEN is_on AND next_time IS NULL THEN day_end
                    ELSE NULL
                END
            ) AS stop_time,
            BOOL_OR(is_on AND next_time IS NULL) AS still_running,
            COUNT(*)::integer AS sample_count,
            MAX(pt) AS pt_max
        FROM energy_marked
        WHERE is_on AND session_no >= 1
        GROUP BY day_start, meter_id, session_no
    ),
    utils_s AS (
        SELECT
            r.meter_id,
            r.device_type,
            r.time,
            r.pt,
            p.day_start,
            p.day_end,
            (COALESCE(r.pt, 0) > p.pt_on) AS is_on,
            LEAD(r.time) OVER (
                PARTITION BY r.meter_id ORDER BY r.time
            ) AS next_time
        FROM public.meter_readings r
        CROSS JOIN params p
        WHERE r.time >= p.day_start
          AND r.time < p.day_end
          AND r.device_type = 'utils'
    ),
    utils_daily AS (
        SELECT
            day_start AS bucket_start,
            meter_id,
            MAX(device_type) AS device_type,
            1 AS session_no,
            MIN(time) FILTER (WHERE is_on) AS start_time,
            MAX(COALESCE(next_time, day_end)) FILTER (WHERE is_on) AS stop_time,
            COALESCE(
                SUM(
                    EXTRACT(EPOCH FROM (COALESCE(next_time, day_end) - time))
                ) FILTER (WHERE is_on),
                0
            ) AS running_seconds,
            BOOL_OR(is_on AND next_time IS NULL) AS still_running,
            COUNT(*) FILTER (WHERE is_on)::integer AS sample_count,
            MAX(pt) FILTER (WHERE is_on) AS pt_max
        FROM utils_s
        GROUP BY day_start, meter_id
        HAVING COUNT(*) FILTER (WHERE is_on) > 0
    )
    SELECT
        bucket_start,
        meter_id,
        device_type,
        session_no,
        start_time,
        stop_time,
        EXTRACT(EPOCH FROM (stop_time - start_time)) AS running_seconds,
        still_running,
        sample_count,
        pt_max
    FROM energy_sessions
    UNION ALL
    SELECT
        bucket_start,
        meter_id,
        device_type,
        session_no,
        start_time,
        stop_time,
        running_seconds,
        still_running,
        sample_count,
        pt_max
    FROM utils_daily
    ORDER BY 1, 2, 4
    """


def _replace_day(
    datamart: Database,
    day_start: datetime,
    rows: list,
) -> int:
    with datamart.cursor() as cur:
        cur.execute(
            """
            DELETE FROM datamart.meter_runtime_session
            WHERE bucket_start = %s
            """,
            (day_start,),
        )
        if not rows:
            return 0
        insert_cols = ", ".join(RUNTIME_COLS)
        placeholders = ", ".join(["%s"] * len(RUNTIME_COLS))
        sql = f"""
            INSERT INTO datamart.meter_runtime_session ({insert_cols})
            VALUES ({placeholders})
        """
        cur.executemany(sql, rows)
        return len(rows)


def reset_runtime(datamart: Database, job_name: str) -> dict:
    """Hapus semua sesi runtime dan watermark job agar bisa dihitung ulang."""
    with datamart.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM datamart.meter_runtime_session")
        n_sessions = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT MIN(bucket_start), MAX(bucket_start)
            FROM datamart.meter_runtime_session
            """
        )
        min_day, max_day = cur.fetchone()
        cur.execute("DELETE FROM datamart.meter_runtime_session")
        cur.execute(
            "DELETE FROM datamart.etl_state WHERE job_name = %s",
            (job_name,),
        )
    log.info(
        "runtime_reset",
        sessions_deleted=n_sessions,
        from_day=min_day.isoformat() if min_day else None,
        to_day=max_day.isoformat() if max_day else None,
        job_name=job_name,
    )
    return {"sessions_deleted": n_sessions}


def _fetch_runtime_rows(
    source: Database,
    day_start: datetime,
    day_end: datetime,
    pt_on: float,
    tz: str,
) -> list:
    with source.cursor() as cur:
        cur.execute(build_select_runtime(tz), (day_start, day_end, pt_on))
        return cur.fetchall()


def process_day(
    source: Database,
    datamart: Database,
    settings: Settings,
    day_start: datetime,
    dry_run: bool = False,
    pt_on_threshold: float | None = None,
) -> int:
    day_end = day_start + timedelta(days=1)
    threshold = settings.pt_on_threshold if pt_on_threshold is None else pt_on_threshold
    rows = _fetch_runtime_rows(
        source, day_start, day_end, threshold, settings.timezone
    )
    log.info(
        "runtime_day",
        day=day_start.isoformat(),
        sessions=len(rows),
        pt_on_threshold=threshold,
        dry_run=dry_run,
    )
    if dry_run:
        return len(rows)
    return _replace_day(datamart, day_start, rows)


def run_runtime_daily(
    settings: Settings,
    init_schema: bool = False,
    dry_run: bool = False,
    only_day: datetime | None = None,
    pt_on_threshold: float | None = None,
    reset: bool = False,
) -> dict:
    threshold = settings.pt_on_threshold if pt_on_threshold is None else pt_on_threshold
    source = Database(settings.source_db_url, "source")
    datamart = Database(settings.datamart_db_url, "datamart")
    stats = {"days_processed": 0, "sessions": 0}

    try:
        source.connect()
        datamart.connect()

        if init_schema:
            datamart.execute_sql_file(settings.schema_path)

        if reset:
            if dry_run:
                log.info("runtime_reset_skipped", reason="dry_run")
            else:
                reset_stats = reset_runtime(datamart, settings.runtime_job_name)
                datamart.commit()
                stats.update(reset_stats)

        if only_day is not None:
            n = process_day(
                source,
                datamart,
                settings,
                only_day,
                dry_run=dry_run,
                pt_on_threshold=threshold,
            )
            if not dry_run:
                datamart.commit()
            stats["days_processed"] = 1
            stats["sessions"] = n
            log.info("runtime_complete", **stats)
            return stats

        watermark = get_watermark(datamart, settings.runtime_job_name)
        if watermark is None:
            earliest = get_earliest_reading_time(source)
            if earliest is None:
                watermark = day_start_of(
                    default_watermark(settings.etl_backfill_days),
                    settings.timezone,
                )
            else:
                watermark = day_start_of(earliest, settings.timezone)
            log.info("runtime_watermark_init", watermark=watermark.isoformat())
        else:
            watermark = day_start_of(watermark, settings.timezone)

        upper_bound = get_upper_bound(source, settings.etl_lag_minutes)
        day_start = watermark

        while day_start + timedelta(days=1) <= upper_bound:
            n = process_day(
                source,
                datamart,
                settings,
                day_start,
                dry_run=dry_run,
                pt_on_threshold=threshold,
            )
            if not dry_run:
                update_watermark(
                    datamart,
                    settings.runtime_job_name,
                    day_start + timedelta(days=1),
                )
                datamart.commit()
            stats["days_processed"] += 1
            stats["sessions"] += n
            day_start = day_start + timedelta(days=1)

        if stats["days_processed"] == 0:
            log.info(
                "runtime_up_to_date",
                watermark=watermark.isoformat(),
                upper_bound=upper_bound.isoformat(),
            )
        else:
            log.info("runtime_complete", **stats)
        return stats
    except Exception:
        source.rollback()
        datamart.rollback()
        raise
    finally:
        source.close()
        datamart.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Job harian start/stop mesin (Pt) → datamart.meter_runtime_session"
    )
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help="Terapkan schema/datamart.sql ke DATAMART_DB_URL",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Hitung sesi tanpa menulis data mart",
    )
    parser.add_argument(
        "--day",
        metavar="YYYY-MM-DD",
        help="Proses 1 hari WIB saja (tidak menggeser watermark)",
    )
    parser.add_argument(
        "--pt-on-threshold",
        type=float,
        default=None,
        metavar="WATT",
        help=(
            "Ambang PT dianggap nyala. Nilai di bawah atau sama dengan ini "
            "dianggap noise/mati (default: PT_ON_THRESHOLD dari .env, sekarang 5)"
        ),
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help=(
            "Hapus semua baris datamart.meter_runtime_session dan watermark "
            "job runtime, lalu hitung ulang dari awal dengan threshold saat ini"
        ),
    )
    args = parser.parse_args()

    settings = get_settings()
    _configure_logging(settings.log_level)
    only_day = parse_day(args.day, settings.timezone) if args.day else None
    run_runtime_daily(
        settings,
        init_schema=args.init_schema,
        dry_run=args.dry_run,
        only_day=only_day,
        pt_on_threshold=args.pt_on_threshold,
        reset=args.reset,
    )


if __name__ == "__main__":
    main()
