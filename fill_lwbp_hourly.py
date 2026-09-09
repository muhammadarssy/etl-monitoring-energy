"""Isi jam LWBP yang bolong di meter_agg_1h dari habit hari acuan, lalu rollup daily/monthly.

Contoh:

  python fill_lwbp_hourly.py --meter meter_id4 --days 2026-07-31,2026-08-01 --total-kwh 82.5 --habit-day 2026-08-04
  python fill_lwbp_hourly.py --meter meter_id4 --days 2026-07-31,2026-08-01 --total-kwh 82.5 --habit-day 2026-08-04 --dry-run
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
from src.load import _upsert_rows
from src.transform import AGG_VALUE_COLS_5MIN_1H, build_upsert_daily_from_1h, build_upsert_monthly_from_daily

log = structlog.get_logger(__name__)

HOUR_COLS = ["bucket_start", "meter_id", *AGG_VALUE_COLS_5MIN_1H]


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
    if tz_name in ("Asia/Jakarta", "Asia/Bangkok", "Asia/Ho_Chi_Minh"):
        return timezone(timedelta(hours=7))
    if tz_name in ("UTC", "Etc/UTC"):
        return timezone.utc
    return timezone(timedelta(hours=7))


def parse_day(value: str, tz_name: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=_tzinfo(tz_name))


def parse_days(value: str, tz_name: str) -> list[datetime]:
    days = [parse_day(part.strip(), tz_name) for part in value.split(",") if part.strip()]
    if not days:
        raise ValueError("Daftar --days kosong")
    return sorted(days)


def _habit_rows(
    datamart: Database,
    meter_id: str,
    habit_day: datetime,
    tz_name: str,
    wbp_start: int,
    wbp_end: int,
    until_hour: int | None = None,
    from_hour: int | None = None,
) -> list[dict]:
    day_end = habit_day + timedelta(days=1)
    hour_cap = wbp_end if until_hour is None else min(until_hour, wbp_end)
    hour_from = 0 if from_hour is None else from_hour
    sql = """
        SELECT *
        FROM datamart.meter_agg_1h
        WHERE meter_id = %s
          AND bucket_start >= %s
          AND bucket_start < %s
          AND tariff_band = 'LWBP'
          AND COALESCE(impep_delta, 0) > 0
          AND EXTRACT(HOUR FROM bucket_start AT TIME ZONE %s) >= %s
          AND EXTRACT(HOUR FROM bucket_start AT TIME ZONE %s) < %s
          AND (
                EXTRACT(HOUR FROM bucket_start AT TIME ZONE %s) < %s
                OR EXTRACT(HOUR FROM bucket_start AT TIME ZONE %s) >= %s
              )
        ORDER BY bucket_start
    """
    with datamart.cursor() as cur:
        cur.execute(
            sql,
            (
                meter_id,
                habit_day,
                day_end,
                tz_name,
                hour_from,
                tz_name,
                hour_cap,
                tz_name,
                wbp_start,
                tz_name,
                wbp_end,
            ),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    if not rows:
        raise SystemExit(
            f"Tidak ada jam LWBP berenergi di hari acuan {habit_day.date()} untuk {meter_id}"
        )
    return rows


def _existing_hours(
    datamart: Database, meter_id: str, day_start: datetime
) -> set[datetime]:
    day_end = day_start + timedelta(days=1)
    with datamart.cursor() as cur:
        cur.execute(
            """
            SELECT bucket_start
            FROM datamart.meter_agg_1h
            WHERE meter_id = %s
              AND bucket_start >= %s
              AND bucket_start < %s
              AND impep_delta IS NOT NULL
            """,
            (meter_id, day_start, day_end),
        )
        return {r[0] for r in cur.fetchall()}


def _allocate(total_kwh: float, weights: list[float]) -> list[float]:
    weight_sum = sum(weights)
    if weight_sum <= 0:
        raise SystemExit("Bobot habit jam LWBP = 0")
    raw = [total_kwh * (w / weight_sum) for w in weights]
    rounded = [round(v, 4) for v in raw]
    drift = round(total_kwh - sum(rounded), 4)
    if rounded:
        rounded[-1] = round(rounded[-1] + drift, 4)
    return rounded


def _scale(value, ratio: float):
    if value is None:
        return None
    return value * ratio


def _safe_pf(value) -> float | None:
    if value is None:
        return None
    if 0 < value <= 1:
        return value
    return 0.95


def _safe_freq(value) -> float | None:
    if value is None:
        return None
    if 49 <= value <= 51:
        return value
    return 50.0


def build_hour_row(
    bucket_start: datetime,
    meter_id: str,
    habit: dict,
    kwh: float,
) -> tuple:
    habit_kwh = float(habit["impep_delta"])
    ratio = kwh / habit_kwh if habit_kwh else 1.0
    pft = _safe_pf(habit.get("pft_avg"))
    freq = _safe_freq(habit.get("frequency_avg"))
    return (
        bucket_start,
        meter_id,
        habit.get("device_type") or "energy",
        0,
        "LWBP",
        habit.get("uab_avg"),
        habit.get("ubc_avg"),
        habit.get("uca_avg"),
        habit.get("ua_avg"),
        habit.get("ub_avg"),
        habit.get("uc_avg"),
        _scale(habit.get("ia_avg"), ratio),
        _scale(habit.get("ib_avg"), ratio),
        _scale(habit.get("ic_avg"), ratio),
        _scale(habit.get("pt_avg"), ratio),
        _scale(habit.get("pa_avg"), ratio),
        _scale(habit.get("pb_avg"), ratio),
        _scale(habit.get("pc_avg"), ratio),
        _scale(habit.get("qt_avg"), ratio),
        _scale(habit.get("qa_avg"), ratio),
        _scale(habit.get("qb_avg"), ratio),
        _scale(habit.get("qc_avg"), ratio),
        pft,
        _safe_pf(habit.get("pfa_avg")),
        _safe_pf(habit.get("pfb_avg")),
        _safe_pf(habit.get("pfc_avg")),
        freq,
        _scale(habit.get("pt_max"), ratio),
        _scale(habit.get("active_power_demand_max"), ratio),
        kwh,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        False,
    )


def _rollup_window(
    datamart: Database,
    settings: Settings,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    with datamart.cursor() as cur:
        cur.execute(
            build_upsert_daily_from_1h(settings.timezone, settings.etl_lookback_hours),
            (window_end, window_start),
        )
        counts["daily"] = cur.rowcount
    with datamart.cursor() as cur:
        cur.execute(
            build_upsert_monthly_from_daily(
                settings.timezone, settings.etl_lookback_hours
            ),
            (window_end, window_start),
        )
        counts["monthly"] = cur.rowcount
    return counts


def fill_lwbp(
    settings: Settings,
    meter_id: str,
    days: list[datetime],
    total_kwh: float,
    habit_day: datetime,
    dry_run: bool = False,
    until_hour: int | None = None,
    from_hour: int | None = None,
) -> dict:
    datamart = Database(settings.datamart_db_url, "datamart")
    datamart.connect()
    stats = {"hours": 0, "skipped": 0, "kwh": 0.0, "daily": 0, "monthly": 0}
    try:
        habit = _habit_rows(
            datamart,
            meter_id,
            habit_day,
            settings.timezone,
            settings.wbp_start_hour,
            settings.wbp_end_hour,
            until_hour=until_hour,
            from_hour=from_hour,
        )
        habit_total = sum(float(r["impep_delta"]) for r in habit)
        kwh_per_day = total_kwh / len(days)
        log.info(
            "lwbp_habit",
            meter_id=meter_id,
            habit_day=str(habit_day.date()),
            habit_hours=len(habit),
            habit_kwh=round(habit_total, 4),
            fill_days=[str(d.date()) for d in days],
            kwh_per_day=round(kwh_per_day, 4),
        )

        rows: list[tuple] = []
        for day in days:
            existing = _existing_hours(datamart, meter_id, day)
            usable = []
            for h in habit:
                hour = h["bucket_start"].astimezone(_tzinfo(settings.timezone)).hour
                bucket = day.replace(hour=hour, minute=0, second=0, microsecond=0)
                if bucket in existing:
                    stats["skipped"] += 1
                    log.info("lwbp_skip_existing", meter_id=meter_id, bucket=bucket.isoformat())
                    continue
                usable.append((bucket, h))
            if not usable:
                log.info("lwbp_day_all_existing", day=str(day.date()))
                continue
            kwhs = _allocate(
                kwh_per_day, [float(h["impep_delta"]) for _, h in usable]
            )
            for (bucket, h), kwh in zip(usable, kwhs):
                rows.append(build_hour_row(bucket, meter_id, h, kwh))
                stats["kwh"] += kwh
                log.info(
                    "lwbp_hour",
                    bucket=bucket.isoformat(),
                    kwh=kwh,
                )

        stats["hours"] = len(rows)
        if dry_run:
            log.info("lwbp_dry_run", **{k: stats[k] for k in ("hours", "skipped", "kwh")})
            return stats

        inserted = _upsert_rows(datamart, "datamart.meter_agg_1h", HOUR_COLS, rows)
        window_start = days[0]
        window_end = days[-1] + timedelta(days=1)
        rollup = _rollup_window(datamart, settings, window_start, window_end)
        datamart.commit()
        stats["daily"] = rollup["daily"]
        stats["monthly"] = rollup["monthly"]
        log.info("lwbp_done", inserted=inserted, **stats)
        return stats
    except Exception:
        datamart.rollback()
        raise
    finally:
        datamart.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Isi meter_agg_1h LWBP dari habit hari acuan, lalu rollup daily/monthly"
    )
    parser.add_argument("--meter", required=True, help="meter_id, contoh meter_id4")
    parser.add_argument(
        "--days",
        required=True,
        help="Hari mesin nyala yang bolong, koma-pisah YYYY-MM-DD",
    )
    parser.add_argument(
        "--total-kwh",
        required=True,
        type=float,
        help="Total kWh LWBP yang dibagi rata ke --days",
    )
    parser.add_argument(
        "--habit-day",
        required=True,
        help="Hari acuan yang datanya lengkap (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--from-hour",
        type=int,
        help="Isi hanya jam LWBP mulai jam ini (contoh 15 = sore 15-...)",
    )
    parser.add_argument(
        "--until-hour",
        type=int,
        help="Isi hanya jam LWBP sebelum jam ini (contoh 13 = pagi 07-12)",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    settings = get_settings()
    _configure_logging(settings.log_level)
    days = parse_days(args.days, settings.timezone)
    habit_day = parse_day(args.habit_day, settings.timezone)
    fill_lwbp(
        settings,
        meter_id=args.meter,
        days=days,
        total_kwh=args.total_kwh,
        habit_day=habit_day,
        dry_run=args.dry_run,
        until_hour=args.until_hour,
        from_hour=args.from_hour,
    )


if __name__ == "__main__":
    main()
