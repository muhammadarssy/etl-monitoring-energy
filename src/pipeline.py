"""Orkestrasi ETL: extract → transform → load (raw tidak dihapus)."""
from __future__ import annotations

import structlog

from src.config import Settings
from src.db import Database
from src.extract import (
    count_readings_in_window,
    default_watermark,
    get_earliest_reading_time,
    get_upper_bound,
    get_watermark,
)
from src.load import (
    aggregate_window_cross_db,
    aggregate_window_same_db,
    update_watermark,
)
from src.transform import next_window_end

log = structlog.get_logger(__name__)


def run_etl(settings: Settings, init_schema: bool = False, dry_run: bool = False) -> dict:
    source = Database(settings.source_db_url, "source")
    datamart = Database(settings.datamart_db_url, "datamart")
    same_db = settings.source_db_url == settings.datamart_db_url

    stats = {
        "windows_processed": 0,
        "rows_5min": 0,
        "rows_1h": 0,
        "rows_daily": 0,
        "rows_counter_daily": 0,
        "rows_monthly": 0,
    }

    try:
        source.connect()
        datamart.connect()

        if init_schema:
            datamart.execute_sql_file(settings.schema_path)

        watermark = get_watermark(datamart, settings.etl_job_name)
        if watermark is None:
            watermark = get_earliest_reading_time(source)
            if watermark is None:
                watermark = default_watermark(settings.etl_backfill_days)
                log.info(
                    "etl_no_source_data_using_backfill",
                    watermark=watermark.isoformat(),
                )

        upper_bound = get_upper_bound(source, settings.etl_lag_minutes)
        if watermark >= upper_bound:
            log.info(
                "etl_up_to_date",
                watermark=watermark.isoformat(),
                upper_bound=upper_bound.isoformat(),
            )
            return stats

        window_start = watermark

        while window_start < upper_bound:
            window_end = next_window_end(
                window_start, settings.etl_chunk_hours, upper_bound
            )
            reading_count = count_readings_in_window(source, window_start, window_end)

            log.info(
                "etl_window_start",
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
                readings=reading_count,
                dry_run=dry_run,
            )

            if reading_count == 0:
                if not dry_run:
                    update_watermark(datamart, settings.etl_job_name, window_end)
                    datamart.commit()
                window_start = window_end
                continue

            if dry_run:
                window_start = window_end
                stats["windows_processed"] += 1
                continue

            if same_db:
                counts = aggregate_window_same_db(
                    source.conn, settings, window_start, window_end
                )
                source.commit()
            else:
                counts = aggregate_window_cross_db(
                    source, datamart, settings, window_start, window_end
                )
                datamart.commit()

            update_watermark(datamart, settings.etl_job_name, window_end)
            datamart.commit()

            stats["windows_processed"] += 1
            stats["rows_5min"] += counts.get("5min", 0)
            stats["rows_1h"] += counts.get("1h", 0)
            stats["rows_daily"] += counts.get("daily", 0)
            stats["rows_counter_daily"] += counts.get("counter_daily", 0)
            stats["rows_monthly"] += counts.get("monthly", 0)

            log.info("etl_window_done", **counts)
            window_start = window_end

        log.info("etl_complete", **stats)
        return stats

    except Exception:
        source.rollback()
        datamart.rollback()
        raise
    finally:
        source.close()
        datamart.close()
