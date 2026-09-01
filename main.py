"""Entry point ETL meter_readings → data mart (WBP/LWBP)."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import structlog

# Pastikan root proyek ada di sys.path saat dijalankan sebagai script
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.config import get_settings
from src.pipeline import run_etl


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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETL meter_readings ke data mart (5min / 1h / daily WBP-LWBP)"
    )
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help="Terapkan schema/datamart.sql ke DATAMART_DB_URL",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulasi tanpa menulis data mart",
    )
    args = parser.parse_args()

    settings = get_settings()
    _configure_logging(settings.log_level)
    run_etl(settings, init_schema=args.init_schema, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
