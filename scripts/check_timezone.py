"""Cek konsistensi waktu: OS UTC, sesi DB UTC, jam dinding Asia/Jakarta (GMT+7)."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.config import get_settings
from src.db import Database

JAKARTA = ZoneInfo("Asia/Jakarta")
JAKARTA_OFFSET = timedelta(hours=7)


def _print(title: str, **fields: object) -> None:
    print(f"== {title} ==")
    for key, value in fields.items():
        print(f"  {key}: {value}")
    print()


def _check_source(dsn: str) -> None:
    db = Database(dsn, "source")
    db.connect()
    try:
        with db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    current_setting('TimeZone') AS session_tz,
                    NOW() AS now_timestamptz,
                    NOW() AT TIME ZONE 'UTC' AS now_utc_naive,
                    NOW() AT TIME ZONE 'Asia/Jakarta' AS now_jakarta_naive,
                    EXTRACT(TIMEZONE FROM NOW()) AS session_offset_seconds
                """
            )
            row = cur.fetchone()
        session_tz, now_ts, now_utc, now_jakarta, offset_sec = row
        offset_hours = float(offset_sec) / 3600.0
        _print(
            "PostgreSQL source (sesi koneksi ETL)",
            session_tz=session_tz,
            now_timestamptz=now_ts,
            now_utc=now_utc,
            now_jakarta=now_jakarta,
            session_offset_hours=offset_hours,
        )
        if session_tz.upper() != "UTC":
            raise SystemExit(
                f"GAGAL: session TimeZone={session_tz!r}, diharapkan UTC "
                "(supaya cluster GMT+7 tidak bocor ke Python)."
            )
        if abs(offset_hours) > 0.01:
            raise SystemExit(
                f"GAGAL: offset sesi {offset_hours} jam, diharapkan 0 (UTC)."
            )
        py_utc = datetime.now(timezone.utc).replace(microsecond=0)
        db_utc = now_ts.astimezone(timezone.utc).replace(microsecond=0)
        drift = abs((py_utc - db_utc).total_seconds())
        if drift > 120:
            raise SystemExit(
                f"GAGAL: jam Python UTC ({py_utc}) vs DB ({db_utc}) "
                f"selisih {int(drift)}s."
            )
        wall_offset = now_jakarta - now_utc
        if wall_offset != JAKARTA_OFFSET:
            raise SystemExit(
                f"GAGAL: selisih jam dinding Jakarta-UTC = {wall_offset}, "
                "diharapkan 7 jam."
            )
    finally:
        db.close()


def main() -> None:
    os_now = datetime.now().astimezone()
    utc_now = datetime.now(timezone.utc)
    jakarta_now = datetime.now(JAKARTA)
    _print(
        "OS / proses Python",
        datetime_now_aware=os_now.isoformat(),
        utc=utc_now.isoformat(),
        asia_jakarta=jakarta_now.isoformat(),
        utc_offset_hours=os_now.utcoffset().total_seconds() / 3600
        if os_now.utcoffset()
        else None,
    )
    if os_now.utcoffset() not in (timedelta(0), None):
        print(
            "PERINGATAN: OS offset bukan UTC. Container/server harus TZ=UTC. "
            "Logika bisnis tetap Asia/Jakarta via TIMEZONE di .env.\n"
        )
    settings = get_settings()
    _print(
        "Konfigurasi ETL",
        TIMEZONE=settings.timezone,
        WBP=f"{settings.wbp_start_hour}:00–{settings.wbp_end_hour}:00 {settings.timezone}",
    )
    if settings.timezone != "Asia/Jakarta":
        raise SystemExit(
            f"GAGAL: TIMEZONE={settings.timezone!r}, diharapkan Asia/Jakarta."
        )
    _check_source(settings.source_db_url)
    print("OK: UTC di server/sesi DB, GMT+7 hanya di jam bisnis (Asia/Jakarta).")


if __name__ == "__main__":
    main()
