"""Preview waktu raw vs bucket yang akan disimpan. Hanya SELECT — tidak menulis DB.

Jalankan di server (dari root proyek):

  python scripts/preview_time.py
  python scripts/preview_time.py --limit 20
  python scripts/preview_time.py --meter <meter_id>
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.config import get_settings
from src.db import Database
from src.transform import _bucket_5min, _day_start_expr, _tariff_case


def _fmt(ts) -> str:
    if ts is None:
        return "-"
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    return str(ts)


def _lock_read_only(db: Database) -> None:
    with db.cursor() as cur:
        cur.execute("SET default_transaction_read_only = on")
        cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")


def preview(limit: int, meter_id: str | None) -> None:
    settings = get_settings()
    tz = settings.timezone
    bucket_5 = _bucket_5min(tz)
    hour_b = (
        f"(date_trunc('hour', r.time AT TIME ZONE '{tz}') AT TIME ZONE '{tz}')"
    )
    day_b = _day_start_expr(tz, "r.time")
    tariff = _tariff_case(
        tz, settings.wbp_start_hour, settings.wbp_end_hour, bucket_5
    )

    where = ""
    params: list = [limit]
    if meter_id:
        where = "WHERE r.meter_id = %s"
        params = [meter_id, limit]

    sql = f"""
    SELECT
        r.meter_id,
        r.device_type,
        r.time AS raw_timestamptz,
        r.time AT TIME ZONE 'UTC' AS raw_wall_utc,
        r.time AT TIME ZONE '{tz}' AS raw_wall_wib,
        {bucket_5} AS would_save_5min,
        ({bucket_5}) AT TIME ZONE '{tz}' AS would_save_5min_wib,
        {hour_b} AS would_save_1h,
        ({hour_b}) AT TIME ZONE '{tz}' AS would_save_1h_wib,
        {day_b} AS would_save_daily,
        ({day_b}) AT TIME ZONE '{tz}' AS would_save_daily_wib,
        {tariff} AS tariff_band
    FROM public.meter_readings r
    {where}
    ORDER BY r.time DESC
    LIMIT %s
    """

    db = Database(settings.source_db_url, "source")
    db.connect()
    try:
        _lock_read_only(db)
        with db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    current_setting('TimeZone') AS session_tz,
                    (SELECT boot_val FROM pg_settings WHERE name = 'TimeZone')
                        AS cluster_boot_tz,
                    NOW() AS db_now,
                    NOW() AT TIME ZONE 'UTC' AS db_now_utc,
                    NOW() AT TIME ZONE %s AS db_now_wib
                """,
                (tz,),
            )
            meta = cur.fetchone()
            cur.execute(sql, params)
            rows = cur.fetchall()
        db.rollback()
    finally:
        db.close()

    print("Mode: READ ONLY (tidak ada INSERT/UPDATE/DELETE).")
    print(f"TIMEZONE bisnis : {tz}")
    print(f"Sesi koneksi    : {meta[0]}  (sengaja UTC, isolasi dari cluster)")
    print(f"Timezone cluster: {meta[1]}")
    print(f"NOW() DB        : {_fmt(meta[2])}")
    print(f"NOW() UTC wall  : {_fmt(meta[3])}")
    print(f"NOW() WIB wall  : {_fmt(meta[4])}")
    print()
    print(
        "raw = kolom meter_readings.time (TIMESTAMPTZ, instant absolut).\n"
        "would_save_* = bucket yang ETL akan tulis ke datamart (belum ditulis).\n"
        "wall WIB harus = jam yang Anda lihat di lapangan; offset UTC = WIB−7 jam.\n"
    )
    if not rows:
        print("Tidak ada baris di meter_readings untuk filter ini.")
        return

    for i, row in enumerate(rows, 1):
        (
            mid,
            dtype,
            raw,
            wall_utc,
            wall_wib,
            b5,
            b5_wib,
            b1h,
            b1h_wib,
            bday,
            bday_wib,
            band,
        ) = row
        print(f"--- sampel {i}  meter={mid}  type={dtype}  tarif={band} ---")
        print(f"  raw TIMESTAMPTZ     {_fmt(raw)}")
        print(f"  jam dinding UTC     {_fmt(wall_utc)}")
        print(f"  jam dinding WIB     {_fmt(wall_wib)}")
        print(f"  akan 5min (ts)      {_fmt(b5)}")
        print(f"  akan 5min (WIB)     {_fmt(b5_wib)}")
        print(f"  akan 1h   (WIB)     {_fmt(b1h_wib)}")
        print(f"  akan daily(WIB)     {_fmt(bday_wib)}")
        print()

    print("Cek cepat:")
    print("  - jam dinding WIB ≈ waktu meter di pabrik/kantor (bukan UTC).")
    print("  - bucket 5min WIB = raw WIB yang dipotong ke 00/05/10/…")
    print("  - WBP hanya jika jam WIB 18, 19, 20, 21.")
    print("OK: preview selesai, database tidak diubah.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview waktu raw vs bucket datamart (tanpa menulis DB)"
    )
    parser.add_argument("--limit", type=int, default=10, help="Jumlah sampel terbaru")
    parser.add_argument("--meter", default=None, help="Filter meter_id")
    args = parser.parse_args()
    preview(max(1, args.limit), args.meter)


if __name__ == "__main__":
    main()
