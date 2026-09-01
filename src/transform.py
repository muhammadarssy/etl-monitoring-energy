"""Transform & agregasi SQL: 5 menit, 1 jam (delta counter), harian WBP/LWBP."""
from __future__ import annotations

from datetime import datetime, timedelta

AVG_COLUMNS: tuple[str, ...] = (
    "uab",
    "ubc",
    "uca",
    "ua",
    "ub",
    "uc",
    "ia",
    "ib",
    "ic",
    "pt",
    "pa",
    "pb",
    "pc",
    "qt",
    "qa",
    "qb",
    "qc",
    "pft",
    "pfa",
    "pfb",
    "pfc",
    "frequency",
)

MAX_COLUMNS: tuple[str, ...] = (
    "pt",
    "active_power_demand",
)

ENERGY_COLUMNS: tuple[str, ...] = (
    "impep",
    "expep",
    "q1eq",
    "q2eq",
    "q3eq",
    "q4eq",
)

AGG_VALUE_COLS_5MIN_1H: tuple[str, ...] = (
    "device_type",
    "sample_count",
    "tariff_band",
    *[f"{c}_avg" for c in AVG_COLUMNS],
    "pt_max",
    "active_power_demand_max",
    *[f"{c}_delta" for c in ENERGY_COLUMNS],
    "counter_reset",
)


def _avg_select() -> str:
    return ",\n        ".join(f"AVG(r.{c}) AS {c}_avg" for c in AVG_COLUMNS)


def _tariff_case(tz: str, wbp_start: int, wbp_end: int, bucket_expr: str) -> str:
    return f"""
        CASE
            WHEN EXTRACT(HOUR FROM ({bucket_expr}) AT TIME ZONE '{tz}') >= {wbp_start}
             AND EXTRACT(HOUR FROM ({bucket_expr}) AT TIME ZONE '{tz}') < {wbp_end}
            THEN 'WBP'
            ELSE 'LWBP'
        END
    """.strip()


def _bucket_5min(tz: str) -> str:
    # Align ke 5 menit dalam timezone lokal, simpan sebagai timestamptz
    return f"""
        (
            date_trunc('hour', r.time AT TIME ZONE '{tz}')
            + floor(EXTRACT(MINUTE FROM r.time AT TIME ZONE '{tz}') / 5)
              * interval '5 minutes'
        ) AT TIME ZONE '{tz}'
    """.strip()


def _safe_delta(end_expr: str, start_expr: str) -> str:
    return (
        f"CASE WHEN {end_expr} IS NULL OR {start_expr} IS NULL THEN NULL "
        f"WHEN {end_expr} < {start_expr} THEN 0 "
        f"ELSE {end_expr} - {start_expr} END"
    )


def _reset_flag(end_expr: str, start_expr: str) -> str:
    return (
        f"CASE WHEN {end_expr} IS NOT NULL AND {start_expr} IS NOT NULL "
        f"AND {end_expr} < {start_expr} THEN TRUE ELSE FALSE END"
    )


def build_upsert_5min(tz: str, wbp_start: int, wbp_end: int) -> str:
    bucket = _bucket_5min(tz)
    tariff = _tariff_case(tz, wbp_start, wbp_end, bucket)
    avg_sel = _avg_select()

    energy_raw = ",\n        ".join(
        f"(array_agg(r.{c} ORDER BY r.time DESC))[1] "
        f"- (array_agg(r.{c} ORDER BY r.time ASC))[1] AS {c}_raw"
        for c in ENERGY_COLUMNS
    )
    energy_sel = ",\n        ".join(
        f"CASE WHEN {c}_raw < 0 THEN 0 ELSE {c}_raw END AS {c}_delta"
        for c in ENERGY_COLUMNS
    )
    reset_sel = " OR ".join(f"({c}_raw < 0)" for c in ("impep", "expep"))

    insert_cols = ", ".join(["bucket_start", "meter_id", *AGG_VALUE_COLS_5MIN_1H])
    update_sets = ",\n        ".join(
        f"{c} = EXCLUDED.{c}" for c in AGG_VALUE_COLS_5MIN_1H
    )

    return f"""
    INSERT INTO datamart.meter_agg_5min ({insert_cols})
    SELECT
        bucket_start,
        meter_id,
        device_type,
        sample_count,
        tariff_band,
        uab_avg, ubc_avg, uca_avg,
        ua_avg, ub_avg, uc_avg,
        ia_avg, ib_avg, ic_avg,
        pt_avg, pa_avg, pb_avg, pc_avg,
        qt_avg, qa_avg, qb_avg, qc_avg,
        pft_avg, pfa_avg, pfb_avg, pfc_avg,
        frequency_avg,
        pt_max,
        active_power_demand_max,
        {energy_sel},
        ({reset_sel}) AS counter_reset
    FROM (
        SELECT
            {bucket} AS bucket_start,
            r.meter_id,
            MAX(r.device_type) AS device_type,
            COUNT(*)::integer AS sample_count,
            {tariff} AS tariff_band,
            {avg_sel},
            MAX(r.pt) AS pt_max,
            MAX(r.active_power_demand) AS active_power_demand_max,
            {energy_raw}
        FROM public.meter_readings r
        WHERE r.time >= %s
          AND r.time < %s
          AND ({bucket}) + interval '5 minutes' <= %s
        GROUP BY 1, 2, 5
    ) s
    ON CONFLICT (bucket_start, meter_id) DO UPDATE SET
        {update_sets}
    """


def build_select_5min(tz: str, wbp_start: int, wbp_end: int) -> str:
    """SELECT agregasi 5 menit (untuk cross-DB)."""
    bucket = _bucket_5min(tz)
    tariff = _tariff_case(tz, wbp_start, wbp_end, bucket)
    avg_sel = _avg_select()
    energy_raw = ",\n        ".join(
        f"(array_agg(r.{c} ORDER BY r.time DESC))[1] "
        f"- (array_agg(r.{c} ORDER BY r.time ASC))[1] AS {c}_raw"
        for c in ENERGY_COLUMNS
    )
    energy_sel = ",\n        ".join(
        f"CASE WHEN {c}_raw < 0 THEN 0 ELSE {c}_raw END AS {c}_delta"
        for c in ENERGY_COLUMNS
    )
    reset_sel = " OR ".join(f"({c}_raw < 0)" for c in ("impep", "expep"))
    return f"""
    SELECT
        bucket_start,
        meter_id,
        device_type,
        sample_count,
        tariff_band,
        uab_avg, ubc_avg, uca_avg,
        ua_avg, ub_avg, uc_avg,
        ia_avg, ib_avg, ic_avg,
        pt_avg, pa_avg, pb_avg, pc_avg,
        qt_avg, qa_avg, qb_avg, qc_avg,
        pft_avg, pfa_avg, pfb_avg, pfc_avg,
        frequency_avg,
        pt_max,
        active_power_demand_max,
        {energy_sel},
        ({reset_sel}) AS counter_reset
    FROM (
        SELECT
            {bucket} AS bucket_start,
            r.meter_id,
            MAX(r.device_type) AS device_type,
            COUNT(*)::integer AS sample_count,
            {tariff} AS tariff_band,
            {avg_sel},
            MAX(r.pt) AS pt_max,
            MAX(r.active_power_demand) AS active_power_demand_max,
            {energy_raw}
        FROM public.meter_readings r
        WHERE r.time >= %s
          AND r.time < %s
          AND ({bucket}) + interval '5 minutes' <= %s
        GROUP BY 1, 2, 5
    ) s
    """

def build_upsert_1h(
    tz: str,
    wbp_start: int,
    wbp_end: int,
    tolerance_minutes: int,
) -> str:
    """
    Agregasi 1 jam:
    - metrik instantaneous dari AVG/MAX dalam jam
    - energi dari selisih snapshot di batas jam (±tolerance)
    """
    hour_bucket = f"(date_trunc('hour', r.time AT TIME ZONE '{tz}') AT TIME ZONE '{tz}')"
    tariff = _tariff_case(tz, wbp_start, wbp_end, "h.bucket_start")
    avg_sel = _avg_select()

    energy_deltas = ",\n        ".join(
        f"{_safe_delta(f's_end.{c}', f's_start.{c}')} AS {c}_delta"
        for c in ENERGY_COLUMNS
    )
    reset_flag = (
        f"({_reset_flag('s_end.impep', 's_start.impep')} OR "
        f"{_reset_flag('s_end.expep', 's_start.expep')})"
    )

    insert_cols = ", ".join(["bucket_start", "meter_id", *AGG_VALUE_COLS_5MIN_1H])
    update_sets = ",\n        ".join(
        f"{c} = EXCLUDED.{c}" for c in AGG_VALUE_COLS_5MIN_1H
    )

    return f"""
    WITH hours AS (
        SELECT DISTINCT
            meter_id,
            (date_trunc('hour', time AT TIME ZONE '{tz}') AT TIME ZONE '{tz}')
                AS bucket_start
        FROM public.meter_readings
        WHERE time >= %s
          AND time < %s
    ),
    complete_hours AS (
        SELECT meter_id, bucket_start
        FROM hours
        WHERE bucket_start + interval '1 hour' <= %s
    ),
    bounds AS (
        SELECT meter_id, bucket_start AS bound FROM complete_hours
        UNION
        SELECT meter_id, bucket_start + interval '1 hour' AS bound FROM complete_hours
    ),
    nearest AS (
        SELECT DISTINCT ON (b.meter_id, b.bound)
            b.meter_id,
            b.bound,
            r.impep,
            r.expep,
            r.q1eq,
            r.q2eq,
            r.q3eq,
            r.q4eq
        FROM bounds b
        INNER JOIN public.meter_readings r
            ON r.meter_id = b.meter_id
           AND r.time >= b.bound - (%s * interval '1 minute')
           AND r.time <= b.bound + (%s * interval '1 minute')
        ORDER BY
            b.meter_id,
            b.bound,
            ABS(EXTRACT(EPOCH FROM (r.time - b.bound)))
    ),
    power_agg AS (
        SELECT
            {hour_bucket} AS bucket_start,
            r.meter_id,
            MAX(r.device_type) AS device_type,
            COUNT(*)::integer AS sample_count,
            {avg_sel},
            MAX(r.pt) AS pt_max,
            MAX(r.active_power_demand) AS active_power_demand_max
        FROM public.meter_readings r
        INNER JOIN complete_hours h
            ON h.meter_id = r.meter_id
           AND {hour_bucket} = h.bucket_start
        WHERE r.time >= %s
          AND r.time < %s
        GROUP BY 1, 2
    )
    INSERT INTO datamart.meter_agg_1h ({insert_cols})
    SELECT
        h.bucket_start,
        h.meter_id,
        COALESCE(p.device_type, 'energy') AS device_type,
        COALESCE(p.sample_count, 0) AS sample_count,
        {tariff} AS tariff_band,
        p.uab_avg, p.ubc_avg, p.uca_avg,
        p.ua_avg, p.ub_avg, p.uc_avg,
        p.ia_avg, p.ib_avg, p.ic_avg,
        p.pt_avg, p.pa_avg, p.pb_avg, p.pc_avg,
        p.qt_avg, p.qa_avg, p.qb_avg, p.qc_avg,
        p.pft_avg, p.pfa_avg, p.pfb_avg, p.pfc_avg,
        p.frequency_avg,
        p.pt_max,
        p.active_power_demand_max,
        {energy_deltas},
        {reset_flag} AS counter_reset
    FROM complete_hours h
    LEFT JOIN power_agg p
        ON p.meter_id = h.meter_id AND p.bucket_start = h.bucket_start
    LEFT JOIN nearest s_start
        ON s_start.meter_id = h.meter_id AND s_start.bound = h.bucket_start
    LEFT JOIN nearest s_end
        ON s_end.meter_id = h.meter_id
       AND s_end.bound = h.bucket_start + interval '1 hour'
    ON CONFLICT (bucket_start, meter_id) DO UPDATE SET
        {update_sets}
    """


def build_select_1h(
    tz: str,
    wbp_start: int,
    wbp_end: int,
    tolerance_minutes: int,
) -> str:
    """SELECT agregasi 1 jam (untuk cross-DB). Parameter sama dengan upsert."""
    hour_bucket = f"(date_trunc('hour', r.time AT TIME ZONE '{tz}') AT TIME ZONE '{tz}')"
    tariff = _tariff_case(tz, wbp_start, wbp_end, "h.bucket_start")
    avg_sel = _avg_select()
    energy_deltas = ",\n        ".join(
        f"{_safe_delta(f's_end.{c}', f's_start.{c}')} AS {c}_delta"
        for c in ENERGY_COLUMNS
    )
    reset_flag = (
        f"({_reset_flag('s_end.impep', 's_start.impep')} OR "
        f"{_reset_flag('s_end.expep', 's_start.expep')})"
    )
    return f"""
    WITH hours AS (
        SELECT DISTINCT
            meter_id,
            (date_trunc('hour', time AT TIME ZONE '{tz}') AT TIME ZONE '{tz}')
                AS bucket_start
        FROM public.meter_readings
        WHERE time >= %s
          AND time < %s
    ),
    complete_hours AS (
        SELECT meter_id, bucket_start
        FROM hours
        WHERE bucket_start + interval '1 hour' <= %s
    ),
    bounds AS (
        SELECT meter_id, bucket_start AS bound FROM complete_hours
        UNION
        SELECT meter_id, bucket_start + interval '1 hour' AS bound FROM complete_hours
    ),
    nearest AS (
        SELECT DISTINCT ON (b.meter_id, b.bound)
            b.meter_id,
            b.bound,
            r.impep,
            r.expep,
            r.q1eq,
            r.q2eq,
            r.q3eq,
            r.q4eq
        FROM bounds b
        INNER JOIN public.meter_readings r
            ON r.meter_id = b.meter_id
           AND r.time >= b.bound - (%s * interval '1 minute')
           AND r.time <= b.bound + (%s * interval '1 minute')
        ORDER BY
            b.meter_id,
            b.bound,
            ABS(EXTRACT(EPOCH FROM (r.time - b.bound)))
    ),
    power_agg AS (
        SELECT
            {hour_bucket} AS bucket_start,
            r.meter_id,
            MAX(r.device_type) AS device_type,
            COUNT(*)::integer AS sample_count,
            {avg_sel},
            MAX(r.pt) AS pt_max,
            MAX(r.active_power_demand) AS active_power_demand_max
        FROM public.meter_readings r
        INNER JOIN complete_hours h
            ON h.meter_id = r.meter_id
           AND {hour_bucket} = h.bucket_start
        WHERE r.time >= %s
          AND r.time < %s
        GROUP BY 1, 2
    )
    SELECT
        h.bucket_start,
        h.meter_id,
        COALESCE(p.device_type, 'energy') AS device_type,
        COALESCE(p.sample_count, 0) AS sample_count,
        {tariff} AS tariff_band,
        p.uab_avg, p.ubc_avg, p.uca_avg,
        p.ua_avg, p.ub_avg, p.uc_avg,
        p.ia_avg, p.ib_avg, p.ic_avg,
        p.pt_avg, p.pa_avg, p.pb_avg, p.pc_avg,
        p.qt_avg, p.qa_avg, p.qb_avg, p.qc_avg,
        p.pft_avg, p.pfa_avg, p.pfb_avg, p.pfc_avg,
        p.frequency_avg,
        p.pt_max,
        p.active_power_demand_max,
        {energy_deltas},
        {reset_flag} AS counter_reset
    FROM complete_hours h
    LEFT JOIN power_agg p
        ON p.meter_id = h.meter_id AND p.bucket_start = h.bucket_start
    LEFT JOIN nearest s_start
        ON s_start.meter_id = h.meter_id AND s_start.bound = h.bucket_start
    LEFT JOIN nearest s_end
        ON s_end.meter_id = h.meter_id
       AND s_end.bound = h.bucket_start + interval '1 hour'
    """


def _day_start_expr(tz: str, time_col: str = "bucket_start") -> str:
    return (
        f"(date_trunc('day', {time_col} AT TIME ZONE '{tz}') "
        f"AT TIME ZONE '{tz}')"
    )


def _month_start_expr(tz: str, time_col: str = "bucket_start") -> str:
    return (
        f"(date_trunc('month', {time_col} AT TIME ZONE '{tz}') "
        f"AT TIME ZONE '{tz}')"
    )


def _touched_days_cte(tz: str) -> str:
    day_expr = _day_start_expr(tz, "bucket_start")
    return f"""
    touched_days AS (
        SELECT DISTINCT {day_expr} AS day_start
        FROM datamart.meter_agg_1h
        WHERE bucket_start < %s
          AND bucket_start + interval '1 hour' > %s
    )
    """.strip()


def build_upsert_daily_from_1h(tz: str) -> str:
    """Rollup harian dari meter_agg_1h + peak dari 5min untuk ia_max."""
    day_expr = _day_start_expr(tz, "h.bucket_start")
    day_expr_h = _day_start_expr(tz, "bucket_start")
    touched = _touched_days_cte(tz)

    daily_update_cols = [
        "device_type",
        "sample_count",
        "impep_wbp_kwh",
        "impep_lwbp_kwh",
        "impep_total_kwh",
        "expep_wbp_kwh",
        "expep_lwbp_kwh",
        "expep_total_kwh",
        "q1eq_total",
        "q2eq_total",
        "q3eq_total",
        "q4eq_total",
        "kvarh_total",
        "pt_avg",
        "pt_max",
        "active_power_demand_max",
        "pft_avg",
        "pft_min",
        "frequency_avg",
        "uab_avg",
        "ia_avg",
        "ia_max",
        "hours_complete",
        "counter_reset_count",
        "pt_max_at",
        "pt_max_tariff_band",
    ]
    update_sets = ",\n        ".join(f"{c} = EXCLUDED.{c}" for c in daily_update_cols)

    return f"""
    WITH {touched},
    hourly AS (
        SELECT h.*
        FROM datamart.meter_agg_1h h
        INNER JOIN touched_days d ON {day_expr} = d.day_start
    ),
    five_min_peak AS (
        SELECT
            {day_expr_h} AS bucket_start,
            m5.meter_id,
            MAX(m5.ia_avg) AS ia_max
        FROM datamart.meter_agg_5min m5
        INNER JOIN touched_days d
            ON {day_expr_h} = d.day_start
        GROUP BY 1, 2
    ),
    peak_hour AS (
        SELECT DISTINCT ON ({day_expr_h}, meter_id)
            {day_expr_h} AS bucket_start,
            meter_id,
            bucket_start AS pt_max_at,
            tariff_band AS pt_max_tariff_band
        FROM hourly
        ORDER BY
            {day_expr_h},
            meter_id,
            pt_max DESC NULLS LAST,
            bucket_start
    ),
    daily_agg AS (
        SELECT
            {day_expr_h} AS bucket_start,
            h.meter_id,
            MAX(h.device_type) AS device_type,
            SUM(h.sample_count)::integer AS sample_count,
            SUM(CASE WHEN h.tariff_band = 'WBP' THEN COALESCE(h.impep_delta, 0) ELSE 0 END)
                AS impep_wbp_kwh,
            SUM(CASE WHEN h.tariff_band = 'LWBP' THEN COALESCE(h.impep_delta, 0) ELSE 0 END)
                AS impep_lwbp_kwh,
            SUM(COALESCE(h.impep_delta, 0)) AS impep_total_kwh,
            SUM(CASE WHEN h.tariff_band = 'WBP' THEN COALESCE(h.expep_delta, 0) ELSE 0 END)
                AS expep_wbp_kwh,
            SUM(CASE WHEN h.tariff_band = 'LWBP' THEN COALESCE(h.expep_delta, 0) ELSE 0 END)
                AS expep_lwbp_kwh,
            SUM(COALESCE(h.expep_delta, 0)) AS expep_total_kwh,
            SUM(COALESCE(h.q1eq_delta, 0)) AS q1eq_total,
            SUM(COALESCE(h.q2eq_delta, 0)) AS q2eq_total,
            SUM(COALESCE(h.q3eq_delta, 0)) AS q3eq_total,
            SUM(COALESCE(h.q4eq_delta, 0)) AS q4eq_total,
            SUM(
                COALESCE(h.q1eq_delta, 0) + COALESCE(h.q2eq_delta, 0)
                + COALESCE(h.q3eq_delta, 0) + COALESCE(h.q4eq_delta, 0)
            ) AS kvarh_total,
            CASE WHEN SUM(h.sample_count) = 0 THEN NULL
                 ELSE SUM(COALESCE(h.pt_avg, 0) * h.sample_count) / SUM(h.sample_count)
            END AS pt_avg,
            MAX(h.pt_max) AS pt_max,
            MAX(h.active_power_demand_max) AS active_power_demand_max,
            CASE WHEN SUM(h.sample_count) = 0 THEN NULL
                 ELSE SUM(COALESCE(h.pft_avg, 0) * h.sample_count) / SUM(h.sample_count)
            END AS pft_avg,
            MIN(h.pft_avg) AS pft_min,
            CASE WHEN SUM(h.sample_count) = 0 THEN NULL
                 ELSE SUM(COALESCE(h.frequency_avg, 0) * h.sample_count) / SUM(h.sample_count)
            END AS frequency_avg,
            CASE WHEN SUM(h.sample_count) = 0 THEN NULL
                 ELSE SUM(COALESCE(h.uab_avg, 0) * h.sample_count) / SUM(h.sample_count)
            END AS uab_avg,
            CASE WHEN SUM(h.sample_count) = 0 THEN NULL
                 ELSE SUM(COALESCE(h.ia_avg, 0) * h.sample_count) / SUM(h.sample_count)
            END AS ia_avg,
            COUNT(*) FILTER (
                WHERE h.impep_delta IS NOT NULL OR h.sample_count > 0
            )::integer AS hours_complete,
            SUM(CASE WHEN h.counter_reset THEN 1 ELSE 0 END)::integer AS counter_reset_count
        FROM hourly h
        GROUP BY 1, 2
    )
    INSERT INTO datamart.meter_agg_daily (
        bucket_start, meter_id, device_type, sample_count,
        impep_wbp_kwh, impep_lwbp_kwh, impep_total_kwh,
        expep_wbp_kwh, expep_lwbp_kwh, expep_total_kwh,
        q1eq_total, q2eq_total, q3eq_total, q4eq_total, kvarh_total,
        pt_avg, pt_max, active_power_demand_max,
        pft_avg, pft_min, frequency_avg,
        uab_avg, ia_avg, ia_max,
        hours_complete, counter_reset_count,
        pt_max_at, pt_max_tariff_band
    )
    SELECT
        d.bucket_start,
        d.meter_id,
        d.device_type,
        d.sample_count,
        d.impep_wbp_kwh,
        d.impep_lwbp_kwh,
        d.impep_total_kwh,
        d.expep_wbp_kwh,
        d.expep_lwbp_kwh,
        d.expep_total_kwh,
        d.q1eq_total,
        d.q2eq_total,
        d.q3eq_total,
        d.q4eq_total,
        d.kvarh_total,
        d.pt_avg,
        d.pt_max,
        d.active_power_demand_max,
        d.pft_avg,
        d.pft_min,
        d.frequency_avg,
        d.uab_avg,
        d.ia_avg,
        f.ia_max,
        d.hours_complete,
        d.counter_reset_count,
        p.pt_max_at,
        p.pt_max_tariff_band
    FROM daily_agg d
    LEFT JOIN five_min_peak f
        ON f.bucket_start = d.bucket_start AND f.meter_id = d.meter_id
    LEFT JOIN peak_hour p
        ON p.bucket_start = d.bucket_start AND p.meter_id = d.meter_id
    ON CONFLICT (bucket_start, meter_id) DO UPDATE SET
        {update_sets}
    """


def build_upsert_counter_daily(tz: str, tolerance_minutes: int) -> str:
    """Snapshot counter ImpEp/ExpEp di awal & akhir hari WIB."""
    day_start = (
        f"(date_trunc('day', r.time AT TIME ZONE '{tz}') AT TIME ZONE '{tz}')"
    )
    counter_update_cols = [
        "impep_start",
        "impep_end",
        "expep_start",
        "expep_end",
    ]
    update_sets = ",\n        ".join(f"{c} = EXCLUDED.{c}" for c in counter_update_cols)

    return f"""
    WITH day_meters AS (
        SELECT DISTINCT
            meter_id,
            {day_start} AS day_start,
            {day_start} + interval '1 day' AS day_end
        FROM public.meter_readings r
        WHERE r.time >= %s
          AND r.time < %s
    ),
    bounds AS (
        SELECT meter_id, day_start AS bucket_start, day_start AS bound
        FROM day_meters
        UNION ALL
        SELECT meter_id, day_start AS bucket_start, day_end AS bound
        FROM day_meters
    ),
    nearest AS (
        SELECT DISTINCT ON (b.meter_id, b.bucket_start, b.bound)
            b.meter_id,
            b.bucket_start,
            b.bound,
            r.impep,
            r.expep
        FROM bounds b
        INNER JOIN public.meter_readings r
            ON r.meter_id = b.meter_id
           AND r.time >= b.bound - (%s * interval '1 minute')
           AND r.time <= b.bound + (%s * interval '1 minute')
        ORDER BY
            b.meter_id,
            b.bucket_start,
            b.bound,
            ABS(EXTRACT(EPOCH FROM (r.time - b.bound)))
    )
    INSERT INTO datamart.meter_counter_daily (
        bucket_start, meter_id,
        impep_start, impep_end, expep_start, expep_end
    )
    SELECT
        dm.day_start AS bucket_start,
        dm.meter_id,
        s.impep AS impep_start,
        e.impep AS impep_end,
        s.expep AS expep_start,
        e.expep AS expep_end
    FROM day_meters dm
    LEFT JOIN nearest s
        ON s.meter_id = dm.meter_id
       AND s.bucket_start = dm.day_start
       AND s.bound = dm.day_start
    LEFT JOIN nearest e
        ON e.meter_id = dm.meter_id
       AND e.bucket_start = dm.day_start
       AND e.bound = dm.day_end
    ON CONFLICT (bucket_start, meter_id) DO UPDATE SET
        {update_sets}
    """


def build_select_counter_daily(tz: str, tolerance_minutes: int) -> str:
    """SELECT snapshot counter harian (untuk cross-DB)."""
    day_start = (
        f"(date_trunc('day', r.time AT TIME ZONE '{tz}') AT TIME ZONE '{tz}')"
    )
    return f"""
    WITH day_meters AS (
        SELECT DISTINCT
            meter_id,
            {day_start} AS day_start,
            {day_start} + interval '1 day' AS day_end
        FROM public.meter_readings r
        WHERE r.time >= %s
          AND r.time < %s
    ),
    bounds AS (
        SELECT meter_id, day_start AS bucket_start, day_start AS bound
        FROM day_meters
        UNION ALL
        SELECT meter_id, day_start AS bucket_start, day_end AS bound
        FROM day_meters
    ),
    nearest AS (
        SELECT DISTINCT ON (b.meter_id, b.bucket_start, b.bound)
            b.meter_id,
            b.bucket_start,
            b.bound,
            r.impep,
            r.expep
        FROM bounds b
        INNER JOIN public.meter_readings r
            ON r.meter_id = b.meter_id
           AND r.time >= b.bound - (%s * interval '1 minute')
           AND r.time <= b.bound + (%s * interval '1 minute')
        ORDER BY
            b.meter_id,
            b.bucket_start,
            b.bound,
            ABS(EXTRACT(EPOCH FROM (r.time - b.bound)))
    )
    SELECT
        dm.day_start AS bucket_start,
        dm.meter_id,
        s.impep AS impep_start,
        e.impep AS impep_end,
        s.expep AS expep_start,
        e.expep AS expep_end
    FROM day_meters dm
    LEFT JOIN nearest s
        ON s.meter_id = dm.meter_id
       AND s.bucket_start = dm.day_start
       AND s.bound = dm.day_start
    LEFT JOIN nearest e
        ON e.meter_id = dm.meter_id
       AND e.bucket_start = dm.day_start
       AND e.bound = dm.day_end
    """


def build_upsert_monthly_from_daily(tz: str) -> str:
    """Rollup bulanan dari meter_agg_daily untuk bulan yang disentuh window."""
    month_expr = _month_start_expr(tz, "d.bucket_start")
    month_expr_plain = _month_start_expr(tz, "bucket_start")

    monthly_update_cols = [
        "device_type",
        "sample_count",
        "impep_wbp_kwh",
        "impep_lwbp_kwh",
        "impep_total_kwh",
        "expep_wbp_kwh",
        "expep_lwbp_kwh",
        "expep_total_kwh",
        "q1eq_total",
        "q2eq_total",
        "q3eq_total",
        "q4eq_total",
        "kvarh_total",
        "pt_avg",
        "pt_max",
        "active_power_demand_max",
        "pft_avg",
        "pft_min",
        "frequency_avg",
        "days_complete",
        "counter_reset_count",
        "pt_max_at",
        "pt_max_tariff_band",
    ]
    update_sets = ",\n        ".join(f"{c} = EXCLUDED.{c}" for c in monthly_update_cols)

    return f"""
    WITH touched_months AS (
        SELECT DISTINCT {month_expr_plain} AS month_start
        FROM datamart.meter_agg_daily
        WHERE bucket_start < %s
          AND bucket_start + interval '1 day' > %s
    ),
    daily AS (
        SELECT d.*
        FROM datamart.meter_agg_daily d
        INNER JOIN touched_months m ON {month_expr} = m.month_start
    ),
    peak_day AS (
        SELECT DISTINCT ON ({month_expr_plain}, meter_id)
            {month_expr_plain} AS bucket_start,
            meter_id,
            pt_max_at,
            pt_max_tariff_band
        FROM daily
        ORDER BY
            {month_expr_plain},
            meter_id,
            pt_max DESC NULLS LAST,
            bucket_start
    ),
    monthly_agg AS (
        SELECT
            {month_expr} AS bucket_start,
            d.meter_id,
            MAX(d.device_type) AS device_type,
            SUM(d.sample_count)::integer AS sample_count,
            SUM(COALESCE(d.impep_wbp_kwh, 0)) AS impep_wbp_kwh,
            SUM(COALESCE(d.impep_lwbp_kwh, 0)) AS impep_lwbp_kwh,
            SUM(COALESCE(d.impep_total_kwh, 0)) AS impep_total_kwh,
            SUM(COALESCE(d.expep_wbp_kwh, 0)) AS expep_wbp_kwh,
            SUM(COALESCE(d.expep_lwbp_kwh, 0)) AS expep_lwbp_kwh,
            SUM(COALESCE(d.expep_total_kwh, 0)) AS expep_total_kwh,
            SUM(COALESCE(d.q1eq_total, 0)) AS q1eq_total,
            SUM(COALESCE(d.q2eq_total, 0)) AS q2eq_total,
            SUM(COALESCE(d.q3eq_total, 0)) AS q3eq_total,
            SUM(COALESCE(d.q4eq_total, 0)) AS q4eq_total,
            SUM(COALESCE(d.kvarh_total, 0)) AS kvarh_total,
            CASE WHEN SUM(d.sample_count) = 0 THEN NULL
                 ELSE SUM(COALESCE(d.pt_avg, 0) * d.sample_count) / SUM(d.sample_count)
            END AS pt_avg,
            MAX(d.pt_max) AS pt_max,
            MAX(d.active_power_demand_max) AS active_power_demand_max,
            CASE WHEN SUM(d.sample_count) = 0 THEN NULL
                 ELSE SUM(COALESCE(d.pft_avg, 0) * d.sample_count) / SUM(d.sample_count)
            END AS pft_avg,
            MIN(d.pft_min) AS pft_min,
            CASE WHEN SUM(d.sample_count) = 0 THEN NULL
                 ELSE SUM(COALESCE(d.frequency_avg, 0) * d.sample_count) / SUM(d.sample_count)
            END AS frequency_avg,
            COUNT(*) FILTER (WHERE COALESCE(d.hours_complete, 0) > 0)::integer AS days_complete,
            SUM(COALESCE(d.counter_reset_count, 0))::integer AS counter_reset_count
        FROM daily d
        GROUP BY 1, 2
    )
    INSERT INTO datamart.meter_agg_monthly (
        bucket_start, meter_id, device_type, sample_count,
        impep_wbp_kwh, impep_lwbp_kwh, impep_total_kwh,
        expep_wbp_kwh, expep_lwbp_kwh, expep_total_kwh,
        q1eq_total, q2eq_total, q3eq_total, q4eq_total, kvarh_total,
        pt_avg, pt_max, active_power_demand_max,
        pft_avg, pft_min, frequency_avg,
        days_complete, counter_reset_count,
        pt_max_at, pt_max_tariff_band
    )
    SELECT
        m.bucket_start,
        m.meter_id,
        m.device_type,
        m.sample_count,
        m.impep_wbp_kwh,
        m.impep_lwbp_kwh,
        m.impep_total_kwh,
        m.expep_wbp_kwh,
        m.expep_lwbp_kwh,
        m.expep_total_kwh,
        m.q1eq_total,
        m.q2eq_total,
        m.q3eq_total,
        m.q4eq_total,
        m.kvarh_total,
        m.pt_avg,
        m.pt_max,
        m.active_power_demand_max,
        m.pft_avg,
        m.pft_min,
        m.frequency_avg,
        m.days_complete,
        m.counter_reset_count,
        p.pt_max_at,
        p.pt_max_tariff_band
    FROM monthly_agg m
    LEFT JOIN peak_day p
        ON p.bucket_start = m.bucket_start AND p.meter_id = m.meter_id
    ON CONFLICT (bucket_start, meter_id) DO UPDATE SET
        {update_sets}
    """


def next_window_end(
    window_start: datetime,
    chunk_hours: int,
    upper_bound: datetime,
) -> datetime:
    candidate = window_start + timedelta(hours=chunk_hours)
    return min(candidate, upper_bound)
