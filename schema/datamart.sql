-- Data mart untuk agregasi meter_readings (bukan salinan raw).
-- Jalankan ke database target (DATAMART_DB_URL).

CREATE SCHEMA IF NOT EXISTS datamart;

CREATE TABLE IF NOT EXISTS datamart.etl_state (
    job_name              TEXT PRIMARY KEY,
    last_processed_until  TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Grain 5 menit
CREATE TABLE IF NOT EXISTS datamart.meter_agg_5min (
    bucket_start              TIMESTAMPTZ NOT NULL,
    meter_id                  TEXT NOT NULL,
    device_type               TEXT NOT NULL DEFAULT 'energy',
    sample_count              INTEGER NOT NULL,
    tariff_band               TEXT NOT NULL,  -- WBP | LWBP
    uab_avg                   DOUBLE PRECISION,
    ubc_avg                   DOUBLE PRECISION,
    uca_avg                   DOUBLE PRECISION,
    ua_avg                    DOUBLE PRECISION,
    ub_avg                    DOUBLE PRECISION,
    uc_avg                    DOUBLE PRECISION,
    ia_avg                    DOUBLE PRECISION,
    ib_avg                    DOUBLE PRECISION,
    ic_avg                    DOUBLE PRECISION,
    pt_avg                    DOUBLE PRECISION,
    pa_avg                    DOUBLE PRECISION,
    pb_avg                    DOUBLE PRECISION,
    pc_avg                    DOUBLE PRECISION,
    pt_max                    DOUBLE PRECISION,
    active_power_demand_max   DOUBLE PRECISION,
    qt_avg                    DOUBLE PRECISION,
    qa_avg                    DOUBLE PRECISION,
    qb_avg                    DOUBLE PRECISION,
    qc_avg                    DOUBLE PRECISION,
    pft_avg                   DOUBLE PRECISION,
    pfa_avg                   DOUBLE PRECISION,
    pfb_avg                   DOUBLE PRECISION,
    pfc_avg                   DOUBLE PRECISION,
    frequency_avg             DOUBLE PRECISION,
    impep_delta               DOUBLE PRECISION,
    expep_delta               DOUBLE PRECISION,
    q1eq_delta                DOUBLE PRECISION,
    q2eq_delta                DOUBLE PRECISION,
    q3eq_delta                DOUBLE PRECISION,
    q4eq_delta                DOUBLE PRECISION,
    counter_reset             BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (bucket_start, meter_id)
);

-- Grain 1 jam (energi dari selisih snapshot batas jam)
CREATE TABLE IF NOT EXISTS datamart.meter_agg_1h (
    bucket_start              TIMESTAMPTZ NOT NULL,
    meter_id                  TEXT NOT NULL,
    device_type               TEXT NOT NULL DEFAULT 'energy',
    sample_count              INTEGER NOT NULL,
    tariff_band               TEXT NOT NULL,  -- WBP | LWBP
    uab_avg                   DOUBLE PRECISION,
    ubc_avg                   DOUBLE PRECISION,
    uca_avg                   DOUBLE PRECISION,
    ua_avg                    DOUBLE PRECISION,
    ub_avg                    DOUBLE PRECISION,
    uc_avg                    DOUBLE PRECISION,
    ia_avg                    DOUBLE PRECISION,
    ib_avg                    DOUBLE PRECISION,
    ic_avg                    DOUBLE PRECISION,
    pt_avg                    DOUBLE PRECISION,
    pa_avg                    DOUBLE PRECISION,
    pb_avg                    DOUBLE PRECISION,
    pc_avg                    DOUBLE PRECISION,
    pt_max                    DOUBLE PRECISION,
    active_power_demand_max   DOUBLE PRECISION,
    qt_avg                    DOUBLE PRECISION,
    qa_avg                    DOUBLE PRECISION,
    qb_avg                    DOUBLE PRECISION,
    qc_avg                    DOUBLE PRECISION,
    pft_avg                   DOUBLE PRECISION,
    pfa_avg                   DOUBLE PRECISION,
    pfb_avg                   DOUBLE PRECISION,
    pfc_avg                   DOUBLE PRECISION,
    frequency_avg             DOUBLE PRECISION,
    impep_delta               DOUBLE PRECISION,
    expep_delta               DOUBLE PRECISION,
    q1eq_delta                DOUBLE PRECISION,
    q2eq_delta                DOUBLE PRECISION,
    q3eq_delta                DOUBLE PRECISION,
    q4eq_delta                DOUBLE PRECISION,
    counter_reset             BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (bucket_start, meter_id)
);

-- Grain harian (WIB) — billing harian + dashboard
CREATE TABLE IF NOT EXISTS datamart.meter_agg_daily (
    bucket_start              TIMESTAMPTZ NOT NULL,
    meter_id                  TEXT NOT NULL,
    device_type               TEXT NOT NULL DEFAULT 'energy',
    sample_count              INTEGER NOT NULL,
    impep_wbp_kwh             DOUBLE PRECISION,
    impep_lwbp_kwh            DOUBLE PRECISION,
    impep_total_kwh           DOUBLE PRECISION,
    expep_wbp_kwh             DOUBLE PRECISION,
    expep_lwbp_kwh            DOUBLE PRECISION,
    expep_total_kwh           DOUBLE PRECISION,
    q1eq_total                DOUBLE PRECISION,
    q2eq_total                DOUBLE PRECISION,
    q3eq_total                DOUBLE PRECISION,
    q4eq_total                DOUBLE PRECISION,
    kvarh_total               DOUBLE PRECISION,
    pt_avg                    DOUBLE PRECISION,
    pt_max                    DOUBLE PRECISION,
    active_power_demand_max   DOUBLE PRECISION,
    pft_avg                   DOUBLE PRECISION,
    pft_min                   DOUBLE PRECISION,
    frequency_avg             DOUBLE PRECISION,
    uab_avg                   DOUBLE PRECISION,
    ia_avg                    DOUBLE PRECISION,
    ia_max                    DOUBLE PRECISION,
    hours_complete            INTEGER,
    counter_reset_count       INTEGER NOT NULL DEFAULT 0,
    pt_max_at                 TIMESTAMPTZ,
    pt_max_tariff_band        TEXT,
    PRIMARY KEY (bucket_start, meter_id)
);

-- Snapshot counter harian (audit: end - start ≈ total kWh)
CREATE TABLE IF NOT EXISTS datamart.meter_counter_daily (
    bucket_start              TIMESTAMPTZ NOT NULL,
    meter_id                  TEXT NOT NULL,
    impep_start               DOUBLE PRECISION,
    impep_end                 DOUBLE PRECISION,
    expep_start               DOUBLE PRECISION,
    expep_end                 DOUBLE PRECISION,
    PRIMARY KEY (bucket_start, meter_id)
);

-- Grain bulanan (WIB) — report tagihan
CREATE TABLE IF NOT EXISTS datamart.meter_agg_monthly (
    bucket_start              TIMESTAMPTZ NOT NULL,
    meter_id                  TEXT NOT NULL,
    device_type               TEXT NOT NULL DEFAULT 'energy',
    sample_count              INTEGER NOT NULL,
    impep_wbp_kwh             DOUBLE PRECISION,
    impep_lwbp_kwh            DOUBLE PRECISION,
    impep_total_kwh           DOUBLE PRECISION,
    expep_wbp_kwh             DOUBLE PRECISION,
    expep_lwbp_kwh            DOUBLE PRECISION,
    expep_total_kwh           DOUBLE PRECISION,
    q1eq_total                DOUBLE PRECISION,
    q2eq_total                DOUBLE PRECISION,
    q3eq_total                DOUBLE PRECISION,
    q4eq_total                DOUBLE PRECISION,
    kvarh_total               DOUBLE PRECISION,
    pt_avg                    DOUBLE PRECISION,
    pt_max                    DOUBLE PRECISION,
    active_power_demand_max   DOUBLE PRECISION,
    pft_avg                   DOUBLE PRECISION,
    pft_min                   DOUBLE PRECISION,
    frequency_avg             DOUBLE PRECISION,
    days_complete             INTEGER,
    counter_reset_count       INTEGER NOT NULL DEFAULT 0,
    pt_max_at                 TIMESTAMPTZ,
    pt_max_tariff_band        TEXT,
    PRIMARY KEY (bucket_start, meter_id)
);

CREATE INDEX IF NOT EXISTS idx_agg_5min_meter_time
    ON datamart.meter_agg_5min (meter_id, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_agg_5min_tariff
    ON datamart.meter_agg_5min (bucket_start, tariff_band);

CREATE INDEX IF NOT EXISTS idx_agg_1h_meter_time
    ON datamart.meter_agg_1h (meter_id, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_agg_1h_tariff
    ON datamart.meter_agg_1h (bucket_start, tariff_band);

CREATE INDEX IF NOT EXISTS idx_agg_daily_meter_time
    ON datamart.meter_agg_daily (meter_id, bucket_start DESC);

CREATE INDEX IF NOT EXISTS idx_counter_daily_meter_time
    ON datamart.meter_counter_daily (meter_id, bucket_start DESC);

CREATE INDEX IF NOT EXISTS idx_agg_monthly_meter_time
    ON datamart.meter_agg_monthly (meter_id, bucket_start DESC);

-- Migrasi untuk instalasi lama (aman dijalankan ulang)
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS q1eq_total DOUBLE PRECISION;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS q2eq_total DOUBLE PRECISION;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS q3eq_total DOUBLE PRECISION;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS q4eq_total DOUBLE PRECISION;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS kvarh_total DOUBLE PRECISION;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS pft_min DOUBLE PRECISION;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS uab_avg DOUBLE PRECISION;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS ia_avg DOUBLE PRECISION;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS ia_max DOUBLE PRECISION;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS hours_complete INTEGER;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS counter_reset_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS pt_max_at TIMESTAMPTZ;
ALTER TABLE datamart.meter_agg_daily ADD COLUMN IF NOT EXISTS pt_max_tariff_band TEXT;
