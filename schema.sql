-- Schema PostgreSQL untuk Energy Meter Collection System.
-- Identifier kolom sengaja TIDAK di-quote → PostgreSQL menyimpannya lowercase,
-- konsisten dengan INSERT yang dibangun aplikasi (juga tanpa quote).

CREATE TABLE IF NOT EXISTS meter_readings (
    time                  TIMESTAMPTZ     NOT NULL,
    session_id            UUID,                 -- NULL untuk meter type=utils
    cycle_id              UUID,                 -- NULL untuk meter type=utils
    meter_id              TEXT            NOT NULL,
    device_type           TEXT            NOT NULL DEFAULT 'energy',  -- energy | utils
    -- Voltage L-L
    Uab                   DOUBLE PRECISION,
    Ubc                   DOUBLE PRECISION,
    Uca                   DOUBLE PRECISION,
    -- Voltage L-N
    Ua                    DOUBLE PRECISION,
    Ub                    DOUBLE PRECISION,
    Uc                    DOUBLE PRECISION,
    -- Current
    Ia                    DOUBLE PRECISION,
    Ib                    DOUBLE PRECISION,
    Ic                    DOUBLE PRECISION,
    -- Active Power
    Pt                    DOUBLE PRECISION,
    Pa                    DOUBLE PRECISION,
    Pb                    DOUBLE PRECISION,
    Pc                    DOUBLE PRECISION,
    -- Reactive Power
    Qt                    DOUBLE PRECISION,
    Qa                    DOUBLE PRECISION,
    Qb                    DOUBLE PRECISION,
    Qc                    DOUBLE PRECISION,
    -- Power Factor
    PFt                   DOUBLE PRECISION,
    PFa                   DOUBLE PRECISION,
    PFb                   DOUBLE PRECISION,
    PFc                   DOUBLE PRECISION,
    -- Frequency & Demand
    frequency             DOUBLE PRECISION,
    active_power_demand   DOUBLE PRECISION,
    -- Active Energy
    ImpEp                 DOUBLE PRECISION,
    ExpEp                 DOUBLE PRECISION,
    -- Reactive Energy
    Q1Eq                  DOUBLE PRECISION,
    Q2Eq                  DOUBLE PRECISION,
    Q3Eq                  DOUBLE PRECISION,
    Q4Eq                  DOUBLE PRECISION
);
