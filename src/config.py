"""Konfigurasi ETL dari environment / .env."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    source_db_url: str = Field(
        default="postgresql://user:pass@localhost:5432/energy_db",
        alias="SOURCE_DB_URL",
    )
    datamart_db_url: str = Field(
        default="postgresql://user:pass@localhost:5432/energy_db",
        alias="DATAMART_DB_URL",
    )

    etl_job_name: str = Field(default="meter_readings_wbp", alias="ETL_JOB_NAME")
    etl_lag_minutes: int = Field(default=15, alias="ETL_LAG_MINUTES")
    etl_chunk_hours: int = Field(default=1, alias="ETL_CHUNK_HOURS")
    etl_lookback_hours: int = Field(default=1, alias="ETL_LOOKBACK_HOURS")
    etl_boundary_tolerance_minutes: int = Field(
        default=5, alias="ETL_BOUNDARY_TOLERANCE_MINUTES"
    )
    etl_backfill_days: int = Field(default=30, alias="ETL_BACKFILL_DAYS")

    timezone: str = Field(default="Asia/Jakarta", alias="TIMEZONE")
    wbp_start_hour: int = Field(default=18, alias="WBP_START_HOUR")
    wbp_end_hour: int = Field(default=22, alias="WBP_END_HOUR")

    runtime_job_name: str = Field(
        default="meter_runtime_daily", alias="RUNTIME_JOB_NAME"
    )
    pt_on_threshold: float = Field(
        default=5.0,
        alias="PT_ON_THRESHOLD",
        description=(
            "Ambang PT (Watt). Nilai PT di bawah atau sama dengan ini "
            "dianggap mati/noise, bukan sesi nyala."
        ),
    )

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @property
    def schema_path(self) -> Path:
        return BASE_DIR / "schema" / "datamart.sql"


@lru_cache
def get_settings() -> Settings:
    return Settings()
