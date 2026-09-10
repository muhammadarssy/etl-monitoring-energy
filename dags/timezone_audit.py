"""Audit timezone: OS UTC vs sesi DB vs jam dinding WIB. Trigger manual / harian."""
from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

JAKARTA = "Asia/Jakarta"
ETL_DIR = "/opt/airflow/etl"

with DAG(
    dag_id="timezone_audit",
    description="Cek OS UTC, sesi PostgreSQL UTC, dan jam dinding Asia/Jakarta",
    start_date=pendulum.datetime(2026, 9, 1, tz=JAKARTA),
    schedule="5 0 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "energy",
        "retries": 0,
        "execution_timeout": pendulum.duration(minutes=5),
    },
    tags=["energy", "ops"],
) as dag:
    BashOperator(
        task_id="check_timezone",
        cwd=ETL_DIR,
        bash_command="python scripts/check_timezone.py",
        append_env=True,
        env={"TZ": "UTC"},
    )
