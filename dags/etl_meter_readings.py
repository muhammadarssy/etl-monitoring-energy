"""ETL meter_readings → data mart, setiap 15 menit (zona Asia/Jakarta).

Server/Airflow tetap UTC. Cron dibaca sebagai WIB karena start_date tz-aware.
Job sendiri memakai watermark + TIMEZONE=Asia/Jakarta, bukan logical_date UTC.
"""
from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

JAKARTA = "Asia/Jakarta"
ETL_DIR = "/opt/airflow/etl"

with DAG(
    dag_id="etl_meter_readings",
    description="Agregasi 5min / 1h / daily WBP-LWBP dari meter_readings",
    start_date=pendulum.datetime(2026, 9, 1, tz=JAKARTA),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "energy",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "execution_timeout": pendulum.duration(hours=2),
    },
    tags=["energy", "etl"],
    doc_md="""
    Jadwal 15 menit di **Asia/Jakarta**. OS server UTC tidak menggeser window ETL
    karena `main.py` memakai watermark TIMESTAMPTZ + `AT TIME ZONE 'Asia/Jakarta'`.
    """,
) as dag:
    BashOperator(
        task_id="run_etl",
        cwd=ETL_DIR,
        bash_command="python main.py",
        append_env=True,
        env={"TZ": "UTC"},
    )
