"""Job harian sesi nyala/mati mesin, setelah hari WIB selesai.

00:30 Asia/Jakarta = 17:30 UTC. Jangan jadwalkan 00:30 UTC (itu 07:30 WIB).
"""
from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

JAKARTA = "Asia/Jakarta"
ETL_DIR = "/opt/airflow/etl"

with DAG(
    dag_id="meter_runtime_daily",
    description="Sesi runtime mesin (Pt) per hari kalender WIB",
    start_date=pendulum.datetime(2026, 9, 1, tz=JAKARTA),
    schedule="30 0 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "energy",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=10),
        "execution_timeout": pendulum.duration(hours=4),
    },
    tags=["energy", "runtime"],
    doc_md="""
    Jalan **00:30 WIB** (hari sebelumnya sudah tutup + lag ETL 15 menit).
    `runtime_daily.py` memotong hari dengan midnight Asia/Jakarta, bukan UTC.
    """,
) as dag:
    BashOperator(
        task_id="run_runtime_daily",
        cwd=ETL_DIR,
        bash_command="python runtime_daily.py",
        append_env=True,
        env={"TZ": "UTC"},
    )
