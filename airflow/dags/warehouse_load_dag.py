"""Every 2 minutes: upsert staged flight events into the dimensions and fact table, then clear staging."""
from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from _defaults import DEFAULT_ARGS
from warehouse_sql import LOAD_STATEMENTS

with DAG(
    dag_id="warehouse_load_every_2_min",
    description="Upsert from staging to dims + fact every 2 minutes (one repeatable-read transaction).",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["flights", "warehouse"],
) as dag:
    SQLExecuteQueryOperator(
        task_id="upsert_from_staging",
        conn_id="warehouse_db",
        sql=LOAD_STATEMENTS,     # executed in order on one connection = one transaction
        autocommit=False,
    )
