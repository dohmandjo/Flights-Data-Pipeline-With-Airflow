"""Every minute: append new fact rows to a Google Sheet (which Tableau Public reads)."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import gspread

from _defaults import DEFAULT_ARGS
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from sheets_export import INITIAL_STATE, export_new_rows, make_engine

STATE_VAR_KEY = "gsheet_export_watermark"   # JSON: {"ts": "...", "key": "..."}


def _load_state() -> dict:
    return Variable.get(STATE_VAR_KEY, default_var=INITIAL_STATE, deserialize_json=True)


def _save_state(state: dict) -> None:
    Variable.set(STATE_VAR_KEY, state, serialize_json=True)


def _open_worksheet():
    sheet_id = os.getenv("GSHEET_ID")
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    title = os.getenv("GWORKSHEET_TITLE", "Fact_Status")
    if not sheet_id:
        raise RuntimeError("GSHEET_ID is not set.")
    if not creds or not os.path.exists(creds):
        raise FileNotFoundError("GOOGLE_APPLICATION_CREDENTIALS is not set or the key file is missing.")
    spreadsheet = gspread.service_account(filename=creds).open_by_key(sheet_id)
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=1000, cols=26)


def push_new_rows() -> int:
    worksheet = _open_worksheet()
    with make_engine().connect() as conn:
        exported = export_new_rows(conn, worksheet, _load_state, _save_state)
    print(f"Exported {exported} rows")
    return exported


with DAG(
    dag_id="gsheet_incremental_sync",
    description="Append new fact rows to Google Sheets every minute (keyset watermark, at-least-once).",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["flights", "sheets"],
) as dag:
    PythonOperator(task_id="push_new_rows", python_callable=push_new_rows)
