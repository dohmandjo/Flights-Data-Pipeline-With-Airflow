from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator

import pandas as pd
from sqlalchemy import create_engine, text
import gspread
from gspread_dataframe import set_with_dataframe

# ---------- Config via env ----------
PGHOST = os.getenv("PGHOST", "flight_postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "flight_pipeline")  # fixed name
PGUSER = os.getenv("PGUSER", "flight_user")
PGPASSWORD = os.getenv("PGPASSWORD", "P@ssword123")

GSHEET_ID = os.getenv("GSHEET_ID")  # spreadsheet ID (not URL)
GWORKSHEET_TITLE = os.getenv("GWORKSHEET", "Fact_Status")
GOOGLE_CREDS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")  # path to service acct JSON inside container

MAX_ROWS = int(os.getenv("MAX_ROWS", "100000"))
STATE_VAR_KEY = "gsheet_last_pushed_ingest_time"

# Latest ingested timestamp in your fact table
SQL_MAX = "SELECT MAX(ingest_time) AS max_ingest FROM fact_flight_status;"

# Incremental pull since last pushed to Sheets
SQL_DATA = """
SELECT
  f.flight_key,
  f.dep_scheduled,
  f.dep_actual,
  f.dep_delay_min,
  f.arr_scheduled,
  f.arr_estimated,
  f.arr_actual,
  f.arr_delay_min,
  f.status,
  a.airline_name,
  da.airport_name AS dep_airport_name,
  aa.airport_name AS arr_airport_name,
  f.ingest_time
FROM fact_flight_status f
LEFT JOIN dim_airline a ON a.airline_id = f.airline_id
LEFT JOIN dim_route r    ON r.route_id = f.route_id
LEFT JOIN dim_airport da ON da.airport_id = r.dep_airport_id
LEFT JOIN dim_airport aa ON aa.airport_id = r.arr_airport_id
WHERE f.ingest_time > :last_ingest::timestamptz
ORDER BY f.dep_scheduled DESC
LIMIT :limit;
"""

def pg_engine():
    uri = f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
    return create_engine(uri)

def has_new_data(**context) -> bool:
    """
    Return True if MAX(ingest_time) in fact_flight_status is newer than the last pushed
    timestamp stored in an Airflow Variable. Also push the target timestamp to XCom.
    """
    last_pushed = Variable.get(STATE_VAR_KEY, default_var=None)
    with pg_engine().connect() as conn:
        max_ingest = conn.execute(text(SQL_MAX)).scalar()  # call scalar()

    print("Airflow state last_pushed:", last_pushed)
    print("DB max_ingest:", max_ingest)

    if max_ingest is None:
        return False

    # First run: no state yet -> push latest and proceed
    if last_pushed is None:
        context["ti"].xcom_push(key="target_ingest_ts", value=str(max_ingest))
        return True

    if str(max_ingest) > str(last_pushed):
        context["ti"].xcom_push(key="target_ingest_ts", value=str(max_ingest))
        return True

    return False

def push_to_sheet(**context):
    """
    Fetch new rows since last push and write to the worksheet.
    Update the Airflow Variable on success.
    """
    if not GSHEET_ID:
        raise RuntimeError("GSHEET_ID is not set in environment.")
    if not GOOGLE_CREDS or not os.path.exists(GOOGLE_CREDS):
        raise FileNotFoundError("GOOGLE_APPLICATION_CREDENTIALS not set or file not found")

    last_pushed = Variable.get(STATE_VAR_KEY, default_var="1970-01-01T00:00:00+00:00")

    with pg_engine().connect() as conn:
        df = pd.read_sql(text(SQL_DATA), conn, params={"last_ingest": last_pushed, "limit": MAX_ROWS})

    print(f"Fetched {len(df)} rows since {last_pushed}")

    if df.empty:
        print("No new rows; nothing to push.")
        return

    gc = gspread.service_account(filename=GOOGLE_CREDS)  # fixed method name
    sh = gc.open_by_key(GSHEET_ID)
    print("Spreadsheet URL:", sh.url)

    try:
        ws = sh.worksheet(GWORKSHEET_TITLE)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=GWORKSHEET_TITLE, rows="1000", cols="26")

    rows, cols = (len(df) + 1, len(df.columns))
    ws.resize(rows=max(rows, 1), cols=max(cols, 1))
    set_with_dataframe(ws, df, include_index=False, include_column_header=True)

    # Prefer the sensor’s XCom value, fallback to df max
    target_ts = context["ti"].xcom_pull(key="target_ingest_ts", task_ids="check_new_data")
    if not target_ts:
        target_ts = str(df["ingest_time"].max())

    Variable.set(STATE_VAR_KEY, target_ts)
    print("Updated Airflow Variable", STATE_VAR_KEY, "->", target_ts)

with DAG(
    dag_id="gsheet_incremental_sync",
    description="Push new fact rows to Google Sheets every minute",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=timedelta(minutes=1),  # every 1 minute
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
) as dag:

    check = ShortCircuitOperator(
        task_id="check_new_data",
        python_callable=has_new_data,  # fixed arg name
    )

    push = PythonOperator(
        task_id="push_to_sheet",
        python_callable=push_to_sheet,
    )

    check >> push
