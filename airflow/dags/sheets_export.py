"""Logic for the incremental Google Sheets export (no Airflow imports, so it is unit-testable).

Watermark = (ingest_time, flight_key) of the last exported row. Comparing on the pair (keyset pagination)
means rows that share an ingest_time can be split across batches without ever being skipped.
"""
from __future__ import annotations

import json
import os
from collections.abc import Callable

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

EPOCH = "1970-01-01T00:00:00+00:00"
INITIAL_STATE = {"ts": EPOCH, "key": ""}

# NOTE: the cast is written CAST(:x AS timestamptz), not :x::timestamptz. SQLAlchemy does not recognise a
# bind parameter that is immediately followed by '::', so the '::' form is sent to Postgres verbatim and fails.
SQL_NEW_ROWS = """
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
LEFT JOIN dim_airline a  ON a.airline_id  = f.airline_id
LEFT JOIN dim_route   r  ON r.route_id    = f.route_id
LEFT JOIN dim_airport da ON da.airport_id = r.dep_airport_id
LEFT JOIN dim_airport aa ON aa.airport_id = r.arr_airport_id
WHERE (f.ingest_time, f.flight_key) > (CAST(:last_ts AS timestamptz), :last_key)
ORDER BY f.ingest_time, f.flight_key
LIMIT :batch_size
"""


def make_engine():
    """Engine from PG* environment variables (URL.create escapes special characters in the password)."""
    password = os.getenv("PGPASSWORD")
    if not password:
        raise RuntimeError("PGPASSWORD is not set (see .env.example).")
    url = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("PGUSER", "flight_user"),
        password=password,
        host=os.getenv("PGHOST", "postgres"),
        port=int(os.getenv("PGPORT", "5432")),
        database=os.getenv("PGDATABASE", "flight_pipeline"),
    )
    return create_engine(url)


def fetch_new_rows(conn, state: dict, batch_size: int) -> pd.DataFrame:
    """Rows strictly after the watermark, oldest first."""
    params = {"last_ts": state["ts"], "last_key": state["key"], "batch_size": batch_size}
    return pd.read_sql(text(SQL_NEW_ROWS), conn, params=params)


def to_sheet_values(df: pd.DataFrame) -> list[list]:
    """JSON-safe cells: timestamps -> ISO-8601 strings, NaN/None -> "", numbers stay numbers."""
    rows = json.loads(df.to_json(orient="values", date_format="iso"))
    return [["" if cell is None else cell for cell in row] for row in rows]


def append_rows(worksheet, df: pd.DataFrame) -> None:
    """Append below the header (writing the header first if the sheet is empty)."""
    if not worksheet.row_values(1):
        worksheet.update(range_name="A1", values=[list(df.columns)])
    worksheet.append_rows(
        to_sheet_values(df), value_input_option="RAW", insert_data_option="INSERT_ROWS", table_range="A1"
    )


def export_new_rows(
    conn,
    worksheet,
    load_state: Callable[[], dict],
    save_state: Callable[[dict], None],
    batch_size: int = 2000,
    max_batches: int = 25,
) -> int:
    """Append everything after the watermark, batch by batch. Returns the number of rows exported.

    The watermark is saved only after a batch was appended, so a failure re-sends that batch on retry
    (at-least-once delivery) and never skips rows.
    """
    total = 0
    for _ in range(max_batches):
        state = load_state()
        df = fetch_new_rows(conn, state, batch_size)
        if df.empty:
            break
        append_rows(worksheet, df)
        last = df.iloc[-1]
        save_state({"ts": pd.Timestamp(last["ingest_time"]).isoformat(), "key": str(last["flight_key"])})
        total += len(df)
        if len(df) < batch_size:
            break
    return total
