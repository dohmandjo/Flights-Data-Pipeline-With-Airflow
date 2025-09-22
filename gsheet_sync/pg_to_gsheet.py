import os
import pandas as pd
from sqlalchemy import create_engine
import gspread
from gspread_dataframe import set_with_dataframe

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "flight_pipeline")
PGUSER = os.getenv("PGUSER", "flight_user")
PGPASSWORD = os.getenv("PGPASSWORD", "P@ssword123")

GOOGLE_CREDS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GSHEET_ID = os.getenv("GSHEET_ID")
GWORKSHEET_TITLE = os.getenv("GWORKSHEET_TITLE", "Fact_Status")
MAX_ROWS = int(os.getenv("MAX_ROWS", "100000"))

SQL = """
SELECT
  f.flight_key,
  f.dep_scheduled,
  f.dep_actual,
  f.dep_delay_min,
  f.arr_scheduled,
  f.arr_estimated,
  f.arr_actual,
  f.arr_delay_min,
  f.flight_status,
  a.airline_name,
  da.airport_name AS dep_airport_name,
  aa.airport_name AS arr_airport_name,
  f.ingest_time
FROM fact_flight_status f
LEFT JOIN dim_airline a ON a.airline_id = f.airline_id
LEFT JOIN dim_route r ON r.route_id = f.route_id
LEFT JOIN dim_airport da ON da.airport_id = r.dep_airport_id
LEFT JOIN dim_airport aa ON aa.airport_id = r.arr_airport_id
ORDER BY f.dep_scheduled DESC
LIMIT %s;
"""

def fetch_df(limit):
    uri = f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
    engine = create_engine(uri)
    with engine.connect() as conn:
        df = pd.read_sql(SQL, conn, params=(limit,))
    print(f"Fetched rows: {len(df)}", flush=True)
    return df

def push_to_sheet(df):
    if not GOOGLE_CREDS or not os.path.exists(GOOGLE_CREDS):
        raise FileNotFoundError("GOOGLE_APPLICATION_CREDENTIALS not set or file not found")

    if not GSHEET_ID:
        raise RuntimeError("GSHEET_ID not set. Create a Sheet manually, share it with the service account, and set GSHEET_ID.")

    gc = gspread.service_account(filename=GOOGLE_CREDS)
    # Open the existing file by ID (this never creates a new Drive file)
    sh = gc.open_by_key(GSHEET_ID)
    print("Spreadsheet URL:", sh.url, flush=True)

    # get or create the WORKSHEET (tab) inside the spreadsheet
    try:
        ws = sh.worksheet(GWORKSHEET_TITLE)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=GWORKSHEET_TITLE, rows="1000", cols="26")

    rows, cols = (len(df) + 1, len(df.columns))
    ws.resize(rows=max(rows, 1), cols=max(cols, 1))
    set_with_dataframe(ws, df, include_index=False, include_column_header=True)

def main():
    df = fetch_df(MAX_ROWS)
    if df.empty:
        print("No data, skipping Sheets push.", flush=True)
        return
    push_to_sheet(df)
    print("Push complete.", flush=True)

if __name__ == "__main__":
    main()
