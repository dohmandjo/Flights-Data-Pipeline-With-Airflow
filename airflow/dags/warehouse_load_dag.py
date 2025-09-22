# airflow/dags/warehouse_load_dag.py
from __future__ import annotations
from datetime import datetime, timezone
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

WAREHOUSE_STMTS = [
    # 1) Airlines
    """
    INSERT INTO dim_airline (iata, icao, airline_name)
    SELECT DISTINCT
      NULLIF(TRIM(s.airline_iata), '') AS iata,
      NULLIF(TRIM(s.airline_icao), '') AS icao,
      NULLIF(TRIM(s.airline_name), '') AS airline_name
    FROM fact_flight_status_staging s
    WHERE s.airline_iata IS NOT NULL OR s.airline_icao IS NOT NULL OR s.airline_name IS NOT NULL
    ON CONFLICT (iata) DO UPDATE
    SET airline_name = COALESCE(EXCLUDED.airline_name, dim_airline.airline_name),
        icao         = COALESCE(EXCLUDED.icao,         dim_airline.icao);
    """,

    # 2a) Airports by IATA
    """
    INSERT INTO dim_airport (iata, icao, airport_name)
    SELECT DISTINCT
      NULLIF(TRIM(a.iata), '') AS iata,
      NULLIF(TRIM(a.icao), '') AS icao,
      NULLIF(TRIM(a.airport_name), '') AS airport_name
    FROM (
      SELECT s.dep_airport_iata AS iata, s.dep_airport_icao AS icao, s.dep_airport AS airport_name
      FROM fact_flight_status_staging s WHERE s.dep_airport_iata IS NOT NULL
      UNION
      SELECT s.arr_airport_iata, s.arr_airport_icao, s.arr_airport
      FROM fact_flight_status_staging s WHERE s.arr_airport_iata IS NOT NULL
    ) a
    ON CONFLICT (iata) DO UPDATE
    SET airport_name = COALESCE(EXCLUDED.airport_name, dim_airport.airport_name),
        icao         = COALESCE(EXCLUDED.icao,         dim_airport.icao);
    """,

    # 2b) Airports by ICAO when no IATA match row exists
    """
    INSERT INTO dim_airport (iata, icao, airport_name)
    SELECT DISTINCT
      NULLIF(TRIM(a.iata), '') AS iata,
      NULLIF(TRIM(a.icao), '') AS icao,
      NULLIF(TRIM(a.airport_name), '') AS airport_name
    FROM (
      SELECT s.dep_airport_iata AS iata, s.dep_airport_icao AS icao, s.dep_airport AS airport_name
      FROM fact_flight_status_staging s WHERE s.dep_airport_icao IS NOT NULL
      UNION
      SELECT s.arr_airport_iata, s.arr_airport_icao, s.arr_airport
      FROM fact_flight_status_staging s WHERE s.arr_airport_icao IS NOT NULL
    ) a
    WHERE NOT EXISTS (SELECT 1 FROM dim_airport d WHERE d.iata = a.iata)
    ON CONFLICT (icao) DO UPDATE
    SET airport_name = COALESCE(EXCLUDED.airport_name, dim_airport.airport_name),
        iata         = COALESCE(EXCLUDED.iata,         dim_airport.iata);
    """,

    # 3) Routes
    """
    INSERT INTO dim_route (dep_airport_id, arr_airport_id)
    SELECT DISTINCT
      da.airport_id, aa.airport_id
    FROM fact_flight_status_staging s
    JOIN dim_airport da ON da.iata = s.dep_airport_iata
    JOIN dim_airport aa ON aa.iata = s.arr_airport_iata
    WHERE s.dep_airport_iata IS NOT NULL AND s.arr_airport_iata IS NOT NULL
    ON CONFLICT (dep_airport_id, arr_airport_id) DO NOTHING;
    """,

    # 4) Facts (names & types match your init.sql)
    """
    INSERT INTO fact_flight_status (
      flight_key,
      flight_date,
      status,
      ingest_time,
      airline_id,
      route_id,
      dep_scheduled, dep_estimated, dep_actual, dep_delay_min,
      arr_scheduled, arr_estimated, arr_actual, arr_delay_min
    )
    SELECT
      s.flight_key,
      s.flight_date,
      s.status,
      s.ingest_time,
      a.airline_id,
      r.route_id,
      s.dep_scheduled, s.dep_estimated, s.dep_actual, s.dep_delay_min,
      s.arr_scheduled, s.arr_estimated, s.arr_actual, s.arr_delay_min
    FROM fact_flight_status_staging s
    LEFT JOIN dim_airline a ON a.iata = s.airline_iata
    LEFT JOIN dim_route   r
      ON r.dep_airport_id = (SELECT airport_id FROM dim_airport WHERE iata = s.dep_airport_iata LIMIT 1)
     AND r.arr_airport_id = (SELECT airport_id FROM dim_airport WHERE iata = s.arr_airport_iata LIMIT 1)
    ON CONFLICT (flight_key) DO UPDATE SET
      flight_date   = EXCLUDED.flight_date,
      status        = EXCLUDED.status,
      ingest_time   = EXCLUDED.ingest_time,
      airline_id    = EXCLUDED.airline_id,
      route_id      = EXCLUDED.route_id,
      dep_scheduled = EXCLUDED.dep_scheduled,
      dep_estimated = EXCLUDED.dep_estimated,
      dep_actual    = EXCLUDED.dep_actual,
      dep_delay_min = EXCLUDED.dep_delay_min,
      arr_scheduled = EXCLUDED.arr_scheduled,
      arr_estimated = EXCLUDED.arr_estimated,
      arr_actual    = EXCLUDED.arr_actual,
      arr_delay_min = EXCLUDED.arr_delay_min;
    """,

    # 5) Clear staging
    "DELETE FROM fact_flight_status_staging;"
]

with DAG(
    dag_id="warehouse_load_every_2_min",
    description="Upsert from staging to dims+fact every 2 minutes",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
) as dag:
    load_warehouse = PostgresOperator(
        task_id="upsert_from_staging",
        postgres_conn_id="warehouse_db",
        sql=WAREHOUSE_STMTS,      # <— list of statements, not one big blob
        autocommit=False,         # default; Airflow wraps these in one txn
    )
