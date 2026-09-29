"""SQL that moves staged flight events into the star schema.

Kept free of Airflow imports so it can be unit-tested against a plain Postgres.
"""

# Must be the FIRST statement of the transaction. With a repeatable-read snapshot every statement below
# sees the same staging rows, so the final DELETE removes exactly the rows that were loaded. Under the default
# read-committed level, rows Spark commits between the upserts and the DELETE would be deleted unloaded.
SNAPSHOT_ISOLATION = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;"

# Spark appends a staging row on every poll, so one load usually sees SEVERAL rows per flight / airline / airport.
# Every INSERT ... ON CONFLICT DO UPDATE below therefore collapses its source to the newest row per key first
# (DISTINCT ON ... ORDER BY ingest_time DESC); otherwise Postgres raises
# "ON CONFLICT DO UPDATE command cannot affect row a second time" and the DAG fails on every run.
WAREHOUSE_STMTS = [
    # 1) Airlines (keyed by IATA code)
    """
    INSERT INTO dim_airline (iata, icao, airline_name)
    SELECT DISTINCT ON (iata) iata, icao, airline_name
    FROM (
      SELECT NULLIF(TRIM(airline_iata), '') AS iata,
             NULLIF(TRIM(airline_icao), '') AS icao,
             NULLIF(TRIM(airline_name), '') AS airline_name,
             ingest_time
      FROM fact_flight_status_staging
    ) s
    WHERE iata IS NOT NULL
    ORDER BY iata, ingest_time DESC
    ON CONFLICT (iata) DO UPDATE
    SET airline_name = COALESCE(EXCLUDED.airline_name, dim_airline.airline_name),
        icao         = COALESCE(EXCLUDED.icao,         dim_airline.icao);
    """,

    # 2a) Airports that have an IATA code (departure and arrival sides)
    """
    INSERT INTO dim_airport (iata, icao, airport_name)
    SELECT DISTINCT ON (iata) iata, icao, airport_name
    FROM (
      SELECT NULLIF(TRIM(dep_airport_iata), '') AS iata, NULLIF(TRIM(dep_airport_icao), '') AS icao,
             NULLIF(TRIM(dep_airport), '') AS airport_name, ingest_time
      FROM fact_flight_status_staging
      UNION ALL
      SELECT NULLIF(TRIM(arr_airport_iata), ''), NULLIF(TRIM(arr_airport_icao), ''),
             NULLIF(TRIM(arr_airport), ''), ingest_time
      FROM fact_flight_status_staging
    ) a
    WHERE iata IS NOT NULL
    ORDER BY iata, ingest_time DESC
    ON CONFLICT (iata) DO UPDATE
    SET airport_name = COALESCE(EXCLUDED.airport_name, dim_airport.airport_name),
        icao         = COALESCE(EXCLUDED.icao,         dim_airport.icao);
    """,

    # 2b) Airports that only have an ICAO code
    """
    INSERT INTO dim_airport (iata, icao, airport_name)
    SELECT DISTINCT ON (icao) NULL::text, icao, airport_name
    FROM (
      SELECT NULLIF(TRIM(dep_airport_icao), '') AS icao, NULLIF(TRIM(dep_airport), '') AS airport_name, ingest_time
      FROM fact_flight_status_staging WHERE NULLIF(TRIM(dep_airport_iata), '') IS NULL
      UNION ALL
      SELECT NULLIF(TRIM(arr_airport_icao), ''), NULLIF(TRIM(arr_airport), ''), ingest_time
      FROM fact_flight_status_staging WHERE NULLIF(TRIM(arr_airport_iata), '') IS NULL
    ) a
    WHERE icao IS NOT NULL
    ORDER BY icao, ingest_time DESC
    ON CONFLICT (icao) DO UPDATE
    SET airport_name = COALESCE(EXCLUDED.airport_name, dim_airport.airport_name);
    """,

    # 3) Routes (an airport is matched by IATA, or by ICAO when the feed has no IATA code)
    """
    INSERT INTO dim_route (dep_airport_id, arr_airport_id)
    SELECT DISTINCT da.airport_id, aa.airport_id
    FROM fact_flight_status_staging s
    JOIN dim_airport da ON da.iata = NULLIF(TRIM(s.dep_airport_iata), '')
                        OR (NULLIF(TRIM(s.dep_airport_iata), '') IS NULL AND da.icao = NULLIF(TRIM(s.dep_airport_icao), ''))
    JOIN dim_airport aa ON aa.iata = NULLIF(TRIM(s.arr_airport_iata), '')
                        OR (NULLIF(TRIM(s.arr_airport_iata), '') IS NULL AND aa.icao = NULLIF(TRIM(s.arr_airport_icao), ''))
    ON CONFLICT (dep_airport_id, arr_airport_id) DO NOTHING;
    """,

    # 4) Facts: newest staged snapshot per flight (columns match postgres/00_warehouse.sql)
    """
    INSERT INTO fact_flight_status (
      flight_key, flight_date, status, ingest_time, airline_id, route_id,
      dep_scheduled, dep_estimated, dep_actual, dep_delay_min,
      arr_scheduled, arr_estimated, arr_actual, arr_delay_min
    )
    SELECT
      s.flight_key, s.flight_date, s.status, s.ingest_time, a.airline_id, r.route_id,
      s.dep_scheduled, s.dep_estimated, s.dep_actual, s.dep_delay_min,
      s.arr_scheduled, s.arr_estimated, s.arr_actual, s.arr_delay_min
    FROM (
      SELECT DISTINCT ON (flight_key) *
      FROM fact_flight_status_staging
      ORDER BY flight_key, ingest_time DESC
    ) s
    LEFT JOIN dim_airline a ON a.iata = NULLIF(TRIM(s.airline_iata), '')
    LEFT JOIN dim_airport da ON da.iata = NULLIF(TRIM(s.dep_airport_iata), '')
                             OR (NULLIF(TRIM(s.dep_airport_iata), '') IS NULL AND da.icao = NULLIF(TRIM(s.dep_airport_icao), ''))
    LEFT JOIN dim_airport aa ON aa.iata = NULLIF(TRIM(s.arr_airport_iata), '')
                             OR (NULLIF(TRIM(s.arr_airport_iata), '') IS NULL AND aa.icao = NULLIF(TRIM(s.arr_airport_icao), ''))
    LEFT JOIN dim_route r ON r.dep_airport_id = da.airport_id AND r.arr_airport_id = aa.airport_id
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

    # 5) Clear staging (the repeatable-read snapshot guarantees this deletes only rows that were loaded)
    "DELETE FROM fact_flight_status_staging;",
]


# Run in this order inside one transaction.
LOAD_STATEMENTS = [SNAPSHOT_ISOLATION, *WAREHOUSE_STMTS]
