"""The staging -> star-schema SQL, run against a real Postgres (TEST_PG_DSN, disposable database!)."""
import threading

import pytest
from sqlalchemy import text

from pg import DSN, apply_schema, make_engine, requires_pg
from warehouse_sql import LOAD_STATEMENTS

pytestmark = requires_pg


@pytest.fixture(scope="module")
def engine():
    if not DSN:
        pytest.skip("TEST_PG_DSN not set")
    eng = make_engine()
    apply_schema(eng)
    yield eng
    eng.dispose()


@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as c:
        c.exec_driver_sql(
            "TRUNCATE fact_flight_status_staging, fact_flight_status, dim_route, dim_airport, dim_airline "
            "RESTART IDENTITY CASCADE"
        )


def stage(engine, flight_key="DL1_a", ingest="2025-08-21 10:00:00+00", **cols):
    row = {
        "flight_key": flight_key, "status": "active", "flight_date": "2025-08-21", "ingest_time": ingest,
        "airline_iata": "DL", "airline_icao": "DAL", "airline_name": "Delta",
        "dep_airport": "John F Kennedy", "dep_airport_iata": "JFK", "dep_airport_icao": "KJFK",
        "arr_airport": "Los Angeles Intl", "arr_airport_iata": "LAX", "arr_airport_icao": "KLAX",
        "dep_delay_min": 5, "arr_delay_min": 0,
    }
    row.update(cols)
    names = ", ".join(row)
    binds = ", ".join(f":{k}" for k in row)
    with engine.begin() as c:
        c.execute(text(f"INSERT INTO fact_flight_status_staging ({names}) VALUES ({binds})"), row)


def load(engine):
    """Run the statements exactly as the DAG's PostgresOperator does: in order, one transaction."""
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        for stmt in LOAD_STATEMENTS:
            cur.execute(stmt)
        raw.commit()
    finally:
        raw.close()


def scalar(engine, sql, **params):
    with engine.connect() as c:
        return c.execute(text(sql), params).scalar()


def test_builds_dimensions_routes_and_fact_then_clears_staging(engine):
    stage(engine, "DL1_a")
    stage(engine, "AA2_b", airline_iata="AA", airline_icao="AAL", airline_name="American")
    load(engine)

    assert scalar(engine, "SELECT count(*) FROM dim_airline") == 2
    assert scalar(engine, "SELECT count(*) FROM dim_airport") == 2
    assert scalar(engine, "SELECT count(*) FROM dim_route") == 1
    assert scalar(engine, "SELECT count(*) FROM fact_flight_status") == 2
    assert scalar(engine, "SELECT count(*) FROM fact_flight_status WHERE airline_id IS NULL OR route_id IS NULL") == 0
    assert scalar(engine, "SELECT count(*) FROM fact_flight_status_staging") == 0


def test_same_flight_staged_many_times_keeps_the_latest_snapshot(engine):
    """Spark appends a new staging row on every poll, so one load sees several rows per flight_key."""
    stage(engine, "DL1_a", status="active", ingest="2025-08-21 10:00:00+00")
    stage(engine, "DL1_a", status="landed", ingest="2025-08-21 10:30:00+00", arr_delay_min=12)
    stage(engine, "DL1_a", status="active", ingest="2025-08-21 10:10:00+00")
    load(engine)

    assert scalar(engine, "SELECT count(*) FROM fact_flight_status") == 1
    assert scalar(engine, "SELECT status FROM fact_flight_status WHERE flight_key = 'DL1_a'") == "landed"
    assert scalar(engine, "SELECT arr_delay_min FROM fact_flight_status WHERE flight_key = 'DL1_a'") == 12


def test_conflicting_airline_names_in_one_batch_do_not_break_the_load(engine):
    stage(engine, "DL1_a", airline_name="Delta", ingest="2025-08-21 10:00:00+00")
    stage(engine, "DL2_b", airline_name="Delta Air Lines", ingest="2025-08-21 10:05:00+00")
    load(engine)

    assert scalar(engine, "SELECT count(*) FROM dim_airline") == 1
    assert scalar(engine, "SELECT airline_name FROM dim_airline WHERE iata = 'DL'") == "Delta Air Lines"


def test_is_idempotent(engine):
    stage(engine, "DL1_a")
    load(engine)
    load(engine)  # nothing staged: no-op
    assert scalar(engine, "SELECT count(*) FROM fact_flight_status") == 1


def test_row_committed_during_the_load_is_not_deleted_unloaded(engine):
    """Read committed would DELETE a row Spark inserts after the upserts ran; repeatable read must not."""
    stage(engine, "DL1_a")
    raw = engine.raw_connection()
    cur = raw.cursor()
    for stmt in LOAD_STATEMENTS[:-1]:          # everything except the final DELETE
        cur.execute(stmt)

    late = threading.Thread(target=stage, args=(engine, "LATE_x"))
    late.start()
    late.join()                                # committed in another session, mid-load

    cur.execute(LOAD_STATEMENTS[-1])           # the DELETE
    raw.commit()
    raw.close()

    assert scalar(engine, "SELECT count(*) FROM fact_flight_status WHERE flight_key = 'DL1_a'") == 1
    assert scalar(engine, "SELECT count(*) FROM fact_flight_status_staging WHERE flight_key = 'LATE_x'") == 1
    load(engine)
    assert scalar(engine, "SELECT count(*) FROM fact_flight_status WHERE flight_key = 'LATE_x'") == 1


def test_airports_with_only_an_icao_code_still_get_a_route(engine):
    stage(engine, "DL1_a", dep_airport_iata=None, arr_airport_iata=None)
    load(engine)
    assert scalar(engine, "SELECT count(*) FROM dim_airport WHERE iata IS NULL AND icao IN ('KJFK', 'KLAX')") == 2
    assert scalar(engine, "SELECT route_id IS NOT NULL FROM fact_flight_status WHERE flight_key = 'DL1_a'") is True
