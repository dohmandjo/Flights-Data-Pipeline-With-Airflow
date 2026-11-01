"""Tests for the Google Sheets export logic (no Airflow needed)."""
import re

import pandas as pd
import pytest
from sqlalchemy import text

from pg import DSN, apply_schema, make_engine, requires_pg
from sheets_export import EPOCH, INITIAL_STATE, SQL_NEW_ROWS, export_new_rows, to_sheet_values

# ---------------------------------------------------------------- pure unit tests


def test_sql_uses_cast_not_double_colon_after_bind_params():
    """SQLAlchemy 1.4 does not treat ':name::type' as a bind parameter, so Postgres would receive ':name' verbatim."""
    assert not re.search(r":\w+::", SQL_NEW_ROWS)
    assert set(text(SQL_NEW_ROWS).compile().params) == {"last_ts", "last_key", "batch_size"}


def test_to_sheet_values_is_json_safe():
    df = pd.DataFrame(
        {
            "flight_key": ["A1"],
            "dep_delay_min": [float("nan")],
            "arr_delay_min": [12.0],
            "ingest_time": [pd.Timestamp("2025-08-21T10:00:00+00:00")],
            "airline_name": [None],
        }
    )
    [row] = to_sheet_values(df)
    assert row[0] == "A1"
    assert row[1] == ""
    assert row[2] == 12.0
    assert row[3].startswith("2025-08-21T10:00:00")
    assert row[4] == ""


def test_initial_state_starts_at_epoch():
    assert INITIAL_STATE == {"ts": EPOCH, "key": ""}


# ---------------------------------------------------------------- against a real Postgres


class FakeWorksheet:
    """Minimal stand-in for gspread.Worksheet."""

    def __init__(self, fail_on_append_call=None):
        self.header = []
        self.rows = []
        self.append_calls = 0
        self.fail_on_append_call = fail_on_append_call

    def row_values(self, n):
        return self.header

    def update(self, range_name, values):
        self.header = values[0]

    def append_rows(self, values, **kwargs):
        self.append_calls += 1
        if self.append_calls == self.fail_on_append_call:
            raise RuntimeError("Sheets API quota exceeded")
        self.rows.extend(values)


class StateBox:
    def __init__(self):
        self.state = dict(INITIAL_STATE)
        self.saves = 0

    def load(self):
        return dict(self.state)

    def save(self, state):
        self.state = dict(state)
        self.saves += 1


@pytest.fixture(scope="module")
def engine():
    if not DSN:
        pytest.skip("TEST_PG_DSN not set")
    eng = make_engine()
    apply_schema(eng)
    yield eng
    eng.dispose()


@pytest.fixture()
def conn(engine):
    with engine.begin() as c:
        c.exec_driver_sql("TRUNCATE fact_flight_status, dim_route, dim_airport, dim_airline RESTART IDENTITY CASCADE")
    with engine.connect() as c:
        yield c


def add_facts(engine, keys, ingest="2025-08-21 10:00:00+00"):
    with engine.begin() as c:
        for key in keys:
            c.execute(
                text("INSERT INTO fact_flight_status (flight_key, status, ingest_time) VALUES (:k, 'active', :t)"),
                {"k": key, "t": ingest},
            )


@requires_pg
def test_rows_sharing_one_timestamp_are_never_skipped_across_batches(engine, conn):
    """A whole load cycle shares one ingest_time; batches smaller than the cycle must still export every row."""
    keys = [f"F{i:03d}" for i in range(25)]
    add_facts(engine, keys)
    ws, box = FakeWorksheet(), StateBox()

    exported = export_new_rows(conn, ws, box.load, box.save, batch_size=10)

    assert exported == 25
    assert sorted(r[0] for r in ws.rows) == keys
    assert box.saves == 3


@requires_pg
def test_second_run_only_sends_new_rows(engine, conn):
    add_facts(engine, ["A", "B"])
    ws, box = FakeWorksheet(), StateBox()
    export_new_rows(conn, ws, box.load, box.save, batch_size=10)

    add_facts(engine, ["C"], ingest="2025-08-21 10:05:00+00")
    assert export_new_rows(conn, ws, box.load, box.save, batch_size=10) == 1
    assert export_new_rows(conn, ws, box.load, box.save, batch_size=10) == 0
    assert [r[0] for r in ws.rows] == ["A", "B", "C"]


@requires_pg
def test_failed_append_does_not_advance_the_watermark(engine, conn):
    """At-least-once: after an API failure the same rows are sent again on the retry."""
    add_facts(engine, ["A", "B", "C"])
    ws, box = FakeWorksheet(fail_on_append_call=1), StateBox()

    with pytest.raises(RuntimeError):
        export_new_rows(conn, ws, box.load, box.save, batch_size=10)
    assert box.state == INITIAL_STATE and box.saves == 0

    ws.fail_on_append_call = None
    assert export_new_rows(conn, ws, box.load, box.save, batch_size=10) == 3


@requires_pg
def test_header_is_written_once_and_matches_columns(engine, conn):
    add_facts(engine, ["A"])
    ws, box = FakeWorksheet(), StateBox()
    export_new_rows(conn, ws, box.load, box.save)
    assert ws.header[0] == "flight_key" and ws.header[-1] == "ingest_time"
    assert len(ws.rows[0]) == len(ws.header)
