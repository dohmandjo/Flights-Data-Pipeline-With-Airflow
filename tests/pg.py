"""Helpers for tests that need a real (disposable!) Postgres, given by TEST_PG_DSN."""
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

DSN = os.getenv("TEST_PG_DSN")
requires_pg = pytest.mark.skipif(not DSN, reason="TEST_PG_DSN not set (needs a disposable Postgres)")
SCHEMA_FILE = Path(__file__).resolve().parents[1] / "postgres" / "00_warehouse.sql"


def sqlalchemy_url() -> URL:
    """TEST_PG_DSN is libpq keyword syntax ("host=... dbname=... user=..."); convert it for SQLAlchemy."""
    from psycopg2.extensions import parse_dsn

    p = parse_dsn(DSN)
    query = {"host": p["host"]} if p.get("host", "").startswith("/") else {}
    return URL.create(
        "postgresql+psycopg2",
        username=p.get("user"),
        password=p.get("password"),
        host=None if query else p.get("host"),
        port=int(p["port"]) if p.get("port") else None,
        database=p.get("dbname"),
        query=query,
    )


def make_engine():
    return create_engine(sqlalchemy_url())


def apply_schema(engine) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql(SCHEMA_FILE.read_text())
