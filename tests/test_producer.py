"""Producer helpers (imports need only requests, python-dateutil and kafka-python)."""
import sys
from pathlib import Path

import pytest

pytest.importorskip("kafka")
pytest.importorskip("dateutil")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "kafkaproducer"))

import kafka_prod  # noqa: E402

RECORD = {
    "flight_date": "2025-08-21",
    "flight_status": "active",
    "airline": {"iata": "NH", "icao": "ANA", "name": "ANA"},
    "flight": {"number": "849", "iata": "NH849", "icao": "ANA849"},
    "departure": {"airport": "Haneda", "iata": "HND", "icao": "RJTT", "scheduled": "2025-08-21T00:05:00+00:00", "delay": 48},
    "arrival": {"airport": "Suvarnabhumi", "iata": "BKK", "icao": "VTBS", "scheduled": "2025-08-21T04:35:00+00:00"},
}


def test_normalize_keeps_iata_codes():
    """Regression: the original read dep.get('aita'), so every IATA code was NULL and no routes were built."""
    evt = kafka_prod.normalize(RECORD)
    assert evt["departure"]["iata"] == "HND"
    assert evt["arrival"]["iata"] == "BKK"


def test_normalize_builds_stable_utc_flight_key():
    assert kafka_prod.normalize(RECORD)["flight_key"] == "NH849_2025-08-21T00:05:00+00:00"


def test_normalize_survives_missing_sections():
    evt = kafka_prod.normalize({"flight_status": "active", "departure": None, "arrival": None})
    assert evt["departure"]["iata"] is None and evt["flight_key"].startswith("Unknown_")


def test_fetch_live_raises_on_error_body(monkeypatch):
    class Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"error": {"code": "usage_limit_reached"}}

    monkeypatch.setenv("AVIATIONSTACK_KEY", "k")
    monkeypatch.setattr(kafka_prod.requests, "get", lambda *a, **k: Resp())
    with pytest.raises(RuntimeError, match="usage_limit_reached"):
        kafka_prod.fetch_live()


def test_fetch_live_requires_key(monkeypatch):
    monkeypatch.delenv("AVIATIONSTACK_KEY", raising=False)
    with pytest.raises(RuntimeError, match="AVIATIONSTACK_KEY"):
        kafka_prod.fetch_live()
