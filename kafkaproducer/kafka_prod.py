import json
import os
import time
from datetime import datetime, timezone

import requests
from dateutil import parser
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

r'''
below is how the data looks like when pulled from the API
{
"flight_date":"2025-08-21",
"flight_status":"active",
"departure":{
    "airport":"Haneda Airport",
    "timezone":"Asia\/Tokyo",
    "iata":"HND",
    "icao":"RJTT",
    "terminal":"2",
    "gate":"73",
    "delay":48,
    "scheduled":"2025-08-21T00:05:00+00:00",
    "estimated":"2025-08-21T00:05:00+00:00",
    "actual":"2025-08-21T00:53:00+00:00",
    "estimated_runway":"2025-08-21T00:53:00+00:00",
    "actual_runway":"2025-08-21T00:53:00+00:00"
    },
"arrival":{
    "airport":"Suvarnabhumi International",
    "timezone":"Asia\/Bangkok",
    "iata":"BKK",
    "icao":"VTBS",
    "terminal":"MAIN",
    "gate":null,
    "baggage":"20",
    "scheduled":"2025-08-21T04:35:00+00:00",
    "delay":null,
    "estimated":"2025-08-21T04:20:00+00:00",
    "actual":null,
    "estimated_runway":null,
    "actual_runway":null
    },
"airline":{
    "name":"ANA",
    "iata":"NH",
    "icao":"ANA"
    },
"flight":{
    "number":"849",
    "iata":"NH849",
    "icao":"ANA849",
    "codeshared":null
    },
"aircraft":null,
"live":null
}
'''

BASE = os.environ.get("API_URL", "http://api.aviationstack.com/v1/flights")   # free tier is HTTP-only
TOPIC = os.environ.get("TOPIC", "flights_live")
BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")  # default for the Docker network
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "15"))


def make_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        linger_ms=50,
        acks="all",
        retries=5,
        request_timeout_ms=20000,
    )


def fetch_live(limit=50, offset=0):
    """One page of active flights. Raises on HTTP errors AND on Aviationstack's HTTP-200 error bodies."""
    api_key = os.environ.get("AVIATIONSTACK_KEY")
    if not api_key:
        raise RuntimeError("AVIATIONSTACK_KEY is not set (see .env.example)")
    params = {"access_key": api_key, "flight_status": "active", "limit": limit, "offset": offset}
    r = requests.get(BASE, params=params, timeout=20)
    r.raise_for_status()
    payload = r.json()
    if "error" in payload:      # e.g. invalid key or monthly quota exhausted, returned with status 200
        raise RuntimeError(f"Aviationstack error: {payload['error']}")
    return payload


def iso(ts):
    if not ts:
        return None
    return parser.isoparse(ts).astimezone(timezone.utc).isoformat()   # always UTC, whatever the container's zone


def ensure_topic(bootstrap, topic, partitions=1, rf=1):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=15000)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=rf)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

def normalize(rec):
    dep = rec.get("departure", {}) or {}
    arr = rec.get("arrival", {}) or {}
    airline = rec.get("airline", {}) or {}
    flight = rec.get("flight", {}) or {}

    dep_sched = iso(dep.get("scheduled"))
    flight_num = flight.get("iata") or flight.get("icao") or flight.get("number") or "Unknown"
    flight_key = f"{flight_num}_{dep_sched}"

    return {
        "flight_key": flight_key,
        "flight_date": rec.get("flight_date"),
        "status": rec.get("flight_status"),
        "airline": {"iata": airline.get("iata"), "icao": airline.get("icao"), "name": airline.get("name")},
        "flight": {"number": flight.get("number"), "iata": flight.get("iata"), "icao": flight.get("icao")},
        "departure": {
            "airport": dep.get("airport"),
            "iata": dep.get("iata"),
            "icao": dep.get("icao"),
            "schedule": iso(dep.get("scheduled")),
            "estimated": iso(dep.get("estimated")),
            "actual": iso(dep.get("actual")),
            "gate": dep.get("gate"),
            "terminal": dep.get("terminal"),
            "delay_min": dep.get("delay"),
        },
        "arrival": {
            "airport": arr.get("airport"),
            "iata": arr.get("iata"),
            "icao": arr.get("icao"),
            "schedule": iso(arr.get("scheduled")),
            "estimated": iso(arr.get("estimated")),
            "actual": iso(arr.get("actual")),
            "gate": arr.get("gate"),
            "terminal": arr.get("terminal"),
            "delay_min": arr.get("delay"),
        },
        "ingest_time": datetime.now(timezone.utc).isoformat(),
        "source": "aviationstack",
    }

def main():
    ensure_topic(BOOTSTRAP, TOPIC, partitions=1, rf=1)
    producer = make_producer()
    failures = 0
    while True:
        try:
            payload = fetch_live(limit=50, offset=0)
            sent = 0
            for rec in payload.get("data", []):
                evt = normalize(rec)
                if evt and evt["flight_key"]:
                    producer.send(TOPIC, evt)
                    sent += 1
            producer.flush()
            print(f"sent {sent} events", flush=True)
            failures = 0
            time.sleep(POLL_SECONDS)
        except Exception as exc:    # keep the container alive, but never fail silently
            failures += 1
            wait = min(300, 10 * 2 ** min(failures, 5))
            print(f"poll failed ({exc!r}); retrying in {wait}s", flush=True)
            time.sleep(wait)


if __name__ == "__main__":
    main()
