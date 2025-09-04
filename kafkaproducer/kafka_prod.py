import os, time, json, requests
from kafka import KafkaProducer
from datetime import datetime, timezone
from dateutil import parser
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

'''
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

BASE = "http://api.aviationstack.com/v1/flights"
API_KEY = os.environ.get("AVIATIONSTACK_KEY")
if not API_KEY:
    raise ValueError("Please set AVIATIONSTACK_KEY env var")

TOPIC = os.environ.get("TOPIC", "flights_live")
BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")  # <— default for Docker network

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    linger_ms=50,
    acks="all",
    retries=5,
    request_timeout_ms=20000,
)

def fetch_live(limit=50, offset=0):
    params = {"access_key": API_KEY, "flight_status": "active", "limit": limit, "offset": offset}
    r = requests.get(BASE, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def iso(ts):
    if not ts: return None
    return parser.isoparse(ts).astimezone().isoformat()


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
            "schedule": iso(dep.get("scheduled")),
            "estimated": iso(dep.get("estimated")),
            "actual": iso(dep.get("actual")),
            "gate": dep.get("gate"),
            "terminal": dep.get("terminal"),
            "delay_min": dep.get("delay"),
        },
        "arrival": {
            "airport": arr.get("airport"),
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

if __name__ == "__main__":
    ensure_topic(BOOTSTRAP, TOPIC, partitions=1, rf=1)
    offset = 0
    while True:
        try:
            payload = fetch_live(limit=50, offset=offset)
            for rec in payload.get("data", []):
                evt = normalize(rec)
                if evt and evt["flight_key"]:
                    producer.send(TOPIC, evt)
            time.sleep(15)
        except requests.HTTPError:
            time.sleep(30)
        except Exception:
            time.sleep(10)