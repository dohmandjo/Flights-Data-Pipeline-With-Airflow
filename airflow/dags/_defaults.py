"""Settings shared by every DAG."""
from datetime import timedelta

# Retry transient failures (Postgres restarting, Sheets API quota) with exponential backoff
# (30s, 60s, 120s ...) before the run is marked failed.
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}
