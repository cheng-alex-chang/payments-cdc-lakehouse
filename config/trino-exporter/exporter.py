from collections import Counter
import time

from prometheus_client import start_http_server, Gauge
import requests


HEADERS = {"X-Trino-User": "admin"}
TRINO_BASE_URL = "http://trino:8080"
SCRAPE_INTERVAL_SECONDS = 15
EXPORTER_PORT = 8000

running_queries  = Gauge("trino_running_queries",  "Queries in RUNNING state")
queued_queries   = Gauge("trino_queued_queries",   "Queries in QUEUED state")
blocked_queries  = Gauge("trino_blocked_queries",  "Queries in BLOCKED state")
finished_queries = Gauge("trino_finished_queries", "Queries in FINISHED state")
failed_queries   = Gauge("trino_failed_queries",   "Queries in FAILED state")
coordinator_up   = Gauge("trino_coordinator_up",   "1 if coordinator is active")


def collect() -> None:
    try:
        info = requests.get(f"{TRINO_BASE_URL}/v1/info", headers=HEADERS, timeout=5).json()
        coordinator_up.set(1 if info.get("state") == "ACTIVE" else 0)
    except Exception:
        coordinator_up.set(0)

    try:
        queries = requests.get(f"{TRINO_BASE_URL}/v1/query", headers=HEADERS, timeout=5).json()
        states = Counter(q["state"] for q in queries)
        running_queries.set(states.get("RUNNING", 0))
        queued_queries.set(states.get("QUEUED", 0))
        blocked_queries.set(states.get("BLOCKED", 0))
        finished_queries.set(states.get("FINISHED", 0))
        failed_queries.set(states.get("FAILED", 0))
    except Exception:
        pass


def main() -> None:
    start_http_server(EXPORTER_PORT)
    while True:
        collect()
        time.sleep(SCRAPE_INTERVAL_SECONDS)


if __name__ == "__main__":  # pragma: no cover
    main()
