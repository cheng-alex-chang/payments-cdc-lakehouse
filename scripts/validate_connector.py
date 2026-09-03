from __future__ import annotations

import json
import logging
import os
import sys
from urllib.request import urlopen


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

CONNECTOR = "postgres-payments-cdc"

# Kafka Connect sits at a different address depending on where this runs, and the two
# vantage points are not interchangeable:
#
#   in-network (Airflow container, Kubernetes pod)  ->  http://kafka-connect:8083
#   on the host (a CI runner driving Compose)       ->  http://localhost:8083
#
# The default is the in-network name because that is where the Airflow DAG's
# validate_connector task runs; changing it would break the pipeline. CI overrides it.
DEFAULT_CONNECT_URL = "http://kafka-connect:8083"


def connect_url() -> str:
    return os.environ.get("CONNECT_URL", DEFAULT_CONNECT_URL).rstrip("/")


def main() -> None:
    url = f"{connect_url()}/connectors/{CONNECTOR}/status"
    LOGGER.info("Checking Debezium connector status at %s", url)
    with urlopen(url, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))

    connector_state = payload["connector"]["state"]
    LOGGER.info("Connector state: %s", connector_state)

    tasks = payload.get("tasks", [])
    failed = [task for task in tasks if task.get("state") != "RUNNING"]
    if failed:
        raise SystemExit(f"Connector tasks unhealthy: {failed}")

    # UNASSIGNED is a transient rebalance state; the tasks being RUNNING means CDC is active
    if connector_state not in ("RUNNING", "UNASSIGNED"):
        raise SystemExit(f"Connector not healthy: {connector_state}")

    LOGGER.info("Connector tasks healthy: %s", len(tasks))
    print("Connector healthy")


if __name__ == "__main__":  # pragma: no cover
    try:
        main()
    except Exception as exc:
        LOGGER.exception("Connector validation failed")
        print(str(exc), file=sys.stderr)
        raise
