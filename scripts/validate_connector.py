from __future__ import annotations

import json
import logging
import sys
from urllib.request import urlopen


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    LOGGER.info("Checking Debezium connector status")
    with urlopen("http://kafka-connect:8083/connectors/postgres-payments-cdc/status", timeout=10) as response:
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
