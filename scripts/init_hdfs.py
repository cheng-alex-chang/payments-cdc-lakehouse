from __future__ import annotations

import subprocess


def run_hdfs(command: str) -> None:
    subprocess.run(f"docker exec dp-namenode hdfs dfs {command}", shell=True, check=True)


def main() -> None:
    run_hdfs("-mkdir -p /data/bronze /data/silver /data/gold /warehouse /warehouse/analytics.db")


if __name__ == "__main__":  # pragma: no cover
    main()
