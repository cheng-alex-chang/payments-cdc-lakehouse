#!/usr/bin/env bash
set -euo pipefail

job_name="${2:-}"

case "$job_name" in
  bronze)
    echo "Would run Spark job: config/spark/jobs/bronze_from_kafka.py"
    ;;
  silver)
    echo "Would run Spark job: config/spark/jobs/silver_payments.py"
    ;;
  gold)
    echo "Would run Spark job: config/spark/jobs/gold_metrics.py"
    ;;
  *)
    echo "Unknown job: $job_name" >&2
    exit 1
    ;;
esac
