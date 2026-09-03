"""Canonical payment business rules, shared by every runtime.

These four definitions are the contract the medallion enforces: which field is PII and
how it is masked, and which payment methods and statuses are considered valid. They lived
in three places -- bronze_from_kafka.py, silver_payments.py, and (copied verbatim)
databricks/src/dlt_pipeline.py -- with nothing asserting the copies agreed. Adding a
payment method meant remembering all three.

Deliberately imports nothing beyond the standard library: no pyspark, no dlt. That is what
lets the Spark jobs, the tests, and the drift guard all import it, and what keeps the rules
testable in milliseconds without a session.

Ships to Kubernetes through the spark-jobs ConfigMap alongside common.py; the file list in
k8s/base/kustomization.yaml is asserted against this directory by
tests/test_payment_rules.py, so a new module here cannot be left out of the pods.

The Databricks DLT pipeline still inlines its own copy on purpose -- it is loaded from a
Volume where sibling imports are unreliable on Free Edition, which is why it is written
self-contained. That copy is no longer free to drift: test_payment_rules.py parses it and
fails if it disagrees with the values below.
"""
from __future__ import annotations

import hashlib
import json

# Hashed before writing to Bronze so PII never lands in the lakehouse.
PII_FIELDS = frozenset({"shopper_id"})

ALLOWED_PAYMENT_METHODS = (
    "apple_pay",
    "bank_transfer",
    "card",
    "google_pay",
    "paypal",
)

ALLOWED_PAYMENT_STATUSES = (
    "authorized",
    "cancelled",
    "chargeback",
    "failed",
    "pending",
    "refunded",
)


def mask_pii_fields(value: str | None) -> str | None:
    """Hash PII fields in both `before` and `after` sections of a Debezium envelope.

    Returns the input unchanged when it is not JSON: a malformed envelope is the
    dead-letter path's problem, not this function's, and raising here would fail the whole
    micro-batch over one bad record.
    """
    if value is None:
        return None
    try:
        envelope = json.loads(value)
        for section in ("before", "after"):
            if isinstance(envelope.get(section), dict):
                for field in PII_FIELDS:
                    if field in envelope[section] and envelope[section][field] is not None:
                        raw = str(envelope[section][field]).encode()
                        envelope[section][field] = hashlib.sha256(raw).hexdigest()
        return json.dumps(envelope)
    except (json.JSONDecodeError, TypeError):
        return value
