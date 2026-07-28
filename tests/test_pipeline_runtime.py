from __future__ import annotations

import importlib
import importlib.util
import json
import sys
import types
from pathlib import Path

from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Fake streaming primitives
# ---------------------------------------------------------------------------

class FakeStreamQuery:
    def awaitTermination(self) -> None:
        pass


class FakeStreamWriter:
    def __init__(self) -> None:
        self.format_value: str | None = None
        self.output_mode_value: str | None = None
        self.trigger_kwargs: dict = {}
        self.options: dict = {}
        self.foreach_batch_fn = None
        self.to_table_name: str | None = None

    def format(self, name: str) -> "FakeStreamWriter":
        self.format_value = name
        return self

    def outputMode(self, mode: str) -> "FakeStreamWriter":
        self.output_mode_value = mode
        return self

    def trigger(self, **kwargs) -> "FakeStreamWriter":
        self.trigger_kwargs = kwargs
        return self

    def option(self, key: str, value: str) -> "FakeStreamWriter":
        self.options[key] = value
        return self

    def foreachBatch(self, fn) -> "FakeStreamWriter":
        self.foreach_batch_fn = fn
        return self

    def start(self) -> FakeStreamQuery:
        return FakeStreamQuery()

    def toTable(self, name: str) -> FakeStreamQuery:
        self.to_table_name = name
        return FakeStreamQuery()


class FakeStreamFrame:
    def __init__(self) -> None:
        self.writeStream = FakeStreamWriter()

    def select(self, *args) -> "FakeStreamFrame":
        return self

    def filter(self, *args) -> "FakeStreamFrame":
        return self

    def withColumn(self, *args) -> "FakeStreamFrame":
        return self

    def __getattr__(self, name: str) -> "FakeStreamFrame":
        return self


class FakeStreamReader:
    def __init__(self) -> None:
        self.format_value: str | None = None
        self.options: dict = {}
        self.load_path: str | None = None
        self._frame = FakeStreamFrame()

    def format(self, name: str) -> "FakeStreamReader":
        self.format_value = name
        return self

    def option(self, key: str, value: str) -> "FakeStreamReader":
        self.options[key] = value
        return self

    def load(self, path: str | None = None) -> FakeStreamFrame:
        self.load_path = path
        return self._frame


# ---------------------------------------------------------------------------
# Fake batch primitives
# ---------------------------------------------------------------------------

class FakeExpr:
    def __init__(self, name: str | None = None) -> None:
        self.name = name

    def cast(self, _value: str) -> "FakeExpr": return self
    def alias(self, _value: str) -> "FakeExpr": return self
    def desc(self) -> "FakeExpr": return self
    def over(self, _window: object) -> "FakeExpr": return self
    def otherwise(self, _value: object) -> "FakeExpr": return self
    def isNotNull(self) -> "FakeExpr": return self
    def isNull(self) -> "FakeExpr": return self
    def isin(self, *args) -> "FakeExpr": return self
    def rlike(self, _value: str) -> "FakeExpr": return self
    def __truediv__(self, _other: object) -> "FakeExpr": return self
    def __lt__(self, _other: object) -> "FakeExpr": return self
    def __gt__(self, _other: object) -> "FakeExpr": return self
    def __eq__(self, _other: object) -> "FakeExpr": return self  # type: ignore[override]
    def __invert__(self) -> "FakeExpr": return self
    def __and__(self, _other: object) -> "FakeExpr": return self


class FakeWriter:
    def __init__(self) -> None:
        self.mode_value: str | None = None
        self.parquet_path: str | None = None

    def mode(self, value: str) -> "FakeWriter":
        self.mode_value = value
        return self

    def parquet(self, path: str) -> None:
        self.parquet_path = path


class FakeDataFrameWriterV2:
    def append(self) -> None:
        pass


class FakeGroupedFrame:
    def __init__(self, frame: "FakeFrame") -> None:
        self.frame = frame

    def agg(self, *expressions: object) -> "FakeFrame":
        self.frame.operations.append(("agg", len(expressions)))
        return self.frame


class FakeFrame:
    def __init__(self) -> None:
        self.operations: list[tuple[str, object]] = []
        self.write = FakeWriter()

    def withColumn(self, name: str, _value: object) -> "FakeFrame":
        self.operations.append(("withColumn", name))
        return self

    def filter(self, _value: object) -> "FakeFrame":
        self.operations.append(("filter", "applied"))
        return self

    def drop(self, *columns: str) -> "FakeFrame":
        self.operations.append(("drop", columns))
        return self

    def select(self, *columns: object) -> "FakeFrame":
        self.operations.append(("select", len(columns)))
        return self

    def distinct(self) -> "FakeFrame":
        self.operations.append(("distinct", True))
        return self

    def groupBy(self, *columns: str) -> FakeGroupedFrame:
        self.operations.append(("groupBy", columns))
        return FakeGroupedFrame(self)

    def isEmpty(self) -> bool:
        return False

    def count(self) -> int:
        return 0

    def writeTo(self, _table: str) -> FakeDataFrameWriterV2:
        return FakeDataFrameWriterV2()

    def createOrReplaceTempView(self, name: str) -> None:
        pass

    @property
    def sparkSession(self) -> "FakeSparkSession":
        return _current_fake_spark

    def __getattr__(self, name: str) -> FakeExpr:
        return FakeExpr(name)


class FakeReader:
    def __init__(self, frame: FakeFrame) -> None:
        self.frame = frame
        self.options: list[tuple[str, object]] = []
        self.format_name: str | None = None
        self.parquet_path: str | None = None

    def option(self, key: str, value: object) -> "FakeReader":
        self.options.append((key, value))
        return self

    def format(self, name: str) -> "FakeReader":
        self.format_name = name
        return self

    def parquet(self, path: str) -> FakeFrame:
        self.parquet_path = path
        return self.frame

    def load(self) -> FakeFrame:
        return self.frame


# global ref so FakeFrame.sparkSession can reach it
_current_fake_spark: "FakeSparkSession | None" = None


class FakeSparkSession:
    def __init__(self) -> None:
        self.frame = FakeFrame()
        self.read = FakeReader(self.frame)
        self.readStream = FakeStreamReader()
        self.sql_calls: list[str] = []
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True

    def sql(self, query: str) -> FakeFrame:
        self.sql_calls.append(query.strip())
        return self.frame


class FakeBuilder:
    def __init__(self, spark: FakeSparkSession) -> None:
        self.spark = spark
        self.app_name: str | None = None
        self.config_values: list[tuple[str, str]] = []

    def appName(self, value: str) -> "FakeBuilder":
        self.app_name = value
        return self

    def config(self, key: str, value: str) -> "FakeBuilder":
        self.config_values.append((key, value))
        return self

    def getOrCreate(self) -> FakeSparkSession:
        return self.spark


def load_module_with_fake_pyspark(monkeypatch: pytest.MonkeyPatch, module_name: str):
    global _current_fake_spark
    spark = FakeSparkSession()
    _current_fake_spark = spark
    builder = FakeBuilder(spark)
    spark_session_class = type("FakeSparkSessionClass", (), {"builder": builder})

    sql_module = types.ModuleType("pyspark.sql")
    sql_module.SparkSession = spark_session_class
    sql_module.DataFrame = object

    functions_module = types.ModuleType("pyspark.sql.functions")
    for name in [
        "avg", "col", "count", "current_timestamp", "date_trunc",
        "from_unixtime", "get_json_object", "input_file_name", "lit", "lower",
        "regexp_replace", "sum", "trim", "upper", "when",
    ]:
        setattr(functions_module, name, lambda *args, name=name, **kwargs: FakeExpr(name))
    functions_module.row_number = lambda: FakeExpr("row_number")
    functions_module.udf = lambda fn, returnType=None: (lambda *args, **kwargs: FakeExpr("udf"))

    types_module = types.ModuleType("pyspark.sql.types")
    types_module.StringType = type("StringType", (), {})

    window_module = types.ModuleType("pyspark.sql.window")
    window_module.Window = type(
        "FakeWindow", (),
        {"partitionBy": staticmethod(lambda *args: type("FakeOrderedWindow", (), {"orderBy": lambda self, *cols: object()})())},
    )

    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", types_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.window", window_module)
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)
    return module, spark, builder


# ---------------------------------------------------------------------------
# run_local_job
# ---------------------------------------------------------------------------

_ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
_KAFKA   = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"

@pytest.mark.parametrize(
    ("job_name", "script", "packages"),
    [
        ("bronze", "bronze_from_kafka.py", f"{_KAFKA},{_ICEBERG}"),
        ("silver", "silver_payments.py", _ICEBERG),
        ("gold", "gold_metrics.py", _ICEBERG),
    ],
)
def test_run_local_job_dispatches_expected_command(
    monkeypatch: pytest.MonkeyPatch, job_name: str, script: str, packages: str
) -> None:
    # This is the Compose submission path only; Kubernetes creates a pod per job instead. The
    # bounded driver (local[2] + --driver-memory) is asserted here and cross-checked against the
    # Kubernetes Job templates in tests/test_spark_jobs.py.
    from scripts import run_local_job

    recorded: list[tuple[str, bool, bool]] = []
    monkeypatch.setattr(run_local_job.subprocess, "run",
                        lambda command, shell, check: recorded.append((command, shell, check)))
    run_local_job.main(job_name)

    assert len(recorded) == 1
    command, shell, check = recorded[0]
    assert (shell, check) == (True, True)
    assert command.startswith("docker exec dp-spark /opt/spark/bin/spark-submit")
    assert "--master local[2]" in command
    assert "--driver-memory 1g" in command
    assert "local[*]" not in command
    assert f"--packages {packages}" in command
    assert command.endswith(f"/opt/project/config/spark/jobs/{script}")


def test_run_local_job_rejects_unknown_job() -> None:
    from scripts import run_local_job

    with pytest.raises(SystemExit, match="Unsupported job: invalid"):
        run_local_job.main("invalid")


# ---------------------------------------------------------------------------
# init_hdfs
# ---------------------------------------------------------------------------

def test_init_hdfs_creates_every_directory_the_pipeline_writes_to() -> None:
    from scripts import init_hdfs

    # Bronze and silver checkpoint into HDFS, and the Iceberg warehouse lives there; a missing
    # directory surfaces much later as a Spark write failure.
    assert set(init_hdfs.DIRECTORIES) == {
        "/data/bronze", "/data/silver", "/data/gold",
        "/warehouse", "/warehouse/analytics.db",
        "/checkpoints/bronze", "/checkpoints/silver",
    }


def test_init_hdfs_targets_the_namenode_by_service_name() -> None:
    from scripts import init_hdfs

    # "namenode" resolves as a Compose service name and a Kubernetes Service DNS name alike --
    # the point of moving off `docker exec dp-namenode`.
    url = init_hdfs.mkdirs_url("/warehouse", env={})

    assert url == "http://namenode:9870/webhdfs/v1/warehouse?op=MKDIRS&user.name=root"


def test_init_hdfs_honours_environment_overrides() -> None:
    from scripts import init_hdfs

    url = init_hdfs.mkdirs_url(
        "/data/bronze",
        env={"HDFS_NAMENODE_HOST": "localhost", "HDFS_NAMENODE_HTTP_PORT": "19870"},
    )

    assert url.startswith("http://localhost:19870/webhdfs/v1/data/bronze?op=MKDIRS")


def test_init_hdfs_rejects_a_non_numeric_port() -> None:
    from scripts import init_hdfs

    with pytest.raises(ValueError, match="HDFS_NAMENODE_HTTP_PORT"):
        init_hdfs.mkdirs_url("/warehouse", env={"HDFS_NAMENODE_HTTP_PORT": "ninety-eight-seventy"})


def test_init_hdfs_raises_on_a_webhdfs_remote_exception() -> None:
    from scripts import init_hdfs

    # WebHDFS reports failures in the body with HTTP 200, so a status check alone would pass over
    # a directory that was never created.
    payload = {
        "RemoteException": {
            "exception": "AccessControlException",
            "message": "Permission denied: user=root",
        }
    }

    with pytest.raises(RuntimeError, match="AccessControlException"):
        init_hdfs.raise_for_webhdfs_error(payload, "/warehouse")


def test_init_hdfs_raises_when_mkdirs_returns_false() -> None:
    from scripts import init_hdfs

    with pytest.raises(RuntimeError, match="returned false"):
        init_hdfs.raise_for_webhdfs_error({"boolean": False}, "/warehouse")


def test_init_hdfs_accepts_a_successful_mkdirs() -> None:
    from scripts import init_hdfs

    # MKDIRS is idempotent: an existing directory returns true, so retries are safe.
    init_hdfs.raise_for_webhdfs_error({"boolean": True}, "/warehouse")


def test_init_hdfs_puts_to_every_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import init_hdfs

    calls: list[str] = []

    class FakeResponse:
        def raise_for_status(self) -> None: return None
        def json(self) -> dict: return {"boolean": True}

    class FakeHttp:
        def put(self, url: str, timeout: int) -> FakeResponse:
            calls.append(url)
            return FakeResponse()

    monkeypatch.setattr(init_hdfs, "requests", FakeHttp())
    init_hdfs.main()

    assert len(calls) == len(init_hdfs.DIRECTORIES)
    assert all("op=MKDIRS" in url for url in calls)


def test_init_hdfs_does_not_shell_into_a_compose_container() -> None:
    source = (Path(__file__).resolve().parents[1] / "scripts" / "init_hdfs.py").read_text()

    assert "docker exec" not in source
    assert "dp-namenode" not in source


# ---------------------------------------------------------------------------
# trino scripts
# ---------------------------------------------------------------------------

def test_trino_scripts_do_not_shell_into_a_compose_container() -> None:
    """These tasks must run unchanged under Compose and Kubernetes.

    They used to invoke `docker exec dp-trino ...`, naming a container that exists in exactly one
    runtime -- which is why the Kubernetes path drove its Spark jobs with `kubectl patch` instead
    of through the DAG. They now speak Trino's HTTP protocol against a service name that resolves
    in both.
    """
    root = Path(__file__).resolve().parents[1] / "scripts"
    for name in ("publish_trino_tables.py", "validate_trino.py", "maintain_iceberg.py"):
        source = (root / name).read_text(encoding="utf-8")

        assert "docker exec" not in source, name
        assert "dp-trino" not in source, name
        assert "trino_http" in source, name


def test_publish_trino_tables_fails_when_a_medallion_table_is_missing() -> None:
    """The old version echoed SHOW TABLES and exited 0 regardless.

    A missing gold table sailed through the DAG and surfaced much later as an empty dashboard.
    """
    from scripts import publish_trino_tables

    rows = [["payments_bronze"], ["payments_silver"]]

    assert publish_trino_tables.missing_tables(rows) == {"payment_metrics_gold"}
    assert publish_trino_tables.missing_tables(rows + [["payment_metrics_gold"]]) == set()


def test_publish_trino_tables_raises_on_missing_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import publish_trino_tables

    monkeypatch.setattr(publish_trino_tables.trino_http, "run_statement", lambda _sql: [])

    with pytest.raises(SystemExit, match="payment_metrics_gold"):
        publish_trino_tables.main()


# ---------------------------------------------------------------------------
# validate_connector
# ---------------------------------------------------------------------------

def test_validate_connector_accepts_healthy_payload(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from scripts import validate_connector

    payload = {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}, {"state": "RUNNING"}]}

    class Response:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return None
        def read(self) -> bytes: return json.dumps(payload).encode()

    monkeypatch.setattr(validate_connector, "urlopen", lambda *args, **kwargs: Response())
    validate_connector.main()
    assert "Connector healthy" in capsys.readouterr().out


def test_validate_connector_rejects_unhealthy_connector(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import validate_connector

    payload = {"connector": {"state": "FAILED"}, "tasks": []}

    class Response:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return None
        def read(self) -> bytes: return json.dumps(payload).encode()

    monkeypatch.setattr(validate_connector, "urlopen", lambda *args, **kwargs: Response())
    with pytest.raises(SystemExit, match="Connector not healthy: FAILED"):
        validate_connector.main()


def test_validate_connector_accepts_unassigned_with_running_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import validate_connector

    payload = {"connector": {"state": "UNASSIGNED"}, "tasks": [{"state": "RUNNING"}]}

    class Response:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return None
        def read(self) -> bytes: return json.dumps(payload).encode()

    monkeypatch.setattr(validate_connector, "urlopen", lambda *args, **kwargs: Response())
    validate_connector.main()  # should not raise


def test_validate_connector_rejects_failed_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import validate_connector

    payload = {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}, {"state": "FAILED"}]}

    class Response:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return None
        def read(self) -> bytes: return json.dumps(payload).encode()

    monkeypatch.setattr(validate_connector, "urlopen", lambda *args, **kwargs: Response())
    with pytest.raises(SystemExit, match="Connector tasks unhealthy"):
        validate_connector.main()


# ---------------------------------------------------------------------------
# bronze job
# ---------------------------------------------------------------------------

def test_mask_pii_fields_hashes_shopper_id_in_after(monkeypatch: pytest.MonkeyPatch) -> None:
    module, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.bronze_from_kafka")
    payload = json.dumps({"after": {"payment_id": "p1", "shopper_id": "user-123"}, "op": "c"})
    result = json.loads(module._mask_pii_fields(payload))
    assert result["after"]["shopper_id"] != "user-123"
    assert len(result["after"]["shopper_id"]) == 64  # SHA-256 hex digest
    assert result["after"]["payment_id"] == "p1"


def test_mask_pii_fields_hashes_shopper_id_in_before(monkeypatch: pytest.MonkeyPatch) -> None:
    module, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.bronze_from_kafka")
    payload = json.dumps({"before": {"payment_id": "p1", "shopper_id": "user-123"}, "after": None, "op": "d"})
    result = json.loads(module._mask_pii_fields(payload))
    assert result["before"]["shopper_id"] != "user-123"
    assert len(result["before"]["shopper_id"]) == 64


def test_mask_pii_fields_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    module, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.bronze_from_kafka")
    payload = json.dumps({"after": {"shopper_id": "user-123"}, "op": "c"})
    first = json.loads(module._mask_pii_fields(payload))["after"]["shopper_id"]
    second = json.loads(module._mask_pii_fields(payload))["after"]["shopper_id"]
    assert first == second


def test_mask_pii_fields_returns_none_for_null_input(monkeypatch: pytest.MonkeyPatch) -> None:
    module, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.bronze_from_kafka")
    assert module._mask_pii_fields(None) is None


def test_mask_pii_fields_passes_through_malformed_json(monkeypatch: pytest.MonkeyPatch) -> None:
    module, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.bronze_from_kafka")
    bad = "not-json"
    assert module._mask_pii_fields(bad) == bad


def test_mask_pii_fields_skips_envelope_without_pii(monkeypatch: pytest.MonkeyPatch) -> None:
    module, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.bronze_from_kafka")
    payload = json.dumps({"after": {"payment_id": "p1", "amount": "99.00"}, "op": "c"})
    result = json.loads(module._mask_pii_fields(payload))
    assert result["after"] == {"payment_id": "p1", "amount": "99.00"}


# ---------------------------------------------------------------------------
# bronze job
# ---------------------------------------------------------------------------


def test_bronze_from_kafka_streams_to_iceberg(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.bronze_from_kafka")
    module.main()

    assert builder.app_name == "bronze-from-kafka"
    assert any(k == "spark.sql.catalog.iceberg.type" for k, _ in builder.config_values)
    assert spark.readStream.format_value == "kafka"
    assert spark.readStream.options.get("subscribe") == module.KAFKA_TOPIC
    assert spark.readStream._frame.writeStream.trigger_kwargs == {"availableNow": True}
    assert spark.readStream._frame.writeStream.options.get("checkpointLocation") == module.CHECKPOINT_PATH
    assert spark.readStream._frame.writeStream.to_table_name == module.BRONZE_TABLE
    assert spark.stopped is True


# ---------------------------------------------------------------------------
# silver job
# ---------------------------------------------------------------------------

def test_silver_payments_streams_from_bronze(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.silver_payments")
    module.main()

    assert builder.app_name == "silver-payments"
    assert any(k == "spark.sql.catalog.iceberg.type" for k, _ in builder.config_values)
    assert spark.readStream.format_value == "iceberg"
    assert spark.readStream.load_path == module.BRONZE_TABLE
    assert spark.readStream._frame.writeStream.trigger_kwargs == {"availableNow": True}
    assert spark.readStream._frame.writeStream.options.get("checkpointLocation") == module.CHECKPOINT_PATH
    assert spark.stopped is True


def test_silver_upsert_fn_merges_upserts(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.silver_payments")
    monkeypatch.setattr(module, "_validate_upserts", lambda upserts: None)
    monkeypatch.setattr(module, "_write_to_dlq", lambda *_: None)

    module._upsert_to_silver(FakeFrame(), 0)

    merge_calls = [c for c in spark.sql_calls if "MERGE INTO" in c]
    assert len(merge_calls) >= 1
    assert module.SILVER_TABLE in merge_calls[0]


def test_silver_upsert_fn_deletes_on_d_op(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.silver_payments")
    monkeypatch.setattr(module, "_validate_upserts", lambda upserts: None)
    monkeypatch.setattr(module, "_write_to_dlq", lambda *_: None)

    module._upsert_to_silver(FakeFrame(), 0)

    delete_calls = [c for c in spark.sql_calls if "DELETE FROM" in c]
    assert len(delete_calls) >= 1
    assert module.SILVER_TABLE in delete_calls[0]


def test_silver_upsert_routes_malformed_to_dlq(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.silver_payments")
    monkeypatch.setattr(module, "_validate_upserts", lambda upserts: None)

    dlq_calls: list[tuple] = []
    monkeypatch.setattr(module, "_write_to_dlq", lambda records, batch_id, reason: dlq_calls.append((batch_id, reason)))

    module._upsert_to_silver(FakeFrame(), 42)

    reasons = [reason for _, reason in dlq_calls]
    assert "null_op" in reasons


def test_silver_upsert_routes_unexpected_op_to_dlq(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.silver_payments")
    monkeypatch.setattr(module, "_validate_upserts", lambda upserts: None)

    dlq_calls: list[tuple] = []
    monkeypatch.setattr(module, "_write_to_dlq", lambda records, batch_id, reason: dlq_calls.append((batch_id, reason)))

    module._upsert_to_silver(FakeFrame(), 7)

    reasons = [reason for _, reason in dlq_calls]
    assert "unexpected_op" in reasons


_CLEAN_QUALITY_METRICS = {
    "null_payment_id": 0,
    "null_merchant_id": 0,
    "null_amount": 0,
    "negative_amount": 0,
    "null_currency": 0,
    "invalid_currency": 0,
    "null_payment_method": 0,
    "invalid_payment_method": 0,
    "null_payment_status": 0,
    "invalid_payment_status": 0,
    "null_country_code": 0,
    "invalid_country_code": 0,
    "null_created_at": 0,
    "null_updated_at": 0,
    "updated_before_created": 0,
}


def _make_dq_mock(metrics: dict, duplicate_count: int = 0) -> MagicMock:
    mock_df = MagicMock()
    mock_df.select.return_value.collect.return_value = [MagicMock(asDict=lambda: metrics)]
    mock_df.groupBy.return_value.agg.return_value.filter.return_value.count.return_value = duplicate_count
    return mock_df


def test_validate_upserts_raises_on_null_payment_id(monkeypatch: pytest.MonkeyPatch) -> None:
    module, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.silver_payments")

    bad = {**_CLEAN_QUALITY_METRICS, "null_payment_id": 2}
    with pytest.raises(ValueError, match="null_payment_id"):
        module._validate_upserts(_make_dq_mock(bad))


def test_validate_upserts_raises_on_duplicate_payment_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    module, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.silver_payments")

    with pytest.raises(ValueError, match="duplicate_payment_ids"):
        module._validate_upserts(_make_dq_mock(_CLEAN_QUALITY_METRICS, duplicate_count=3))


def test_validate_upserts_passes_on_clean_data(monkeypatch: pytest.MonkeyPatch) -> None:
    module, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.silver_payments")

    module._validate_upserts(_make_dq_mock(_CLEAN_QUALITY_METRICS))  # must not raise


def test_build_upserts_dedups_latest_per_payment_id(monkeypatch: pytest.MonkeyPatch) -> None:
    module, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.silver_payments")

    frame = FakeFrame()
    result = module._build_upserts(frame)

    # Dedup must add row_number then filter to row 1, then drop helper columns
    op_names = [name for name, _ in result.operations]
    assert "withColumn" in op_names
    drops = [payload for name, payload in result.operations if name == "drop"]
    assert ("_rn", "_kafka_offset") in drops


# ---------------------------------------------------------------------------
# gold job
# ---------------------------------------------------------------------------

def test_gold_metrics_recomputes_from_silver(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.gold_metrics")
    module.main()

    assert builder.app_name == "gold-metrics"
    assert any(k == "spark.sql.catalog.iceberg.type" for k, _ in builder.config_values)
    # Gold is a batch recompute over silver — no streaming read of bronze.
    assert spark.readStream.format_value is None
    assert spark.readStream.load_path is None
    overwrite_calls = [c for c in spark.sql_calls if "INSERT OVERWRITE" in c]
    assert len(overwrite_calls) >= 1
    assert module.GOLD_TABLE in overwrite_calls[0]
    assert module.SILVER_TABLE in overwrite_calls[0]
    assert spark.stopped is True


def test_gold_recompute_sql_aggregates_silver(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.gold_metrics")

    module.main()

    overwrite_calls = [c for c in spark.sql_calls if "INSERT OVERWRITE" in c]
    assert len(overwrite_calls) >= 1
    sql = overwrite_calls[0]
    assert module.GOLD_TABLE in sql
    assert module.SILVER_TABLE in sql
    assert "DECIMAL(18,2)" in sql


# ---------------------------------------------------------------------------
# trino-exporter
# ---------------------------------------------------------------------------

def _load_exporter(monkeypatch: pytest.MonkeyPatch):
    from prometheus_client import REGISTRY
    for collector in list(REGISTRY._collector_to_names):
        try:
            REGISTRY.unregister(collector)
        except (KeyError, ValueError):
            pass
    sys.modules.pop("exporter", None)
    exporter_dir = Path(__file__).resolve().parents[1] / "config" / "trino-exporter"
    monkeypatch.syspath_prepend(str(exporter_dir))
    return importlib.import_module("exporter")


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    def json(self) -> object:
        return self._payload


def test_trino_exporter_collect_sets_coordinator_up_when_active(monkeypatch: pytest.MonkeyPatch) -> None:
    exporter = _load_exporter(monkeypatch)
    monkeypatch.setattr(
        exporter.requests, "get",
        lambda url, **kw: _FakeResponse({"state": "ACTIVE"}) if url.endswith("/v1/info") else _FakeResponse([]),
    )
    exporter.collect()
    assert exporter.coordinator_up._value.get() == 1


def test_trino_exporter_collect_sets_coordinator_down_when_info_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    exporter = _load_exporter(monkeypatch)

    def failing_get(url, **kw):
        if url.endswith("/v1/info"):
            raise RuntimeError("boom")
        return _FakeResponse([])

    monkeypatch.setattr(exporter.requests, "get", failing_get)
    exporter.collect()
    assert exporter.coordinator_up._value.get() == 0


def test_trino_exporter_collect_counts_query_states(monkeypatch: pytest.MonkeyPatch) -> None:
    exporter = _load_exporter(monkeypatch)
    queries = [
        {"state": "RUNNING"}, {"state": "RUNNING"},
        {"state": "QUEUED"},
        {"state": "FINISHED"}, {"state": "FINISHED"}, {"state": "FINISHED"},
        {"state": "FAILED"},
    ]
    monkeypatch.setattr(
        exporter.requests, "get",
        lambda url, **kw: _FakeResponse({"state": "ACTIVE"}) if url.endswith("/v1/info") else _FakeResponse(queries),
    )
    exporter.collect()
    assert exporter.running_queries._value.get() == 2
    assert exporter.queued_queries._value.get() == 1
    assert exporter.blocked_queries._value.get() == 0
    assert exporter.finished_queries._value.get() == 3
    assert exporter.failed_queries._value.get() == 1


def test_trino_exporter_collect_swallows_query_endpoint_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    exporter = _load_exporter(monkeypatch)
    exporter.running_queries.set(99)

    def failing_get(url, **kw):
        if url.endswith("/v1/info"):
            return _FakeResponse({"state": "ACTIVE"})
        raise RuntimeError("query endpoint down")

    monkeypatch.setattr(exporter.requests, "get", failing_get)
    exporter.collect()  # must not raise
    # query gauges are left untouched (last-known value preserved)
    assert exporter.running_queries._value.get() == 99


# ---------------------------------------------------------------------------
# DAG shape
# ---------------------------------------------------------------------------

def test_payments_pipeline_dag_has_expected_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeDAG:
        def __init__(self, *args, **kwargs) -> None:
            self.schedule_interval = kwargs.get("schedule")
            self.max_active_runs = kwargs.get("max_active_runs")
            self.tasks: dict[str, "FakeBashOperator"] = {}

        def __enter__(self) -> "FakeDAG":
            FakeBashOperator.current_dag = self
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            FakeBashOperator.current_dag = None

        @property
        def task_ids(self) -> set[str]:
            return set(self.tasks)

        def get_task(self, task_id: str) -> "FakeBashOperator":
            return self.tasks[task_id]

    class FakeBashOperator:
        current_dag: FakeDAG | None = None

        def __init__(self, task_id: str, bash_command: str, **kwargs: object) -> None:
            self.task_id = task_id
            self.bash_command = bash_command
            self.downstream_task_ids: set[str] = set()
            assert self.current_dag is not None
            self.current_dag.tasks[task_id] = self

        def __rshift__(self, other: "FakeBashOperator") -> "FakeBashOperator":
            self.downstream_task_ids.add(other.task_id)
            return other

    airflow_module = types.ModuleType("airflow")
    airflow_module.DAG = FakeDAG
    operators_module = types.ModuleType("airflow.operators")
    bash_module = types.ModuleType("airflow.operators.bash")
    bash_module.BashOperator = FakeBashOperator
    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.operators"] = operators_module
    sys.modules["airflow.operators.bash"] = bash_module

    module_path = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "payments_pipeline.py"
    monkeypatch.syspath_prepend(str(module_path.parent))  # so `from alerts import ...` resolves
    spec = importlib.util.spec_from_file_location("repo_payments_pipeline", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dag = module.dag

    assert dag.schedule_interval is None
    assert dag.max_active_runs == 1
    assert dag.task_ids == {
        "init_hdfs", "validate_connector", "validate_schema", "bronze_load",
        "silver_transform", "gold_transform", "publish_trino_tables", "validate_trino",
        "maintain_iceberg",
    }
    assert dag.get_task("init_hdfs").downstream_task_ids == {"validate_connector"}
    assert dag.get_task("validate_connector").downstream_task_ids == {"validate_schema"}
    assert dag.get_task("validate_schema").downstream_task_ids == {"bronze_load"}
    assert dag.get_task("bronze_load").downstream_task_ids == {"silver_transform"}
    assert dag.get_task("silver_transform").downstream_task_ids == {"gold_transform"}
    assert dag.get_task("gold_transform").downstream_task_ids == {"publish_trino_tables"}
    assert dag.get_task("publish_trino_tables").downstream_task_ids == {"validate_trino"}
    # Maintenance runs last, after validation: compaction rewrites files, so running it on an
    # unvalidated load would only preserve bad data more efficiently.
    assert dag.get_task("validate_trino").downstream_task_ids == {"maintain_iceberg"}
    assert dag.get_task("maintain_iceberg").downstream_task_ids == set()


# ---------------------------------------------------------------------------
# cross-job table name contracts
# ---------------------------------------------------------------------------

def test_pipeline_table_contracts_are_consistent(monkeypatch: pytest.MonkeyPatch) -> None:
    bronze, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.bronze_from_kafka")
    silver, _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.silver_payments")
    gold,   _, _ = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.gold_metrics")

    # Silver reads from where Bronze writes
    assert silver.BRONZE_TABLE == bronze.BRONZE_TABLE

    # Gold reads only Silver (linear bronze -> silver -> gold lineage)
    assert gold.SILVER_TABLE == silver.SILVER_TABLE


# ---------------------------------------------------------------------------
# Grafana dashboard / datasource wiring
# ---------------------------------------------------------------------------

def _grafana_paths() -> tuple[Path, Path]:
    root = Path(__file__).resolve().parents[1]
    dashboard = root / "config" / "grafana" / "dashboards" / "pipeline-output.json"
    datasources = root / "config" / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
    return dashboard, datasources


def _dashboard_datasource_uids(dashboard: dict) -> set[str]:
    uids: set[str] = set()
    for panel in dashboard.get("panels", []):
        ds = panel.get("datasource") or {}
        if ds.get("uid"):
            uids.add(ds["uid"])
        for target in panel.get("targets", []):
            tds = target.get("datasource") or {}
            if tds.get("uid"):
                uids.add(tds["uid"])
    return uids


def test_dashboard_datasource_uids_are_provisioned() -> None:
    import yaml

    dashboard_path, datasources_path = _grafana_paths()
    dashboard = json.loads(dashboard_path.read_text())
    provisioned = {
        ds["uid"]
        for ds in yaml.safe_load(datasources_path.read_text())["datasources"]
        if ds.get("uid")
    }

    referenced = _dashboard_datasource_uids(dashboard)
    assert referenced, "dashboard references no datasources"
    dangling = referenced - provisioned
    assert not dangling, f"dashboard references unprovisioned datasource uids: {dangling}"


def test_piecharts_render_one_slice_per_row() -> None:
    """A GROUP BY query needs `reduceOptions.values = true` on a piechart.

    With `values: false` Grafana reduces each *field* to a single number instead of rendering one
    slice per *row*, so a breakdown by payment_method collapsed to one slice labelled `payments`
    -- the column alias. The query was right; the panel was reading it wrong.
    """
    dashboard_path, _ = _grafana_paths()
    dashboard = json.loads(dashboard_path.read_text())

    piecharts = [p for p in dashboard["panels"] if p["type"] == "piechart"]
    assert piecharts, "expected at least one piechart"
    for panel in piecharts:
        options = panel["options"]["reduceOptions"]
        assert options.get("values") is True, panel["title"]


def test_payment_aggregate_panels_read_gold_via_trino() -> None:
    dashboard_path, _ = _grafana_paths()
    dashboard = json.loads(dashboard_path.read_text())
    by_title = {p["title"]: p for p in dashboard["panels"]}

    # Matched by prefix: panel titles carry a parenthetical stating what each one validates
    # (this dashboard is pipeline-output validation, not a business dashboard), and that wording
    # should be free to change without breaking the datasource contract pinned here.
    gold_panels = [
        "Total Payments", "Gross Volume", "Authorization Rate",
        "Volume by Hour", "Method Mix", "Volume by Country",
    ]
    for prefix in gold_panels:
        matches = [p for title, p in by_title.items() if title.startswith(prefix)]
        assert len(matches) == 1, f"expected exactly one panel titled {prefix}*, got {len(matches)}"
        panel = matches[0]
        assert panel["datasource"]["uid"] == "payments-gold-trino", prefix
        for target in panel["targets"]:
            assert target["datasource"]["uid"] == "payments-gold-trino", prefix
            assert "payment_metrics_gold" in target["rawSql"], prefix

    # Refunds are not in the lakehouse yet, so those panels stay on source Postgres.
    for prefix in ("Refund Events", "Refunds Over Time"):
        matches = [p for title, p in by_title.items() if title.startswith(prefix)]
        assert len(matches) == 1, prefix
        assert matches[0]["datasource"]["uid"] == "payments-postgres", prefix
