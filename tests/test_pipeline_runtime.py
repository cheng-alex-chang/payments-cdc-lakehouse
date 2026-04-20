from __future__ import annotations

import importlib
import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest
from unittest.mock import MagicMock


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

    window_module = types.ModuleType("pyspark.sql.window")
    window_module.Window = type(
        "FakeWindow", (),
        {"partitionBy": staticmethod(lambda *args: type("FakeOrderedWindow", (), {"orderBy": lambda self, *cols: object()})())},
    )

    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions_module)
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
    ("job_name", "expected_command"),
    [
        (
            "bronze",
            f"docker exec dp-spark /opt/spark/bin/spark-submit --master local[*] --packages {_KAFKA},{_ICEBERG} /opt/project/config/spark/jobs/bronze_from_kafka.py",
        ),
        (
            "silver",
            f"docker exec dp-spark /opt/spark/bin/spark-submit --master local[*] --packages {_ICEBERG} /opt/project/config/spark/jobs/silver_payments.py",
        ),
        (
            "gold",
            f"docker exec dp-spark /opt/spark/bin/spark-submit --master local[*] --packages {_ICEBERG} /opt/project/config/spark/jobs/gold_metrics.py",
        ),
    ],
)
def test_run_local_job_dispatches_expected_command(
    monkeypatch: pytest.MonkeyPatch, job_name: str, expected_command: str
) -> None:
    from scripts import run_local_job

    recorded: list[tuple[str, bool, bool]] = []
    monkeypatch.setattr(run_local_job.subprocess, "run",
                        lambda command, shell, check: recorded.append((command, shell, check)))
    run_local_job.main(job_name)
    assert recorded == [(expected_command, True, True)]


def test_run_local_job_rejects_unknown_job() -> None:
    from scripts import run_local_job

    with pytest.raises(SystemExit, match="Unsupported job: invalid"):
        run_local_job.main("invalid")


# ---------------------------------------------------------------------------
# init_hdfs
# ---------------------------------------------------------------------------

def test_init_hdfs_runs_expected_mkdir(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import init_hdfs

    recorded: list[str] = []
    monkeypatch.setattr(init_hdfs, "run_hdfs", recorded.append)
    init_hdfs.main()

    assert recorded == [
        "-mkdir -p "
        "/data/bronze /data/silver /data/gold "
        "/warehouse /warehouse/analytics.db "
        "/checkpoints/bronze /checkpoints/silver /checkpoints/gold"
    ]


def test_init_hdfs_run_hdfs_invokes_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import init_hdfs

    recorded: list[tuple[str, bool, bool]] = []
    monkeypatch.setattr(init_hdfs.subprocess, "run",
                        lambda command, shell, check: recorded.append((command, shell, check)))
    init_hdfs.run_hdfs("-ls /data")
    assert recorded == [("docker exec dp-namenode hdfs dfs -ls /data", True, True)]


# ---------------------------------------------------------------------------
# trino scripts
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("module_name", "expected_command"),
    [
        (
            "scripts.publish_trino_tables",
            'docker exec dp-trino trino --execute "SHOW TABLES IN iceberg.analytics"',
        ),
        (
            "scripts.validate_trino",
            "docker exec dp-trino trino --file /opt/project/sql/trino/validation_queries.sql",
        ),
    ],
)
def test_trino_scripts_execute_expected_command(
    monkeypatch: pytest.MonkeyPatch, module_name: str, expected_command: str
) -> None:
    module = importlib.import_module(module_name)
    recorded: list[tuple[str, bool, bool]] = []
    monkeypatch.setattr(module.subprocess, "run",
                        lambda command, shell, check: recorded.append((command, shell, check)))
    module.main()
    assert recorded == [(expected_command, True, True)]


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


# ---------------------------------------------------------------------------
# gold job
# ---------------------------------------------------------------------------

def test_gold_metrics_recomputes_affected_partitions(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.gold_metrics")
    module.main()

    assert builder.app_name == "gold-metrics"
    assert any(k == "spark.sql.catalog.iceberg.type" for k, _ in builder.config_values)
    assert spark.readStream.format_value == "iceberg"
    assert spark.readStream.load_path == module.BRONZE_TABLE
    assert spark.readStream._frame.writeStream.trigger_kwargs == {"availableNow": True}
    assert spark.readStream._frame.writeStream.options.get("checkpointLocation") == module.CHECKPOINT_PATH
    assert spark.readStream._frame.writeStream.foreach_batch_fn == module._recompute_gold_partitions
    assert spark.stopped is True


def test_gold_recompute_fn_refreshes_changed_hours(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.gold_metrics")

    module._recompute_gold_partitions(FakeFrame(), 0)

    delete_calls = [c for c in spark.sql_calls if "DELETE FROM" in c]
    insert_calls = [c for c in spark.sql_calls if "INSERT INTO" in c]
    assert len(delete_calls) >= 1
    assert module.GOLD_TABLE in delete_calls[0]
    assert len(insert_calls) >= 1
    assert module.GOLD_TABLE in insert_calls[0]
    assert module.SILVER_TABLE in insert_calls[0]
    assert "DECIMAL(18,2)" in insert_calls[0]


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

def test_payments_pipeline_dag_has_expected_shape() -> None:
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

        def __init__(self, task_id: str, bash_command: str) -> None:
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
    spec = importlib.util.spec_from_file_location("repo_payments_pipeline", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dag = module.dag

    assert dag.schedule_interval is None
    assert dag.max_active_runs == 1
    assert dag.task_ids == {
        "init_hdfs", "validate_connector", "bronze_load",
        "silver_transform", "gold_transform", "publish_trino_tables", "validate_trino",
    }
    assert dag.get_task("init_hdfs").downstream_task_ids == {"validate_connector"}
    assert dag.get_task("validate_connector").downstream_task_ids == {"bronze_load"}
    assert dag.get_task("bronze_load").downstream_task_ids == {"silver_transform"}
    assert dag.get_task("silver_transform").downstream_task_ids == {"gold_transform"}
    assert dag.get_task("gold_transform").downstream_task_ids == {"publish_trino_tables"}
    assert dag.get_task("publish_trino_tables").downstream_task_ids == {"validate_trino"}
