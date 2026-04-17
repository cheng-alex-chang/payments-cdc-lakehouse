from __future__ import annotations

import importlib
import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest


class FakeExpr:
    def __init__(self, name: str | None = None) -> None:
        self.name = name

    def cast(self, _value: str) -> "FakeExpr":
        return self

    def alias(self, _value: str) -> "FakeExpr":
        return self

    def desc(self) -> "FakeExpr":
        return self

    def over(self, _window: object) -> "FakeExpr":
        return self

    def otherwise(self, _value: object) -> "FakeExpr":
        return self

    def isNotNull(self) -> "FakeExpr":
        return self

    def __truediv__(self, _other: object) -> "FakeExpr":
        return self

    def __eq__(self, _other: object) -> "FakeExpr":  # type: ignore[override]
        return self


class FakeWriter:
    def __init__(self, frame: "FakeFrame") -> None:
        self.frame = frame
        self.mode_value: str | None = None
        self.parquet_path: str | None = None

    def mode(self, value: str) -> "FakeWriter":
        self.mode_value = value
        return self

    def parquet(self, path: str) -> None:
        self.parquet_path = path


class FakeGroupedFrame:
    def __init__(self, frame: "FakeFrame") -> None:
        self.frame = frame

    def agg(self, *expressions: object) -> "FakeFrame":
        self.frame.operations.append(("agg", len(expressions)))
        return self.frame


class FakeFrame:
    def __init__(self) -> None:
        self.operations: list[tuple[str, object]] = []
        self.write = FakeWriter(self)

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

    def groupBy(self, *columns: str) -> FakeGroupedFrame:
        self.operations.append(("groupBy", columns))
        return FakeGroupedFrame(self)

    def __getattr__(self, name: str) -> FakeExpr:
        return FakeExpr(name)


class FakeReader:
    def __init__(self, frame: FakeFrame) -> None:
        self.frame = frame
        self.options: list[tuple[str, object]] = []
        self.format_name: str | None = None
        self.csv_path: str | None = None
        self.parquet_path: str | None = None

    def option(self, key: str, value: object) -> "FakeReader":
        self.options.append((key, value))
        return self

    def format(self, name: str) -> "FakeReader":
        self.format_name = name
        return self

    def csv(self, path: str) -> FakeFrame:
        self.csv_path = path
        return self.frame

    def parquet(self, path: str) -> FakeFrame:
        self.parquet_path = path
        return self.frame

    def load(self) -> FakeFrame:
        return self.frame


class FakeSparkSession:
    def __init__(self) -> None:
        self.frame = FakeFrame()
        self.read = FakeReader(self.frame)
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


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
    spark = FakeSparkSession()
    builder = FakeBuilder(spark)
    spark_session_class = type("FakeSparkSessionClass", (), {"builder": builder})

    sql_module = types.ModuleType("pyspark.sql")
    sql_module.SparkSession = spark_session_class

    functions_module = types.ModuleType("pyspark.sql.functions")
    for name in [
        "avg",
        "col",
        "count",
        "current_timestamp",
        "date_trunc",
        "from_unixtime",
        "get_json_object",
        "input_file_name",
        "lower",
        "regexp_replace",
        "sum",
        "trim",
        "upper",
        "when",
    ]:
        setattr(functions_module, name, lambda *args, **kwargs: FakeExpr(name))

    functions_module.row_number = lambda: FakeExpr("row_number")

    window_module = types.ModuleType("pyspark.sql.window")
    window_module.Window = type(
        "FakeWindow",
        (),
        {
            "partitionBy": staticmethod(lambda *args: type("FakeOrderedWindow", (), {"orderBy": lambda self, *cols: object()})()),
        },
    )

    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.window", window_module)
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)
    return module, spark, builder


@pytest.mark.parametrize(
    ("job_name", "expected_command"),
    [
        (
            "bronze",
            "docker exec dp-spark /opt/spark/bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 /opt/project/config/spark/jobs/bronze_from_kafka.py",
        ),
        ("silver", "docker exec dp-spark /opt/spark/bin/spark-submit --master local[*] /opt/project/config/spark/jobs/silver_payments.py"),
        ("gold", "docker exec dp-spark /opt/spark/bin/spark-submit --master local[*] /opt/project/config/spark/jobs/gold_metrics.py"),
    ],
)
def test_run_local_job_dispatches_expected_command(
    monkeypatch: pytest.MonkeyPatch, job_name: str, expected_command: str
) -> None:
    from scripts import run_local_job

    recorded: list[tuple[str, bool, bool]] = []
    monkeypatch.setattr(
        run_local_job.subprocess,
        "run",
        lambda command, shell, check: recorded.append((command, shell, check)),
    )

    run_local_job.main(job_name)

    assert recorded == [(expected_command, True, True)]


def test_run_local_job_rejects_unknown_job() -> None:
    from scripts import run_local_job

    with pytest.raises(SystemExit, match="Unsupported job: invalid"):
        run_local_job.main("invalid")


def test_init_hdfs_runs_expected_mkdir(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import init_hdfs

    recorded: list[str] = []
    monkeypatch.setattr(init_hdfs, "run_hdfs", recorded.append)

    init_hdfs.main()

    assert recorded == ["-mkdir -p /data/bronze /data/silver /data/gold /warehouse /warehouse/analytics.db"]


def test_init_hdfs_run_hdfs_invokes_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import init_hdfs

    recorded: list[tuple[str, bool, bool]] = []
    monkeypatch.setattr(
        init_hdfs.subprocess,
        "run",
        lambda command, shell, check: recorded.append((command, shell, check)),
    )

    init_hdfs.run_hdfs("-ls /data")

    assert recorded == [("docker exec dp-namenode hdfs dfs -ls /data", True, True)]


@pytest.mark.parametrize(
    ("module_name", "expected_command"),
    [
        ("scripts.publish_trino_tables", "docker exec dp-trino trino --file /opt/project/sql/trino/create_hive_tables.sql"),
        ("scripts.validate_trino", "docker exec dp-trino trino --file /opt/project/sql/trino/validation_queries.sql"),
    ],
)
def test_trino_scripts_execute_expected_command(
    monkeypatch: pytest.MonkeyPatch, module_name: str, expected_command: str
) -> None:
    module = importlib.import_module(module_name)
    recorded: list[tuple[str, bool, bool]] = []
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, shell, check: recorded.append((command, shell, check)),
    )

    module.main()

    assert recorded == [(expected_command, True, True)]


def test_validate_connector_accepts_healthy_payload(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from scripts import validate_connector

    payload = {
        "connector": {"state": "RUNNING"},
        "tasks": [{"state": "RUNNING"}, {"state": "RUNNING"}],
    }

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(payload).encode("utf-8")

    monkeypatch.setattr(validate_connector, "urlopen", lambda *args, **kwargs: Response())

    validate_connector.main()

    assert "Connector healthy" in capsys.readouterr().out


def test_validate_connector_rejects_unhealthy_connector(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import validate_connector

    payload = {"connector": {"state": "FAILED"}, "tasks": []}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(payload).encode("utf-8")

    monkeypatch.setattr(validate_connector, "urlopen", lambda *args, **kwargs: Response())

    with pytest.raises(SystemExit, match="Connector not healthy: FAILED"):
        validate_connector.main()


def test_validate_connector_rejects_failed_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import validate_connector

    payload = {
        "connector": {"state": "RUNNING"},
        "tasks": [{"state": "RUNNING"}, {"state": "FAILED"}],
    }

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(payload).encode("utf-8")

    monkeypatch.setattr(validate_connector, "urlopen", lambda *args, **kwargs: Response())

    with pytest.raises(SystemExit, match="Connector tasks unhealthy"):
        validate_connector.main()


def test_bronze_from_kafka_main_reads_cdc_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.bronze_from_kafka")

    module.main()

    assert builder.app_name == "bronze-from-kafka"
    assert spark.read.format_name == "kafka"
    assert ("subscribe", "cdc.public.payments") in spark.read.options
    assert spark.frame.write.parquet_path == "hdfs://namenode:9000/data/bronze/payments_cdc"
    assert spark.stopped is True


def test_silver_payments_main_writes_silver_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.silver_payments")

    module.main()

    assert builder.app_name == "silver-payments"
    assert spark.read.parquet_path == module.BRONZE_PATH
    assert ("filter", "applied") in spark.frame.operations
    assert any(operation == ("drop", ("row_number",)) for operation in spark.frame.operations)
    assert sum(1 for operation in spark.frame.operations if operation == ("filter", "applied")) == 2
    assert spark.frame.write.parquet_path == module.SILVER_PATH
    assert spark.stopped is True


def test_gold_metrics_main_writes_gold_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    module, spark, builder = load_module_with_fake_pyspark(monkeypatch, "config.spark.jobs.gold_metrics")

    module.main()

    assert builder.app_name == "gold-metrics"
    assert spark.read.parquet_path == module.SILVER_PATH
    assert any(operation[0] == "groupBy" for operation in spark.frame.operations)
    assert any(operation == ("agg", 3) for operation in spark.frame.operations)
    assert spark.frame.write.parquet_path == module.GOLD_PATH
    assert spark.stopped is True


def test_payments_pipeline_dag_has_expected_shape() -> None:
    class FakeDAG:
        def __init__(self, *args, **kwargs) -> None:
            self.schedule_interval = kwargs.get("schedule")
            self.max_active_runs = kwargs.get("max_active_runs")
            self.tasks: dict[str, FakeBashOperator] = {}

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
        "init_hdfs",
        "validate_connector",
        "bronze_load",
        "silver_transform",
        "gold_transform",
        "publish_trino_tables",
        "validate_trino",
    }
    assert dag.get_task("init_hdfs").downstream_task_ids == {"validate_connector"}
    assert dag.get_task("validate_connector").downstream_task_ids == {"bronze_load"}
    assert dag.get_task("bronze_load").downstream_task_ids == {"silver_transform"}
    assert dag.get_task("silver_transform").downstream_task_ids == {"gold_transform"}
    assert dag.get_task("gold_transform").downstream_task_ids == {"publish_trino_tables"}
    assert dag.get_task("publish_trino_tables").downstream_task_ids == {"validate_trino"}
