"""Enterprise Production Orchestration: Config Parameters, Retries & Audit Logs.

Scenario & Scale:
    A mission-critical nightly financial DAG runs 15 dependent Spark stages
    across dev/staging/prod. Transient cloud throttling (HTTP 503) requires
    exponential backoff retries, webhook alerting, and durable Delta audit logs.
"""

from __future__ import annotations

import os
import random
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


AUDIT_SCHEMA = T.StructType([
    T.StructField("run_env", T.StringType(), nullable=False),
    T.StructField("batch_date", T.StringType(), nullable=False),
    T.StructField("task_name", T.StringType(), nullable=False),
    T.StructField("status", T.StringType(), nullable=False),
    T.StructField("attempt", T.IntegerType(), nullable=False),
    T.StructField("duration_seconds", T.DoubleType(), nullable=False),
    T.StructField("error_message", T.StringType(), nullable=True),
    T.StructField("executed_at_utc", T.StringType(), nullable=False),
])


@dataclass(frozen=True)
class WorkflowConfig:
    """Portable runtime parameters injected via CLI args or env vars."""

    run_env: str
    batch_date: str
    audit_uri: str
    max_retries: int = 3
    base_delay_sec: float = 0.05

    @classmethod
    def from_env(cls, audit_uri: str) -> WorkflowConfig:
        return cls(
            run_env=os.getenv("PIPELINE_ENV", "dev"),
            batch_date=os.getenv("BATCH_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d")),
            audit_uri=audit_uri,
        )


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Swallowing exceptions silently, retrying immediately in a
# tight loop without backoff/jitter, or coupling task params to vendor globals.
# Otherwise: Downstream tasks publish corrupt/partial Gold tables, tight retry
# storms worsen cloud 429/503 throttling, and pipelines fail local/CI testing.
def run_task_unsafe_bad(task_fn: Callable[[], None]) -> None:
    try:
        task_fn()
    except Exception:
        pass  # Silent corruption of downstream DAG stages


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Parameterize via `WorkflowConfig`, retry transient failures
# with exponential backoff + jitter, dispatch alerts on terminal failure, and
# commit structured audit telemetry cleanly via DataFrame schema.
class EnterpriseTaskRunner:
    """Portable Spark workflow task runner with retry telemetry & alerting."""

    def __init__(
        self,
        config: WorkflowConfig,
        alert_fn: Callable[[str, str], None] | None = None,
    ) -> None:
        self.config = config
        self.alert_fn = alert_fn or self._default_alert
        self.audit_records: list[tuple] = []

    @staticmethod
    def _default_alert(task_name: str, error_msg: str) -> None:
        print(f"[ALERT] Task='{task_name}' failed: {error_msg}")

    def _record(
        self,
        task_name: str,
        status: str,
        attempt: int,
        duration_sec: float,
        error_msg: str | None = None,
    ) -> None:
        self.audit_records.append((
            self.config.run_env,
            self.config.batch_date,
            task_name,
            status,
            attempt,
            round(duration_sec, 4),
            error_msg,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        ))

    def execute_with_retry(self, task_name: str, task_fn: Callable[[], None]) -> None:
        """Execute task with bounded exponential backoff + jitter and audit trail."""
        for attempt in range(1, self.config.max_retries + 1):
            t0 = time.monotonic()
            try:
                task_fn()
                self._record(task_name, "SUCCESS", attempt, time.monotonic() - t0)
                return
            except Exception as exc:
                duration = time.monotonic() - t0
                if attempt == self.config.max_retries:
                    self._record(task_name, "FAILED", attempt, duration, str(exc))
                    self.alert_fn(task_name, str(exc))
                    raise
                self._record(task_name, "RETRY_WAIT", attempt, duration, str(exc))
                jitter = random.uniform(0.0, self.config.base_delay_sec * 0.25)
                time.sleep((self.config.base_delay_sec * (2 ** (attempt - 1))) + jitter)

    def to_audit_dataframe(self, spark: SparkSession) -> DataFrame:
        """Convert collected lifecycle events into an ANSI-typed audit DataFrame."""
        return spark.createDataFrame(self.audit_records, AUDIT_SCHEMA).withColumn(
            "executed_ts",
            F.try_to_timestamp(F.col("executed_at_utc"), F.lit("yyyy-MM-dd HH:mm:ss")),
        )


def transform_summarize_audit_sla(df_audit: DataFrame) -> DataFrame:
    """Summarize task SLA durations and retry counts from audit telemetry."""
    return df_audit.groupBy("run_env", "batch_date", "task_name").agg(
        F.max("attempt").alias("attempts_taken"),
        F.round(F.sum("duration_seconds"), 3).alias("total_duration_sec"),
        F.last("status").alias("final_status"),
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - Keep audit telemetry small (1 row per task attempt, written once at DAG end
#   or per stage) so audit Delta commits never create small-file overhead.
# - Tag Spark jobs via `spark.addTag(f"{run_env}:{task_name}")` (Spark 3.5+/4.0)
#   to correlate Spark UI SQL queries directly back to orchestrator DAG tasks.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Always add jitter (`+ random.uniform(...)`) to exponential backoff so 50
#    failing parallel tasks don't retry in lockstep (thundering herd).
# 2. Only retry *transient* errors (HTTP 429/503, network reset)—fail fast on
#    deterministic code/schema/ANSI errors so you don't burn cluster DBU budget.
# 3. Decouple config injection (`WorkflowConfig.from_env`) from orchestrators so
#    the same module runs identically in Airflow, Databricks Jobs, K8s, and pytest.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("105_enterprise_orchestration")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    attempts = {"count": 0}

    def flaky_storage_sync() -> None:
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise ConnectionResetError("HTTP 503 storage endpoint busy")

    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = WorkflowConfig(run_env="prod", batch_date="2025-01-20", audit_uri=tmp_dir)
        runner = EnterpriseTaskRunner(cfg)
        runner.execute_with_retry("bronze_ingest", lambda: None)
        runner.execute_with_retry("cloud_sync", flaky_storage_sync)
        df_audit = runner.to_audit_dataframe(spark)
        df_audit.write.format("parquet").mode("append").save(tmp_dir)
        transform_summarize_audit_sla(spark.read.parquet(tmp_dir)).show(truncate=False)
