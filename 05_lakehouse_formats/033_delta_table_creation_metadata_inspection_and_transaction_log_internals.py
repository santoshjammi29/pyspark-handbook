"""
Module 05 / 033: Delta Table Creation, Metadata Inspection & Transaction Log Internals

Scenario & Scale:
    A financial ledger platform processes 200M immutable accounting postings/day
    on object storage. Standard Parquet writes leave orphaned partial files when
    spot executors terminate mid-task, breaking downstream regulatory audits.

Goal:
    Create a Delta Lake table with an explicit schema, inspect `_delta_log/`
    JSON commit entries and Parquet checkpoints, and query `DESCRIBE DETAIL` /
    `DESCRIBE HISTORY` without relying on legacy JVM context APIs or hardcoded paths.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark() -> SparkSession:
    """Create a Spark 4.0+ session configured with Delta Lake extensions."""
    return (
        SparkSession.builder.appName("033_Delta_Transaction_Log_Internals")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# (2) Anti-Pattern: Writing raw Parquet without an ACID transaction log
# ---------------------------------------------------------------------------
def anti_pattern_raw_parquet_overwrite(df: DataFrame, target_dir: str) -> None:
    # ❌ Bad Practice: Overwriting raw Parquet directories directly on S3/ADLS without
    # an ACID transaction log or explicit schema validation.
    # Otherwise: A mid-job driver/spot failure leaves half-deleted or half-written
    # Parquet part files (`_temporary` artifacts), causing concurrent BI readers to
    # observe torn snapshots or FileNotFoundException errors with zero rollback audit.
    df.write.format("parquet").mode("overwrite").save(target_dir)


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Explicit-Schema Delta + Log/Metadata Audit
# ---------------------------------------------------------------------------
def good_practice_delta_log_inspection(spark: SparkSession, delta_dir: Path) -> None:
    # ✅ Good Practice: Persist with explicit StructType/DECIMAL types to Delta Lake so
    # every commit is serialized atomically into `_delta_log/00000000000000000000.json`.
    schema = T.StructType(
        [
            T.StructField("txn_id", T.StringType(), nullable=False),
            T.StructField("account_id", T.StringType(), nullable=False),
            T.StructField("amount", T.DecimalType(14, 2), nullable=False),
            T.StructField("txn_type", T.StringType(), nullable=False),
            T.StructField("created_at", T.TimestampType(), nullable=False),
        ]
    )
    rows = [
        ("TX-001", "ACC-101", "1500.00", "DEPOSIT", "2026-01-10 10:00:00"),
        ("TX-002", "ACC-102", "450.00", "WITHDRAWAL", "2026-01-10 10:05:00"),
        ("TX-003", "ACC-101", "200.00", "TRANSFER", "2026-01-10 10:15:00"),
    ]
    raw_df = spark.createDataFrame(
        rows, "txn_id STRING, account_id STRING, amount STRING, txn_type STRING, created_at STRING"
    )
    ledger_df = raw_df.select(
        F.col("txn_id"),
        F.col("account_id"),
        F.try_cast(F.col("amount"), T.DecimalType(14, 2)).alias("amount"),
        F.col("txn_type"),
        F.try_cast(F.col("created_at"), T.TimestampType()).alias("created_at"),
    )
    assert ledger_df.schema == schema

    target_uri = str(delta_dir)
    ledger_df.write.format("delta").mode("overwrite").save(target_uri)

    log_dir = delta_dir / "_delta_log"
    commit_files = sorted(log_dir.glob("*.json"))
    for commit in commit_files:
        preview_lines = commit.read_text(encoding="utf-8").splitlines()[:2]
        print(f"Commit {commit.name}: {preview_lines}")

    spark.sql(f"DESCRIBE DETAIL delta.`{target_uri}`").select(
        "format", "id", "numFiles", "sizeInBytes"
    ).show(truncate=False)
    spark.sql(f"DESCRIBE HISTORY delta.`{target_uri}`").select(
        "version", "timestamp", "operation", "operationParameters"
    ).show(truncate=False)


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - Running `spark.read.format("delta").load(target_uri).explain("formatted")` shows
#   `FileScan parquet` over only the active files resolved from `_delta_log`, plus
#   `PushedFilters` backed by per-file min/max JSON stats stored inside `add` actions.
# - In the Spark UI SQL tab, check `number of files read` vs `number of files pruned`
#   and `metadata time` to spot log replay bottlenecks when tables lack checkpoints.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Mutual Exclusion on Object Storage: Delta guarantees serializable ACID commits on
#    S3/ADLS/GCS via conditional put-if-absent primitives or coordinated lock services
#    (e.g., DynamoDB / Unity Catalog commit coordinator) so two writers never overwrite
#    `00000...01.json`.
# 2. Checkpoints Bound State Reconstruction: Every N commits (default 10 in Delta 2.x/3.x,
#    configurable via `delta.checkpointInterval`), Delta writes a Parquet checkpoint
#    (`.checkpoint.parquet` or V2 sidecar checkpoints) so readers skip replaying old JSONs.
# 3. Never Bypass `_delta_log`: Writing raw Parquet directly into a Delta table directory
#    leaves those files invisible to Delta readers until `FSCK` or table rewrite.
# 4. Log Retention vs File Retention: `delta.logRetentionDuration` (default 30 days)
#    is independent from `delta.deletedFileRetentionDuration` (default 7 days).


if __name__ == "__main__":
    spark = build_spark()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            good_practice_delta_log_inspection(spark, Path(tmp_dir) / "delta_ledger")
    finally:
        spark.stop()
