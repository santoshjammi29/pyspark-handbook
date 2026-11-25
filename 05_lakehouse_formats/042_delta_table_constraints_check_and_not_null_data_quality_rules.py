"""
Module 05 / 042: Delta Table Constraints: CHECK & NOT NULL Data Quality Rules

Scenario & Scale:
    A core banking platform enforces strict compliance invariants on 500M
    account balance rows: `acc_id` must never be NULL, `email` must contain `@`,
    and `balance` must never breach -$500.00 overdraft limits. Post-write batch
    checks leave corrupt records exposed to downstream settlement consumers.

Goal:
    Enforce storage-engine `NOT NULL` and `CHECK` constraints via `ALTER TABLE`
    DDL so violating writes abort atomically before committing to `_delta_log`,
    while quarantine pipelines isolate bad upstream rows safely.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    """Create an ANSI-enabled Spark 4.0+ session with Delta Lake."""
    return (
        SparkSession.builder.appName("042_Delta_Table_Constraints")
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
# (2) Anti-Pattern: Post-write validation leaving toxic rows temporarily live
# ---------------------------------------------------------------------------
def anti_pattern_post_write_dq(df: DataFrame, delta_uri: str) -> None:
    # ❌ Bad Practice: Appending unvalidated data directly into a Gold table and
    # running an audit `SELECT COUNT(*) WHERE balance < -500` afterwards to delete them.
    # Otherwise: Between the append commit and cleanup delete, downstream BI reports
    # and payment APIs read illegal overdraft records, and a failed cleanup script leaves
    # toxic rows permanently persisted in production.
    df.write.format("delta").mode("append").save(delta_uri)


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Write-Time Delta NOT NULL & CHECK Constraints
# ---------------------------------------------------------------------------
def good_practice_delta_constraints(spark: SparkSession, delta_dir: Path) -> None:
    # ✅ Good Practice: Define `NOT NULL` and `CHECK` constraints in Delta table
    # metadata so Spark evaluates invariants during write execution and rolls back
    # the entire transaction if even a single row fails.
    target_uri = str(delta_dir)
    table_name = "delta_accounts_042"

    init_df = spark.createDataFrame(
        [("ACC-1", "user1@bank.com", "250.00")], "acc_id STRING, email STRING, balance STRING"
    ).withColumn("balance", F.try_cast(F.col("balance"), "DECIMAL(12,2)"))
    init_df.write.format("delta").mode("overwrite").save(target_uri)

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{target_uri}'")
    try:
        spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN acc_id SET NOT NULL")
        spark.sql(
            f"ALTER TABLE {table_name} ADD CONSTRAINT valid_email CHECK (email LIKE '%@%')"
        )
        spark.sql(
            f"ALTER TABLE {table_name} ADD CONSTRAINT min_overdraft CHECK (balance >= -500.00)"
        )

        # Violating batch is rejected atomically before commit.
        bad_df = spark.createDataFrame(
            [("ACC-2", "user2@bank.com", "-1000.00")],
            "acc_id STRING, email STRING, balance STRING",
        ).withColumn("balance", F.try_cast(F.col("balance"), "DECIMAL(12,2)"))
        try:
            bad_df.write.format("delta").mode("append").save(target_uri)
        except Exception as exc:
            print(f"Constraint blocked invalid batch: {str(exc).splitlines()[0]}")

        valid_df = spark.createDataFrame(
            [("ACC-2", "user2@bank.com", "500.00"), ("ACC-3", "user3@bank.com", "-150.00")],
            "acc_id STRING, email STRING, balance STRING",
        ).withColumn("balance", F.try_cast(F.col("balance"), "DECIMAL(12,2)"))
        valid_df.write.format("delta").mode("append").save(target_uri)

        spark.read.format("delta").load(target_uri).orderBy("acc_id").show(truncate=False)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - When writing to a Delta table with `CHECK` constraints, Spark injects an inline
#   `DeltaInvariantCheckerExec` node directly above the Parquet writer in the physical
#   plan (`explain("formatted")`).
# - In the Spark UI SQL/Stages tab, invariant checks run in-memory inside the same
#   write tasks with zero extra shuffles; if any task fails an invariant, the stage
#   aborts with `DeltaInvariantViolationException` before any `_delta_log` commit occurs.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Existing Data Validation on `ADD CONSTRAINT`: Adding a `CHECK` constraint scans
#    existing table rows first—if any historical row violates the rule, `ALTER TABLE`
#    fails immediately until existing data is cleaned.
# 2. Deterministic SQL Expressions Only: `CHECK` constraints cannot reference subqueries,
#    non-deterministic functions (`current_timestamp()`, `rand()`), or Python UDFs.
# 3. `CHECK` Constraints vs DLT Expectations: Delta `CHECK` constraints only support
#    fail-fast transaction abort (hard stop), whereas DLT / quarantine pipelines allow
#    routing bad rows (`WARN` / `DROP` / DLQ) without halting healthy rows in the batch.


if __name__ == "__main__":
    spark = build_spark()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            good_practice_delta_constraints(spark, Path(tmp_dir) / "delta_constraints")
    finally:
        spark.stop()
