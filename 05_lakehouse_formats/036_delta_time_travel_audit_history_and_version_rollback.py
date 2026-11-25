"""
Module 05 / 036: Delta Time Travel, Audit History & Atomic Version Rollback

Scenario & Scale:
    An automated overnight ETL pricing job mistakenly applies a 90% discount
    across a 400M-row luxury retail catalog table. Operations must audit the
    exact commit, verify the pre-incident snapshot, and restore production in
    under 60 seconds without copying terabytes of Parquet files.

Goal:
    Audit `_delta_log` commits via `DESCRIBE HISTORY`, inspect historical
    snapshots with `versionAsOf` / `timestampAsOf`, and perform an atomic
    zero-copy rollback using `RESTORE TABLE ... TO VERSION AS OF`.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    """Create an ANSI-enabled Spark 4.0+ session with Delta Lake."""
    return (
        SparkSession.builder.appName("036_Delta_Time_Travel_And_Restore")
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
# (2) Anti-Pattern: Blindly overwriting with historical reads or manual file deletes
# ---------------------------------------------------------------------------
def anti_pattern_manual_copy_rollback(spark: SparkSession, delta_dir: str) -> None:
    # ❌ Bad Practice: Reading an older version and calling `.write.mode("overwrite")`
    # on a 50 TB table, or manually deleting recent `_delta_log/*.json` files on S3.
    # Otherwise: Full-table overwrite wastes hours of cluster compute and duplicates
    # storage, while deleting JSON commit files corrupts checkpoint parity and breaks
    # Delta's immutable audit trail.
    old_df = spark.read.format("delta").option("versionAsOf", 1).load(delta_dir)
    old_df.write.format("delta").mode("overwrite").save(delta_dir)


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: History Audit + Time Travel + RESTORE TABLE
# ---------------------------------------------------------------------------
def good_practice_time_travel_restore(spark: SparkSession, delta_dir: Path) -> None:
    # ✅ Good Practice: Audit `DESCRIBE HISTORY`, validate the pre-bug snapshot using
    # `versionAsOf`, and run `RESTORE TABLE`—a metadata-only atomic commit that restores
    # pointers to original Parquet files without rewriting data.
    target_uri = str(delta_dir)

    commits = [
        [("PROD-1", "100.00"), ("PROD-2", "500.00"), ("PROD-3", "1000.00")],  # v0
        [("PROD-1", "110.00"), ("PROD-2", "525.00"), ("PROD-3", "1050.00")],  # v1 (good)
        [("PROD-1", "11.00"), ("PROD-2", "52.50"), ("PROD-3", "105.00")],     # v2 (buggy)
    ]
    for batch in commits:
        df = spark.createDataFrame(batch, "id STRING, price STRING").select(
            "id", F.try_cast(F.col("price"), "DECIMAL(10,2)").alias("price")
        )
        df.write.format("delta").mode("overwrite").save(target_uri)

    spark.sql(f"DESCRIBE HISTORY delta.`{target_uri}`").select(
        "version", "timestamp", "operation", "operationParameters"
    ).show(truncate=False)

    # Verify v1 snapshot before committing rollback.
    pre_incident_df = spark.read.format("delta").option("versionAsOf", 1).load(target_uri)
    pre_incident_df.orderBy("id").show(truncate=False)

    spark.sql(f"RESTORE TABLE delta.`{target_uri}` TO VERSION AS OF 1")
    spark.read.format("delta").load(target_uri).orderBy("id").show(truncate=False)


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - `RESTORE TABLE` creates a new monotonically increasing commit (v3 here) containing
#   `remove` actions for v2 files and `add` actions re-referencing v1 Parquet files—
#   so `explain("formatted")` on reads after restore scans only the untouched v1 files.
# - In Spark UI SQL -> Delta log metrics, `RESTORE` completes in seconds with near-zero
#   executor shuffle/write I/O because only `_delta_log` metadata is rewritten.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `RESTORE` Preserves Full Auditability: Rolling back to v1 writes commit v3; v2
#    remains visible in `DESCRIBE HISTORY` for post-mortem compliance forensics.
# 2. `VACUUM` Limits Time Travel: If `VACUUM` already deleted unreferenced files from v1,
#    querying `versionAsOf 1` or running `RESTORE` fails with `FileNotFoundException`.
# 3. `timestampAsOf` Out-of-Range Pitfall: Supplying a timestamp before commit v0 or
#    after the latest commit raises an error unless `spark.databricks.delta.timeTravel.
#    resolveOnEmptyData.enabled` / exact commit bounds are respected.
# 4. Concurrent Writers During `RESTORE`: Like `OVERWRITE`, `RESTORE` conflicts with
#    concurrent streaming appends unless coordinated with maintenance windows.


if __name__ == "__main__":
    spark = build_spark()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            good_practice_time_travel_restore(spark, Path(tmp_dir) / "delta_pricing")
    finally:
        spark.stop()
