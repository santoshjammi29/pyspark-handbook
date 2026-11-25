"""
Module 05 / 037: VACUUM Command, Retention Hours & Safe Tombstone Management

Scenario & Scale:
    A high-frequency streaming Delta table accumulates 50,000 tombstoned Parquet
    files per week across 120 TB of historical data, inflating S3/ADLS storage
    bills by 4x. Storage engineers must reclaim unreferenced files safely
    without breaking active concurrent BI queries or streaming jobs.

Goal:
    Audit eligible files with `VACUUM ... DRY RUN`, enforce safe retention
    windows (>=168 hours in production), and verify physical storage cleanup
    without corrupting active snapshots.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from pyspark.sql import SparkSession


def build_spark() -> SparkSession:
    """Create an ANSI-enabled Spark 4.0+ session with Delta Lake."""
    return (
        SparkSession.builder.appName("037_Delta_VACUUM_Safe_Tombstones")
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
# (2) Anti-Pattern: Disabling retention safety check with RETAIN 0 HOURS in prod
# ---------------------------------------------------------------------------
def anti_pattern_zero_hour_vacuum(spark: SparkSession, delta_uri: str) -> None:
    # ❌ Bad Practice: Turning off `retentionDurationCheck` and running `RETAIN 0 HOURS`
    # on a live production table with active readers or concurrent MERGE/streaming jobs.
    # Otherwise: `VACUUM` physically deletes files referenced by snapshots started
    # minutes earlier, crashing concurrent readers with `FileNotFoundException` and
    # permanently destroying Time Travel / `RESTORE` recovery points.
    spark.conf.set("spark.databricks.delta.vacuum.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM delta.`{delta_uri}` RETAIN 0 HOURS")


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: DRY RUN Audit + Retention-Guarded VACUUM
# ---------------------------------------------------------------------------
def good_practice_safe_vacuum(spark: SparkSession, delta_dir: Path) -> None:
    # ✅ Good Practice: Always execute `VACUUM ... DRY RUN` first to audit candidate
    # paths, keep `retentionDurationCheck.enabled=true` (>=168 hours) in production,
    # and run `VACUUM` using parallel deletion (`parallelDelete.enabled=true`).
    target_uri = str(delta_dir)
    for batch_idx in range(1, 4):
        rows = [(f"ROW-{batch_idx}-{i}", f"batch_{batch_idx}") for i in range(5)]
        spark.createDataFrame(rows, "id STRING, batch STRING").write.format("delta").mode(
            "overwrite"
        ).save(target_uri)

    # Preview unreferenced files safely; 168h default protects 7 days of Time Travel.
    dry_run_default = spark.sql(f"VACUUM delta.`{target_uri}` DRY RUN")
    print(f"Files eligible under default 168h retention: {dry_run_default.count()}")

    # Local ephemeral test cleanup only (demonstrating DRY RUN -> VACUUM flow):
    spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
    spark.conf.set("spark.databricks.delta.vacuum.retentionDurationCheck.enabled", "false")
    try:
        candidates = spark.sql(f"VACUUM delta.`{target_uri}` RETAIN 0 HOURS DRY RUN")
        candidates.show(truncate=False)
        spark.sql(f"VACUUM delta.`{target_uri}` RETAIN 0 HOURS")
    finally:
        spark.conf.set("spark.databricks.delta.vacuum.retentionDurationCheck.enabled", "true")

    active_files = list(delta_dir.glob("*.parquet"))
    print(f"Active Parquet files remaining: {len(active_files)}")
    spark.read.format("delta").load(target_uri).orderBy("id").show(truncate=False)


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - `VACUUM` executes a distributed Spark listing job (`parallelDelete`) comparing
#   physical storage objects against active `add` actions and non-expired `remove`
#   tombstones in `_delta_log`.
# - In the Spark UI Jobs tab, monitor the directory-listing stage duration; on 100K+
#   partitioned tables, ensure `spark.databricks.delta.vacuum.parallelDelete.enabled`
#   is `true` so object-store `DELETE` API calls scale across executors instead of
#   bottlenecking on the driver.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `VACUUM` Does NOT Delete `_delta_log/*.json`: Transaction logs are managed
#    separately by `delta.logRetentionDuration` (default 30 days) during checkpoints.
# 2. Retention Floor Alignment: Set `delta.deletedFileRetentionDuration` >= longest
#    running batch/streaming job SLA + required Time Travel window (e.g., `interval 7 days`).
# 3. `VACUUM` Requires No Manual `OPTIMIZE`: `VACUUM` only deletes files already marked
#    `remove` in `_delta_log`; run `OPTIMIZE` first to compact active small files.
# 4. Shallow Clones Warning: Vacuuming a parent table deletes files referenced by any
#    shallow clone pointing to it—use `VACUUM` cautiously when shallow clones exist.


if __name__ == "__main__":
    spark = build_spark()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            good_practice_safe_vacuum(spark, Path(tmp_dir) / "delta_vacuum")
    finally:
        spark.stop()
