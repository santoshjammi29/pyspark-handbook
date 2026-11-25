"""
Module 05 / 043: In-Place Parquet to Delta Lake Migration (CONVERT TO DELTA)

Scenario & Scale:
    An enterprise warehouse holds 100 TB of historical Hive-partitioned Parquet
    directories across S3/ADLS. Rewriting all 100 TB into Delta Lake via
    `spark.read.parquet(...).write.format("delta")` would take days and burn
    tens of thousands of dollars in cluster compute and cross-AZ egress.

Goal:
    Execute zero-copy, metadata-only in-place migration using `CONVERT TO DELTA`
    with explicit `PARTITIONED BY` specification, then verify immediate ACID
    `UPDATE` / Time Travel capabilities on existing Parquet files.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    """Create an ANSI-enabled Spark 4.0+ session with Delta Lake."""
    return (
        SparkSession.builder.appName("043_InPlace_Convert_To_Delta")
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
# (2) Anti-Pattern: Full-table read + rewrite to migrate legacy Parquet
# ---------------------------------------------------------------------------
def anti_pattern_full_parquet_rewrite(
    spark: SparkSession, src_parquet_uri: str, dst_delta_uri: str
) -> None:
    # ❌ Bad Practice: Reading 100 TB of existing Parquet files and rewriting every
    # byte into a brand-new Delta directory.
    # Otherwise: You pay 2x storage during migration, burn massive executor compute
    # decoding/re-encoding identical Parquet row groups, and break downstream paths
    # that still point to the original object-store prefix.
    spark.read.parquet(src_parquet_uri).write.format("delta").mode("overwrite").save(
        dst_delta_uri
    )


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Zero-Copy Metadata-Only CONVERT TO DELTA
# ---------------------------------------------------------------------------
def good_practice_convert_to_delta(spark: SparkSession, base_dir: Path) -> None:
    # ✅ Good Practice: Run `CONVERT TO DELTA parquet.`...` PARTITIONED BY (...)` to
    # read existing Parquet footers and synthesize `_delta_log/00000...00.json` in-place
    # without copying or rewriting a single data file.
    target_uri = str(base_dir)
    records = [
        ("EMP-01", "Alice", "90000.00", "HR"),
        ("EMP-02", "Bob", "110000.00", "Engineering"),
        ("EMP-03", "Charlie", "95000.00", "HR"),
        ("EMP-04", "Diana", "140000.00", "Engineering"),
    ]
    df = spark.createDataFrame(
        records, "emp_id STRING, name STRING, salary STRING, department STRING"
    ).withColumn("salary", F.try_cast(F.col("salary"), "DECIMAL(12,2)"))

    df.write.format("parquet").partitionBy("department").mode("overwrite").save(target_uri)

    # Zero-copy in-place conversion (NO STATISTICS speeds up 100TB+ footer scans).
    spark.sql(
        f"CONVERT TO DELTA parquet.`{target_uri}` PARTITIONED BY (department STRING)"
    )
    assert (base_dir / "_delta_log").exists(), "_delta_log must exist after conversion"

    # Full ACID updates and Time Travel now work immediately on the converted table.
    dt = DeltaTable.forPath(spark, target_uri)
    dt.update(condition=F.col("emp_id") == F.lit("EMP-01"), set={"salary": F.lit(98000.00)})
    dt.toDF().orderBy("emp_id").show(truncate=False)


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - During `CONVERT TO DELTA`, Spark launches a parallel footer-metadata listing job
#   (reading Parquet footers only, not row data) to populate file sizes and min/max
#   data-skipping statistics inside commit `00000000000000000000.json`.
# - In the Spark UI Jobs tab, verify zero shuffle/data-write stages occur during
#   conversion; appending `NO STATISTICS` skips footer reads altogether on 100 TB+
#   tables so conversion completes in seconds, followed later by `ANALYZE TABLE`.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Freeze Concurrent Parquet Writers During Conversion: If an external non-Delta job
#    writes raw Parquet files into the directory after `CONVERT TO DELTA`, Delta's
#    `_delta_log` ignores those files (silent data loss until table repair/rewrite).
# 2. `PARTITIONED BY` Is Mandatory for Partitioned Parquet: Omitting `PARTITIONED BY`
#    on Hive-partitioned folders causes conversion to fail or miss partition columns.
# 3. Parquet-Only Support: `CONVERT TO DELTA` works only on Parquet (and Iceberg
#    metadata via `CONVERT TO DELTA iceberg.`...``); CSV/JSON/Avro always require a
#    full read-and-write ingestion pass.
# 4. Schema Incompatibilities: Legacy Parquet files with inconsistent column types
#    across partitions (e.g., `INT` in 2022, `BIGINT` in 2024) must be reconciled or
#    rewritten before converting to a unified Delta schema.


if __name__ == "__main__":
    spark = build_spark()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            good_practice_convert_to_delta(spark, Path(tmp_dir) / "parquet_legacy")
    finally:
        spark.stop()
