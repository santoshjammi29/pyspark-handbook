"""Parquet & ORC Columnar Ingestion: Predicate Pushdown & Column Pruning.

Scenario & Scale:
    A petabyte-scale connected-vehicle lakehouse processes 500M daily telemetry
    events across 80-column wide tables. Analysts filter by `vehicle_id` and
    project 2-3 columns; inefficient readers trigger full-terabyte I/O scans.
"""

import tempfile

from pyspark.sql import DataFrame, SparkSession, functions as F


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Wrapping filter columns in functions or reading unpruned columns.
# Otherwise: Disables Parquet/ORC row-group min/max predicate pushdown and forces
# full columnar page decompression across every row group in storage.
def transform_vehicle_telemetry_bad(df: DataFrame, target_vehicle_id: str) -> DataFrame:
    return df.filter(F.upper(F.col("vehicle_id")) == target_vehicle_id.upper())


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Keep predicates sargable (bare column comparisons), project
# required columns immediately, and write Parquet with ZSTD/Snappy + sorting.
def transform_filter_vehicle_telemetry(
    df: DataFrame, target_vehicle_id: str
) -> DataFrame:
    """Filter with pushdown-compatible predicate and prune unused columns."""
    return df.filter(F.col("vehicle_id") == F.lit(target_vehicle_id)).select(
        "reading_id",
        "vehicle_id",
        "speed_kmh",
        "event_ts",
    )


def write_columnar_telemetry(df: DataFrame, target_uri: str) -> None:
    """Write partitioned Parquet sorted within partitions for row-group stats."""
    (
        df.repartition("vehicle_id")
        .sortWithinPartitions("vehicle_id", "event_ts")
        .write.format("parquet")
        .option("compression", "zstd")
        .mode("overwrite")
        .save(target_uri)
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_out.explain("formatted")` on Parquet read displays:
#     `ReadSchema: struct<reading_id:bigint,vehicle_id:string,speed_kmh:double,event_ts:timestamp>`
#     `PushedFilters: [IsNotNull(vehicle_id), EqualTo(vehicle_id,V-042)]`
# - In Spark UI SQL tab, verify `number of output rows` << total row-group rows
#   and `scan time` drops proportionally via footer min/max & dictionary skipping.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Nested structs/maps disabled Parquet predicate pushdown in older engines;
#    enable `spark.sql.parquet.filterPushdown=true` and `recordLevelFilter.enabled=true`.
# 2. Dictionary encoding compresses repetitive strings (e.g., `firmware`, `status`)
#    to tiny integer IDs, but high-cardinality UUIDs fall back to plain encoding.
# 3. Sorting within partitions (`sortWithinPartitions`) narrows Parquet row-group
#    min/max ranges, dramatically increasing row-group skip rates on read.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("045_columnar_pushdown")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    base_uri = "abfss://lakehouse@storage.dfs.core.windows.net/telemetry/parquet/"
    df_src = spark.range(1, 1001).select(
        F.col("id").alias("reading_id"),
        F.concat(F.lit("V-"), F.lpad((F.col("id") % 50).cast("string"), 3, "0")).alias("vehicle_id"),
        F.round(F.rand(seed=42) * 120.0, 2).alias("speed_kmh"),
        F.current_timestamp().alias("event_ts"),
        F.lit("OK").alias("status"),
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        write_columnar_telemetry(df_src, tmp_dir)
        df_out = transform_filter_vehicle_telemetry(spark.read.parquet(tmp_dir), "V-042")
        df_out.explain("formatted")
        df_out.show(5, truncate=False)
