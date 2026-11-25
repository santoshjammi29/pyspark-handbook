"""
Module 05 / 039: Z-Order Multi-Dimensional Indexing vs Liquid Clustering (CLUSTER BY)

Scenario & Scale:
    An ad-tech analytics warehouse executes 10,000+ interactive queries/day over
    a 60 TB telemetry table filtering arbitrarily on `device_id`, `campaign_id`,
    and `event_date`. Hive partitioning on all three columns creates millions of
    tiny partition directories, while single-column sorting leaves other filters
    scanning 100% of files.

Goal:
    Eliminate over-partitioning by applying Delta `OPTIMIZE ... ZORDER BY`
    (space-filling curve co-location) and modern Liquid Clustering (`CLUSTER BY`),
    enabling incremental Hilbert clustering without partition lock-in.
"""

from __future__ import annotations

from pathlib import Path
import random
import tempfile

from pyspark.sql import DataFrame, SparkSession


def build_spark() -> SparkSession:
    """Create an ANSI-enabled Spark 4.0+ session with Delta Lake."""
    return (
        SparkSession.builder.appName("039_ZOrder_vs_Liquid_Clustering")
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
# (2) Anti-Pattern: Hive partitioning on high-cardinality filter columns
# ---------------------------------------------------------------------------
def anti_pattern_over_partitioning(df: DataFrame, target_uri: str) -> None:
    # ❌ Bad Practice: Partitioning physically by high-cardinality columns such as
    # `device_id` and `campaign_id` alongside `event_date`.
    # Otherwise: 100K devices x 500 campaigns x 365 days explodes into billions of
    # tiny KB-sized Parquet files and directory prefixes, saturating object-store
    # metadata APIs and making `OPTIMIZE` practically impossible across partitions.
    (
        df.write.format("delta")
        .partitionBy("event_date", "campaign_id", "device_id")
        .mode("overwrite")
        .save(target_uri)
    )


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Z-Order Indexing & Liquid Clustering DDL
# ---------------------------------------------------------------------------
def good_practice_clustering(spark: SparkSession, delta_dir: Path) -> None:
    # ✅ Good Practice: Keep tables unpartitioned (or coarse-grained) and co-locate
    # multi-dimensional query predicates using Liquid Clustering (`CLUSTER BY`) for
    # incremental Hilbert clustering, or `OPTIMIZE ... ZORDER BY` on legacy tables.
    target_uri = str(delta_dir)
    rng = random.Random(42)
    rows = [
        (
            f"DEV-{rng.randint(1, 100):03d}",
            f"CAMP-{rng.randint(1, 20):02d}",
            f"2026-01-{rng.randint(1, 10):02d}",
            round(rng.uniform(10.0, 500.0), 2),
        )
        for _ in range(1000)
    ]
    df = spark.createDataFrame(
        rows, "device_id STRING, campaign_id STRING, event_date STRING, metric_value DOUBLE"
    )
    df.write.format("delta").mode("overwrite").save(target_uri)

    # Path-based Z-ORDER BY for existing non-clustered tables:
    spark.sql(f"OPTIMIZE delta.`{target_uri}` ZORDER BY (device_id, campaign_id)").select(
        "path", "metrics.numFilesAdded", "metrics.numFilesRemoved"
    ).show(truncate=False)

    # Modern Delta 3.x/4.0 Liquid Clustering (incremental, partition-free):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS ad_telemetry_liquid (
            device_id STRING,
            campaign_id STRING,
            event_date DATE,
            metric_value DOUBLE
        )
        USING DELTA
        CLUSTER BY (device_id, campaign_id)
        LOCATION '{target_uri}_liquid'
        """
    )
    # Subsequent `OPTIMIZE ad_telemetry_liquid` clusters only newly ingested data
    # incrementally via Hilbert curves, and `ALTER TABLE ... CLUSTER BY (...)`
    # changes clustering keys in metadata without rewriting historical files.
    spark.sql("DROP TABLE IF EXISTS ad_telemetry_liquid")


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - In `explain("formatted")`, filtering on `device_id = 'DEV-042'` or
#   `campaign_id = 'CAMP-05'` pushes `PushedFilters` down to Delta's file-level
#   min/max column statistics (`_delta_log` stats JSON).
# - In the Spark UI SQL tab, compare `number of files pruned` vs `number of files read`
#   and `scan time`: effective Z-Order / Liquid Clustering prunes 85–99% of Parquet
#   files without creating a single Hive partition directory.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Z-Order vs Liquid Clustering (`CLUSTER BY`): `ZORDER BY` is NOT incremental—re-running
#    it re-touches all files in the partition; Liquid Clustering uses Hilbert curves and
#    tracks clustering state in `_delta_log` so `OPTIMIZE` only re-clusters unclustered data.
# 2. Keep Clustering Keys to 2–4 Columns: Adding 8+ columns dilutes space-filling curve
#    locality so each column gets wide, overlapping min/max ranges and zero file skipping.
# 3. Stats Collection Columns: Delta only writes min/max stats for the first 32 columns
#    by default (`delta.dataSkippingNumIndexedCols`); place clustering/Z-ordered columns
#    within the first 32 columns (or widen the property).
# 4. `CLUSTER BY` Cannot Combine with `PARTITIONED BY`: Liquid Clustering replaces Hive
#    partitioning entirely—never mix both on the same table.


if __name__ == "__main__":
    spark = build_spark()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            good_practice_clustering(spark, Path(tmp_dir) / "delta_zorder")
    finally:
        spark.stop()
