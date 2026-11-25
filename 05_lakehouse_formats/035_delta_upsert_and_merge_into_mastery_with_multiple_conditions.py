"""
Module 05 / 035: Delta Upsert & MERGE INTO Mastery with Multiple Conditions

Scenario & Scale:
    A retail inventory platform reconciles daily CDC batches (15M SKU updates)
    from 500 warehouses against a 2B-row partitioned/clustered inventory Delta
    table: updating quantities, deleting discontinued SKUs (-999), and inserting
    new SKUs atomically.

Goal:
    Implement deduplicated, partition-pruned `MERGE INTO` with `whenMatchedDelete`,
    `whenMatchedUpdate`, and `whenNotMatchedInsert` clauses that avoid shuffle
    explosions and `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW` failures.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    """Create an ANSI-enabled Spark 4.0+ session with Delta Lake."""
    return (
        SparkSession.builder.appName("035_Delta_MERGE_INTO_Mastery")
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
# (2) Anti-Pattern: Merging raw non-deduplicated CDC without partition pruning
# ---------------------------------------------------------------------------
def anti_pattern_unpruned_duplicate_merge(
    target: DeltaTable, raw_updates: DataFrame
) -> None:
    # ❌ Bad Practice: Feeding raw CDC feeds containing duplicate SKU keys directly into
    # `MERGE` without deduplicating or adding partition/clustering predicates.
    # Otherwise: Multiple source rows matching the same target row abort the job with
    # `DeltaUnsupportedOperationException`, and omitting partition predicates triggers a
    # full-table outer join scan across terabytes of cold historical partitions.
    (
        target.alias("t")
        .merge(raw_updates.alias("s"), "t.sku = s.sku")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Deduplicated + Pruned Multi-Clause MERGE
# ---------------------------------------------------------------------------
def good_practice_pruned_merge(spark: SparkSession, delta_dir: Path) -> None:
    # ✅ Good Practice: Deduplicate source CDC by primary key + latest event timestamp,
    # narrow the join predicate with target partition bounds when partitioned, and order
    # `whenMatchedDelete` before `whenMatchedUpdate` cleanly.
    target_uri = str(delta_dir)
    schema = "sku STRING, region STRING, product_name STRING, quantity INT, unit_price DECIMAL(10,2), updated_at TIMESTAMP"

    initial = [
        ("SKU-101", "US", "Wireless Mouse", 50, "24.99", "2026-01-01 00:00:00"),
        ("SKU-102", "US", "Mechanical Keyboard", 20, "89.99", "2026-01-01 00:00:00"),
        ("SKU-104", "EU", "Obsolete Cable", 5, "4.99", "2026-01-01 00:00:00"),
    ]
    init_df = spark.createDataFrame(
        initial,
        "sku STRING, region STRING, product_name STRING, quantity INT, unit_price STRING, updated_at STRING",
    ).select(
        "sku",
        "region",
        "product_name",
        "quantity",
        F.try_cast(F.col("unit_price"), "DECIMAL(10,2)").alias("unit_price"),
        F.try_cast(F.col("updated_at"), "TIMESTAMP").alias("updated_at"),
    )
    init_df.write.format("delta").partitionBy("region").mode("overwrite").save(target_uri)

    # Raw batch contains a duplicate update for SKU-101; keep latest only.
    incoming = [
        ("SKU-101", "US", "Wireless Mouse", 60, "24.99", "2026-01-20 08:00:00"),
        ("SKU-101", "US", "Wireless Mouse", 80, "24.99", "2026-01-20 09:00:00"),
        ("SKU-104", "EU", "Obsolete Cable", -999, "0.00", "2026-01-20 09:00:00"),
        ("SKU-105", "US", "Noise Cancelling Headset", 40, "199.99", "2026-01-20 09:00:00"),
    ]
    raw_updates = spark.createDataFrame(
        incoming,
        "sku STRING, region STRING, product_name STRING, quantity INT, unit_price STRING, updated_at STRING",
    ).select(
        "sku",
        "region",
        "product_name",
        "quantity",
        F.try_cast(F.col("unit_price"), "DECIMAL(10,2)").alias("unit_price"),
        F.try_cast(F.col("updated_at"), "TIMESTAMP").alias("updated_at"),
    )

    dedup_win = Window.partitionBy("sku").orderBy(F.col("updated_at").desc())
    dedup_updates = (
        raw_updates.withColumn("_rn", F.row_number().over(dedup_win))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    assert dedup_updates.schema == spark.createDataFrame([], schema).schema

    active_regions = [r.region for r in dedup_updates.select("region").distinct().collect()]
    region_literals = ", ".join(f"'{r}'" for r in active_regions)

    target_table = DeltaTable.forPath(spark, target_uri)
    (
        target_table.alias("target")
        .merge(
            source=dedup_updates.alias("source"),
            condition=f"target.sku = source.sku AND target.region IN ({region_literals})",
        )
        .whenMatchedDelete(condition="source.quantity = -999")
        .whenMatchedUpdate(
            condition="source.updated_at >= target.updated_at",
            set={
                "quantity": F.col("source.quantity"),
                "unit_price": F.col("source.unit_price"),
                "updated_at": F.col("source.updated_at"),
            },
        )
        .whenNotMatchedInsert(
            values={
                "sku": F.col("source.sku"),
                "region": F.col("source.region"),
                "product_name": F.col("source.product_name"),
                "quantity": F.col("source.quantity"),
                "unit_price": F.col("source.unit_price"),
                "updated_at": F.col("source.updated_at"),
            }
        )
        .execute()
    )

    target_table.toDF().orderBy("sku").show(truncate=False)


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - Delta `MERGE` executes in two phases under the hood: (1) an inner/left-anti/outer
#   scan identifying touched Parquet files (applying `target.region IN (...)` partition
#   pruning + min/max data skipping), and (2) an outer join rewriting only matched files
#   (or writing Deletion Vectors when DVs are enabled).
# - Inspect `DESCRIBE HISTORY` `operationMetrics`: watch `numTargetFilesRemoved` vs
#   `numTargetFilesAdded` and `scanTimeMs` vs `rewriteTimeMs` to tune partition pruning
#   and Low-Shuffle Merge.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Clause Evaluation Order Matters: Delta evaluates `whenMatched*` clauses in written
#    order and executes only the FIRST matching clause per row—always put
#    `whenMatchedDelete` before catch-all `whenMatchedUpdate`.
# 2. Idempotency Guard (`source.updated_at >= target.updated_at`): Prevents replayed
#    late CDC batches from overwriting newer warehouse state on rerun.
# 3. `whenNotMatchedBySourceDelete`: Essential for full-snapshot syncs where missing
#    source keys must be tombstoned in the target table.
# 4. Dynamic Partition Pruning in Condition: Literal partition bounds (`target.region IN
#    (...)`) prune untouched partitions during Phase 1 file matching; dynamic joins on
#    partition columns without explicit bounds often fail to skip cold files.


if __name__ == "__main__":
    spark = build_spark()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            good_practice_pruned_merge(spark, Path(tmp_dir) / "delta_inventory")
    finally:
        spark.stop()
