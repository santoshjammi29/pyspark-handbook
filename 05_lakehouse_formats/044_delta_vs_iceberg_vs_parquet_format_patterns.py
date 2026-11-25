"""
Module 05 / 044: Delta Lake vs Apache Iceberg (v2/v3) vs Raw Parquet Patterns

Scenario & Scale:
    A multi-engine lakehouse platform serves Spark 4.0 ETL, Trino/Flink readers,
    and Snowflake external tables across 300 TB of curated datasets. Architects
    must choose and standardize catalog writes, time travel, schema evolution,
    partitioning/clustering, and compaction across Delta Lake, Iceberg, and
    raw Parquet.

Goal:
    Contrast concrete PySpark 4.0+ / Spark SQL patterns across Delta Lake,
    Apache Iceberg (v2/v3), and raw Parquet across all 5 core lifecycle pillars
    so engineers select the right open table format without vendor lock-in traps.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_multi_format_spark(warehouse_dir: str) -> SparkSession:
    """Create a Spark 4.0+ session configured with Delta Lake & Iceberg catalogs."""
    return (
        SparkSession.builder.appName("044_Delta_vs_Iceberg_vs_Parquet")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        # Delta Lake extension + spark_catalog
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Apache Iceberg catalog example (requires iceberg-spark-runtime JAR at runtime)
        .config("spark.sql.catalog.iceberg_cat", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_cat.type", "hadoop")
        .config("spark.sql.catalog.iceberg_cat.warehouse", warehouse_dir)
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# (2) Anti-Pattern: Treating Raw Parquet as a Mutable Lakehouse Table
# ---------------------------------------------------------------------------
def anti_pattern_raw_parquet_lakehouse(df: DataFrame, parquet_uri: str) -> None:
    # ❌ Bad Practice: Using raw Parquet directories for multi-writer pipelines,
    # relying on `partitionOverwriteMode=dynamic` and `mergeSchema=true` on reads.
    # Otherwise: Raw Parquet has zero ACID snapshots, zero time travel, zero concurrent
    # writer isolation, and `spark.read.option("mergeSchema", "true").parquet(...)`
    # forces the driver to list and merge footers across thousands of files per query.
    df.write.format("parquet").partitionBy("event_date").mode("overwrite").save(parquet_uri)


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Canonical Delta vs Iceberg vs Parquet Matrix
# ---------------------------------------------------------------------------
def delta_lifecycle_patterns(spark: SparkSession, delta_dir: Path) -> None:
    """1) Delta Lake 3.x/4.0: Catalog Writes, Time Travel, Liquid Clustering, Compaction."""
    uri = str(delta_dir)
    rows = [
        ("ord_1", "US", "120.50", "2026-01-10"),
        ("ord_2", "EU", "85.00", "2026-01-11"),
    ]
    df = spark.createDataFrame(
        rows, "order_id STRING, region STRING, amount STRING, event_date STRING"
    ).select(
        "order_id",
        "region",
        F.try_cast(F.col("amount"), "DECIMAL(12,2)").alias("amount"),
        F.try_cast(F.col("event_date"), "DATE").alias("event_date"),
    )

    # Catalog Write + Liquid Clustering + Deletion Vectors
    df.write.format("delta").option("delta.enableDeletionVectors", "true").mode("overwrite").save(uri)
    # Time Travel:
    _ = spark.read.format("delta").option("versionAsOf", 0).load(uri)
    # Schema Evolution (additive):
    # df_new.write.format("delta").option("mergeSchema", "true").mode("append").save(uri)
    # Compaction & Retention:
    spark.sql(f"OPTIMIZE delta.`{uri}`")
    spark.sql(f"VACUUM delta.`{uri}` DRY RUN")


def iceberg_ddl_and_sql_patterns() -> dict[str, str]:
    """2) Apache Iceberg v2/v3: Equivalent Catalog, Time Travel, Evolution & Maintenance."""
    return {
        "catalog_create_hidden_partitioning": """
            CREATE TABLE iceberg_cat.db.orders (
                order_id STRING NOT NULL,
                region STRING NOT NULL,
                amount DECIMAL(12, 2) NOT NULL,
                event_ts TIMESTAMP NOT NULL
            )
            USING iceberg
            PARTITIONED BY (days(event_ts), bucket(16, order_id))
            TBLPROPERTIES (
                'format-version' = '2',
                'write.delete.mode' = 'merge-on-read',
                'write.update.mode' = 'merge-on-read',
                'write.merge.mode' = 'merge-on-read'
            )
        """,
        "time_travel": """
            -- Query by snapshot ID or timestamp (no extra option needed in Spark 3.5+/4.0):
            SELECT * FROM iceberg_cat.db.orders VERSION AS OF 1234567890;
            SELECT * FROM iceberg_cat.db.orders TIMESTAMP AS OF '2026-01-11 00:00:00';
        """,
        "schema_evolution": """
            -- Field-ID tracked evolution: safe renames, drops, and reordering with zero file rewrites
            ALTER TABLE iceberg_cat.db.orders ADD COLUMN currency STRING;
            ALTER TABLE iceberg_cat.db.orders RENAME COLUMN region TO market_region;
        """,
        "compaction_and_expiry": """
            CALL iceberg_cat.system.rewrite_data_files(
                table => 'db.orders',
                strategy => 'sort',
                sort_order => 'zorder(market_region, order_id)'
            );
            CALL iceberg_cat.system.expire_snapshots(
                table => 'db.orders',
                older_than => TIMESTAMP '2026-01-04 00:00:00',
                retain_last => 5
            );
            CALL iceberg_cat.system.remove_orphan_files(table => 'db.orders');
        """,
    }


def parquet_immutable_exchange_pattern(spark: SparkSession, parquet_dir: Path) -> DataFrame:
    """3) Raw Parquet: Reserved strictly for immutable staging / cross-tool export."""
    # ✅ Good Practice: Restrict raw Parquet to append-only or immutable dated export
    # drops (`spark.read.schema(explicit_schema).parquet(...)`), never mutable tables.
    uri = str(parquet_dir)
    df = spark.createDataFrame([("exp_1", 42)], "export_id STRING, count_val BIGINT")
    df.write.format("parquet").mode("overwrite").save(uri)
    return spark.read.schema("export_id STRING, count_val BIGINT").parquet(uri)


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - Delta Lake renders `FileScan parquet` after driver-side `_delta_log` snapshot
#   pruning; Iceberg renders `BatchScan iceberg_cat.db.orders` using DataSourceV2
#   manifest-list pruning (`PartitionTransforms` like `days(event_ts)` are pushed
#   automatically even when queries filter raw `event_ts >= '2026-01-10'`).
# - In the Spark UI SQL tab, Iceberg's `BatchScan` exposes `skippedDataFiles` and
#   `skippedDeleteFiles` (v2 position/equality delete files) directly inside scan metrics,
#   mirroring Delta's `numFilesPruned` and `numDeletionVectorsAdded`.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Hidden Partitioning (Iceberg) vs Liquid Clustering (Delta): Iceberg's `days(ts)`
#    hides partition transforms from users (queries filter `ts` directly without a
#    redundant `event_date` column); Delta solves the same UX problem via `CLUSTER BY`.
# 2. MoR Delete Representations: Iceberg v2 uses positional + equality delete files
#    (v3 adds Deletion Vectors); Delta 3.x/4.0 uses RoaringBitmap Deletion Vectors—both
#    avoid CoW amplification on sparse deletes but still require periodic compaction.
# 3. Interoperability (Delta UniForm / Iceberg REST Catalog): Delta UniForm generates
#    Iceberg metadata asynchronously alongside `_delta_log` so Iceberg-native engines
#    can read Delta tables without duplicating Parquet files.
# 4. Raw Parquet Schema Drift Trap: Never enable `mergeSchema=true` on massive raw
#    Parquet lakes—pass an explicit `StructType` / DDL string to `spark.read.schema(...)`
#    or migrate in-place via `CONVERT TO DELTA` / Iceberg `migrate` procedure.


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        spark = build_multi_format_spark(str(tmp_path / "warehouse"))
        try:
            delta_lifecycle_patterns(spark, tmp_path / "delta_orders")
            parquet_immutable_exchange_pattern(spark, tmp_path / "parquet_export").show()
            print("Iceberg DDL templates:", list(iceberg_ddl_and_sql_patterns().keys()))
        finally:
            spark.stop()
