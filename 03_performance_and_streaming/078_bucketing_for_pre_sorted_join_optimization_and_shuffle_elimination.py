"""Scenario & Scale: Hourly join of two multi-billion-row fact tables (5 TB).

Eliminates wide SortMergeJoin network shuffles (`Exchange`) using managed
table bucketing (`bucketBy` + `sortBy`) on matching keys and bucket counts.
"""

from pyspark.sql import DataFrame, SparkSession


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Joining two multi-TB unbucketed tables repeatedly on the
# same high-cardinality key, or bucketing with incompatible bucket counts /
# writing via path-based `.save()` (which ignores bucket metadata).
# Otherwise: Every hourly job re-shuffles and re-sorts 5 TB over the network
# (`Exchange hashpartitioning` + `Sort` on both sides), wasting 40+ min/run.
def bad_mismatched_bucket_write(df_orders: DataFrame, path_uri: str) -> None:
    # Path-based save() cannot register bucketing metadata in the Hive/catalog
    df_orders.write.format("parquet").mode("overwrite").save(path_uri)


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Write both managed catalog tables via `saveAsTable()` with
# identical `bucketBy(num_buckets, key)` and `sortBy(key)`, and enable
# `spark.sql.sources.bucketing.enabled` (+ bucket coalescing when multiples).
def write_bucketed_pair(
    df_orders: DataFrame,
    df_shipments: DataFrame,
    db_name: str = "bucket_perf_db",
    num_buckets: int = 8,
) -> None:
    """Persist pre-bucketed and pre-sorted managed tables for zero-shuffle joins."""
    spark = df_orders.sparkSession
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    (
        df_orders.write.format("parquet")
        .bucketBy(num_buckets, "order_id")
        .sortBy("order_id")
        .mode("overwrite")
        .saveAsTable(f"{db_name}.t_orders")
    )
    (
        df_shipments.write.format("parquet")
        .bucketBy(num_buckets, "order_id")
        .sortBy("order_id")
        .mode("overwrite")
        .saveAsTable(f"{db_name}.t_shipments")
    )


def join_bucketed_tables(
    spark: SparkSession,
    db_name: str = "bucket_perf_db",
) -> DataFrame:
    """Join two identically bucketed tables without triggering Exchange."""
    spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
    spark.conf.set("spark.sql.sources.bucketing.autoBucketedScan.enabled", "true")
    spark.conf.set(
        "spark.sql.bucketing.coalesceBucketsInJoin.enabled", "true"
    )
    df_o = spark.table(f"{db_name}.t_orders")
    df_s = spark.table(f"{db_name}.t_shipments")
    return df_o.join(df_s, on="order_id", how="inner")


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_joined.explain("formatted") confirms shuffle-free SortMergeJoin:
#   * SortMergeJoin [order_id], [order_id], Inner
#   :- * Sort [order_id ASC NULLS FIRST]
#   :  +- FileScan parquet ... SelectedBucketsCount: 8 out of 8
#   +- * Sort [order_id ASC NULLS FIRST]
#      +- FileScan parquet ... SelectedBucketsCount: 8 out of 8
# Notice: NO `Exchange hashpartitioning(...)` node appears above either FileScan!
# Spark UI -> Stages tab:
#   - Entire join collapses into a single map-side stage (0 Shuffle Write /
#     0 Shuffle Read bytes).


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. Bucketing creates `num_buckets` files *per task* per partition, so
#    `(partitions * tasks * num_buckets)` can explode small-file counts unless
#    paired with `repartition(num_buckets, key)` before `bucketBy`.
# 2. If Table A has 8 buckets and Table B has 16 buckets, enable
#    `spark.sql.bucketing.coalesceBucketsInJoin.enabled=true` (when one bucket
#    count is an integer multiple of the other) to coalesce 16->8 without shuffle.
# 3. Delta Lake / Iceberg generally avoid Hive bucketing in favor of Liquid
#    Clustering / Z-Order + Deletion Vectors / storage-partitioned joins.


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as warehouse_dir:
        spark = (
            SparkSession.builder.appName("078_BucketingShuffleElimination")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .getOrCreate()
        )
        df_ord = spark.createDataFrame(
            [(f"ORD-{i:03d}", 100.0 + i) for i in range(1, 21)],
            "order_id STRING, amount DOUBLE",
        )
        df_shp = spark.createDataFrame(
            [(f"ORD-{i:03d}", f"CARRIER-{i % 3}") for i in range(1, 21)],
            "order_id STRING, carrier STRING",
        )
        write_bucketed_pair(df_ord, df_shp, num_buckets=4)
        df_res = join_bucketed_tables(spark)
        df_res.explain("formatted")
        df_res.show(5)
        spark.sql("DROP TABLE IF EXISTS bucket_perf_db.t_orders")
        spark.sql("DROP TABLE IF EXISTS bucket_perf_db.t_shipments")
