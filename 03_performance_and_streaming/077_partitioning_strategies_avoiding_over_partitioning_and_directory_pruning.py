"""Scenario & Scale: Telemetry table degraded by 2M tiny 10 KB partition dirs.

Demonstrates Hive-style directory pruning vs over-partitioning collapse,
targeting 100 MB–1 GB files per partition (or clustering on modern tables).
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Partitioning storage directories by high-cardinality columns
# (`device_id`, `user_id`, `timestamp`, or `year/month/day/hour/device_id`).
# Otherwise: Creates millions of tiny <1 MB directories/files, causing driver OOM
# and 10+ minute cloud object-store `LIST` metadata bottlenecks before a single
# data byte is read (the Small Files / Over-Partitioning Death Spiral).
def bad_over_partitioned_write(df_telemetry: DataFrame, target_uri: str) -> None:
    df_telemetry.write.format("parquet").partitionBy(
        "year", "region", "order_id"  # order_id explodes directory count!
    ).mode("overwrite").save(target_uri)


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Partition strictly by low-cardinality, frequently filtered
# columns (e.g. `year`, `region`) where each partition yields >= 100 MB–1 GB,
# and cap files per partition with `repartition("year", "region")` or `maxRecordsPerFile`.
def write_well_partitioned_table(
    df_orders: DataFrame,
    target_uri: str,
) -> None:
    """Write Parquet partitioned by low-cardinality filter keys."""
    (
        df_orders.repartition(F.col("year"), F.col("region"))
        .write.format("parquet")
        .partitionBy("year", "region")
        .mode("overwrite")
        .save(target_uri)
    )


def read_with_partition_pruning(
    spark: SparkSession,
    source_uri: str,
    year: int,
    region: str,
) -> DataFrame:
    """Filter on partition keys so Catalyst prunes non-matching directories."""
    return spark.read.parquet(source_uri).filter(
        (F.col("year") == F.lit(year)) & (F.col("region") == F.lit(region))
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_pruned.explain("formatted") confirms directory skipping:
#   FileScan parquet [order_id#..., amount#..., year#..., region#...]
#     PartitionFilters: [isnotnull(year#...), isnotnull(region#...),
#                        (year#... = 2025), (region#... = US)]
#     Location: InMemoryFileIndex(1 paths)[...]
# Spark UI -> SQL tab -> Scan parquet node:
#   - Verify `number of files read` and `number of partitions read` match ONLY
#     the filtered slice (e.g., 1/4 partitions) rather than scanning all dirs.
#   - Watch `metadata time (total)`—seconds/minutes here signals over-partitioning.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. Applying functions to partition columns in WHERE (`F.year(col) == 2025` or
#    `cast(year as string)`) can defeat partition pruning unless the column type
#    matches directly; filter on raw partition columns.
# 2. Rule of thumb: keep total partition directories < 10,000 and >= 100 MB–1 GB
#    per partition; for high-cardinality filtering (device_id, customer_id),
#    use Iceberg hidden partitioning / Delta Liquid Clustering (`CLUSTER BY`)
#    instead of physical Hive folders.
# 3. `repartition("year", "region")` before `partitionBy(...)` ensures each task
#    writes 1 file per partition instead of N tasks each writing tiny fragments.


if __name__ == "__main__":
    import tempfile

    spark = (
        SparkSession.builder.appName("077_PartitioningStrategies")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    records = [
        ("ORD-1", 2025, "US", 100.0),
        ("ORD-2", 2025, "US", 250.0),
        ("ORD-3", 2025, "EU", 180.0),
        ("ORD-4", 2024, "US", 90.0),
    ]
    df = spark.createDataFrame(
        records, "order_id STRING, year INT, region STRING, amount DOUBLE"
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        write_well_partitioned_table(df, tmp_dir)
        df_pruned = read_with_partition_pruning(spark, tmp_dir, 2025, "US")
        df_pruned.explain("formatted")
        df_pruned.show()
