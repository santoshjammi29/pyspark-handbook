"""JDBC Parallel Ingestion: Partitioning, fetchsize & Connection Sizing.

Scenario & Scale:
    A nightly batch job extracts 50M sales orders from an operational PostgreSQL
    cluster into the Lakehouse under a 30-minute SLA without saturating OLTP
    database CPU or exhausting connection pools.
"""

from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Reading JDBC without `partitionColumn`, bounds, or `fetchsize`.
# Otherwise: Spark issues a single unpartitioned `SELECT * FROM table` on one
# executor task, taking 6+ hours while risking driver/executor socket timeouts.
def read_jdbc_single_thread_bad(
    spark: SparkSession, jdbc_url: str, table: str
) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .load()
    )


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Slice the read via indexed numeric `partitionColumn`, tuned
# `fetchsize`, bounded `numPartitions`, and ANSI-safe downstream conformance.
def build_jdbc_options(
    jdbc_url: str,
    dbtable: str,
    partition_column: str,
    lower_bound: int,
    upper_bound: int,
    num_partitions: int = 16,
    fetch_size: int = 10000,
    user: str = "",
    password: str = "",
) -> Dict[str, str]:
    """Construct bounded parallel JDBC read options protecting OLTP capacity."""
    return {
        "url": jdbc_url,
        "dbtable": dbtable,
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "partitionColumn": partition_column,
        "lowerBound": str(lower_bound),
        "upperBound": str(upper_bound),
        "numPartitions": str(num_partitions),
        "fetchsize": str(fetch_size),
        "queryTimeout": "300",
    }


def read_jdbc_parallel(spark: SparkSession, options: Dict[str, str]) -> DataFrame:
    """Execute partitioned JDBC extraction across executor tasks."""
    return spark.read.format("jdbc").options(**options).load()


def transform_audit_jdbc_orders(df: DataFrame) -> DataFrame:
    """Validate ingested JDBC numeric fields and attach partition skew metrics."""
    return (
        df.withColumn("order_amount", F.try_cast(F.col("order_amount"), T.DecimalType(14, 2)))
        .withColumn("_spark_partition_id", F.spark_partition_id())
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` displays `Scan JDBCRelation(public.orders) [numPartitions=16]`
#   with `PushedFilters` pushed down into each partition's SQL `WHERE` clause.
# - In Spark UI Stages tab, compare `Duration` and `Input Size / Records` across
#   the 16 tasks; uneven row counts indicate skew in `partitionColumn` values.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `lowerBound` and `upperBound` ONLY compute partition stride boundaries—they
#    do NOT filter rows! Rows `< lowerBound` go to partition 0; `>= upperBound`
#    go to partition `N-1`, so stale bounds create massive straggler tasks.
# 2. `numPartitions` directly equals concurrent OLTP connections; never exceed
#    what the source DB replica connection pool safely allows (typically 8-32).
# 3. For skewed or UUID primary keys, use a deterministic `predicates=[...]` list
#    (e.g., date/hash buckets) instead of numeric `lowerBound`/`upperBound`.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("047_jdbc_parallel_ingestion")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    opts = build_jdbc_options(
        jdbc_url="jdbc:postgresql://replica.internal:5432/retail",
        dbtable="public.orders",
        partition_column="order_id",
        lower_bound=1,
        upper_bound=10_000_000,
        num_partitions=16,
        fetch_size=10_000,
    )
    print({k: v for k, v in opts.items() if k != "password"})

    df_sim = spark.range(1, 101, 1, 4).select(
        F.col("id").alias("order_id"),
        F.round(F.rand(seed=42) * 500 + 10, 2).alias("order_amount"),
    )
    transform_audit_jdbc_orders(df_sim).groupBy("_spark_partition_id").count().show()
