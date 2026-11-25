"""Scenario & Scale: Retail daily regional sales rollup (200M POS txns/day, 10m SLA).

Business Context:
Compute multi-metric department revenue, maximum discounts, and shopper cardinality
in a single `HashAggregate` pass while avoiding `count_distinct` shuffle bottlenecks
on multi-billion row high-cardinality keys.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "023_Multi_Column_Aggregations") -> SparkSession:
    """Create or reuse a portable SparkSession with ANSI mode enabled."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 1. Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice:
# - Calling multiple separate `.groupBy().sum()` / `.groupBy().count()` queries and
#   joining them back, or running exact `count_distinct()` across billions of rows.
# Otherwise:
# - Separate `groupBy()` passes multiply source scans and shuffles unnecessarily.
# - Exact `count_distinct()` expands every distinct key in memory before shuffling
#   (via `Expand`), causing severe shuffle spill and executor GC stalls.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Single-pass `groupBy().agg(...)` with explicit `.alias()` and
# HyperLogLog++ `approx_count_distinct()` for high-cardinality approximation.
SALES_SCHEMA = T.StructType(
    [
        T.StructField("txn_id", T.StringType(), nullable=False),
        T.StructField("region", T.StringType(), nullable=False),
        T.StructField("department", T.StringType(), nullable=False),
        T.StructField("amount", T.DoubleType(), nullable=False),
        T.StructField("discount", T.DoubleType(), nullable=False),
        T.StructField("customer_id", T.StringType(), nullable=False),
    ]
)


def summarize_regional_sales(df: DataFrame) -> DataFrame:
    """Aggregate department KPIs with explicit aliases in a single two-phase pass."""
    return (
        df.groupBy("region", "department")
        .agg(
            F.count("txn_id").alias("total_transactions"),
            F.round(F.sum("amount"), 2).alias("gross_revenue"),
            F.round(F.avg("amount"), 2).alias("avg_basket_size"),
            F.max("amount").alias("max_single_sale"),
            F.round(F.max("discount") * F.lit(100.0), 1).alias("max_discount_pct"),
            F.count_distinct("customer_id").alias("unique_shoppers"),
        )
        .orderBy("region", F.desc("gross_revenue"))
    )


def summarize_approx_shoppers(df: DataFrame, rsd: float = 0.05) -> DataFrame:
    """Estimate regional unique shoppers via HyperLogLog++ with bounded memory."""
    return df.groupBy("region").agg(
        F.count_distinct("customer_id").alias("exact_distinct_shoppers"),
        F.approx_count_distinct("customer_id", rsd=rsd).alias("approx_distinct_shoppers"),
    )


def main() -> None:
    spark = build_spark()

    sales = [
        ("TXN-1", "West", "Electronics", 450.00, 0.10, "C-01"),
        ("TXN-2", "West", "Electronics", 1200.00, 0.15, "C-02"),
        ("TXN-3", "West", "Groceries", 65.50, 0.00, "C-01"),
        ("TXN-4", "East", "Electronics", 800.00, 0.05, "C-03"),
        ("TXN-5", "East", "Fashion", 150.00, 0.20, "C-04"),
        ("TXN-6", "East", "Fashion", 95.00, 0.10, "C-04"),
        ("TXN-7", "North", "Groceries", 45.00, 0.00, "C-05"),
        ("TXN-8", "North", "Groceries", 120.00, 0.05, "C-06"),
    ]
    df_sales = spark.createDataFrame(sales, schema=SALES_SCHEMA)

    df_metrics = summarize_regional_sales(df_sales)
    df_approx = summarize_approx_shoppers(df_sales)

    west_elec = df_metrics.filter(
        (F.col("region") == "West") & (F.col("department") == "Electronics")
    ).first()
    assert west_elec["gross_revenue"] == 1650.0 and west_elec["unique_shoppers"] == 2
    assert df_approx.filter(F.col("region") == "West").first()["exact_distinct_shoppers"] == 2


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_metrics.explain("formatted")` shows two `HashAggregate` operators split by
#   `Exchange hashpartitioning(region, department)`: partial local combine on map side,
#   followed by final merge after shuffle.
# - `F.sum` on integer types in ANSI mode throws `ARITHMETIC_OVERFLOW` if the sum
#   exceeds `Long.MaxValue`; use `F.try_sum` when overflow safety is required.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `F.count("*")` counts NULL rows, whereas `F.count("col")` skips NULL values in `col`.
# 2. `HashAggregate` uses off-heap/Tungsten binary buffers for fixed-width primitives;
#    non-mutable types (strings/maps in certain aggs) fall back to `SortAggregate` or
#    `ObjectHashAggregate`, which is slower and uses more JVM heap.
# 3. `approx_count_distinct(..., rsd=0.05)` uses HyperLogLog++ with constant memory per
#    group—ideal for dashboard DAU/MAU metrics, unsuitable for financial billing counts.

if __name__ == "__main__":
    main()
