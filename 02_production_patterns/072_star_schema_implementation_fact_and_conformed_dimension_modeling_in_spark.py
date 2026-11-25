"""Star Schema Implementation: Fact & Conformed Dimension Modeling in PySpark 4.0+.

Scenario & Scale:
    A retail enterprise serves 2B+ fact_sales rows joined against conformed
    Customer, Product, and Date dimensions for BI reporting. Star joins must
    trigger Dynamic Partition Pruning (DPP) and Broadcast Hash Joins in < 5s.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Joining unpartitioned fact tables on raw string natural keys
# without dimension surrogate keys or broadcast hints / DPP partition filters.
# Otherwise: Forces multi-terabyte SortMergeJoin shuffles across the fact table
# and disables Dynamic Partition Pruning on filtered dimension attributes.
def transform_star_join_bad(
    fact_sales: DataFrame, dim_cust: DataFrame, dim_prod: DataFrame
) -> DataFrame:
    return (
        fact_sales.join(dim_cust, on="customer_id")
        .join(dim_prod, on="product_id")
        .groupBy("country", "category")
        .agg(F.sum("total_sales_amount").alias("gross_revenue"))
    )


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Build conformed dimensions with deterministic `xxhash64`
# surrogate keys, store Decimal measures on the partitioned Fact table, and
# broadcast small dimensions (`F.broadcast`) to enable DPP + BroadcastHashJoin.
def transform_build_conformed_dim(
    df_raw: DataFrame, natural_key_col: str, sk_alias: str
) -> DataFrame:
    """Attach deterministic 64-bit surrogate key to a conformed dimension."""
    return df_raw.withColumn(
        sk_alias,
        F.xxhash64(F.coalesce(F.col(natural_key_col).cast("string"), F.lit("__NULL__"))),
    )


def transform_star_revenue_report(
    fact_sales: DataFrame,
    dim_cust: DataFrame,
    dim_prod: DataFrame,
    target_country: str | None = None,
) -> DataFrame:
    """Execute broadcast-optimized Star Join aggregating Decimal revenue."""
    cust_filtered = (
        dim_cust.filter(F.col("country") == F.lit(target_country))
        if target_country
        else dim_cust
    )
    return (
        fact_sales.withColumn(
            "total_sales_amount",
            F.try_cast(F.col("total_sales_amount"), T.DecimalType(14, 2)),
        )
        .join(F.broadcast(cust_filtered), on="dim_customer_sk", how="inner")
        .join(F.broadcast(dim_prod), on="dim_product_sk", how="inner")
        .groupBy("country", "category")
        .agg(
            F.sum("total_sales_amount").alias("gross_revenue"),
            F.sum("quantity").alias("units_sold"),
        )
        .orderBy(F.desc("gross_revenue"))
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows `BroadcastHashJoin` for both dimensions and
#   `dynamicpruningexpression(...)` in `FileScan` when `fact_sales` is
#   partitioned by a dimension join key (e.g., `order_date_sk`).
# - Zero `Exchange` shuffle appears on `fact_sales` during the join phase.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Dynamic Partition Pruning (DPP) requires `spark.sql.optimizer.dynamicPartitionPruning.enabled=true`
#    and triggers only when the dimension side is broadcastable and the join
#    key matches the fact table's partition column.
# 2. Keep degenerate dimensions (e.g., `txn_id`, `invoice_no`) directly on the
#    Fact table rather than building a 2B-row 1:1 dimension table.
# 3. For dimensions exceeding `autoBroadcastJoinThreshold` (default 10 MB, tune
#    up to ~100 MB safely), avoid `F.broadcast()` to prevent driver/executor OOM.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("072_star_schema")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    dim_cust = spark.createDataFrame(
        [(101, "CUST-A", "Alice", "Gold", "US"), (102, "CUST-B", "Bob", "Silver", "UK")],
        "dim_customer_sk LONG, customer_id STRING, full_name STRING, loyalty_tier STRING, country STRING",
    )
    dim_prod = spark.createDataFrame(
        [(501, "PROD-X", "Keyboard", "Hardware"), (502, "PROD-Y", "USB-C Hub", "Hardware")],
        "dim_product_sk LONG, product_id STRING, product_name STRING, category STRING",
    )
    fact_sales = spark.createDataFrame(
        [("TXN-901", 101, 501, 2, "179.98", "2025-01-20"), ("TXN-903", 102, 501, 1, "89.99", "2025-01-20")],
        "txn_id STRING, dim_customer_sk LONG, dim_product_sk LONG, quantity INT, total_sales_amount STRING, order_date STRING",
    )
    transform_star_revenue_report(fact_sales, dim_cust, dim_prod).show(truncate=False)
