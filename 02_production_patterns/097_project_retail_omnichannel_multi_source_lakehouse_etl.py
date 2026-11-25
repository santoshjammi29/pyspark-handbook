"""Project Retail: Omnichannel Multi-Source Lakehouse ETL (PySpark 4.0+).

Scenario & Scale:
    An omnichannel retailer reconciles 80M daily store POS transactions, web
    clickstream orders, and CRM profiles into a unified Customer 360 CLV model
    with < 30-minute end-to-end batch freshness.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Combining channels with positional `union()` when source
# schemas have drifted column order, or joining unbroadcasted CRM tables blindly.
# Otherwise: `union()` silently swaps columns (e.g. `channel_id` into `amount`),
# corrupting financial totals or triggering ANSI cast exceptions downstream.
def transform_union_positional_bad(df_pos: DataFrame, df_web: DataFrame) -> DataFrame:
    return df_pos.union(df_web)  # Unsafe if column positions differ


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Align heterogeneous channels with `unionByName(allowMissingColumns=True)`,
# cast amounts with `try_cast`, enrich via broadcast CRM join, and build Gold CLV.
def transform_conform_omnichannel_sales(
    df_pos: DataFrame, df_web: DataFrame, df_crm: DataFrame
) -> DataFrame:
    """Conform multi-channel sales and enrich with CRM customer segments."""
    df_all_sales = df_pos.unionByName(df_web, allowMissingColumns=True).withColumn(
        "amount_dec", F.try_cast(F.col("amount"), T.DecimalType(14, 2))
    )
    return (
        df_all_sales.join(F.broadcast(df_crm), on="cust_id", how="left")
        .withColumn("segment", F.coalesce(F.col("segment"), F.lit("UNKNOWN")))
        .withColumn("_conformed_at", F.current_timestamp())
    )


def transform_gold_customer_clv(df_silver: DataFrame) -> DataFrame:
    """Compute per-customer omnichannel spend and hybrid shopper classification."""
    return (
        df_silver.groupBy("cust_id", "name", "segment")
        .agg(
            F.round(F.sum("amount_dec"), 2).alias("total_lifetime_spend"),
            F.count("txn_id").alias("total_purchases"),
            F.sum(F.when(F.col("channel") == "IN_STORE", F.col("amount_dec")).otherwise(F.lit(0))).alias(
                "store_spend"
            ),
            F.sum(F.when(F.col("channel") == "ONLINE", F.col("amount_dec")).otherwise(F.lit(0))).alias(
                "online_spend"
            ),
            F.countDistinct("channel").alias("active_channels_count"),
        )
        .withColumn(
            "shopper_type",
            F.when(F.col("active_channels_count") > 1, F.lit("OMNICHANNEL_HYBRID"))
            .when(F.col("store_spend") > 0, F.lit("IN_STORE_ONLY"))
            .otherwise(F.lit("ONLINE_ONLY")),
        )
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows `Union` feeding directly into `BroadcastHashJoin`
#   for `df_crm`, followed by a two-stage `HashAggregate` for Gold CLV metrics.
# - Watch for `NULL` `cust_id` skew (guest checkouts) during `groupBy("cust_id")`;
#   filter or isolate guest orders before customer-level aggregations.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Always use `unionByName(..., allowMissingColumns=True)` across independent
#    source feeds so schema evolution on one channel never misaligns columns.
# 2. `countDistinct("channel")` compiles to a two-stage `HashAggregate` with an
#    extra `Expand` or grouping key; acceptable for 2-5 channels, but prefer
#    `approx_count_distinct` on high-cardinality columns.
# 3. Identity resolution across email cookies and loyalty IDs should run in a
#    dedicated deterministic Silver identity graph before Gold CLV aggregation.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("097_retail_omnichannel")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    df_pos = spark.createDataFrame(
        [("POS-1", "CUST-A", 120.0, "STORE_01")], "txn_id STRING, cust_id STRING, amount DOUBLE, channel_id STRING"
    ).withColumn("channel", F.lit("IN_STORE"))
    df_web = spark.createDataFrame(
        [("WEB-91", "CUST-A", 89.99, "WEB_US"), ("WEB-92", "CUST-C", 310.0, "WEB_US")],
        "txn_id STRING, cust_id STRING, amount DOUBLE, channel_id STRING",
    ).withColumn("channel", F.lit("ONLINE"))
    df_crm = spark.createDataFrame(
        [("CUST-A", "Alice", "VIP"), ("CUST-C", "Charlie", "VIP")],
        "cust_id STRING, name STRING, segment STRING",
    )
    df_silver = transform_conform_omnichannel_sales(df_pos, df_web, df_crm)
    transform_gold_customer_clv(df_silver).show(truncate=False)
