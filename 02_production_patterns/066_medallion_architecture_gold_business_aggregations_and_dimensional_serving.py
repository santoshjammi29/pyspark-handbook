"""Medallion Architecture: Gold Business Aggregations & Dimensional Serving.

Scenario & Scale:
    Executive dashboards and BI tools query quarterly vendor scorecards across
    100M+ Silver invoices. Direct BI queries against Silver cause 40s+ scans;
    pre-aggregated Gold tables deliver sub-second SLAs with single-source KPIs.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Dividing directly (`approved / total`) without zero-division
# protection and serving unclustered Gold tables using floating-point doubles.
# Otherwise: Under PySpark 4.0 ANSI mode, a 0-row/0-count denominator raises
# `SparkArithmeticException: DIVIDE_BY_ZERO`, and BI tools suffer full scans.
def transform_gold_scorecard_bad(df_silver: DataFrame) -> DataFrame:
    return df_silver.groupBy("vendor_id").agg(
        (F.count(F.when(F.col("status") == "APPROVED", 1)) / F.count("invoice_no")).alias("approval_rate")
    )


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Compute exact Decimal KPIs, guard division with
# `F.try_divide`, and land Gold tables clustered on BI filter columns.
def transform_build_vendor_scorecard(df_silver: DataFrame) -> DataFrame:
    """Aggregate Silver invoices into a conformed Gold vendor KPI scorecard."""
    df_agg = df_silver.groupBy("vendor_id").agg(
        F.count("invoice_no").alias("total_invoices_submitted"),
        F.round(F.sum(F.try_cast(F.col("amount"), T.DecimalType(18, 2))), 2).alias("total_invoiced_spend"),
        F.round(F.avg(F.try_cast(F.col("amount"), T.DecimalType(18, 2))), 2).alias("avg_invoice_value"),
        F.sum(F.when(F.col("status") == "APPROVED", F.lit(1)).otherwise(F.lit(0))).alias("approved_invoices_count"),
        F.sum(F.when(F.col("status") == "REJECTED", F.lit(1)).otherwise(F.lit(0))).alias("rejected_invoices_count"),
    )

    return (
        df_agg.withColumn(
            "approval_rate_pct",
            F.round(
                F.coalesce(
                    F.try_divide(F.col("approved_invoices_count"), F.col("total_invoices_submitted")),
                    F.lit(0.0),
                )
                * F.lit(100.0),
                1,
            ),
        )
        .withColumn(
            "vendor_volume_tier",
            F.when(F.col("total_invoiced_spend") >= F.lit(10000), F.lit("STRATEGIC_TIER_1"))
            .when(F.col("total_invoiced_spend") >= F.lit(2000), F.lit("PREFERRED_TIER_2"))
            .otherwise(F.lit("STANDARD_TIER_3")),
        )
    )


def write_gold_clustered(df_gold: DataFrame, gold_table: str) -> None:
    """Persist Gold table with Delta Liquid Clustering (`clusterBy`) for BI."""
    df_gold.write.format("delta").clusterBy("vendor_volume_tier", "vendor_id").mode("overwrite").saveAsTable(
        gold_table
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows two-stage `HashAggregate` (partial map-side
#   aggregation before `Exchange hashpartitioning(vendor_id)` and final merge).
# - In Spark UI SQL tab, verify `HashAggregate` spills = 0 B; high skew on one
#   `vendor_id` benefits from AQE skew-split or two-phase salting.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `F.try_divide` is mandatory under Spark 4.0 ANSI mode whenever denominators
#    can evaluate to 0; regular `/` fails the entire query immediately.
# 2. Liquid Clustering (`clusterBy`) replaces rigid `partitionBy` + `ZORDER BY`
#    on Gold Delta tables, adapting incrementally as BI filter patterns evolve.
# 3. Never embed business KPI definitions in BI workbooks alone—codify them once
#    in Gold so Finance, Ops, and Executive dashboards show identical metrics.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("066_medallion_gold")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    silver_rows = [
        ("INV-1", "V-01", 1500.0, "APPROVED", "2025-01-01"),
        ("INV-2", "V-01", 3200.0, "APPROVED", "2025-01-15"),
        ("INV-3", "V-01", 800.0, "REJECTED", "2025-01-20"),
        ("INV-4", "V-03", 12500.0, "APPROVED", "2025-01-12"),
    ]
    df_silver_in = spark.createDataFrame(
        silver_rows, "invoice_no STRING, vendor_id STRING, amount DOUBLE, status STRING, invoice_date STRING"
    )
    transform_build_vendor_scorecard(df_silver_in).show(truncate=False)
