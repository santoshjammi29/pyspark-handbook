"""
092 — FinOps & DBU Cost Attribution Using system.billing.usage System Tables.

Scenario & Scale:
    A multi-workspace platform tracks millions of hourly SKU billing records in
    `system.billing.usage` to attribute DBU spend across Finance, Marketing, and ML,
    and to detect untagged interactive `ALL_PURPOSE` clusters running scheduled ETL.
"""

from __future__ import annotations


# ==============================================================================
# 2. Anti-Pattern: Unfiltered System Table Scans & Unsafe Division
# ==============================================================================
# ❌ Bad Practice:
#   spark.sql("""
#       SELECT usage_metadata.job_id, SUM(usage_quantity) / COUNT(*) AS avg_dbu
#       FROM system.billing.usage GROUP BY 1
#   """)
#
# Otherwise:
#   Scanning `system.billing.usage` without `usage_date` pruning reads months of
#   historical billing telemetry, while `/` throws `DIVIDE_BY_ZERO` if `COUNT`
#   or denominator metrics are zero under Spark 4.0 ANSI mode.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Partition-Pruned Attribution & Anomaly Audit
# ==============================================================================
# ✅ Good Practice: Prune on `usage_date`, join/map contract DBU rates safely with
#                   `try_divide`, and flag interactive `ALL_PURPOSE` untagged runs.
def compute_departmental_dbu_report(df_usage, start_date: str, dbu_rate_usd: float = 0.30):
    """Aggregate pruned DBU consumption and estimated USD spend by department & SKU."""
    from pyspark.sql import functions as F

    return (
        df_usage.filter(F.col("usage_date") >= F.lit(start_date))
        .groupBy("department", "sku_name")
        .agg(
            F.count("*").alias("run_events"),
            F.round(F.sum("usage_quantity"), 2).alias("total_dbus"),
            F.round(F.sum("usage_quantity") * F.lit(dbu_rate_usd), 2).alias("estimated_cost_usd"),
            F.round(F.expr("try_divide(sum(usage_quantity), count(*))"), 2).alias("avg_dbus_per_event"),
        )
        .orderBy(F.col("total_dbus").desc())
    )


def detect_untagged_all_purpose_anomalies(df_usage, dbu_threshold: float = 50.0):
    """Flag high-cost interactive ALL_PURPOSE compute lacking a scheduled job_id."""
    from pyspark.sql import functions as F

    return (
        df_usage.filter((F.col("sku_name") == "ALL_PURPOSE") & F.col("job_id").isNull())
        .groupBy("workload_name", "department")
        .agg(F.round(F.sum("usage_quantity"), 2).alias("total_dbus"))
        .filter(F.col("total_dbus") >= F.lit(dbu_threshold))
        .withColumn("recommendation", F.lit("Migrate scheduled workload to Jobs Compute (~55-60% DBU savings)"))
    )


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `df.explain("formatted")`, confirm `usage_date >= start_date` is pushed into
#   `PartitionFilters` / `PushedFilters` inside `FileScan` before `HashAggregate`.
# - Look for two-stage `HashAggregate` (partial aggregate -> `Exchange hashpartitioning`
#   -> final aggregate) keeping shuffle bytes tiny before final `Sort`.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: `system.billing.usage` reports `usage_quantity` (DBUs), NOT dollars;
#   join against `system.billing.list_prices` (on `sku_name` and price effective window)
#   to compute exact list-price spend before enterprise discount multipliers.
# - Gotcha 2: Automated Jobs Compute is ~50–60% cheaper per DBU than interactive
#   All-Purpose Compute; enforce Cluster Policies requiring `cost_center` & `env` tags.
# - Gotcha 3: System tables have ingestion latency (up to 24h for billing records),
#   so daily FinOps SLAs should use a 2-day rolling watermark rather than `CURRENT_DATE()`.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_092_FinOps_Billing").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "true")

    usage_records = [
        ("WS-1", "JOB-101", "Nightly Ingestion", "JOBS", 45.2, "Finance", "2026-01-14"),
        ("WS-1", "JOB-102", "Customer 360", "JOBS", 12.8, "Marketing", "2026-01-15"),
        ("WS-1", None, "Ad-Hoc Data Science", "ALL_PURPOSE", 110.5, "DataScience", "2026-01-15"),
    ]
    df_usage = spark.createDataFrame(
        usage_records,
        "workspace_id STRING, job_id STRING, workload_name STRING, sku_name STRING, usage_quantity DOUBLE, department STRING, usage_date STRING",
    )
    compute_departmental_dbu_report(df_usage, "2026-01-15").show(truncate=False)
    detect_untagged_all_purpose_anomalies(df_usage).show(truncate=False)
