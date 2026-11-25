"""
095 — Semantic Layer Modeling & Databricks Metric Views for BI and AI Agents.

Scenario & Scale:
    Multiple BI teams and Databricks Genie AI agents compute KPIs like `Net Revenue`
    and `Discount %` across 500M+ sales rows. Duplicating SQL formulas in Power BI,
    Tableau, and notebooks causes metric drift and ANSI `DIVIDE_BY_ZERO` crashes.
"""

from __future__ import annotations


# ==============================================================================
# 2. Anti-Pattern: Duplicated Ratios & Unsafe `/` Division Under ANSI Mode
# ==============================================================================
# ❌ Bad Practice:
#   # Computing `ROUND((discount / gross_revenue) * 100, 1)` at row grain, then
#   # averaging percentages across categories in BI (`AVG(discount_pct)`), which is
#   # mathematically wrong AND throws `DIVIDE_BY_ZERO` when `gross_revenue = 0`.
#
# Otherwise:
#   Averaging pre-computed row ratios produces Simpson's paradox errors, while raw
#   `/` crashes Spark 4.0 ANSI queries on zero-revenue returns or promo orders.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Certified Metric Layer & Ratio-of-Sums
# ==============================================================================
# ✅ Good Practice: Define additive base measures in a Metric View and compute
#                   non-additive ratios post-aggregation using `try_divide`.
METRIC_VIEW_YAML_BLUEPRINT = """
version: 0.1
source: enterprise_prod.sales.fact_sales
dimensions:
  - name: sale_date
    expr: sale_date
  - name: country
    expr: country
  - name: category
    expr: category
measures:
  - name: total_gross_revenue
    expr: SUM(gross_revenue)
  - name: total_discount
    expr: SUM(discount)
  - name: net_revenue
    expr: SUM(gross_revenue - discount)
  - name: discount_pct
    expr: ROUND(TRY_DIVIDE(SUM(discount), SUM(gross_revenue)) * 100, 2)
""".strip()


def aggregate_certified_metrics(df_sales, group_cols: list[str]):
    """Compute consistent additive and ratio metrics at any dimensional grain."""
    from pyspark.sql import functions as F

    return (
        df_sales.groupBy(*group_cols)
        .agg(
            F.round(F.sum("gross_revenue"), 2).alias("total_gross_revenue"),
            F.round(F.sum("discount"), 2).alias("total_discount"),
            F.round(F.sum(F.col("gross_revenue") - F.col("discount")), 2).alias("net_revenue"),
            F.round(
                F.expr("try_divide(sum(discount), sum(gross_revenue))") * F.lit(100.0),
                2,
            ).alias("discount_pct"),
        )
        .orderBy(*group_cols)
    )


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `df.explain("formatted")`, Catalyst expands semantic views during Analysis and
#   pushes dimensional filters (`country = 'US'`) below `HashAggregate` to `FileScan`.
# - Confirm `try_divide` compiles cleanly inside `HashAggregate` final projection
#   without triggering ANSI `DIVIDE_BY_ZERO` runtime exceptions.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: Non-additive metrics (ratios, `COUNT(DISTINCT)`, percentiles) MUST be
#   computed at query grain (`SUM(num) / SUM(den)`), never averaged from row-level ratios.
# - Gotcha 2: AI agents like Databricks Genie rely on rich `COMMENT` metadata on views
#   and columns to disambiguate `gross_revenue` vs `net_revenue` accurately.
# - Gotcha 3: Always use `TRY_DIVIDE` in semantic KPIs for Spark 4.0 ANSI safety when
#   handling zero-dollar refunds, cancellations, or free trials.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_095_Semantic_Metric_Views").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "true")
    print(METRIC_VIEW_YAML_BLUEPRINT)

    sales = [
        ("S-01", "2026-01-01", "US", "Electronics", 1200.0, 100.0),
        ("S-02", "2026-01-02", "US", "Home", 0.0, 0.0),  # tests ANSI try_divide safety
        ("S-03", "2026-01-03", "EU", "Electronics", 800.0, 80.0),
    ]
    df_sales = spark.createDataFrame(
        sales,
        "sale_id STRING, sale_date STRING, country STRING, category STRING, gross_revenue DOUBLE, discount DOUBLE",
    )
    aggregate_certified_metrics(df_sales, ["country"]).show(truncate=False)
    aggregate_certified_metrics(df_sales, ["category"]).show(truncate=False)
