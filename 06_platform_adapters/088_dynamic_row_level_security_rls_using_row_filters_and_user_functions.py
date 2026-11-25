"""
088 — Dynamic Row-Level Security (RLS) via Unity Catalog Row Filters.

Scenario & Scale:
    A global `sales_pipeline` Delta table (500M+ rows) serves regional sales teams
    (`US`, `EU`, `APAC`) and executives. Maintaining duplicated regional tables or
    hundreds of views creates storage bloat, stale sync bugs, and governance drift.
"""

from __future__ import annotations


# ==============================================================================
# 2. Anti-Pattern: Duplicated Regional Tables & Hardcoded View Sprawl
# ==============================================================================
# ❌ Bad Practice:
#   spark.sql("CREATE OR REPLACE VIEW v_sales_us AS SELECT * FROM sales WHERE deal_region = 'US'")
#   # Plus v_sales_eu, v_sales_apac, and physical regional table clones
#
# Otherwise:
#   Duplicating tables multiplies storage and ETL cost, while per-region views break
#   cross-region executive BI queries and force analysts to remember view names.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Unity Catalog Row Filters & Group Predicates
# ==============================================================================
# ✅ Good Practice: Attach a SQL Row Filter UDF using `is_account_group_member()`
#                   directly to the base table, and test policy logic portably.
ROW_FILTER_DDL = """
CREATE OR REPLACE FUNCTION enterprise_prod.sales.region_row_filter(region STRING)
RETURN IF(
    is_account_group_member('global_sales_executives'),
    TRUE,
    (is_account_group_member('us_sales_reps') AND region = 'US')
    OR (is_account_group_member('eu_sales_reps') AND region = 'EU')
);

ALTER TABLE enterprise_prod.sales.t_deals
SET ROW FILTER enterprise_prod.sales.region_row_filter ON (deal_region);
""".strip()


def apply_portable_rls_policy(df, user_groups: set[str]):
    """Portable PySpark 4.0+ row-filter evaluator for CI unit testing."""
    from pyspark.sql import functions as F

    if "global_sales_executives" in user_groups:
        return df

    allowed_regions: list[str] = []
    if "us_sales_reps" in user_groups:
        allowed_regions.append("US")
    if "eu_sales_reps" in user_groups:
        allowed_regions.append("EU")

    if not allowed_regions:
        return df.filter(F.lit(False))
    return df.filter(F.col("deal_region").isin(allowed_regions))


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - Catalyst injects the Row Filter predicate directly above `FileScan` / `Relation`
#   in `df.explain("formatted")` (`Filter isnotnull(deal_region) AND (deal_region = US)`).
# - On partitioned or Liquid-Clustered Delta tables (`CLUSTER BY (deal_region)`),
#   Unity Catalog skips non-matching files via `PushedFilters` / Delta Z-Cube stats.
# - In Spark UI -> SQL tab, verify `number of files pruned` increases for regional queries.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: Always use `is_account_group_member('group')` (account-level groups),
#   not workspace-local groups, so policies stay consistent across all workspaces.
# - Gotcha 2: Row Filters cannot be bypassed by `CREATE TABLE AS SELECT` or views;
#   downstream derived tables inherit only the filtered rows visible to the caller.
# - Gotcha 3: Keep Row Filter UDFs pure and deterministic—avoid joining lookup tables
#   inside the filter if you want Catalyst file-skipping and predicate pushdown.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_088_UnityCatalog_RLS").getOrCreate()
    print(ROW_FILTER_DDL)

    deals = [
        ("DEAL-1", "US", 150_000.0, "Acme Corp"),
        ("DEAL-2", "EU", 220_000.0, "Bavaria Ltd"),
        ("DEAL-3", "APAC", 95_000.0, "Tokyo Corp"),
    ]
    df_deals = spark.createDataFrame(deals, "deal_id STRING, deal_region STRING, value DOUBLE, account STRING")
    apply_portable_rls_policy(df_deals, {"us_sales_reps"}).show(truncate=False)
