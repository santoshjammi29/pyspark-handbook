"""Scenario & Scale: Enterprise financial OLAP subtotal reporting (100M ledger rows/day).

Business Context:
Produce multi-level subtotals (Year -> Quarter -> Product, all combinations, and grand
total) in a single scan using `rollup()`, `cube()`, and `grouping_id()` bitmasks.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "024_OLAP_Rollup_Cube_Grouping") -> SparkSession:
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
# - Computing detail, subtotal, and grand total via 4 separate `groupBy()` queries
#   combined with `.unionByName()`, and checking `col.isNull()` to detect subtotals.
# Otherwise:
# - Separate `groupBy()` + `union` triggers 4x table scans and 4x shuffles.
# - Checking `col.isNull()` conflates a genuine `NULL` dimension value in source data
#   with a `NULL` synthesized by a subtotal/grand-total row.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Single-scan `rollup()` / `cube()` + `F.grouping_id()` bitmask.
REVENUE_SCHEMA = T.StructType(
    [
        T.StructField("fiscal_year", T.IntegerType(), nullable=False),
        T.StructField("fiscal_quarter", T.StringType(), nullable=False),
        T.StructField("product_line", T.StringType(), nullable=False),
        T.StructField("revenue", T.DoubleType(), nullable=False),
    ]
)


def compute_hierarchical_rollup(df: DataFrame) -> DataFrame:
    """Compute Year -> Quarter -> Product subtotals and grand total in one pass."""
    return (
        df.rollup("fiscal_year", "fiscal_quarter", "product_line")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.grouping_id("fiscal_year", "fiscal_quarter", "product_line").alias("grouping_level"),
        )
        .orderBy(
            F.col("fiscal_year").asc_nulls_last(),
            F.col("fiscal_quarter").asc_nulls_last(),
            F.col("product_line").asc_nulls_last(),
        )
    )


def compute_year_product_cube(df: DataFrame) -> DataFrame:
    """Compute all 2^N combinations across fiscal_year and product_line."""
    return (
        df.cube("fiscal_year", "product_line")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.grouping_id("fiscal_year", "product_line").alias("grouping_level"),
        )
        .orderBy(F.col("fiscal_year").asc_nulls_last(), F.col("product_line").asc_nulls_last())
    )


def main() -> None:
    spark = build_spark()

    revenue_data = [
        (2022, "Q1", "Software", 50000.0),
        (2022, "Q1", "Hardware", 80000.0),
        (2022, "Q2", "Software", 60000.0),
        (2022, "Q2", "Hardware", 75000.0),
        (2023, "Q1", "Software", 70000.0),
        (2023, "Q1", "Hardware", 90000.0),
        (2023, "Q2", "Software", 85000.0),
        (2023, "Q2", "Hardware", 95000.0),
    ]
    df_rev = spark.createDataFrame(revenue_data, schema=REVENUE_SCHEMA)

    df_rollup = compute_hierarchical_rollup(df_rev)
    df_cube = compute_year_product_cube(df_rev)

    grand_total = df_rollup.filter(F.col("grouping_level") == 7).first()
    assert grand_total["total_revenue"] == 605000.0
    assert df_cube.count() == 9  # (2 years + 1) * (2 products + 1)


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_rollup.explain("formatted")` emits a single `Expand` operator multiplying each
#   input row by `N + 1` (for `rollup`) or `2^N` (for `cube`) tagged with `spark_grouping_id`,
#   followed by one `HashAggregate` shuffle.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `cube()` generates $2^N$ combinations per input row before shuffling; never pass
#    more than 4-5 dimensions to `cube()` on big tables or row expansion explodes (32x+).
# 2. `grouping_id(c1, c2, c3)` returns an integer bitmask where bit `k` is `1` when that
#    column is rolled up (`7` = grand total for 3 cols, `0` = finest-grain leaf row).
# 3. For arbitrary non-hierarchical subtotal combinations (neither `rollup` nor full
#    `cube`), use `df.groupingSets(...)` (Spark 4.0+) or SQL `GROUPING SETS (...)`.

if __name__ == "__main__":
    main()
