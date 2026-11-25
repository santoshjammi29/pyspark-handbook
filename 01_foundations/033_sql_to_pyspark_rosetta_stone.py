"""Scenario & Scale: Enterprise SQL-to-PySpark migration & Rosetta Stone (100M+ rows/day).

Business Context:
Translate common ANSI SQL patterns (`TRY_CAST`, `CASE WHEN`, `GROUP BY ... HAVING`,
`QUALIFY`, `EXISTS`/`NOT EXISTS`, and CTEs) into modular, strongly-typed PySpark 4.0+
DataFrame pipelines using `.transform()` chaining.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "033_SQL_to_PySpark_Rosetta_Stone") -> SparkSession:
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
# - Dumping 400-line monolithic SQL strings inside `spark.sql(...)` with `CAST(...)` on
#   unclean strings and global temp views for every intermediate CTE step.
# Otherwise:
# - Monolithic SQL strings cannot be unit-tested stage-by-stage, share no typed Python
#   helpers, pollute session temp-view catalogs, and crash under Spark 4.0 ANSI mode
#   when `CAST()` encounters a dirty record.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix (SQL -> PySpark Rosetta Stone)
# ---------------------------------------------------------------------------
# ✅ Good Practice: Pure composable `DataFrame -> DataFrame` functions chained via `.transform()`.
ORDER_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), nullable=False),
        T.StructField("customer_id", T.StringType(), nullable=False),
        T.StructField("raw_amount", T.StringType(), nullable=True),
        T.StructField("order_ts", T.StringType(), nullable=False),
        T.StructField("status", T.StringType(), nullable=False),
    ]
)

VIP_SCHEMA = T.StructType(
    [T.StructField("customer_id", T.StringType(), nullable=False)]
)


# Rosetta 1: SELECT, WHERE, CASE WHEN, TRY_CAST -> select / filter / when / try_cast
def stage_clean_orders(df: DataFrame) -> DataFrame:
    """SQL equiv: SELECT ..., TRY_CAST(raw_amount AS DOUBLE), CASE WHEN ... WHERE ..."""
    amount = F.try_cast(F.col("raw_amount"), T.DoubleType())
    return df.filter((F.col("status") != "CANCELLED") & amount.isNotNull()).select(
        "order_id",
        "customer_id",
        amount.alias("amount"),
        F.try_to_timestamp(F.col("order_ts")).alias("order_ts"),
        F.when(amount >= 500.0, "HIGH").otherwise("STANDARD").alias("order_tier"),
    )


# Rosetta 2: QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1
def stage_qualify_latest_per_customer(df: DataFrame) -> DataFrame:
    """SQL equiv: QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC) = 1"""
    w = Window.partitionBy("customer_id").orderBy(
        F.col("order_ts").desc(),
        F.col("order_id").desc(),
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


# Rosetta 3: WHERE EXISTS (...) / NOT EXISTS (...) -> left_semi / left_anti
def stage_filter_vip_exists(df: DataFrame, df_vip: DataFrame) -> DataFrame:
    """SQL equiv: WHERE EXISTS (SELECT 1 FROM vip v WHERE v.customer_id = o.customer_id)"""
    return df.join(df_vip, on="customer_id", how="left_semi")


# Rosetta 4: GROUP BY ... HAVING SUM(amount) >= 200 -> groupBy().agg().filter()
def stage_group_having(df: DataFrame) -> DataFrame:
    """SQL equiv: SELECT order_tier, COUNT(*), SUM(amount) ... GROUP BY 1 HAVING SUM >= 200"""
    return (
        df.groupBy("order_tier")
        .agg(
            F.count("*").alias("order_count"),
            F.round(F.sum("amount"), 2).alias("total_revenue"),
        )
        .filter(F.col("total_revenue") >= 200.0)
    )


def main() -> None:
    spark = build_spark()

    raw_orders = [
        ("O-1", "C-1", "250.00", "2025-01-01 10:00:00", "PAID"),
        ("O-2", "C-1", "600.00", "2025-01-02 12:00:00", "PAID"),
        ("O-3", "C-2", "BAD_AMT", "2025-01-02 13:00:00", "PAID"),
        ("O-4", "C-3", "900.00", "2025-01-03 09:00:00", "CANCELLED"),
        ("O-5", "C-4", "150.00", "2025-01-03 11:00:00", "PAID"),
    ]
    df_orders = spark.createDataFrame(raw_orders, schema=ORDER_SCHEMA)
    df_vip = spark.createDataFrame([("C-1",), ("C-3",)], schema=VIP_SCHEMA)

    # Rosetta 5: WITH cte1 AS (...), cte2 AS (...) -> df.transform(...) pipeline
    df_pipeline = (
        df_orders.transform(stage_clean_orders)
        .transform(stage_qualify_latest_per_customer)
        .transform(lambda d: stage_filter_vip_exists(d, df_vip))
        .transform(stage_group_having)
    )

    row = df_pipeline.first()
    assert row["order_tier"] == "HIGH"
    assert row["order_count"] == 1 and row["total_revenue"] == 600.0


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - Chaining `.transform(...)` produces the exact same Catalyst physical plan as nested
#   SQL CTEs (`WindowGroupLimit`, `LeftSemi` join, `HashAggregate`), while remaining
#   100% unit-testable function-by-function in `pytest`.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `DataFrame.transform(fn)` accepts `fn(df, *args, **kwargs)` in PySpark 3.0+, making
#    it the direct Pythonic replacement for SQL CTEs without polluting `spark.catalog`.
# 2. `HAVING` in SQL simply translates to `.filter(...)` immediately after `.groupBy().agg()`.
# 3. Use `left_anti` instead of SQL `NOT IN (SELECT ...)` because SQL `NOT IN` silently
#    evaluates to empty if the subquery ever contains a single `NULL`.

if __name__ == "__main__":
    main()
