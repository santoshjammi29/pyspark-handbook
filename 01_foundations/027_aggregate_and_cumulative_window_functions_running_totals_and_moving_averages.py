"""Scenario & Scale: Wealth management running spend & moving averages (100M txns/day).

Business Context:
Calculate cumulative spend trajectories and rolling 3-day moving averages while
avoiding the classic duplicate-key `RANGE BETWEEN` window frame bug.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "027_Cumulative_Moving_Windows") -> SparkSession:
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
#   w = Window.partitionBy("account_id").orderBy("txn_date")
#   df.withColumn("cum_spend", F.sum("amount").over(w))  # omits rowsBetween!
# Otherwise:
# - When `orderBy()` is present without an explicit frame, Spark defaults to
#   `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. If two transactions share the
#   same `txn_date`, Spark lumps both into `CURRENT ROW`, returning identical inflated
#   totals instead of row-by-row progression.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Explicit `rowsBetween(...)` for physical row frames vs intentional
# `rangeBetween(...)` when peer grouping or timestamp-interval windows are desired.
SPENDING_SCHEMA = T.StructType(
    [
        T.StructField("account_id", T.StringType(), nullable=False),
        T.StructField("txn_date", T.StringType(), nullable=False),
        T.StructField("amount", T.DoubleType(), nullable=False),
    ]
)


def compute_cumulative_and_moving(df: DataFrame) -> DataFrame:
    """Compute physical cumulative spend and 3-row trailing moving average."""
    cum_window = (
        Window.partitionBy("account_id")
        .orderBy("txn_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    moving_window = (
        Window.partitionBy("account_id")
        .orderBy("txn_date")
        .rowsBetween(-2, Window.currentRow)
    )

    return df.withColumns(
        {
            "cumulative_spend": F.round(F.sum("amount").over(cum_window), 2),
            "moving_avg_3day": F.round(F.avg("amount").over(moving_window), 2),
        }
    )


def compare_rows_vs_range(df_dup: DataFrame) -> DataFrame:
    """Demonstrate physical ROWS frame vs value-based RANGE frame on duplicate order keys."""
    w_rows = (
        Window.partitionBy("id")
        .orderBy("val")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    w_range = (
        Window.partitionBy("id")
        .orderBy("val")
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return df_dup.withColumns(
        {
            "sum_rows": F.sum("amt").over(w_rows),
            "sum_range": F.sum("amt").over(w_range),
        }
    )


def main() -> None:
    spark = build_spark()

    spending = [
        ("ACC-1", "2023-11-01", 120.0),
        ("ACC-1", "2023-11-02", 45.0),
        ("ACC-1", "2023-11-03", 210.0),
        ("ACC-1", "2023-11-04", 80.0),
        ("ACC-1", "2023-11-05", 15.0),
    ]
    df_spend = spark.createDataFrame(spending, schema=SPENDING_SCHEMA)
    df_analytics = compute_cumulative_and_moving(df_spend)

    day4 = df_analytics.filter(F.col("txn_date") == "2023-11-04").first()
    assert day4["cumulative_spend"] == 455.0
    assert day4["moving_avg_3day"] == 111.67

    dup_schema = T.StructType(
        [
            T.StructField("id", T.StringType(), False),
            T.StructField("val", T.IntegerType(), False),
            T.StructField("amt", T.DoubleType(), False),
        ]
    )
    df_dup = spark.createDataFrame(
        [("A", 1, 10.0), ("A", 2, 20.0), ("A", 2, 30.0), ("A", 3, 40.0)],
        schema=dup_schema,
    )
    df_comp = compare_rows_vs_range(df_dup)
    sums = [r["sum_rows"] for r in df_comp.orderBy("amt").collect()]
    range_sums = [r["sum_range"] for r in df_comp.orderBy("amt").collect()]
    assert sums == [10.0, 30.0, 60.0, 100.0]
    assert range_sums == [10.0, 60.0, 60.0, 100.0]


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - Both windows in `compute_cumulative_and_moving` share identical `(account_id, txn_date)`
#   partitioning and ordering, so Catalyst merges them into ONE `Exchange` + ONE `Sort`
#   operator before `Window` evaluation.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Remember the default frame rule: WITH `orderBy`, default is `RANGE BETWEEN UNBOUNDED
#    PRECEDING AND CURRENT ROW`; WITHOUT `orderBy`, default is `UNBOUNDED PRECEDING AND
#    UNBOUNDED FOLLOWING` (entire partition).
# 2. For irregular calendar-based 30-day rolling windows (missing days), cast `TIMESTAMP`
#    or `DATE` to epoch seconds/days (`LONG`) and use `rangeBetween(-30 * 86400, 0)`.
# 3. Never omit `Window.unboundedFollowing` if calculating `% of partition total`; with
#    `orderBy`, `F.sum("x").over(w)` only sums up to the current row unless framed explicitly.

if __name__ == "__main__":
    main()
