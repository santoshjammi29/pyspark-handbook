"""Scenario & Scale: BI monthly revenue crosstab & ML feature melting (60M rows/day).

Business Context:
Pivot narrow monthly sales into wide BI columns without triggering an extra discovery
scan, and unpivot wide tables back into tidy long format via Spark 4.0+ `unpivot()`.
"""

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "025_Pivot_Unpivot_Stack") -> SparkSession:
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
#   df_narrow.groupBy("dept").pivot("month").agg(F.sum("sales"))
# Otherwise:
# - Calling `.pivot("month")` WITHOUT an explicit value list forces Spark to run an
#   extra eager distributed job to collect distinct values of `month` before planning.
# - On high-cardinality pivot columns (10,000+ values), Spark crashes with
#   `pivot column ... has more than spark.sql.pivotMaxValues (10000)`!


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Pass explicit `values` list to `pivot()` and use native `unpivot()`.
SALES_SCHEMA = T.StructType(
    [
        T.StructField("dept", T.StringType(), nullable=False),
        T.StructField("month", T.StringType(), nullable=False),
        T.StructField("sales", T.DoubleType(), nullable=False),
    ]
)


def pivot_monthly_sales(df: DataFrame, months: Sequence[str]) -> DataFrame:
    """Pivot narrow sales into wide monthly columns with zero distinct-discovery scan."""
    return (
        df.groupBy("dept")
        .pivot("month", list(months))
        .agg(F.coalesce(F.sum("sales"), F.lit(0.0)))
    )


def unpivot_monthly_sales(df_wide: DataFrame, months: Sequence[str]) -> DataFrame:
    """Melt wide monthly columns back into tidy long rows via native DataFrame.unpivot."""
    return df_wide.unpivot(
        ids=["dept"],
        values=list(months),
        variableColumnName="month",
        valueColumnName="sales",
    )


def main() -> None:
    spark = build_spark()

    monthly_sales = [
        ("Electronics", "Jan", 12000.0),
        ("Electronics", "Feb", 14500.0),
        ("Electronics", "Mar", 16000.0),
        ("Groceries", "Jan", 8500.0),
        ("Groceries", "Feb", 9200.0),
        ("Groceries", "Mar", 8900.0),
        ("Apparel", "Jan", 4500.0),
        ("Apparel", "Feb", 5100.0),
        ("Apparel", "Mar", 6300.0),
    ]
    df_narrow = spark.createDataFrame(monthly_sales, schema=SALES_SCHEMA)

    months = ["Jan", "Feb", "Mar"]
    df_pivoted = pivot_monthly_sales(df_narrow, months)
    df_unpivoted = unpivot_monthly_sales(df_pivoted, months)

    elec = df_pivoted.filter(F.col("dept") == "Electronics").first()
    assert elec["Jan"] == 12000.0 and elec["Mar"] == 16000.0
    assert df_unpivoted.count() == 9


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_pivoted.explain("formatted")` emits a single `HashAggregate` with `PivotFirst`
#   and zero preceding `Distinct` job when `months` is explicitly provided.
# - `df_unpivoted.explain("formatted")` compiles to an `Expand` projection (equivalent
#   to `stack(3, 'Jan', Jan, 'Feb', Feb, 'Mar', Mar)`) with zero shuffle.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Always pass `pivot(col, known_values)` in production pipelines; omitting `values`
#    executes an extra Spark action during DataFrame plan construction.
# 2. `df.unpivot()` (alias `df.melt()`, Spark 3.4+) is cleaner and less error-prone
#    than raw `F.expr("stack(...)")`, and requires all melted value columns to share a
#    common compatible data type.
# 3. For 1,000+ dynamic categories, avoid `pivot()` entirely—store as `MapType` or keep
#    the long normalized schema and let the BI/OLAP layer pivot at query time.

if __name__ == "__main__":
    main()
