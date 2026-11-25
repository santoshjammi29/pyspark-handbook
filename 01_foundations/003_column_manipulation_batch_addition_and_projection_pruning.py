"""Scenario & Scale: SaaS telemetry feature enrichment pipeline (200M events/day, 15m SLA).

Business Context:
Enrich raw client telemetry logs with derived duration, engagement flags, and platform
tags while pruning unused columns early and avoiding deep Catalyst logical plan trees.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "003_Batch_Column_Ops_Projection_Pruning") -> SparkSession:
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
#   for col_name, expr in derived_cols.items():
#       df = df.withColumn(col_name, expr)
#   df = df.withColumn("rate", F.col("actions_count") / (F.col("duration_seconds") / 60.0))
# Otherwise:
# - Chaining 30+ `.withColumn()` calls in a loop builds a deeply nested `Project` tree,
#   causing quadratic Catalyst analyzer/optimizer overhead and driver StackOverflow/GC pauses.
# - Standard `/` division raises `DIVIDE_BY_ZERO` under ANSI mode when `duration_seconds == 0`.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Single-node `withColumns()`, `withColumnsRenamed()`, and `try_divide()`.
TELEMETRY_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=False),
        T.StructField("user_id", T.StringType(), nullable=False),
        T.StructField("client_type", T.StringType(), nullable=False),
        T.StructField("client_os", T.StringType(), nullable=False),
        T.StructField("duration_seconds", T.IntegerType(), nullable=False),
        T.StructField("actions_count", T.IntegerType(), nullable=False),
        T.StructField("event_date", T.StringType(), nullable=False),
    ]
)

RENAME_MAP = {
    "event_id": "id",
    "user_id": "account_id",
    "duration_seconds": "duration_sec",
}


def enrich_telemetry(df: DataFrame) -> DataFrame:
    """Apply batch column enrichments, renames, and column pruning in minimal Catalyst nodes."""
    duration_min = F.try_divide(F.col("duration_seconds"), F.lit(60.0))

    enrichments = {
        "duration_minutes": F.round(duration_min, 2),
        "is_high_engagement": (
            (F.col("duration_seconds") > 300) & (F.col("actions_count") > 5)
        ),
        "platform_tag": F.concat_ws("::", F.upper(F.col("client_type")), F.col("client_os")),
        "actions_per_minute": F.round(F.try_divide(F.col("actions_count"), duration_min), 2),
    }

    return (
        df.withColumns(enrichments)
        .withColumnsRenamed(RENAME_MAP)
        .drop("client_type", "client_os")
    )


def main() -> None:
    spark = build_spark()

    telemetry_data = [
        ("EVT-1", "user_101", "mobile_app", "iOS", 120, 2, "2023-11-01"),
        ("EVT-2", "user_102", "web_browser", "Chrome", 450, 8, "2023-11-01"),
        ("EVT-3", "user_103", "mobile_app", "Android", 0, 1, "2023-11-01"),
        ("EVT-4", "user_104", "api_client", "Postman", 600, 15, "2023-11-01"),
    ]
    df_telemetry = spark.createDataFrame(telemetry_data, schema=TELEMETRY_SCHEMA)
    df_final = enrich_telemetry(df_telemetry)

    assert "id" in df_final.columns and "client_os" not in df_final.columns
    assert df_final.filter(F.col("id") == "EVT-3").select("actions_per_minute").first()[0] is None
    assert df_final.filter(F.col("is_high_engagement")).count() == 2


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_final.explain("formatted")` collapses all `withColumns` + `withColumnsRenamed` +
#   `drop` into a single `Project` node with zero shuffle (`Exchange`) stages.
# - When backed by Parquet/Delta, Catalyst's `ColumnPruning` rule pushes projected
#   columns into `FileScan` (`ReadSchema`), skipping unreferenced columns at I/O time.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Prefer `df.withColumns({...})` or a single `df.select(...)` over looping `.withColumn()`;
#    also prefer `df.withColumnsRenamed({...})` (Spark 3.4+) over looping `withColumnRenamed`.
# 2. Note that `withColumns` expressions cannot reference another newly added key from
#    the same dictionary—reuse a Python `Column` variable like `duration_min` instead.
# 3. `F.try_divide(num, denom)` returns `NULL` on division by zero in Spark 4.0 ANSI mode
#    rather than throwing `ArithmeticException: divide by zero`.

if __name__ == "__main__":
    main()
