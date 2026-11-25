"""Scenario & Scale: Payments gateway Bronze-to-Silver ingestion (50M txns/day, 10m SLA).

Business Context:
External payment feeds arrive with currency symbols, locale-formatted dates, and
corrupted strings. Under Spark 4.0+ ANSI mode (enabled by default), unsafe casts crash
the entire batch; dirty rows must be coerced cleanly to NULL and routed to quarantine.
"""

from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "002_Schema_Enforcement_Try_Cast") -> SparkSession:
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
#   df.withColumn("amount", F.col("raw_amount").cast("double"))
#   df.withColumn("ts", F.to_timestamp(F.col("raw_timestamp"), "yyyy-MM-dd HH:mm:ss"))
# Otherwise:
# - In Spark 4.0+ (`spark.sql.ansi.enabled=true` by default), `.cast()` and
#   `F.to_timestamp()` raise `CAST_INVALID_INPUT` / `CANNOT_PARSE_TIMESTAMP` on the
#   first malformed record, aborting a 50M-row batch job without isolating the bad row.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Sanitize strings and use `F.try_cast` + `F.try_to_timestamp`.
RAW_SCHEMA = T.StructType(
    [
        T.StructField("txn_id", T.StringType(), nullable=False),
        T.StructField("raw_amount", T.StringType(), nullable=True),
        T.StructField("raw_timestamp", T.StringType(), nullable=True),
        T.StructField("status", T.StringType(), nullable=True),
    ]
)


def parse_and_split_payments(df_raw: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Sanitize currency/timestamps with ANSI-safe try_* functions and split quarantine."""
    clean_amount_str = F.nullif(
        F.regexp_replace(F.col("raw_amount"), r"[^0-9.\-]", ""),
        F.lit(""),
    )

    df_typed = df_raw.withColumns(
        {
            "clean_amount": F.try_cast(clean_amount_str, T.DoubleType()),
            "txn_timestamp": F.coalesce(
                F.try_to_timestamp(F.col("raw_timestamp"), F.lit("yyyy-MM-dd HH:mm:ss")),
                F.try_to_timestamp(F.col("raw_timestamp"), F.lit("dd/MM/yyyy HH:mm")),
                F.try_to_timestamp(F.col("raw_timestamp"), F.lit("yyyy-MM-dd")),
            ),
        }
    )

    is_valid = F.col("clean_amount").isNotNull() & F.col("txn_timestamp").isNotNull()
    return df_typed.filter(is_valid), df_typed.filter(~is_valid)


def main() -> None:
    spark = build_spark()

    raw_feed = [
        ("TXN-001", "$1,450.50", "2023-10-01 10:30:00", "APPROVED"),
        ("TXN-002", "EUR 320.00", "01/10/2023 11:15", "APPROVED"),
        ("TXN-003", "CORRUPTED_AMT", "2023-10-02", "FAILED"),
        ("TXN-004", "$8,900.75", "2023-10-03 14:00:00", "APPROVED"),
        ("TXN-005", "N/A", "INVALID_DATE", "PENDING"),
    ]
    df_raw = spark.createDataFrame(raw_feed, schema=RAW_SCHEMA)

    df_valid, df_quarantine = parse_and_split_payments(df_raw)

    assert df_valid.count() == 3
    assert df_quarantine.count() == 2


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_valid.explain("formatted")` shows a single `Project` containing `try_cast` and
#   `try_to_timestamp` expressions followed by `Filter (isnotnull(...) AND ...)`.
# - Both outputs should share a single cached/persisted `df_typed` scan in production
#   so Catalyst does not re-read and re-parse Bronze files twice.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Spark 4.0 flipped `spark.sql.ansi.enabled` to `true` by default: `cast()` and
#    `to_timestamp()` fail fast on invalid input, whereas `try_cast()` and
#    `try_to_timestamp()` return NULL safely.
# 2. `F.regexp_replace` on invalid strings (e.g. `"..."` or `""`) still leaves an
#    unparseable string; pairing `F.nullif(..., F.lit(""))` with `F.try_cast` handles all.
# 3. Prefer `DECIMAL(18, 2)` over `DOUBLE` for monetary amounts in Silver/Gold tables
#    to eliminate IEEE-754 floating-point rounding drift.

if __name__ == "__main__":
    main()
