"""Custom PySpark UDFs vs Vectorized Arrow/Pandas UDFs (PySpark 4.0+).

Scenario & Scale:
    A credit scoring pipeline evaluates 50M+ customer accounts daily under a
    15-minute batch SLA. Row-by-row Python UDFs cause severe CPU throttling and
    worker GC spikes; native Catalyst expressions or Arrow-backed Pandas UDFs
    are required to sustain throughput.
"""

from collections.abc import Iterator

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Row-by-row Python @F.udf with unsafe division.
# Otherwise: Py4J/pickle row serialization stalls executors 10-50x, disables
# Catalyst optimizations, and raises ZeroDivisionError or SparkArithmeticException
# in Spark 4.0 ANSI mode when income == 0.
@F.udf(returnType=T.DoubleType())
def calculate_risk_row_udf(credit_score, debt, income):
    if credit_score is None or income is None:
        return 0.0
    dti = debt / income  # Crashes on income == 0
    return round((dti * 0.6) + (((850 - credit_score) / 550.0) * 0.4), 4)


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice 1: Prefer native Spark SQL expressions (zero serialization).
def transform_credit_risk_native(df: DataFrame) -> DataFrame:
    """Compute credit risk score using pure Catalyst ANSI-safe expressions."""
    safe_income = F.try_cast(F.col("annual_income"), T.DoubleType())
    safe_debt = F.try_cast(F.col("debt_amount"), T.DoubleType())
    safe_score = F.try_cast(F.col("credit_score"), T.DoubleType())

    # try_divide returns NULL instead of throwing on division by zero in ANSI mode
    dti = F.coalesce(F.try_divide(safe_debt, safe_income), F.lit(0.0))
    score_factor = F.coalesce((F.lit(850.0) - safe_score) / F.lit(550.0), F.lit(0.0))

    return df.withColumn(
        "risk_score",
        F.round((dti * F.lit(0.6)) + (score_factor * F.lit(0.4)), 4),
    )


# ✅ Good Practice 2: When Python libraries (SciPy/NumPy) are mandatory, use an
# Arrow-backed scalar iterator Pandas UDF (Iterator[pd.Series] / tuple batch).
@F.pandas_udf(T.DoubleType())
def adjust_risk_curve_iter(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """Single-column Iterator[pd.Series] UDF reusing per-partition state."""
    # Heavy lookup tables or model weights initialize once per partition here
    calibration_bias = 0.015
    for risk_batch in batches:
        yield (risk_batch.fillna(0.0) + calibration_bias).clip(lower=0.0, upper=1.0).round(4)


@F.pandas_udf(T.DoubleType())
def calculate_risk_arrow_iter(
    batches: Iterator[tuple[pd.Series, pd.Series, pd.Series]],
) -> Iterator[pd.Series]:
    """Multi-column vectorized Arrow iterator UDF with zero-division guard."""
    for credit_score, debt, income in batches:
        safe_income = income.where(income > 0)
        dti = (debt / safe_income).fillna(0.0)
        score_factor = ((850.0 - credit_score) / 550.0).fillna(0.0)
        yield ((dti * 0.6) + (score_factor * 0.4)).round(4)


def transform_credit_risk_arrow(df: DataFrame) -> DataFrame:
    """Apply Arrow-vectorized risk calculation and curve calibration."""
    return (
        df.withColumn(
            "risk_vec",
            calculate_risk_arrow_iter(
                F.col("credit_score"),
                F.col("debt_amount"),
                F.col("annual_income"),
            ),
        )
        .withColumn("risk_calibrated", adjust_risk_curve_iter(F.col("risk_vec")))
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - Native `transform_credit_risk_native`: `explain("formatted")` shows a single
#   `Project` inside `WholeStageCodegen`, enabling predicate pushdown & Tungsten.
# - Arrow UDF `transform_credit_risk_arrow`: shows `ArrowEvalPython` instead of
#   `BatchEvalPython` (standard UDF). In Spark UI SQL tab, check Arrow batch
#   sizes (`spark.sql.execution.arrow.maxRecordsPerBatch`, default 10,000).


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Catalyst treats both `@F.udf` and `@F.pandas_udf` as black boxes: filters
#    on UDF output columns cannot be pushed down to Parquet/Delta scans.
# 2. Iterator Pandas UDFs (`Iterator[pd.Series]`) prevent OOM on wide batches and
#    amortize model/config initialization once per partition instead of per batch.
# 3. Always enable `spark.sql.execution.arrow.pyspark.enabled = true` (default in
#    Spark 4.0) and size `maxRecordsPerBatch` carefully for wide string columns.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("011_udf_vs_arrow_pandas_udf")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    df_sample = spark.createDataFrame(
        [(1, 720.0, 12000.0, 95000.0), (2, 580.0, 8000.0, 0.0)],
        "account_id INT, credit_score DOUBLE, debt_amount DOUBLE, annual_income DOUBLE",
    )
    transform_credit_risk_native(df_sample).show()
    transform_credit_risk_arrow(df_sample).show()
