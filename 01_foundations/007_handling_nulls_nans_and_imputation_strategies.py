"""Scenario & Scale: Clinical lab biomarker quality audit & imputation (20M panels/day).

Business Context:
Distinguish SQL NULLs from IEEE-754 float NaNs, audit missingness across all columns
in a single aggregation pass, drop incomplete panels, and impute department averages.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "007_Nulls_NaNs_Imputation") -> SparkSession:
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
#   df.fillna(120.0, subset=["systolic_bp"])  # or df.filter(F.col("systolic_bp").isNotNull())
# Otherwise:
# - `fillna()` and `isNotNull()` treat IEEE-754 `Float.NaN` as a valid non-null number!
#   Passing `NaN` into `F.avg()` or ML models poisons downstream aggregates with `NaN`.
# - Computing `dept_avgs` via `groupBy` + `join` triggers an extra table scan & shuffle.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Normalize NaN -> NULL first, audit in one pass, impute via Window.
CLINICAL_SCHEMA = T.StructType(
    [
        T.StructField("patient_id", T.StringType(), nullable=False),
        T.StructField("dept", T.StringType(), nullable=False),
        T.StructField("body_temp", T.DoubleType(), nullable=True),
        T.StructField("systolic_bp", T.DoubleType(), nullable=True),
        T.StructField("assessment", T.StringType(), nullable=True),
    ]
)


def audit_missing_values(df: DataFrame) -> DataFrame:
    """Count NULL and NaN values across all columns in a single distributed pass."""
    audit_exprs = [
        F.count(
            F.when(
                F.col(name).isNull()
                | (F.isnan(F.col(name)) if dtype in ("double", "float") else F.lit(False)),
                F.lit(1),
            )
        ).alias(f"{name}_missing_count")
        for name, dtype in df.dtypes
    ]
    return df.agg(*audit_exprs)


def clean_and_impute_clinical(df: DataFrame) -> DataFrame:
    """Convert NaN to NULL, enforce minimum column threshold, and impute dept mean."""
    df_normalized = df.withColumns(
        {
            "body_temp": F.nanvl(F.col("body_temp"), F.lit(None).cast(T.DoubleType())),
            "systolic_bp": F.nanvl(F.col("systolic_bp"), F.lit(None).cast(T.DoubleType())),
        }
    ).dropna(thresh=3)

    dept_window = Window.partitionBy("dept")
    dept_avg_bp = F.round(F.avg("systolic_bp").over(dept_window), 1)

    return df_normalized.withColumns(
        {
            "systolic_bp_imputed": F.coalesce(F.col("systolic_bp"), dept_avg_bp, F.lit(120.0)),
            "assessment_clean": F.coalesce(F.col("assessment"), F.lit("PENDING_REVIEW")),
        }
    )


def main() -> None:
    spark = build_spark()

    clinical_data = [
        ("P-01", "Metabolic", 98.6, 120.0, "NORMAL"),
        ("P-02", "Cardiac", float("nan"), 145.0, "ELEVATED"),
        ("P-03", "Metabolic", None, None, None),
        ("P-04", "Cardiac", 101.2, float("nan"), "CRITICAL"),
        ("P-05", "Metabolic", 97.9, 115.0, "NORMAL"),
        ("P-06", "Neurology", None, 130.0, None),
    ]
    df_clinical = spark.createDataFrame(clinical_data, schema=CLINICAL_SCHEMA)

    audit_row = audit_missing_values(df_clinical).first()
    df_imputed = clean_and_impute_clinical(df_clinical)

    assert audit_row["body_temp_missing_count"] == 3
    assert audit_row["systolic_bp_missing_count"] == 2
    assert df_imputed.count() == 5  # P-03 dropped (only 2 non-null columns)
    p04 = df_imputed.filter(F.col("patient_id") == "P-04").first()
    assert p04["systolic_bp_imputed"] == 145.0


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `audit_missing_values` compiles into a single two-phase `HashAggregate` scan.
# - Using `Window.partitionBy("dept")` for imputation requires a single shuffle by
#   `dept` rather than a self-join (`SortMergeJoin` / `BroadcastHashJoin`).

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `F.isnan()` only accepts `FLOAT`/`DOUBLE` columns; calling it on `STRING` or
#    `DECIMAL` raises an analysis exception—guard by checking `df.dtypes`.
# 2. `df.dropna(thresh=3)` only drops SQL `NULL`s, not `NaN`s; always convert `NaN`
#    to `NULL` via `F.nanvl(..., None)` or `F.when(F.isnan(...), None)` first.
# 3. `F.coalesce(col_a, col_b)` is a column expression for null fallback; do not
#    confuse it with `df.coalesce(n)`, which reduces DataFrame partition count.

if __name__ == "__main__":
    main()
