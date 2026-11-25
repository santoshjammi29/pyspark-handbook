"""CSV Ingestion Edge Cases: Multiline, Escaped Quotes & Custom Delimiters.

Scenario & Scale:
    A financial auditing pipeline ingests 20 GB/day of legacy bank statement
    extracts using pipe (`|`) delimiters, backslash-escaped quotes, embedded
    newlines inside notes, and occasional malformed rows under a 20-minute SLA.
"""

import tempfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


BANK_CSV_SCHEMA = T.StructType([
    T.StructField("txn_id", T.StringType(), nullable=True),
    T.StructField("customer_name", T.StringType(), nullable=True),
    T.StructField("raw_amount", T.StringType(), nullable=True),
    T.StructField("notes", T.StringType(), nullable=True),
    T.StructField("raw_txn_date", T.StringType(), nullable=True),
    T.StructField("_corrupt_record", T.StringType(), nullable=True),
])


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Reading complex CSVs without explicit schema or multiline/escape
# configuration (`spark.read.option("header", "true").csv(uri)`).
# Otherwise: Embedded newlines split single records into corrupt multi-row garbage,
# unescaped quotes shift columns silently, and schema inference wastes an extra pass.
def read_bank_csv_bad(spark: SparkSession, source_uri: str) -> DataFrame:
    return spark.read.option("header", "true").csv(source_uri)


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Enforce explicit StructType, explicit escape/quote/multiLine
# options, PERMISSIVE corrupt-record capture, and ANSI-safe `try_cast` parsing.
def read_bank_csv(spark: SparkSession, source_uri: str) -> DataFrame:
    """Read pipe-delimited multiline CSV with explicit schema and quarantine col."""
    return (
        spark.read.format("csv")
        .schema(BANK_CSV_SCHEMA)
        .option("header", "true")
        .option("sep", "|")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", "\\")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(source_uri)
    )


def transform_parse_bank_csv(df: DataFrame) -> DataFrame:
    """Cast CSV strings safely under Spark 4.0 ANSI mode and flag invalid rows."""
    return (
        df.withColumn("txn_id_num", F.try_cast(F.col("txn_id"), T.LongType()))
        .withColumn("amount", F.try_cast(F.col("raw_amount"), T.DecimalType(14, 2)))
        .withColumn("txn_date", F.try_to_date(F.col("raw_txn_date"), "yyyy-MM-dd"))
        .withColumn(
            "is_valid",
            F.col("_corrupt_record").isNull()
            & F.col("txn_id_num").isNotNull()
            & F.col("amount").isNotNull()
            & F.col("txn_date").isNotNull(),
        )
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows `FileScan csv [...] ReadSchema` matching the
#   explicit StructType with zero preceding schema-inference job in Spark UI.
# - With `multiLine=true`, Spark UI shows 1 task per CSV file (file splitting is
#   disabled); keep raw CSV files <= 256 MB or pre-split upstream.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `multiLine=true` disables byte-offset splitting: a single 10 GB CSV file
#    runs on 1 executor core and frequently causes executor OOM.
# 2. If you omit `_corrupt_record` in `.select(...)` during `PERMISSIVE` reads,
#    Spark throws an analysis error unless `_corrupt_record` is cached or selected.
# 3. Always parse raw numeric/date CSV columns as STRING first and convert via
#    `F.try_cast` / `F.try_to_date` so one dirty row does not abort the whole batch.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("044_csv_edge_cases")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    sample_csv = (
        "txn_id|customer_name|raw_amount|notes|raw_txn_date\n"
        '101|Alice Corp|1500.50|"Invoice #1234\nSplit payment"|2025-01-01\n'
        '102|Bob & Co|450.00|"Payment for \\"consulting\\""|2025-01-02\n'
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        csv_file = Path(tmp_dir) / "statements.csv"
        csv_file.write_text(sample_csv, encoding="utf-8")
        df_raw = read_bank_csv(spark, tmp_dir)
        transform_parse_bank_csv(df_raw).show(truncate=False)
