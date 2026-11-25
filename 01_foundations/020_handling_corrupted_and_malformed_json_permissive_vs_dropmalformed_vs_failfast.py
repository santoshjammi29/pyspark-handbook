"""Scenario & Scale: Third-party webhook JSON quarantine pipeline (50M payloads/day).

Business Context:
External HTTP feeds contain up to 5% malformed JSON (unquoted tokens, truncated text).
Ingest resiliently in `PERMISSIVE` mode with `_corrupt_record` so valid rows flow to
Silver while broken payloads route cleanly to a dead-letter audit table.
"""

from pathlib import Path
import tempfile

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "020_Corrupt_JSON_Quarantine") -> SparkSession:
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
# - Reading unvalidated vendor JSON with `option("mode", "DROPMALFORMED")` or without
#   `columnNameOfCorruptRecord` (or calling `sparkContext.parallelize` / hardcoded `/tmp/`).
# Otherwise:
# - `DROPMALFORMED` silently discards up to 5% of revenue/audit events with zero trace.
# - `FAILFAST` halts the entire 50M-row pipeline on a single broken JSON line.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Explicit schema + `PERMISSIVE` mode + `_corrupt_record` quarantine split.
QUARANTINE_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), nullable=True),
        T.StructField("user", T.StringType(), nullable=True),
        T.StructField("score", T.IntegerType(), nullable=True),
        T.StructField("_corrupt_record", T.StringType(), nullable=True),
    ]
)


def ingest_json_with_quarantine(
    spark: SparkSession,
    input_uri: str,
) -> tuple[DataFrame, DataFrame]:
    """Ingest JSON files permissively and separate valid records from dead-letter rows."""
    df_permissive = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(QUARANTINE_SCHEMA)
        .json(input_uri)
    )
    df_clean = df_permissive.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    df_dead_letter = df_permissive.filter(F.col("_corrupt_record").isNotNull()).select(
        "_corrupt_record"
    )
    return df_clean, df_dead_letter


def main() -> None:
    spark = build_spark()

    bad_json_lines = [
        '{"id": "EVT-1", "user": "alice", "score": 95}',
        '{"id": "EVT-2", "user": "bob", "score": INVALID_NUMBER}',
        '{"id": "EVT-3", "user": "charlie", "score": 82}',
        "NOT_EVEN_JSON_PAYLOAD",
        '{"id": "EVT-4", "user": "diana", "score": 100}',
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        json_file = Path(tmp_dir) / "events.jsonl"
        json_file.write_text("\n".join(bad_json_lines) + "\n", encoding="utf-8")

        df_clean, df_dead_letter = ingest_json_with_quarantine(spark, str(json_file))
        assert df_clean.count() == 3
        assert df_dead_letter.count() == 2


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_clean.explain("formatted")` shows `FileScan json` with `ReadSchema` including
#   `_corrupt_record` and `Filter isnull(_corrupt_record)`.
# - Note: Spark requires you to materialize/persist `df_permissive` (or write via two
#   sinks in a single job) if you do not want `FileScan json` executed twice.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `_corrupt_record` MUST be declared explicitly in `QUARANTINE_SCHEMA` when passing
#    `.schema(...)`; otherwise Spark drops corrupt records silently even in `PERMISSIVE` mode.
# 2. Calling `.select("_corrupt_record")` alone without any data column can fail on older
#    Spark versions unless `df_permissive` is cached or another column is projected.
# 3. For streaming cloud ingestion, compare `PERMISSIVE` `_corrupt_record` with
#    vendor rescued-data columns (`_rescued_data`) which also capture type-mismatch fields.

if __name__ == "__main__":
    main()
