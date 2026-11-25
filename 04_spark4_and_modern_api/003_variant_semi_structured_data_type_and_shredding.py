"""
Module 04 / 003: Spark 4.0 VARIANT Semi-Structured Data Type & Shredding

Scenario & Scale:
    An IoT & clickstream ingestion gateway receives 80M events/hour with
    rapidly evolving JSON payloads (nested attributes, polymorphic fields where
    `latency` arrives as int or string, and schema drift across 300+ devices).

Goal:
    Replace slow raw JSON string parsing (`get_json_object` / rigid `from_json`)
    with Spark 4.0's binary `VARIANT` type (`parse_json`, `try_variant_get`,
    `schema_of_variant_agg`) and Delta/Parquet Variant Shredding for columnar
    predicate pushdown and sub-second path extraction.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark() -> SparkSession:
    """Build a Spark 4.0+ session configured for Variant shredding."""
    return (
        SparkSession.builder.appName("003_Variant_Semi_Structured_Type")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.variant.shredding.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# (2) Anti-Pattern: Storing raw JSON strings + repeated get_json_object / from_json
# ---------------------------------------------------------------------------
def anti_pattern_raw_json_strings(df: DataFrame) -> DataFrame:
    # ❌ Bad Practice: Keeping raw JSON strings and extracting multiple paths with
    # `get_json_object` or rigid `from_json` structs on every downstream query.
    # Otherwise: Spark re-parses the entire JSON string once per column expression
    # (3-10x CPU overhead), Parquet cannot shred/skip nested fields, and polymorphic
    # values (e.g., `10` vs `"10.5"`) either coerce the entire struct to NULL or fail.
    rigid_schema = T.StructType([T.StructField("device_id", T.StringType(), True)])
    return (
        df.withColumn("device_id", F.get_json_object(F.col("raw_json"), "$.device_id"))
        .withColumn("region", F.get_json_object(F.col("raw_json"), "$.meta.region"))
        .withColumn("parsed", F.from_json(F.col("raw_json"), rigid_schema))
    )


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Native VARIANT + try_variant_get + profiling
# ---------------------------------------------------------------------------
def good_practice_variant_pipeline(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    # ✅ Good Practice: Parse JSON once at Bronze ingestion into compact binary VARIANT,
    # extract typed scalars safely via `try_variant_get` (ANSI-safe on type drift), and
    # discover evolving schemas via `schema_of_variant_agg`.
    bronze_variant = df.withColumn("payload", F.parse_json(F.col("raw_json")))

    silver_typed = bronze_variant.select(
        F.col("event_id"),
        F.try_variant_get(F.col("payload"), "$.device_id", "STRING").alias("device_id"),
        F.try_variant_get(F.col("payload"), "$.meta.region", "STRING").alias("region"),
        F.try_variant_get(F.col("payload"), "$.metrics.latency_ms", "DOUBLE").alias(
            "latency_ms"
        ),
        F.col("payload"),
    )

    inferred_schema_df = bronze_variant.agg(
        F.schema_of_variant_agg(F.col("payload")).alias("unified_variant_ddl")
    )
    return silver_typed, inferred_schema_df


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - In `explain("formatted")`, Bronze ingestion shows `parse_json(raw_json#...)`
#   producing binary `variant` columns without `JsonToStructs` per-query overhead.
# - When Variant Shredding is active on Parquet/Delta tables, `FileScan parquet`
#   pushes sub-column filters (e.g., `try_variant_get(payload, '$.meta.region', 'STRING')
#   = 'eu-west'`) directly into shredded Parquet statistics (`PushedFilters`) so cold
#   row groups are skipped without deserializing unreferenced JSON keys.
# - In Spark UI SQL -> WholeStageCodegen / Project metrics, compare task CPU time against
#   `get_json_object` baselines: shredded Variant reads reduce CPU time by avoiding
#   repeated Jackson JSON tokenization across executors.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `variant_get` vs `try_variant_get`: Under Spark 4.0 ANSI=true, `variant_get` throws
#    an exception if a field cannot cast to the target type (e.g., `"err"` -> `DOUBLE`),
#    whereas `try_variant_get` safely returns `NULL` for downstream DQ handling.
# 2. No Direct Equality/Grouping on VARIANT: You cannot `GROUP BY`, `ORDER BY`, or join
#    directly on a `VARIANT` column; always extract a typed scalar (`try_variant_get` or
#    SQL `payload:device_id::STRING`) first.
# 3. 16 MiB Value Limit: Each individual `VARIANT` value is capped at 16 MiB; oversized
#    batch payloads should be split into one row per event before calling `parse_json`.
# 4. Schema Discovery at Scale: Run `schema_of_variant_agg` over bounded sample windows
#    to auto-generate Silver `STRUCT` promotion DDLs without scanning full historical data.


if __name__ == "__main__":
    spark = build_spark()
    try:
        events = [
            ("e1", '{"device_id":"d-10","meta":{"region":"eu-west"},"metrics":{"latency_ms":12.4}}'),
            ("e2", '{"device_id":"d-11","meta":{"region":"us-east"},"metrics":{"latency_ms":"timeout"}}'),
        ]
        raw = spark.createDataFrame(events, ["event_id", "raw_json"])
        typed_df, schema_df = good_practice_variant_pipeline(raw)
        typed_df.select("event_id", "device_id", "region", "latency_ms").show(truncate=False)
        schema_df.show(truncate=False)
    finally:
        spark.stop()
