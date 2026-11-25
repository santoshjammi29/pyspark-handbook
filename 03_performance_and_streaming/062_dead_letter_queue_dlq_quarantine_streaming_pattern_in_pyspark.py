"""Scenario & Scale: Real-time clickstream pipeline ingesting 10k events/sec.

Isolates malformed JSON and business-rule violations into a Quarantine DLQ
table without halting analytics, enabling audited repair and zero-loss replay.
"""


from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


PAYLOAD_SCHEMA = T.StructType([
    T.StructField("user_id", T.StringType(), nullable=True),
    T.StructField("amount", T.DoubleType(), nullable=True),
])


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Using FAILFAST parsing or dropping null rows silently after
# from_json(), or using unsafe .cast("double") under Spark 4.0 ANSI mode.
# Otherwise: FAILFAST crashes the entire 24/7 stream on a single bad client
# payload; unsafe .cast() raises NumberFormatException under ANSI mode; silent
# filtering causes undetected revenue/clickstream data loss.
def bad_fragile_json_parse(df_raw: DataFrame) -> DataFrame:
    return df_raw.select(
        "event_id",
        F.get_json_object("raw_payload", "$.amount")
        .cast("double")  # Throws CAST_INVALID_INPUT in Spark 4.0 ANSI mode
        .alias("amount"),
    ).dropna(subset=["amount"])  # Silently drops corrupt records


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Route incoming records into clean Silver vs Quarantine DLQ
# using from_json + F.try_cast() for ANSI-safe repair/replay and reconciliation.
def route_clean_and_dlq(df_incoming: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split raw payloads into (clean_silver_df, dlq_quarantine_df)."""
    df_classified = (
        df_incoming.withColumn(
            "parsed", F.from_json(F.col("raw_payload"), PAYLOAD_SCHEMA)
        )
        .withColumn(
            "error_reason",
            F.when(
                F.col("parsed").isNull() | F.col("parsed.user_id").isNull(),
                F.lit("MALFORMED_JSON_SYNTAX"),
            )
            .when(
                F.col("parsed.amount").isNull() | (F.col("parsed.amount") <= 0),
                F.lit("INVALID_NON_POSITIVE_AMOUNT"),
            )
            .otherwise(F.lit(None).cast("string")),
        )
    )

    df_clean = df_classified.filter(F.col("error_reason").isNull()).select(
        F.col("event_id"),
        F.col("parsed.user_id").alias("user_id"),
        F.col("parsed.amount").alias("amount"),
        F.current_timestamp().alias("processed_at"),
    )

    df_dlq = df_classified.filter(F.col("error_reason").isNotNull()).select(
        F.col("event_id"),
        F.col("raw_payload"),
        F.col("error_reason"),
        F.lit("PENDING_TRIAGE").alias("dlq_status"),
        F.current_timestamp().alias("quarantined_at"),
    )
    return df_clean, df_dlq


def repair_dlq_records(df_dlq_pending: DataFrame) -> DataFrame:
    """Repair quarantined rows safely using try_cast under ANSI mode."""
    parsed_amt = F.try_cast(
        F.get_json_object(F.col("raw_payload"), "$.amount"), "double"
    )
    return df_dlq_pending.filter(
        F.col("dlq_status") == "PENDING_TRIAGE"
    ).select(
        F.col("event_id"),
        F.when(
            F.col("error_reason") == "MALFORMED_JSON_SYNTAX",
            F.lit("U_UNKNOWN_FALLBACK"),
        )
        .otherwise(F.get_json_object(F.col("raw_payload"), "$.user_id"))
        .alias("user_id"),
        F.when(
            F.col("error_reason") == "INVALID_NON_POSITIVE_AMOUNT",
            F.coalesce(F.abs(parsed_amt), F.lit(0.0)),
        )
        .otherwise(F.lit(0.0))
        .alias("amount"),
        F.current_timestamp().alias("processed_at"),
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_clean.explain("formatted") / df_dlq.explain("formatted"):
#   When invoked inside foreachBatch, persist/cache the classified batch
#   (StorageLevel.MEMORY_AND_DISK) before splitting into Silver + DLQ sinks so
#   from_json() is NOT evaluated twice!
# Spark UI -> SQL tab:
#   - Verify `InMemoryTableScan` feeds both Silver append and DLQ append jobs
#     without duplicating upstream Kafka/file reads.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. Always preserve `raw_payload`, `error_reason`, and ingestion timestamp in
#    the DLQ so upstream bug fixes can replay exact historical events.
# 2. Emit a DLQ ratio metric inside foreachBatch (dlq_count / batch_count) and
#    trip a circuit-breaker alert if >5% of a batch fails (schema break).
# 3. Under Spark 4.0 ANSI mode, use `F.try_cast` when parsing unvalidated raw
#    JSON strings during DLQ repair so bad strings yield NULL instead of aborting.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("062_DLQQuarantinePattern")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    sample = [
        ("E-01", '{"user_id": "U1", "amount": 150.0}'),
        ("E-02", "CORRUPTED_NON_JSON_STRING"),
        ("E-03", '{"user_id": "U2", "amount": -9999.0}'),
        ("E-04", '{"user_id": "U3", "amount": 85.0}'),
    ]
    df_raw = spark.createDataFrame(sample, "event_id STRING, raw_payload STRING")
    df_clean, df_dlq = route_clean_and_dlq(df_raw)
    df_repaired = repair_dlq_records(df_dlq)
    assert df_clean.count() + df_repaired.count() == df_raw.count()
    df_clean.show(truncate=False)
    df_dlq.show(truncate=False)
