"""Avro Binary Ingestion & Schema Registry Compatibility (PySpark 4.0+).

Scenario & Scale:
    An enterprise Kafka event bus streams 100k+ customer account updates/sec
    encoded in Apache Avro with Confluent Schema Registry 5-byte wire framing.
    Pipelines must decode binary payloads cleanly while surviving schema evolution.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


AVRO_SCHEMA_JSON = """{
  "type": "record",
  "name": "CustomerEvent",
  "namespace": "com.enterprise.events",
  "fields": [
    {"name": "customer_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "account_balance", "type": "double"},
    {"name": "event_timestamp", "type": "long"}
  ]
}"""


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Decoding Avro bytes with a row-by-row Python UDF (fastavro)
# or assuming raw Kafka `value` bytes lack the 5-byte Schema Registry header.
# Otherwise: Massive Py4J serialization overhead and immediate binary decode
# failures on magic-byte + schema-ID headers.
@F.udf(returnType=T.StringType())
def decode_avro_python_bad(payload: bytes) -> str:
    return payload.decode("utf-8", errors="ignore")  # Corrupts binary Avro payload


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Strip the 5-byte Confluent wire header (1 magic + 4 schema ID)
# and decode via native JVM `from_avro(..., mode="PERMISSIVE")` + ANSI casting.
def transform_decode_avro_confluent(
    df: DataFrame,
    avro_schema_json: str = AVRO_SCHEMA_JSON,
) -> DataFrame:
    """Decode Confluent-framed Avro bytes using Spark native Avro functions."""
    from pyspark.sql.avro.functions import from_avro

    df_stripped = df.withColumn(
        "schema_id",
        F.conv(F.hex(F.expr("substring(value, 2, 4)")), 16, 10).cast(T.LongType()),
    ).withColumn(
        "avro_payload",
        F.expr("substring(value, 6, length(value) - 5)"),
    )

    return (
        df_stripped.withColumn(
            "decoded",
            from_avro(
                F.col("avro_payload"),
                avro_schema_json,
                {"mode": "PERMISSIVE"},
            ),
        )
        .select(
            "schema_id",
            "decoded.*",
            F.col("decoded").isNull().alias("is_corrupt_avro"),
        )
        .withColumn(
            "event_ts",
            F.try_cast(F.col("event_timestamp"), T.TimestampType()),
        )
    )


def transform_package_avro_envelope(df: DataFrame) -> DataFrame:
    """Prepare structured events into key/value struct ready for `to_avro`."""
    return df.select(
        F.col("customer_id").alias("key"),
        F.struct(
            "customer_id",
            "event_type",
            "account_balance",
            "event_timestamp",
        ).alias("value"),
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows native Catalyst `AvroDataToCatalyst` /
#   `CatalystDataToAvro` expressions with zero Python worker subprocesses.
# - Watch for null `decoded` structs (`is_corrupt_avro == true`) in metrics when
#   an upstream producer emits a backwards-incompatible schema ID.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Requires `org.apache.spark:spark-avro_2.13:<spark_version>` on classpath.
# 2. If a single Kafka topic carries multiple Avro schema IDs, group/filter by
#    `schema_id` (bytes 2..5) or integrate ABRiS / Schema Registry REST resolution.
# 3. Backward-compatible Avro evolution requires new reader fields to specify a
#    `"default"` value in JSON (`{"name": "tier", "type": ["null", "string"], "default": null}`).


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("046_avro_schema_registry")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    df_events = spark.createDataFrame(
        [("CUST-101", "PURCHASE", 320.50, 1735689600)],
        "customer_id STRING, event_type STRING, account_balance DOUBLE, event_timestamp LONG",
    )
    transform_package_avro_envelope(df_events).show(truncate=False)
