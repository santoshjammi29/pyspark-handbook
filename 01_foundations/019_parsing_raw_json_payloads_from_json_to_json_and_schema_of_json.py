"""Scenario & Scale: Kafka JSON event parsing & serialization pipeline (300M events/day).

Business Context:
Parse raw JSON payload strings streamed from Kafka using an explicit `StructType`
schema inside JVM Jackson parsers, and serialize structured outputs via `to_json()`.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "019_JSON_Parsing_Serialization") -> SparkSession:
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
# - Calling `F.udf(lambda s: json.loads(s), ...)` in Python, or running
#   `schema_of_json` / `spark.read.json(..., inferSchema=True)` on every production batch.
# Otherwise:
# - Python `json.loads` UDFs serialize every payload across Py4J/pickle (10x-30x slower).
# - Production schema inference triggers an extra full scan and risks schema drift
#   (e.g. `amount` inferred as `LONG` in one batch and `DOUBLE` in the next).


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Explicit `StructType` with `F.from_json()` and `F.to_json()`.
KAFKA_RAW_SCHEMA = T.StructType(
    [
        T.StructField("kafka_offset", T.StringType(), nullable=False),
        T.StructField("payload", T.StringType(), nullable=True),
    ]
)

ORDER_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), nullable=True),
        T.StructField(
            "customer",
            T.StructType(
                [
                    T.StructField("id", T.StringType(), nullable=True),
                    T.StructField("email", T.StringType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        T.StructField("amount", T.DoubleType(), nullable=True),
        T.StructField("tags", T.ArrayType(T.StringType()), nullable=True),
    ]
)


def parse_and_reserialize_orders(df_kafka: DataFrame) -> DataFrame:
    """Parse JSON payloads with an explicit schema and re-serialize normalized JSON."""
    return (
        df_kafka.withColumn("data", F.from_json(F.col("payload"), ORDER_PAYLOAD_SCHEMA))
        .select(
            "kafka_offset",
            F.col("data.order_id").alias("order_id"),
            F.col("data.customer.email").alias("customer_email"),
            F.col("data.amount").alias("amount"),
            F.col("data.tags").alias("tags"),
            F.to_json(F.col("data")).alias("exported_json"),
        )
    )


def main() -> None:
    spark = build_spark()

    kafka_stream = [
        (
            "K-001",
            '{"order_id": "ORD-1", "customer": {"id": "C-10", "email": "c10@mail.com"}, '
            '"amount": 149.99, "tags": ["sale", "prime"]}',
        ),
        (
            "K-002",
            '{"order_id": "ORD-2", "customer": {"id": "C-20", "email": "c20@mail.com"}, '
            '"amount": 89.50, "tags": ["clearance"]}',
        ),
        (
            "K-003",
            '{"order_id": "ORD-3", "customer": {"id": "C-30", "email": "c30@mail.com"}, '
            '"amount": 420.00, "tags": []}',
        ),
    ]
    df_kafka = spark.createDataFrame(kafka_stream, schema=KAFKA_RAW_SCHEMA)

    # Dev-time helper only: derive a starter DDL string from one sample literal
    sample_json = df_kafka.select("payload").first()[0]
    inferred_ddl = spark.range(1).select(F.schema_of_json(F.lit(sample_json))).first()[0]
    assert "order_id" in inferred_ddl

    df_parsed = parse_and_reserialize_orders(df_kafka)
    k001 = df_parsed.filter(F.col("kafka_offset") == "K-001").first()
    assert k001["order_id"] == "ORD-1" and k001["customer_email"] == "c10@mail.com"
    assert k001["amount"] == 149.99


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_parsed.explain("formatted")` shows native JVM `JsonToStructs` and `StructsToJson`
#   expressions inside `Project`—zero Python UDF serialization overhead.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `F.from_json` silently ignores extra JSON keys not in `ORDER_PAYLOAD_SCHEMA`; add a
#    `unparsed_ext STRING` / `VARIANT` or `_corrupt_record` field if unknown keys matter.
# 2. Keep `schema_of_json()` strictly for offline schema discovery; never invoke `.first()`
#    to infer schemas dynamically inside a production streaming micro-batch.
# 3. For deeply dynamic JSON with changing keys in Spark 4.0+, store the column as
#    `VARIANT` (`F.parse_json`) instead of brittle rigid `StructType` definitions.

if __name__ == "__main__":
    main()
