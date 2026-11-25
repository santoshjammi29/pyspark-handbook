"""Change Data Capture (CDC) Ingestion from Debezium/Kafka into Delta Lake.

Scenario & Scale:
    A transactional MySQL inventory cluster streams 50k+ row mutations/min via
    Debezium Kafka envelopes (`before`, `after`, `op`, `ts_ms`). Out-of-order
    micro-batch deliveries must reconstruct exact OLTP state with hard deletes.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.window import Window


DEBEZIUM_SCHEMA = T.StructType([
    T.StructField(
        "before",
        T.StructType([
            T.StructField("sku", T.StringType(), True),
            T.StructField("stock", T.IntegerType(), True),
            T.StructField("price", T.DoubleType(), True),
        ]),
        True,
    ),
    T.StructField(
        "after",
        T.StructType([
            T.StructField("sku", T.StringType(), True),
            T.StructField("stock", T.IntegerType(), True),
            T.StructField("price", T.DoubleType(), True),
        ]),
        True,
    ),
    T.StructField("op", T.StringType(), True),
    T.StructField("ts_ms", T.LongType(), True),
])


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Merging raw Debezium batches directly without deduplicating
# by `(primary_key, ts_ms)` or ignoring `before` payload on `op == 'd'` deletes.
# Otherwise: Multiple events for one SKU within a micro-batch crash Delta `MERGE`,
# and hard deletes have `after == null`, leaving orphaned `sku = NULL` rows.
def transform_cdc_unsafe_bad(df_parsed: DataFrame) -> DataFrame:
    return df_parsed.select("after.*", "op", "ts_ms")  # Drops key on delete & leaves dupes


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Parse with explicit `DEBEZIUM_SCHEMA`, coalesce `after`/`before`
# keys, deduplicate by latest `ts_ms` (and LSN/sequence tie-breaker), and execute
# conditional `whenMatchedDelete` / `whenMatchedUpdate` / `whenNotMatchedInsert`.
def transform_parse_and_dedup_cdc(df_raw_json: DataFrame) -> DataFrame:
    """Decode Debezium JSON envelopes and retain only latest state per SKU."""
    df_flat = (
        df_raw_json.withColumn("env", F.from_json(F.col("raw_json"), DEBEZIUM_SCHEMA))
        .select(
            F.coalesce(F.col("env.after.sku"), F.col("env.before.sku")).alias("sku"),
            F.try_cast(F.col("env.after.stock"), T.IntegerType()).alias("stock"),
            F.try_cast(F.col("env.after.price"), T.DecimalType(12, 2)).alias("price"),
            F.col("env.op").alias("op"),
            F.col("env.ts_ms").alias("ts_ms"),
        )
        .filter(F.col("sku").isNotNull())
    )

    win_latest = Window.partitionBy("sku").orderBy(F.col("ts_ms").desc(), F.col("op").desc())
    return (
        df_flat.withColumn("_rn", F.row_number().over(win_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def apply_cdc_delta_merge(
    spark: SparkSession, target_uri: str, df_latest_cdc: DataFrame
) -> None:
    """Apply deduplicated CDC inserts, updates, and hard deletes into Delta."""
    from delta.tables import DeltaTable

    dt_target = DeltaTable.forPath(spark, target_uri)
    (
        dt_target.alias("target")
        .merge(source=df_latest_cdc.alias("source"), condition="target.sku = source.sku")
        .whenMatchedDelete(condition="source.op = 'd'")
        .whenMatchedUpdate(
            condition="source.ts_ms >= target.last_ts",
            set={
                "stock": F.col("source.stock"),
                "price": F.col("source.price"),
                "last_ts": F.col("source.ts_ms"),
            },
        )
        .whenNotMatchedInsert(
            condition="source.op != 'd'",
            values={
                "sku": F.col("source.sku"),
                "stock": F.col("source.stock"),
                "price": F.col("source.price"),
                "last_ts": F.col("source.ts_ms"),
            },
        )
        .execute()
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows `Window` (`row_number` partitioned by `sku`)
#   preceding the Delta `MERGE`; inside `foreachBatch`, keep micro-batches sized
#   (e.g., `maxOffsetsPerTrigger`) so Window shuffle stays in memory without spill.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Always guard `whenMatchedUpdate` with `source.ts_ms >= target.last_ts` so
#    late/out-of-order Kafka replays never overwrite newer state with stale rows.
# 2. If two mutations share identical millisecond `ts_ms`, break ties using
#    Debezium's binlog file+pos / WAL LSN (`source.pos` > `target.pos`).
# 3. For `op = 'r'` (initial Debezium snapshot), treat it identically to `'c'`
#    insert; use `condition="source.op != 'd'"` on `whenNotMatchedInsert` so
#    tombstone deletes for already-absent keys are safely ignored.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("071_debezium_cdc")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    events = [
        ('{"before": null, "after": {"sku": "SKU-101", "stock": 100, "price": 20.0}, "op": "c", "ts_ms": 1700560000}',),
        ('{"before": {"sku": "SKU-101", "stock": 100, "price": 20.0}, "after": {"sku": "SKU-101", "stock": 80, "price": 20.0}, "op": "u", "ts_ms": 1700560500}',),
        ('{"before": {"sku": "SKU-102", "stock": 50, "price": 45.0}, "after": null, "op": "d", "ts_ms": 1700560800}',),
    ]
    df_in = spark.createDataFrame(events, "raw_json STRING")
    transform_parse_and_dedup_cdc(df_in).show(truncate=False)
