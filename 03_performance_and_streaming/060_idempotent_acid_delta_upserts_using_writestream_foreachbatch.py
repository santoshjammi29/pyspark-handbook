"""Scenario & Scale: Payments streaming ledger processing purchases and refunds.

Executes idempotent ACID MERGE INTO upserts across 10k+ updates/sec via
writeStream.foreachBatch, surviving micro-batch retries without duplicates.
"""

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.streaming import StreamingQuery


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Merging a raw micro-batch directly into the target table
# without deduplicating duplicate keys inside the batch, or writing append-only
# logs for mutable transaction statuses.
# Otherwise: Duplicate primary keys within a single micro-batch throw a fatal
# MERGE cardinality violation (`Multiple source rows matched`), crashing the
# stream; non-idempotent side effects corrupt ledger balances on retry.
def bad_non_deduped_merge(micro_batch_df: DataFrame, target_table: str) -> None:
    micro_batch_df.createOrReplaceTempView("raw_updates")
    micro_batch_df.sparkSession.sql(
        f"""
        MERGE INTO {target_table} AS t
        USING raw_updates AS s
        ON t.txn_id = s.txn_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Deduplicate each micro-batch deterministically on primary
# key ordered by latest event timestamp, then execute an idempotent SQL MERGE
# INTO (compatible with Delta Lake and Apache Iceberg v2/v3 catalogs).
def deduplicate_micro_batch(micro_batch_df: DataFrame) -> DataFrame:
    """Keep the latest state per txn_id within a micro-batch."""
    window_spec = Window.partitionBy("txn_id").orderBy(
        F.col("updated_at").desc()
    )
    return (
        micro_batch_df.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def upsert_micro_batch_sql(
    micro_batch_df: DataFrame,
    batch_id: int,
    target_table: str,
) -> None:
    """Idempotent foreachBatch handler executing deduplicated ACID MERGE."""
    if micro_batch_df.isEmpty():
        return

    deduped = deduplicate_micro_batch(micro_batch_df)
    view_name = f"_stg_ledger_batch_{batch_id}"
    deduped.createOrReplaceTempView(view_name)
    try:
        micro_batch_df.sparkSession.sql(
            f"""
            MERGE INTO {target_table} AS target
            USING {view_name} AS source
            ON target.txn_id = source.txn_id
            WHEN MATCHED AND source.updated_at >= target.updated_at THEN
                UPDATE SET
                    target.amount = source.amount,
                    target.status = source.status,
                    target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (txn_id, customer_id, amount, status, updated_at)
                VALUES (
                    source.txn_id,
                    source.customer_id,
                    source.amount,
                    source.status,
                    source.updated_at
                )
            """
        )
    finally:
        micro_batch_df.sparkSession.catalog.dropTempView(view_name)


def start_idempotent_upsert_stream(
    df_stream: DataFrame,
    target_table: str,
    checkpoint_uri: str,
) -> StreamingQuery:
    """Start streaming query writing idempotent ACID upserts via foreachBatch."""
    return (
        df_stream.writeStream.outputMode("update")
        .option("checkpointLocation", checkpoint_uri)
        .foreachBatch(
            lambda batch_df, batch_id: upsert_micro_batch_sql(
                batch_df, batch_id, target_table
            )
        )
        .start()
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# Inside foreachBatch, deduped.explain("formatted") shows:
#   Exchange hashpartitioning(txn_id) -> Sort [txn_id, updated_at DESC]
#   -> Window [row_number()] -> Filter (_rn = 1)
# Spark UI -> SQL tab:
#   - Each micro-batch appears as its own SQL execution ID running MERGE INTO.
#   - Check `numTargetRowsUpdated` / `numTargetFilesRemoved` to spot write
#     amplification; partition/cluster target_table by join key or date.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. foreachBatch guarantees at-least-once delivery on retries: re-running the
#    same batch_id must yield the identical target state (hence the monotonic
#    `source.updated_at >= target.updated_at` guard).
# 2. Never reuse a hardcoded global temp view name if concurrent streams write
#    in the same SparkSession; scope view names by batch_id or table name.
# 3. Calling micro_batch_df.count() before write triggers double evaluation of
#    upstream transforms unless cached or avoided via isEmpty() / SQL metrics.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("060_ForeachBatchACIDUpserts")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    df_sample = spark.createDataFrame(
        [
            ("TXN-101", "CUST-1", 100.0, "PENDING", "2025-01-15 10:00:00"),
            ("TXN-101", "CUST-1", 100.0, "SETTLED", "2025-01-15 10:15:00"),
            ("TXN-103", "CUST-3", 75.0, "SETTLED", "2025-01-15 10:20:00"),
        ],
        "txn_id STRING, customer_id STRING, amount DOUBLE, "
        "status STRING, updated_at STRING",
    )
    deduplicate_micro_batch(df_sample).show(truncate=False)
