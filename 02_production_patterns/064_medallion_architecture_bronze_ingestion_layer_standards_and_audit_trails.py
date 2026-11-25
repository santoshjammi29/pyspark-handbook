"""Medallion Architecture: Bronze Ingestion Standards & Audit Trails.

Scenario & Scale:
    An enterprise lakehouse lands 150M daily raw vendor JSON/ERP invoices into
    an append-only Bronze Delta layer. Zero records may be dropped or mutated so
    Silver/Gold tables can always be deterministically recomputed on demand.
"""

import tempfile

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


RAW_INVOICE_SCHEMA = T.StructType([
    T.StructField("vendor_id", T.StringType(), nullable=True),
    T.StructField("invoice_no", T.StringType(), nullable=True),
    T.StructField("gross_amount", T.StringType(), nullable=True),
    T.StructField("status", T.StringType(), nullable=True),
])


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Dropping malformed rows, applying business filters, or using
# `mode("overwrite")` in Bronze without lineage columns or payload checksums.
# Otherwise: Raw source history is permanently destroyed, preventing replay after
# downstream logic bugs and failing financial/regulatory audit traceability.
def land_bronze_destructive_bad(df_raw: DataFrame, target_uri: str) -> None:
    df_raw.dropna().filter(F.col("status") == "APPROVED").write.format("delta").mode("overwrite").save(target_uri)


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Preserve raw payloads losslessly, attach standard audit
# lineage columns, compute SHA-256 payload checksums, and write append-only.
def transform_parse_and_audit_bronze(
    df_raw_strings: DataFrame,
    source_uri: str,
    batch_id: str,
) -> DataFrame:
    """Parse raw JSON payloads permissively and attach immutable Bronze lineage."""
    payload_cols = ["vendor_id", "invoice_no", "gross_amount", "status"]
    return (
        df_raw_strings.withColumn("parsed", F.from_json(F.col("raw_payload"), RAW_INVOICE_SCHEMA))
        .select("raw_payload", "parsed.*")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file_path", F.lit(source_uri))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn(
            "_raw_payload_checksum",
            F.sha2(F.to_json(F.struct(*payload_cols)), 256),
        )
    )


def write_bronze_append(df_bronze: DataFrame, bronze_uri: str) -> None:
    """Persist immutable Bronze records into append-only Delta storage."""
    (
        df_bronze.write.format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .save(bronze_uri)
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows `JsonToStructs` (`from_json`) executing natively
#   in Tungsten JVM codegen via pure DataFrame builders (no driver parallelization).
# - Check Delta transaction log (`DESCRIBE HISTORY`) to verify each `_batch_id`
#   maps 1:1 to an atomic `APPEND` commit for fast rollback or replay.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Keep Bronze columns raw (STRING/VARIANT/BINARY) rather than strict numeric
#    casts so upstream formatting quirks land in Bronze and quarantine in Silver.
# 2. Never run `DELETE` or `UPDATE` on Bronze except for legal/GDPR erasure;
#    use partition/date pruning when replaying incremental slices into Silver.
# 3. Computing `_raw_payload_checksum` enables zero-cost idempotency checks when
#    partners accidentally re-deliver identical files under new filenames.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("064_medallion_bronze")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    raw_rows = [
        ('{"vendor_id": "V-01", "invoice_no": "INV-1001", "gross_amount": "1500.0", "status": "SUBMITTED"}',),
        ('{"vendor_id": "V-02", "invoice_no": "INV-1002", "gross_amount": "420.50", "status": "APPROVED"}',),
    ]
    df_in = spark.createDataFrame(raw_rows, "raw_payload STRING")
    df_bronze = transform_parse_and_audit_bronze(
        df_in,
        source_uri="abfss://landing@storage.dfs.core.windows.net/invoices/batch_01.json",
        batch_id="BATCH-20250115-001",
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        df_bronze.write.format("parquet").mode("overwrite").save(tmp_dir)
        spark.read.parquet(tmp_dir).show(truncate=False)
