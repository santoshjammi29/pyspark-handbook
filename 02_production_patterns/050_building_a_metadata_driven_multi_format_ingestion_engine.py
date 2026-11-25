"""Metadata-Driven Multi-Format Ingestion Engine (PySpark 4.0+).

Scenario & Scale:
    A centralized lakehouse platform onboards 200+ heterogeneous datasets daily
    (CSV, JSON, Parquet, ORC) into Bronze Delta tables under strict auditability
    and 30-minute SLA windows without maintaining 200 bespoke ingestion scripts.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


@dataclass(frozen=True)
class IngestionSpec:
    """Declarative metadata contract for a single dataset ingestion."""

    table_name: str
    fmt: str
    raw_uri: str
    target_uri: str
    schema_ddl: str
    partition_cols: list[str] = field(default_factory=list)
    options: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Hardcoding per-table readers with implicit schema inference
# and skipping audit lineage columns.
# Otherwise: Any upstream type drift triggers silent schema mismatches, 2x read
# scans double I/O cost, and lineage failures prevent Bronze replayability.
def ingest_table_bad(spark: SparkSession, fmt: str, raw_uri: str, target_uri: str) -> None:
    spark.read.format(fmt).load(raw_uri).write.format("delta").mode("append").save(target_uri)


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Configuration-driven engine enforcing explicit schemas,
# unified audit metadata (`_metadata.file_path`), and idempotent Delta writes.
def transform_attach_audit_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    """Attach standard Bronze lineage columns compatible with Spark Connect."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(batch_id))
    )


class UnifiedIngestionEngine:
    """Metadata-driven multi-format ingestion runner."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read_source(self, spec: IngestionSpec) -> DataFrame:
        reader = self.spark.read.format(spec.fmt).schema(spec.schema_ddl)
        for key, value in spec.options.items():
            reader = reader.option(key, value)
        return reader.load(spec.raw_uri)

    def ingest(self, spec: IngestionSpec, batch_id: str | None = None) -> DataFrame:
        run_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        df_raw = self.read_source(spec)
        df_enriched = transform_attach_audit_metadata(df_raw, run_id)

        writer = df_enriched.write.format("delta").mode("append")
        if spec.partition_cols:
            writer = writer.partitionBy(*spec.partition_cols)
        writer.save(spec.target_uri)
        return df_enriched


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows `_metadata.file_path` resolved natively inside
#   `FileScan` (replacing legacy `input_file_name()` which breaks in some plans).
# - Explicit `.schema(spec.schema_ddl)` eliminates the extra schema-discovery
#   Spark job visible in Spark UI for JSON/CSV sources.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Prefer `_metadata.file_path` over `F.input_file_name()` in Spark 3.3+/4.0+;
#    `_metadata` works reliably across joins, unions, and Spark Connect.
# 2. Store `IngestionSpec` rows in a governed Delta control table (or YAML in
#    Git) rather than Python dicts so onboarding a table requires zero code PRs.
# 3. Never enable implicit schema inference in metadata configs; enforce explicit
#    DDL/StructType plus `mode="PERMISSIVE"` with a corrupt-record quarantine path.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("050_metadata_ingestion_engine")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        raw_json = Path(tmp_dir) / "orders.json"
        raw_json.write_text(
            '{"order_id": "O1", "amount": 120.0, "order_status": "COMPLETED"}\n',
            encoding="utf-8",
        )
        spec = IngestionSpec(
            table_name="src_orders",
            fmt="json",
            raw_uri=str(raw_json),
            target_uri="abfss://lakehouse@storage.dfs.core.windows.net/bronze/orders",
            schema_ddl="order_id STRING, amount DOUBLE, order_status STRING",
            partition_cols=["order_status"],
        )
        engine = UnifiedIngestionEngine(spark)
        transform_attach_audit_metadata(engine.read_source(spec), "BATCH-001").show(truncate=False)
