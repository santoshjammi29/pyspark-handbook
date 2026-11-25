"""
098 — GCP Dataproc Serverless, GCS Connector & BigQuery Storage Read/Write API Adapter.

Scenario & Scale:
    A cloud data platform runs serverless Spark 4/3 pipelines on GCP Dataproc
    Serverless reading/writing multi-TB BigQuery tables and `gs://` Cloud Storage
    buckets with Workload Identity / ADC keyless authentication.
"""

from __future__ import annotations


# ==============================================================================
# 2. Anti-Pattern: Static Service Account JSON Keys & Indirect Unfiltered Scans
# ==============================================================================
# ❌ Bad Practice:
#   spark.conf.set("google.cloud.auth.service.account.json.keyfile", "/etc/secrets/sa.json")
#   df = spark.read.format("bigquery").option("table", "proj.ds.events").load()
#   df.filter("country = 'US'")  # Without pushdown config or reading huge views blindly
#
# Otherwise:
#   Exporting JSON Service Account keys creates long-lived credential leaks. Without
#   Storage Read API filter pushdown or `viewsEnabled` / `materializationDataset`,
#   BigQuery reads fail on views or pull unnecessary terabytes across the network.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: ADC/Workload Identity + BQ Storage APIs
# ==============================================================================
# ✅ Good Practice: Authenticate via Workload Identity / ADC (zero JSON keys),
#                   configure GCS connector, and use BigQuery Storage Read/Write APIs.
def get_dataproc_serverless_configs() -> dict[str, str]:
    """Return Dataproc Serverless auto-scaling & GCS connector defaults (no JSON keys)."""
    return {
        "spark.sql.ansi.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.dataproc.scaling.version": "2",
        "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        "spark.hadoop.fs.gs.auth.type": "APPLICATION_DEFAULT",
    }


def build_bigquery_read_options(
    table_fqn: str,
    pushdown_filter: str,
    materialization_dataset: str | None = None,
) -> dict[str, str]:
    """Build Spark BigQuery Storage Read API options with predicate pushdown."""
    opts = {
        "table": table_fqn,
        "filter": pushdown_filter,
        "readDataFormat": "ARROW",
    }
    if materialization_dataset:
        opts["viewsEnabled"] = "true"
        opts["materializationDataset"] = materialization_dataset
    return opts


def build_bigquery_write_options(
    table_fqn: str,
    write_method: str = "direct",
    temporary_gcs_bucket: str | None = None,
) -> dict[str, str]:
    """Build Spark BigQuery write options favoring Storage Write API (`direct`)."""
    if write_method not in {"direct", "indirect"}:
        raise ValueError("write_method must be 'direct' or 'indirect'.")
    opts = {"table": table_fqn, "writeMethod": write_method}
    if write_method == "indirect":
        if not temporary_gcs_bucket:
            raise ValueError("temporary_gcs_bucket is required when writeMethod='indirect'.")
        opts["temporaryGcsBucket"] = temporary_gcs_bucket
    return opts


def normalize_bq_events(df):
    """ANSI-safe PySpark 4.0+ transform prior to BigQuery Storage Write API sink."""
    from pyspark.sql import functions as F

    return (
        df.withColumn("revenue_usd", F.expr("try_cast(raw_revenue AS NUMERIC)"))
        .filter(F.col("revenue_usd").isNotNull())
    )


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `df.explain("formatted")` for BigQuery Storage Read API (`BatchScan`), check
#   `PushedFilters` and `ReadSchema`—both filters and column projections execute
#   inside BigQuery storage servers before Arrow streams reach Spark executors.
# - In Spark UI -> `Executors` tab on Dataproc Serverless, observe dynamic executor
#   containers scaling down automatically between wide shuffle stages.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: `writeMethod="direct"` uses the BigQuery Storage Write API (gRPC streaming,
#   low latency, zero GCS staging bucket), whereas `"indirect"` stages Parquet/Avro in
#   `temporaryGcsBucket` and triggers a BigQuery load job (better for free batch quotas).
# - Gotcha 2: Querying BigQuery logical views requires `viewsEnabled="true"` AND a
#   writable `materializationDataset` where BigQuery caches temporary materialized results.
# - Gotcha 3: On Dataproc Serverless, never rely on YARN-specific configs or static
#   `spark.executor.instances`; Spark 4/3 batches auto-scale via Dataproc dynamic allocation.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_098_GCP_Dataproc_BQ").getOrCreate()
    print("Dataproc configs:", get_dataproc_serverless_configs())
    print("BQ Read opts:", build_bigquery_read_options("prod_proj.analytics.v_orders", "order_date >= '2026-01-01'", "temp_mat_ds"))
    print("BQ Write opts:", build_bigquery_write_options("prod_proj.analytics.fact_orders", write_method="direct"))

    df_raw = spark.createDataFrame(
        [("EVT-1", "120.45"), ("EVT-2", "NOT_A_NUM")],
        "event_id STRING, raw_revenue STRING",
    )
    normalize_bq_events(df_raw).show(truncate=False)
