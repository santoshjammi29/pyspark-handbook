"""
097 — AWS EMR (Serverless / EC2), Glue Data Catalog & S3A Magic Committer Adapter.

Scenario & Scale:
    An enterprise runs high-concurrency ETL jobs on AWS EMR Serverless and EMR on EC2
    writing multi-terabyte Iceberg and Delta datasets to Amazon S3, governed by
    AWS Glue Data Catalog and IAM Role / IRSA keyless authentication.
"""

from __future__ import annotations


# ==============================================================================
# 2. Anti-Pattern: Static IAM Keys & FileOutputCommitter v1/v2 Rename Storms
# ==============================================================================
# ❌ Bad Practice:
#   spark.conf.set("spark.hadoop.fs.s3a.access.key", "AKIA...")
#   spark.conf.set("spark.hadoop.fs.s3a.secret.key", "wJalr...")
#   spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
#
# Otherwise:
#   Static AWS keys in code/SparkConf leak in Git and the Spark UI `Environment` tab.
#   S3 lacks atomic directory `rename()`, so FileOutputCommitter v1 performs slow
#   O(N) driver copy-deletes, while v2 risks partial/corrupt data on task failure.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: IAM/IRSA Auth + S3A Magic Committer + Glue Catalog
# ==============================================================================
# ✅ Good Practice: Use Instance Profile / WebIdentity (IRSA) credentials, enable
#                   S3A Magic Committer for zero-rename commits, and bind Glue Catalog.
def get_emr_s3a_magic_committer_configs() -> dict[str, str]:
    """Return S3A Magic Committer & keyless IAM credential provider configs."""
    return {
        "spark.sql.ansi.enabled": "true",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "com.amazonaws.auth.WebIdentityTokenCredentialsProvider,"
            "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        ),
        "spark.hadoop.fs.s3a.committer.name": "magic",
        "spark.hadoop.fs.s3a.committer.magic.enabled": "true",
        "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a": (
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
        ),
        "spark.sql.sources.commitProtocolClass": (
            "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"
        ),
        "spark.sql.parquet.output.committer.class": (
            "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"
        ),
    }


def get_glue_iceberg_catalog_configs(catalog_name: str, warehouse_s3_uri: str) -> dict[str, str]:
    """Configure Apache Iceberg backed by AWS Glue Data Catalog."""
    if not warehouse_s3_uri.startswith("s3://"):
        raise ValueError(f"Glue Iceberg warehouse must be an s3:// URI: {warehouse_s3_uri}")
    prefix = f"spark.sql.catalog.{catalog_name}"
    return {
        f"{prefix}": "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.warehouse": warehouse_s3_uri,
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    }


def get_glue_delta_catalog_configs() -> dict[str, str]:
    """Configure Delta Lake with AWS Glue Data Catalog via Hive-compatible metastore client."""
    return {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.hive.metastore.client.factory.class": (
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        ),
    }


def build_s3_curated_batch(df):
    """Prepare an ANSI-safe partitioned dataset ready for S3A Magic Committer writes."""
    from pyspark.sql import functions as F

    return (
        df.withColumn("amount_usd", F.expr("try_cast(raw_amount AS DECIMAL(18, 2))"))
        .withColumn("ingest_date", F.to_date("event_ts"))
        .filter(F.col("amount_usd").isNotNull())
    )


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `df.explain("formatted")`, verify `PartitionFilters` and `PushedFilters` push
#   down cleanly into Iceberg/Delta `BatchScan` or `FileScan` on `s3://` / `s3a://`.
# - On EMR Spark UI -> `Jobs` / `SQL` tab, Magic Committer eliminates the long post-task
#   driver `commitJob` stall caused by S3 copy-and-delete object renames.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: Never use `FileOutputCommitter` v2 on S3—speculative execution or task
#   retries leave orphaned/corrupted files in the target prefix before job completion.
# - Gotcha 2: On EMR Serverless, you do NOT manage EC2 instance profiles directly;
#   attach an Execution Role to the EMR Serverless job run instead.
# - Gotcha 3: AWS Glue Data Catalog enforces lowercase database and table names;
#   mixed-case identifiers in Spark SQL are automatically lowercased in Glue.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_097_AWS_EMR_Glue").getOrCreate()
    for k, v in get_emr_s3a_magic_committer_configs().items():
        print(f"{k} = {v}")
    print("Glue Iceberg keys:", list(get_glue_iceberg_catalog_configs("glue_prod", "s3://lakehouse-prod/iceberg").keys()))

    df_raw = spark.createDataFrame(
        [("TX-1", "42.50", "2026-01-15T09:00:00Z"), ("TX-2", "ERR", "2026-01-15T09:05:00Z")],
        "txn_id STRING, raw_amount STRING, event_ts STRING",
    )
    build_s3_curated_batch(df_raw).show(truncate=False)
