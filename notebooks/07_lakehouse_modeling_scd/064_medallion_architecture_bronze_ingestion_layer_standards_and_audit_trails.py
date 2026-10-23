# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 064: Medallion Architecture: Bronze Ingestion Layer Standards and Audit Trails
# MAGIC
# MAGIC **Module:** Module 07: Lakehouse Data Modeling, CDC & SCD Implementation  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC An enterprise data lakehouse ingests raw transactional data from ERP, CRM, and partner APIs. Data must land in an append-only Bronze layer preserving raw formats, capturing metadata lineage, and ensuring raw historical payloads are never lost.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement the Bronze layer of the Medallion Architecture: configure append-only raw landing, add mandatory audit columns (`_ingest_timestamp`, `_source_file_path`, `_batch_id`), enforce schema preservation, and handle schema evolution without rewriting data.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC The Bronze layer is an append-only, raw historical archive of source data. No cleansing, deduplication, or business logic is applied here. If a downstream pipeline bug corrupts Silver or Gold tables, the entire lakehouse can be recomputed from the Bronze layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Environment Setup & SparkSession Initialization
# MAGIC We configure a resilient `SparkSession` compatible with Databricks runtime, local PySpark development, or CI/CD test runners.

# COMMAND ----------

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Initialize SparkSession with Delta Lake support and optimized local configs
spark = (
    SparkSession.builder
    .appName("UseCase_064_Medallion_Architecture_Bronze_Ingestion_Layer_Standards_and_Audit_Trails")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.sparkContext.appName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Simulating Inbound Vendor Files
# MAGIC Create raw JSON vendor payloads.

# COMMAND ----------

bronze_path = "/tmp/lakehouse_bronze_064"
if Path(bronze_path).exists():
    shutil.rmtree(bronze_path)

raw_records = [
    '{"vendor_id": "V-01", "invoice_no": "INV-1001", "gross_amount": 1500.0, "status": "SUBMITTED"}',
    '{"vendor_id": "V-02", "invoice_no": "INV-1002", "gross_amount": 420.50, "status": "APPROVED"}'
]

rdd_raw = spark.sparkContext.parallelize(raw_records)
df_raw = spark.read.json(rdd_raw)
df_raw.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Enriching with Standard Bronze Audit Columns
# MAGIC Add standard enterprise lineage and audit attributes.

# COMMAND ----------

from datetime import datetime

df_bronze = (
    df_raw
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file_path", F.lit("s3://vendor-inbox/invoices/batch_20231120.json"))
    .withColumn("_batch_id", F.lit("BATCH-20231120-001"))
    .withColumn("_raw_payload_checksum", F.sha2(F.to_json(F.struct(df_raw.columns)), 256))
)

df_bronze.printSchema()
df_bronze.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Landing into Append-Only Delta Bronze Table
# MAGIC Write to Delta table with append mode and verify immutability.

# COMMAND ----------

df_bronze.write.format("delta").mode("append").save(bronze_path)

print("Bronze Table Successfully Written:")
spark.read.format("delta").load(bronze_path).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Bronze layer is an immutable, append-only copy of raw incoming source data.**
# MAGIC - **Always inject audit metadata: ingestion timestamp, source file path, batch run ID, and payload hash.**
# MAGIC - **Never apply business filters or destructive transformations in Bronze.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the exact purpose of the Bronze layer in Databricks Medallion Architecture?
# MAGIC 2. Why should you never deduplicate or cleanse data directly in the Bronze layer?
# MAGIC 3. How does the Bronze layer protect organizations against catastrophic downstream pipeline bugs?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
