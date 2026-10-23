# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 051: Incremental Batch File Discovery: Watermarking and Modification Timestamps
# MAGIC
# MAGIC **Module:** Module 05: Data Ingestion Patterns, Auto Loader & File Formats  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A legacy data pipeline runs hourly batch jobs that read from an S3/ADLS bucket where external partners drop new files. The pipeline must process only files added since the last successful run without re-processing older files or utilizing streaming.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement incremental batch file ingestion in PySpark: use file modification timestamps (`modifiedAfter` and `modifiedBefore`), track high-watermark state in a persistent Delta metadata table, and prevent race conditions with actively written files.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Spark's file readers support path options `modifiedAfter` and `modifiedBefore` (ISO 8601 strings). By querying the maximum timestamp of the previous batch and setting `modifiedAfter`, Spark skips listing and reading older files, achieving efficient incremental batch execution.

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
    .appName("UseCase_051_Incremental_Batch_File_Discovery_Watermarking_and_Modification_Timestamps")
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
# MAGIC ## 5. Simulating Landing Bucket with Progressive Timestamps
# MAGIC Create staging directory with multiple files.

# COMMAND ----------

inc_dir = Path("/tmp/incremental_batch_051")
if inc_dir.exists():
    shutil.rmtree(inc_dir)
inc_dir.mkdir(parents=True, exist_ok=True)

# Write batch 1 files
with open(inc_dir / "file_batch1.csv", "w") as f:
    f.write("id,val\n1,A\n2,B\n")

print(f"Initial files written to {inc_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Incremental Discovery Using modifiedAfter Option
# MAGIC Read files filtered by modification timestamp threshold.

# COMMAND ----------

from datetime import datetime, timedelta

# Ingest all files modified after 10 minutes ago
watermark_time = (datetime.utcnow() - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")

df_incremental = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("modifiedAfter", watermark_time)
    .load(str(inc_dir))
)

print(f"Reading files modified after {watermark_time}:")
df_incremental.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Maintaining Watermark State in Metadata Delta Table
# MAGIC Persist the new high-watermark timestamp for the next run.

# COMMAND ----------

# High-watermark metadata record
new_watermark = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
watermark_record = [("landing_partner_bucket", new_watermark, df_incremental.count())]

df_wm = spark.createDataFrame(watermark_record, ["pipeline_name", "last_watermark_ts", "records_processed"])
df_wm.write.format("delta").mode("append").save("/tmp/pipeline_watermarks_051")

print("Updated High-Watermark State Table:")
spark.read.format("delta").load("/tmp/pipeline_watermarks_051").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`modifiedAfter` filters cloud storage files at the source based on object modification timestamps.**
# MAGIC - **Always persist high-watermark state in an ACID Delta table to ensure crash-recovery idempotency.**
# MAGIC - **Apply a small safety overlap buffer (e.g. 1-2 minutes) and deduplicate downstream to catch clock drift.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `modifiedAfter` and using Databricks Auto Loader?
# MAGIC 2. How do you handle files that are still being written by an external producer when the batch job begins?
# MAGIC 3. Why can clock skew between cloud storage servers and Spark nodes cause missing files in timestamp watermarking?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
