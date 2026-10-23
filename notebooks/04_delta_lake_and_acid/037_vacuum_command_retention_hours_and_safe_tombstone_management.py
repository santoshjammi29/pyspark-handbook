# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 037: VACUUM Command, Retention Hours and Safe Tombstone Management
# MAGIC
# MAGIC **Module:** Module 04: Delta Lake Architecture, ACID Operations & Time Travel  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A cloud lakehouse table ingests high-frequency updates, accumulating 50,000 unreferenced obsolete Parquet files and generating massive cloud storage bills. Platform engineers must reclaim storage capacity safely without corrupting active concurrent readers.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Execute Delta Lake `VACUUM` management: understand the default 7-day retention safety check, bypass retention for testing (`spark.databricks.delta.vacuum.parallelDelete.enabled`), evaluate `DRY RUN` mode to audit candidate files before permanent deletion, and prevent race conditions.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Delta `VACUUM` physically deletes data files that have been marked as removed (tombstoned) in the transaction log and are older than the retention threshold (default 168 hours / 7 days). The safety check prevents deleting files still being read by long-running queries.

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
    .appName("UseCase_037_VACUUM_Command,_Retention_Hours_and_Safe_Tombstone_Management")
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
# MAGIC ## 5. Generating Tombstoned Files via Overwrites
# MAGIC Create obsolete historical data files.

# COMMAND ----------

delta_vac_path = "/tmp/delta_vacuum_037"
if Path(delta_vac_path).exists():
    shutil.rmtree(delta_vac_path)

# Write 3 successive batches to generate orphaned parquet files
for b in range(1, 4):
    data = [(f"ROW-{b}-{i}", f"batch_{b}") for i in range(5)]
    spark.createDataFrame(data, ["id", "batch"]).write.format("delta").mode("overwrite").save(delta_vac_path)

# List physical parquet files on disk
parquet_files = list(Path(delta_vac_path).glob("*.parquet"))
print(f"Total physical Parquet files in directory: {len(parquet_files)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Executing VACUUM with DRY RUN First
# MAGIC Audit which files are eligible for deletion without actually deleting them.

# COMMAND ----------

# Disable safety retention check ONLY for local demonstration
spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")

# DRY RUN lists files eligible for deletion without deleting them
df_dry_run = spark.sql(f"VACUUM delta.`{delta_vac_path}` RETAIN 0 HOURS DRY RUN")
print("Candidate Files Identified for Deletion by DRY RUN:")
df_dry_run.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Executing Permanent VACUUM & Storage Verification
# MAGIC Permanently purge unreferenced files and verify disk reclamation.

# COMMAND ----------

# Execute actual vacuum with 0 hours retention (demo environment only)
spark.sql(f"SET spark.databricks.delta.vacuum.retentionDurationCheck.enabled = false")
spark.sql(f"VACUUM delta.`{delta_vac_path}` RETAIN 0 HOURS")

remaining_parquet = list(Path(delta_vac_path).glob("*.parquet"))
print(f"Physical Parquet files remaining on disk after VACUUM: {len(remaining_parquet)}")

# Current data remains 100% accessible
spark.read.format("delta").load(delta_vac_path).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Delta `VACUUM` cleans up orphaned data files no longer referenced by the current table snapshot.**
# MAGIC - **Default retention is 7 days (168 hours) to protect concurrent active queries and time-travel SLA.**
# MAGIC - **Always run `VACUUM ... DRY RUN` first in production to preview files slated for permanent deletion.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What dangerous consequence occurs if you disable `retentionDurationCheck` and vacuum with 0 hours during active concurrent queries?
# MAGIC 2. Does `VACUUM` delete transaction log JSON files inside `_delta_log/`?
# MAGIC 3. How does `VACUUM` interact with Delta Lake's time-travel capabilities?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
