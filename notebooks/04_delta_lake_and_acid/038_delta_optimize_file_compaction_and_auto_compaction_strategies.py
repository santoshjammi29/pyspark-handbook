# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 038: Delta OPTIMIZE, File Compaction and Auto-Compaction Strategies
# MAGIC
# MAGIC **Module:** Module 04: Delta Lake Architecture, ACID Operations & Time Travel
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A streaming ingestion job writes 1,000 tiny 50KB Parquet files into a Delta table every hour. Query latency has plummeted from 2 seconds to 45 seconds due to the classic Big Data 'small file problem' overwhelming driver filesystem metadata operations.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Resolve small file degradation using Delta Lake compaction: execute `OPTIMIZE` command to bin-pack small files into standard 1GB target files, measure file count reduction, and configure Databricks auto-compaction and optimized writes (`spark.databricks.delta.optimizeWrite.enabled`).
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC The small file problem causes high metadata lookup overhead on cloud storage (millions of GET/LIST API calls). `OPTIMIZE` reads groups of tiny files and rewrites them into consolidated 1GB (or user-tuned size) Parquet files, creating a single commit in `_delta_log`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Setup & SparkSession Initialization
# MAGIC If running inside a Databricks notebook, the `spark` session is pre-initialized by the cluster.
# MAGIC If executing outside Databricks (e.g. VS Code, local CLI, CI/CD runner), we initialize a local SparkSession with Delta Lake support.

# COMMAND ----------

import os
import sys

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_038_Delta_OPTIMIZE_File_Compaction_and_Auto_Compaction_Strategies")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

spark.sparkContext.setLogLevel("WARN")
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.sparkContext.appName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Simulating Small File Ingestion Sprawl
# MAGIC Create 20 tiny files across multiple append operations.

# COMMAND ----------

delta_opt_path = "/tmp/delta_optimize_038"
if Path(delta_opt_path).exists():
    shutil.rmtree(delta_opt_path)

# Append 20 micro-batches
for i in range(20):
    batch = [(f"ITEM-{i}-{j}", f"desc_{j}", 10.0 * j) for j in range(5)]
    spark.createDataFrame(batch, ["item_id", "desc", "val"]).write.format("delta").mode("append").save(delta_opt_path)

files_before = list(Path(delta_opt_path).glob("*.parquet"))
print(f"Total small Parquet files created before OPTIMIZE: {len(files_before)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Executing OPTIMIZE Bin-Packing Compaction
# MAGIC Run OPTIMIZE to compact small files into fewer large files.

# COMMAND ----------

df_opt_metrics = spark.sql(f"OPTIMIZE delta.`{delta_opt_path}`")
df_opt_metrics.select("path", "metrics.numFilesAdded", "metrics.numFilesRemoved", "metrics.totalPushedFiles").show(truncate=False)

files_after = list(Path(delta_opt_path).glob("*.parquet"))
print(f"Total Parquet files on disk after OPTIMIZE: {len(files_after)} (Compacted down to active files!)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Configuring Auto-Compaction & Optimized Writes
# MAGIC Enable automatic compaction at write time to prevent small file accumulation.

# COMMAND ----------

# Enable Optimized Writes: Coalesces partitions dynamically before writing to disk
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# Enable Auto Compaction: Automatically runs lightweight bin-packing after successful writes
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

print("Optimized Writes and Auto-Compaction enabled in SparkSession!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`OPTIMIZE` bin-packs small files into target ~1GB files without altering data semantics.**
# MAGIC - **Optimized Writes (`delta.optimizeWrite`) coalesce small partitions during write operations, preventing fragmentation at the source.**
# MAGIC - **Auto-compaction (`delta.autoCompact`) automatically compacts small files right after commit, ideal for streaming ingest.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the Small File Problem and how does it negatively affect query performance and cloud storage costs?
# MAGIC 2. What is the difference between `autoCompact` and manually scheduled `OPTIMIZE` jobs?
# MAGIC 3. Can users continue querying a Delta table while an `OPTIMIZE` command is actively running?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
