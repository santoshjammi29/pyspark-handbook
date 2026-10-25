# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 055: Streaming Triggers: ProcessingTime, Once and AvailableNow for Cost Optimization
# MAGIC
# MAGIC **Module:** Module 06: Streaming & Near Real-Time Processing
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A cloud financial platform runs streaming pipelines. Running clusters 24/7 costs $50,000/month. Because source data arrives in hourly file batches, operations wants to run streaming pipelines incrementally on-demand, process all backlogged data, and terminate clusters immediately.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Compare and configure Structured Streaming triggers: (1) Fixed interval micro-batching (`Trigger.ProcessingTime('10 seconds')`), (2) Legacy single-batch (`Trigger.Once()`), and (3) Modern Databricks/Spark 3.3+ cost-optimized `Trigger.AvailableNow()` which processes all outstanding data across multiple micro-batches before shutting down.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC `Trigger.AvailableNow` solves the major limitation of `Trigger.Once`. While `Trigger.Once` loads all pending data into a single giant micro-batch (causing OutOfMemory crashes on large backlogs), `Trigger.AvailableNow` respects rate-limiting configurations, executing multiple properly-sized micro-batches until all backlogged data is processed, then cleanly terminates.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Setup & SparkSession Initialization
# MAGIC If running inside a Databricks notebook, the `spark` session is pre-initialized by the cluster.
# MAGIC If executing outside Databricks (e.g. VS Code, local CLI, CI/CD runner), we initialize a local SparkSession with Delta Lake support.

# COMMAND ----------

import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_055_Streaming_Triggers_ProcessingTime_Once_and_AvailableNow_for_Cost_Optimization")
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
# MAGIC ### Step 1: Trigger Architecture Comparison Blueprint
# MAGIC Evaluate the execution lifecycle of different streaming triggers.

# COMMAND ----------

# Architectural Comparison of Triggers:
# 1. ProcessingTime('30 seconds'): Continuously running 24/7 cluster. Evaluates every 30s.
# 2. Once(): Single micro-batch. Dangerous if 100GB of backlogged data has accumulated.
# 3. AvailableNow(): Best practice for cost-effective incremental batching!
#    Processes backlog in multiple rate-limited micro-batches, commits checkpoints, and shuts down.

print("Streaming trigger architectural profiles defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Simulating Incremental Processing with Trigger.AvailableNow
# MAGIC Run an incremental streaming pass using AvailableNow.

# COMMAND ----------

import shutil
from pathlib import Path

stream_source_dir = Path("/tmp/stream_trigger_source_055")
stream_sink_dir = Path("/tmp/stream_trigger_sink_055")
stream_chk_dir = Path("/tmp/stream_trigger_chk_055")

for p in [stream_source_dir, stream_sink_dir, stream_chk_dir]:
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)

# Write initial batch of files to source
with open(stream_source_dir / "events_1.json", "w") as f:
    f.write('{"id": 1, "val": "A"}\n{"id": 2, "val": "B"}\n')

schema = "id INT, val STRING"
df_file_stream = spark.readStream.schema(schema).json(str(stream_source_dir))

# Execute using Trigger.AvailableNow (terminates automatically when done)
query = (
    df_file_stream.writeStream
    .format("delta")
    .option("checkpointLocation", str(stream_chk_dir))
    .trigger(availableNow=True)
    .start(str(stream_sink_dir))
)

query.awaitTermination()
print("Trigger.AvailableNow completed successfully and terminated cleanly!")

print("Target Delta Table Output:")
spark.read.format("delta").load(str(stream_sink_dir)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Verifying Incremental Offset Advancement
# MAGIC Add more files and run another AvailableNow pass to verify exactly-once continuation.

# COMMAND ----------

# Add second batch of files
with open(stream_source_dir / "events_2.json", "w") as f:
    f.write('{"id": 3, "val": "C"}\n{"id": 4, "val": "D"}\n')

# Run second AvailableNow pass using the SAME checkpoint directory
query2 = (
    df_file_stream.writeStream
    .format("delta")
    .option("checkpointLocation", str(stream_chk_dir))
    .trigger(availableNow=True)
    .start(str(stream_sink_dir))
)
query2.awaitTermination()

print("Target Delta Table after Second AvailableNow Run (Total 4 records):")
spark.read.format("delta").load(str(stream_sink_dir)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`Trigger.AvailableNow` processes all outstanding backlog across multiple micro-batches and terminates automatically.**
# MAGIC - **Ideal for orchestrating streaming pipelines from Databricks Workflows or Airflow on transient single-job clusters.**
# MAGIC - **Provides the exact streaming checkpointing and exactly-once semantics without running expensive 24/7 idle clusters.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why was `Trigger.AvailableNow` introduced in Spark 3.3 to replace `Trigger.Once`?
# MAGIC 2. How does `Trigger.AvailableNow` enable significant cloud FinOps cost savings?
# MAGIC 3. What happens to streaming state when a query with `Trigger.AvailableNow` completes?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
