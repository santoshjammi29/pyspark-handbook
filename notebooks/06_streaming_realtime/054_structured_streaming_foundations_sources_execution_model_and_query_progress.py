# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 054: Structured Streaming Foundations: Sources, Execution Model and Query Progress
# MAGIC
# MAGIC **Module:** Module 06: Streaming & Near Real-Time Processing
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A payments platform monitors live transaction throughput. As card swipes occur across the globe, an unbounded stream must be parsed, enriched, and aggregated in real time without dropping micro-batches.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master Spark Structured Streaming fundamentals: create a streaming DataFrame from the memory/rate stream source, inspect stream schema, configure streaming queries (`readStream` / `writeStream`), monitor execution metrics via `StreamingQueryProgress`, and manage query lifecycle.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Spark Structured Streaming treats a live stream as an unbounded, continuously appended table. The engine processes micro-batches incrementally, maintaining offsets in a write-ahead log (WAL) and state in RocksDB/memory, providing end-to-end exactly-once fault tolerance.

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
        .appName("UseCase_054_Structured_Streaming_Foundations_Sources_Execution_Model_and_Query_Progress")
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
# MAGIC ### Step 1: Creating an Unbounded Synthetic Streaming Source
# MAGIC Use Spark's built-in 'rate' stream source to generate synthetic streaming timestamps.

# COMMAND ----------

# rate source generates timestamp and monotonic value at configured rows per second
df_stream_raw = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .option("numPartitions", 2)
    .load()
)

print(f"Is DataFrame streaming: {df_stream_raw.isStreaming}")
df_stream_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Streaming Transformations: Deriving Live Transaction Metrics
# MAGIC Apply standard DataFrame transformations over the unbounded streaming dataset.

# COMMAND ----------

df_transactions_stream = df_stream_raw.select(
    F.col("timestamp").alias("event_time"),
    F.concat(F.lit("TXN-"), F.col("value")).alias("transaction_id"),
    F.round((F.col("value") % 500) + 10.50, 2).alias("amount"),
    F.when(F.col("value") % 10 == 0, "HIGH_RISK").otherwise("STANDARD").alias("risk_category")
)

df_transactions_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Starting Memory Sink & Inspecting StreamingQueryProgress
# MAGIC Start stream writing to in-memory table for interactive querying and inspect progress metrics.

# COMMAND ----------

import time

# Start streaming query writing to in-memory table
query = (
    df_transactions_stream.writeStream
    .format("memory")
    .queryName("live_transactions")
    .outputMode("append")
    .start()
)

print(f"Streaming query started! ID: {query.id} | Status: {query.status['message']}")

# Let the stream run for 3 seconds to accumulate micro-batches
time.sleep(3)

# Query recent progress
progress = query.lastProgress
if progress:
    print(f"Input Rate: {progress.get('inputRowsPerSecond', 0):.1f} rows/sec")
    print(f"Processing Rate: {progress.get('processedRowsPerSecond', 0):.1f} rows/sec")
    print(f"Batch ID: {progress.get('batchId', 0)}")

# Query the in-memory streaming table using Spark SQL
print("Live In-Memory Streaming Snapshot:")
spark.sql("SELECT * FROM live_transactions ORDER BY event_time DESC LIMIT 5").show(truncate=False)

# Safely stop streaming query
query.stop()
print("Streaming query safely stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Structured Streaming abstracts streaming into an unbounded table queried using standard DataFrame API operations.**
# MAGIC - **Output modes: `append` (emits new rows), `update` (emits changed rows), `complete` (emits entire aggregated table).**
# MAGIC - **`query.lastProgress` provides vital telemetry: input rows per second, processing latency, and state memory.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between micro-batch execution and Continuous Processing mode in Spark Streaming?
# MAGIC 2. How does the checkpoint directory guarantee exactly-once processing during executor node crashes?
# MAGIC 3. What are the three output modes in Structured Streaming and when must each be used?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
