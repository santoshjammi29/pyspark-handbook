# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 059: Streaming Deduplication Across Time Windows with Watermarks
# MAGIC
# MAGIC **Module:** Module 06: Streaming & Near Real-Time Processing
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC An IoT gateway ingests sensor readings from connected vehicles over cellular networks. When vehicles enter tunnels or experience network reconnects, duplicate messages are re-sent. The pipeline must eliminate duplicate sensor IDs within a rolling 20-minute window.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Implement streaming deduplication: apply `withWatermark()` paired with `dropDuplicates()` on primary keys and event timestamps. Explain how the watermark bounds the historical key state maintained by the streaming engine.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC In Structured Streaming, calling `dropDuplicates(['key'])` without a watermark causes Spark to store every primary key ever seen in memory forever, eventually triggering an Executor OOM. Supplying a watermark allows Spark to evict keys older than the watermark threshold from the state store.

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
        .appName("UseCase_059_Streaming_Deduplication_Across_Time_Windows_with_Watermarks")
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
# MAGIC ### Step 1: Understanding Streaming Deduplication State Store Eviction
# MAGIC Illustrate how watermarking enables bounded key tracking in state stores.

# COMMAND ----------

# Architectural Blueprint:
# stream.withWatermark("event_time", "20 minutes").dropDuplicates(["device_id", "event_time"])
# Keys older than (max_event_time - 20 minutes) are evicted from memory.
# New duplicates within the 20-minute window are safely identified and dropped!

print("Streaming deduplication memory bounds blueprint mapped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Simulating Sensor Stream with Duplicate Ingestion
# MAGIC Create dataset containing both unique and duplicate readings across time.

# COMMAND ----------

from datetime import datetime, timedelta

base_ts = datetime(2023, 11, 20, 15, 0, 0)

telemetry_stream = [
    ("DEV-01", base_ts, 22.5),
    ("DEV-02", base_ts, 25.0),
    ("DEV-01", base_ts, 22.5), # Exact duplicate within window -> MUST DROP
    ("DEV-01", base_ts + timedelta(minutes=5), 23.0), # Valid new reading -> KEEP
    ("DEV-03", base_ts + timedelta(minutes=10), 19.5)
]

schema = "device_id STRING, event_time TIMESTAMP, temp DOUBLE"
df_stream_telemetry = spark.createDataFrame(telemetry_stream, schema)
df_stream_telemetry.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Executing Watermarked Streaming Deduplication
# MAGIC Apply watermark and dropDuplicates over device_id and event_time.

# COMMAND ----------

# Deduplicate streaming data within a 20-minute watermark window
df_deduped = (
    df_stream_telemetry
    .withWatermark("event_time", "20 minutes")
    .dropDuplicates(["device_id", "event_time"])
)

print("Deduplicated Stream Output:")
df_deduped.orderBy("device_id", "event_time").show(truncate=False)

# Verification assertion
assert df_deduped.count() == 4, f"Expected 4 unique readings, found {df_deduped.count()}"
print("Streaming deduplication assertion verified successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Always combine `dropDuplicates([keys, timestamp_col])` with `withWatermark()` in streaming jobs.**
# MAGIC - **Omitting the watermark causes state store memory to grow monotonically until the cluster crashes.**
# MAGIC - **Duplicates arriving after the watermark has passed may be re-processed as new records; size the watermark to cover maximum expected network delay.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What happens to the state store if you call `dropDuplicates(['id'])` without a watermark in Structured Streaming?
# MAGIC 2. Why must the event-time column be included in `dropDuplicates` when using watermarks?
# MAGIC 3. How does RocksDB state store handle state recovery during executor restarts?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
