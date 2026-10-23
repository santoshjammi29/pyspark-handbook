# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 056: Streaming Window Aggregations and Watermarking for Late-Arriving Events
# MAGIC
# MAGIC **Module:** Module 06: Streaming & Near Real-Time Processing  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A mobile gaming platform aggregates player score events over 10-minute tumbling windows. Due to intermittent mobile connectivity and airplane mode, score events arrive up to 15 minutes late. The pipeline must incorporate late events without letting state size grow infinitely.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement streaming window aggregations with watermarking: configure `withWatermark('event_time', '10 minutes')`, compute tumbling and sliding window metrics, and explain how the watermark dictates state eviction from RocksDB memory.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC A watermark specifies the maximum threshold by which event-time can lag behind the maximum event-time observed by the engine ($Watermark = Max(event\_time) - Delay$). Once a window's end time falls behind the watermark, Spark emits the final aggregate and purges the window's state from memory.

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
    .appName("UseCase_056_Streaming_Window_Aggregations_and_Watermarking_for_Late-Arriving_Events")
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
# MAGIC ## 5. Understanding Watermark State Cleanup Mathematics
# MAGIC Illustrate how watermarking bounds memory growth in stateful streaming.

# COMMAND ----------

# Architectural Blueprint:
# Event Time: 12:00 -> Watermark = 12:00 - 10 min = 11:50
# Window [11:30 - 11:40]: Completely older than 11:50 -> State Evicted! No longer accepts updates.
# Window [11:45 - 11:55]: End time (11:55) > 11:50 -> Window still active in state memory.

print("Watermark state eviction semantics mapped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Simulating Event Stream with Deliberate Late-Arriving Records
# MAGIC Create memory stream with varied event timestamps.

# COMMAND ----------

from datetime import datetime, timedelta

base_time = datetime(2023, 11, 20, 12, 0, 0)

# Simulate events arriving out of chronological order
batch_events = [
    # On-time events
    (base_time, "Alice", 100),
    (base_time + timedelta(minutes=2), "Bob", 150),
    # Advance watermark to 12:15
    (base_time + timedelta(minutes=15), "Charlie", 300),
    # Late event from 12:03 (Arrives when max time is 12:15, delta = 12 min. Allowed if watermark >= 15 min!)
    (base_time + timedelta(minutes=3), "Alice", 50)
]

schema = "event_time TIMESTAMP, player STRING, score INT"
df_events = spark.createDataFrame(batch_events, schema)
df_events.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Configuring Window Aggregations with Watermark
# MAGIC Apply 10-minute tumbling window with 15-minute watermark delay.

# COMMAND ----------

# Tumbling window aggregation with watermark
df_windowed = (
    df_events
    .withWatermark("event_time", "15 minutes")
    .groupBy(
        F.window("event_time", "10 minutes"),
        "player"
    )
    .agg(
        F.sum("score").alias("total_score"),
        F.count("score").alias("events_count")
    )
    .select(
        "window.start",
        "window.end",
        "player",
        "total_score",
        "events_count"
    )
    .orderBy("start", "player")
)

print("Aggregated Window Output (Late Event Incorporated):")
df_windowed.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`withWatermark(time_col, delay)` informs Spark how long to retain intermediate aggregation state for late data.**
# MAGIC - **Events arriving older than the current watermark are dropped without affecting state.**
# MAGIC - **Watermarking is mandatory for stateful streaming aggregations in `append` mode to prevent infinite memory leaks.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is a Watermark in Spark Structured Streaming and how is it calculated?
# MAGIC 2. What happens to an event if its `event_time` is strictly older than the current watermark?
# MAGIC 3. Why can stateful streaming aggregations NOT run in `append` mode without a watermark?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
