# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 063: Streaming Observability: StreamingQueryListener and SLA Latency Alerting
# MAGIC
# MAGIC **Module:** Module 06: Streaming & Near Real-Time Processing
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC An enterprise fintech streaming pipeline must maintain an SLA of processing events within 60 seconds of origination. If a backlog builds up or micro-batch processing time spikes, on-call engineers must be alerted before SLA violations occur.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Implement streaming observability in PySpark: register a custom `StreamingQueryListener`, capture query startup, termination, and micro-batch progress events (`onQueryProgress`), calculate input rate vs processing rate, and generate automated SLA breach alerts.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Spark's `StreamingQueryListener` provides an asynchronous callback API. When registered with `spark.streams.addListener()`, it captures event telemetry for all queries running in the SparkSession without introducing runtime overhead into the data processing tasks.

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
        .appName("UseCase_063_Streaming_Observability_StreamingQueryListener_and_SLA_Latency_Alerting")
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
# MAGIC ### Step 1: Defining Custom StreamingQueryListener Class
# MAGIC Implement listener to intercept query progress and measure SLA metrics.

# COMMAND ----------

from pyspark.sql.streaming import StreamingQueryListener

class PipelineSLAListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"[OBSERVABILITY] Query Started! ID: {event.id} | Name: {event.name}")

    def onQueryProgress(self, event):
        progress = event.progress
        batch_id = progress.batchId
        input_rate = progress.inputRowsPerSecond
        proc_rate = progress.processedRowsPerSecond
        num_input = progress.numInputRows

        # Check if processing rate lags behind input rate (Backlog accumulation!)
        status = "HEALTHY"
        if input_rate > 0 and proc_rate < (input_rate * 0.8):
            status = "WARNING: PROCESSING_LAG_DETECTED"

        print(f"[METRIC] Batch: {batch_id} | In: {num_input} rows | InRate: {input_rate:.1f}/s | ProcRate: {proc_rate:.1f}/s | {status}")

    def onQueryTerminated(self, event):
        if event.exception:
            print(f"[ALERT] Query CRASHED with error: {event.exception}")
        else:
            print("[OBSERVABILITY] Query Terminated cleanly.")

print("PipelineSLAListener class defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Registering Listener with SparkSession Streams Manager
# MAGIC Register the listener to begin capturing streaming telemetry.

# COMMAND ----------

# Register the listener
sla_listener = PipelineSLAListener()
spark.streams.addListener(sla_listener)
print("StreamingQueryListener registered with SparkSession.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Running Diagnostic Stream to Trigger Listener Callbacks
# MAGIC Execute a short rate stream to observe listener telemetry in the logs.

# COMMAND ----------

import time

df_rate = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

q_monitored = (
    df_rate.writeStream
    .format("memory")
    .queryName("sla_telemetry_demo")
    .start()
)

# Let run for 2.5 seconds to trigger progress callbacks
time.sleep(2.5)

q_monitored.stop()
print("Monitored query completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`StreamingQueryListener` provides non-invasive, asynchronous observability into all streaming queries.**
# MAGIC - **Monitor `inputRowsPerSecond` vs `processedRowsPerSecond`: when processing rate drops below input rate, backpressure develops.**
# MAGIC - **Send listener alerts to Slack, PagerDuty, or Datadog to detect silent streaming stalls.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What are the three core callback methods in Spark's `StreamingQueryListener`?
# MAGIC 2. How do you detect backpressure in a Spark Structured Streaming query programmatically?
# MAGIC 3. What is the difference between `triggerExecution` duration and `batchDuration` in streaming metrics?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
