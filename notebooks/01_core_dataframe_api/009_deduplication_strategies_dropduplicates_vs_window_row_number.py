# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 009: Deduplication Strategies: dropDuplicates vs Window Row-Number
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC An e-commerce order ingestion pipeline receives duplicate HTTP webhook payloads. Some duplicates are exact identical records, while others represent updated state payloads for the same order with later timestamps.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Implement deduplication: (1) `distinct()` for exact row matches, (2) `dropDuplicates([subset])` for primary key deduplication with arbitrary winner selection, and (3) Deterministic deduplication using `row_number()` over Window partitions to retain the latest timestamp record.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC `dropDuplicates()` uses a HashAggregate under the hood. While efficient, it is non-deterministic regarding which non-key column values survive if not pre-sorted. Window functions (`row_number`) provide 100% deterministic deduplication by explicitly controlling sorting criteria.

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
        .appName("UseCase_009_Deduplication_Strategies_dropDuplicates_vs_Window_Row_Number")
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
# MAGIC ### Step 1: Generating Webhook Ingestion Duplicates
# MAGIC Create dataset containing both exact duplicates and out-of-order state updates.

# COMMAND ----------

webhook_events = [
    ("ORD-101", "CREATED", "2023-11-20 09:00:00", 120.00),
    ("ORD-101", "PAID",    "2023-11-20 09:05:00", 120.00),
    ("ORD-101", "PAID",    "2023-11-20 09:05:00", 120.00), # Exact duplicate
    ("ORD-102", "CREATED", "2023-11-20 09:10:00", 85.50),
    ("ORD-102", "CANCELLED","2023-11-20 09:12:00", 85.50),
    ("ORD-103", "CREATED", "2023-11-20 09:15:00", 430.00)
]

schema = "order_id STRING, order_status STRING, updated_at STRING, total_amount DOUBLE"
df_webhooks = spark.createDataFrame(webhook_events, schema)
df_webhooks.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Strategy 1 & 2: distinct() and dropDuplicates()
# MAGIC Demonstrate the limitations of simple deduplication.

# COMMAND ----------

# Distinct drops identical rows
df_distinct = df_webhooks.distinct()
print(f"Original: {df_webhooks.count()} | After distinct: {df_distinct.count()}")

# dropDuplicates on order_id keeps an arbitrary update
df_arbitrary = df_webhooks.dropDuplicates(["order_id"])
print("Arbitrary dropDuplicates (non-deterministic latest status):")
df_arbitrary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Strategy 3: Deterministic Window Deduplication
# MAGIC Partition by primary key, order by timestamp descending, and filter where row_number == 1.

# COMMAND ----------

window_spec = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())

df_deterministic = (
    df_webhooks
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print("Deterministic Deduplication (Always Keeps Latest Status):")
df_deterministic.orderBy("order_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`distinct()` hashes all columns across the dataset, triggering a shuffle.**
# MAGIC - **`dropDuplicates(subset)` is faster than window functions but does not allow ordering criteria for which duplicate record survives.**
# MAGIC - **For event-driven CDC feeds, always use `row_number().over(Window.partitionBy(pk).orderBy(F.col('ts').desc())) == 1`.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why can `dropDuplicates(['id'])` return non-deterministic values for remaining columns?
# MAGIC 2. What is the memory and shuffle difference between `dropDuplicates` and `row_number()` window filtering?
# MAGIC 3. How does Delta Lake `MERGE INTO` replace the need for batch deduplication in medallion Silver tables?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
