# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 022: Advanced Aggregations: collect_list vs collect_set and Sorted Collections
# MAGIC
# MAGIC **Module:** Module 02: Complex Data Types & Nested Structures Handling
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A digital customer journey pipeline aggregates all URLs visited by users within a session. Product teams need both: (1) The exact chronologically ordered sequence of pages visited (including duplicates), and (2) The unique set of pages visited without duplicates.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Compare `collect_list()` (preserves duplicates) vs `collect_set()` (drops duplicates). Demonstrate sorting collected arrays chronologically by sorting before aggregation or using `array_sort` on collected structs.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC `collect_list` and `collect_set` are wide aggregations that buffer array elements in executor memory per group. If a single key contains millions of entries (key skew), this can trigger severe Executor OutOfMemory (OOM) errors. Pre-sorting rows before `groupBy` is NOT guaranteed to preserve order across shuffles unless using sorted structs.

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
        .appName("UseCase_022_Advanced_Aggregations_collect_list_vs_collect_set_and_Sorted_Collections")
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
# MAGIC ### Step 1: Generating User Session Clickstream
# MAGIC Create chronologically timestamped page views per user session.

# COMMAND ----------

clickstream = [
    ("SESS-1", "2023-11-20 10:00:00", "/home"),
    ("SESS-1", "2023-11-20 10:02:00", "/products"),
    ("SESS-1", "2023-11-20 10:05:00", "/products"),  # Repeated visit
    ("SESS-1", "2023-11-20 10:10:00", "/checkout"),
    ("SESS-2", "2023-11-20 11:00:00", "/home"),
    ("SESS-2", "2023-11-20 11:04:00", "/pricing"),
    ("SESS-2", "2023-11-20 11:08:00", "/home")       # Repeated visit
]

schema = "session_id STRING, event_time STRING, page_url STRING"
df_clicks = spark.createDataFrame(clickstream, schema)
df_clicks.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: collect_list vs collect_set Aggregation
# MAGIC Observe difference in duplicate retention.

# COMMAND ----------

df_aggregated = df_clicks.groupBy("session_id").agg(
    F.collect_list("page_url").alias("all_pages_list"),
    F.collect_set("page_url").alias("unique_pages_set")
)

df_aggregated.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Guaranteed Chronological Sorting of Collected Elements
# MAGIC Pack timestamp and URL into a struct, collect, sort, and extract page URLs.

# COMMAND ----------

# Pack timestamp and URL into struct: array_sort will sort by struct's first field (event_time)
df_sorted_journey = (
    df_clicks
    .groupBy("session_id")
    .agg(
        F.array_sort(F.collect_list(F.struct("event_time", "page_url"))).alias("sorted_struct_events")
    )
    .withColumn(
        "ordered_page_sequence",
        F.expr("transform(sorted_struct_events, x -> x.page_url)")
    )
    .select("session_id", "ordered_page_sequence")
)

print("Guaranteed Chronologically Ordered Customer Journey:")
df_sorted_journey.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`collect_list()` retains duplicates and element count; `collect_set()` removes duplicates without guaranteed order.**
# MAGIC - **Shuffles in Spark do not guarantee row arrival order; to sort collected arrays, bundle values into structs with sort keys and apply `array_sort()`.**
# MAGIC - **Beware of key skew: collecting millions of items on a single key causes executor heap exhaustion.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why is `df.sort('time').groupBy('id').agg(collect_list('val'))` NOT guaranteed to preserve order in distributed Spark?
# MAGIC 2. How do you safely sort elements collected by `collect_list()`?
# MAGIC 3. What risk does `collect_list()` pose when operating on skewed keys?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
