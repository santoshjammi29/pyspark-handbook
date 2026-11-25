# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 029: Sessionization and the Gaps & Islands Problem in PySpark
# MAGIC
# MAGIC **Module:** Module 03: Advanced SQL, Aggregations & Analytical Window Functions
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A digital analytics platform tracks web page views. An active browsing session is defined as consecutive clicks separated by no more than 30 minutes of inactivity. When a gap exceeds 30 minutes, a new unique session identifier must be generated.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Solve the classic Gaps and Islands problem in distributed PySpark: detect temporal gaps using `lag()`, mark island session boundaries with conditional flags, compute cumulative sums over flags to generate sequential session IDs, and aggregate session metrics.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Solving Gaps & Islands in Spark uses a 2-step window pattern: (1) An ordered window with `lag()` to compute time delta between consecutive events and emit a 1/0 boundary indicator, and (2) A cumulative sum window over the boundary indicator to generate monotonic session IDs across the entire partition.

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
        .appName("UseCase_029_Sessionization_and_the_Gaps_and_Islands_Problem_in_PySpark")
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
# MAGIC ### Step 1: Generating Clickstream Events with Inactivity Gaps
# MAGIC Create web clickstream data with varying time intervals.

# COMMAND ----------

clicks = [
    ("U-01", "2023-11-20 10:00:00", "/home"),
    ("U-01", "2023-11-20 10:12:00", "/catalog"),     # 12 min gap (same session)
    ("U-01", "2023-11-20 10:25:00", "/product/42"),  # 13 min gap (same session)
    ("U-01", "2023-11-20 11:30:00", "/home"),        # 65 min gap -> NEW SESSION!
    ("U-01", "2023-11-20 11:40:00", "/cart"),        # 10 min gap (same session)
    ("U-02", "2023-11-20 09:00:00", "/login"),
    ("U-02", "2023-11-20 09:15:00", "/dashboard")
]

schema = "user_id STRING, event_time STRING, page STRING"
df_raw_clicks = spark.createDataFrame(clicks, schema).withColumn("ts", F.to_timestamp("event_time"))
df_raw_clicks.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Calculate Elapsed Inactivity & Identify New Session Boundaries
# MAGIC Calculate difference in minutes from previous click.

# COMMAND ----------

user_window = Window.partitionBy("user_id").orderBy("ts")

df_gaps = df_raw_clicks.withColumn(
    "prev_ts", F.lag("ts").over(user_window)
).withColumn(
    "inactivity_minutes",
    F.coalesce((F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 60.0, F.lit(0.0))
).withColumn(
    "is_new_session",
    F.when((F.col("inactivity_minutes") > 30.0) | (F.col("prev_ts").isNull()), 1).otherwise(0)
)

df_gaps.select("user_id", "event_time", "inactivity_minutes", "is_new_session").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Cumulative Sum to Form Session IDs & Aggregate
# MAGIC Cumulative sum of is_new_session produces a persistent session number.

# COMMAND ----------

cum_session_window = Window.partitionBy("user_id").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_sessionized = df_gaps.withColumn(
    "session_num", F.sum("is_new_session").over(cum_session_window)
).withColumn(
    "session_id", F.concat(F.col("user_id"), F.lit("_SESS_"), F.col("session_num"))
)

print("Sessionized Clickstream:")
df_sessionized.select("session_id", "user_id", "event_time", "page", "inactivity_minutes").show(truncate=False)

# Aggregate session summaries
df_summary = df_sessionized.groupBy("session_id", "user_id").agg(
    F.min("ts").alias("session_start"),
    F.max("ts").alias("session_end"),
    F.count("page").alias("page_views"),
    F.round((F.max("ts").cast("long") - F.min("ts").cast("long")) / 60.0, 1).alias("session_duration_min")
)

print("Aggregated Session Profiles:")
df_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **The Gaps and Islands problem is solved using a two-window approach: flag boundaries with `lag`, then group using cumulative `sum`.**
# MAGIC - **Sessionization enables accurate attribution modeling, user journey mapping, and conversion funnel analysis.**
# MAGIC - **Ensure timestamp columns are parsed to epoch seconds or timestamps to avoid incorrect string comparison ordering.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Explain the two-phase window pattern used to solve Gaps and Islands in PySpark.
# MAGIC 2. What is the difference between physical sessionization in batch vs stateful session windows in Structured Streaming?
# MAGIC 3. How do you handle clock drift across client devices when calculating session boundary gaps?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
