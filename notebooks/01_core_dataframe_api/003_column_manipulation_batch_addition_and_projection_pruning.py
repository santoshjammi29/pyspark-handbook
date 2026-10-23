# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 003: Column Manipulation, Batch Addition & Projection Pruning
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API  
# MAGIC **Difficulty:** `Beginner`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A SaaS telemetry service needs to enrich raw session events with calculated duration, derived flags, normalized client identifiers, and drop redundant legacy headers without incurring multiple Catalyst plan rebuilds.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Compare single `withColumn` chaining against Spark 3.3+ `withColumns` batch operations. Rename multiple columns dynamically using dictionary mapping, drop unwanted columns, and explain projection pruning optimizations.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Chaining dozens of `.withColumn()` calls produces a deep Catalyst logical plan tree that can drastically increase driver planning and compilation time. Using `withColumns` (or a single `select` projection with all expressions) flattens the project node into a single Catalyst Project operation.

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
    .appName("UseCase_003_Column_Manipulation,_Batch_Addition_and_Projection_Pruning")
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
# MAGIC ## 5. Mocking Telemetry Events
# MAGIC Create a dataset of client telemetry logs with raw attributes.

# COMMAND ----------

telemetry_data = [
    ("EVT-1", "user_101", "mobile_app", "iOS", 120, 2, "2023-11-01"),
    ("EVT-2", "user_102", "web_browser", "Chrome", 450, 8, "2023-11-01"),
    ("EVT-3", "user_103", "mobile_app", "Android", 30, 1, "2023-11-01"),
    ("EVT-4", "user_104", "api_client", "Postman", 600, 15, "2023-11-01")
]

columns = ["event_id", "user_id", "client_type", "client_os", "duration_seconds", "actions_count", "event_date"]
df_telemetry = spark.createDataFrame(telemetry_data, columns)
df_telemetry.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. High-Performance Batch Column Addition (withColumns)
# MAGIC In Spark 3.3+, `withColumns` accepts a dictionary of column names to expressions, constructing a single Project plan.

# COMMAND ----------

# Batch enrichments using withColumns
enrichments = {
    "duration_minutes": F.round(F.col("duration_seconds") / 60.0, 2),
    "is_high_engagement": F.when((F.col("duration_seconds") > 300) & (F.col("actions_count") > 5), True).otherwise(False),
    "platform_tag": F.concat_ws("::", F.upper(F.col("client_type")), F.col("client_os")),
    "actions_per_minute": F.round(F.col("actions_count") / (F.col("duration_seconds") / 60.0), 2)
}

df_enriched = df_telemetry.withColumns(enrichments)
df_enriched.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Dynamic Column Renaming via Dictionary Mapping
# MAGIC Rename multiple fields programmatically using `toDF` or comprehension projections.

# COMMAND ----------

rename_map = {
    "event_id": "id",
    "user_id": "account_id",
    "duration_seconds": "duration_sec"
}

# Apply renaming dynamically
df_renamed = df_enriched
for old_col, new_col in rename_map.items():
    df_renamed = df_renamed.withColumnRenamed(old_col, new_col)

# Drop redundant legacy columns
df_final = df_renamed.drop("client_type", "client_os")
df_final.printSchema()
df_final.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Avoid chaining 20+ `.withColumn()` calls in loops; use `withColumns()` (Spark 3.3+) or `.select()` projections.**
# MAGIC - **Projection pruning ensures Catalyst only reads and transmits requested columns from Parquet/Delta storage.**
# MAGIC - **`withColumnRenamed()` is a metadata operation that doesn't trigger shuffles.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. Why is chaining many `.withColumn()` calls considered an anti-pattern in PySpark?
# MAGIC 2. How does Catalyst Optimizer optimize a single `.select()` containing multiple expressions compared to multiple `.withColumn()` steps?
# MAGIC 3. What is projection pruning and how does it leverage columnar storage like Parquet/Delta?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
