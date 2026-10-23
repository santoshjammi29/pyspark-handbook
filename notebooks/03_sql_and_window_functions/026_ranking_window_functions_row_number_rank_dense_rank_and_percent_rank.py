# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 026: Ranking Window Functions: row_number, rank, dense_rank and percent_rank
# MAGIC
# MAGIC **Module:** Module 03: Advanced SQL, Aggregations & Analytical Window Functions  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A national sales management board ranks sales representatives within each geographic territory based on quarterly quota attainment. The ranking system must handle tie scores appropriately and calculate relative percentile percentiles for bonus distribution.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Compare and implement all four ranking window functions: `row_number()`, `rank()`, `dense_rank()`, and `percent_rank()`. Explain how tied values affect sequential numbering and subsequent rank positions. Filter for Top-N performers per partition.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Window ranking functions require a WindowSpec with `partitionBy()` and `orderBy()`. Rows are partitioned across executors by partition key and sorted in-memory. If no `partitionBy()` is provided, all data is moved to a single executor partition, creating a catastrophic bottleneck.

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
    .appName("UseCase_026_Ranking_Window_Functions_row_number,_rank,_dense_rank_and_percent_rank")
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
# MAGIC ## 5. Generating Sales Rep Quota Scores with Ties
# MAGIC Create sales rep performance dataset with identical quota scores.

# COMMAND ----------

reps_data = [
    ("East", "Alice", 125.0),
    ("East", "Bob", 110.0),
    ("East", "Charlie", 110.0), # Tied with Bob
    ("East", "David", 95.0),
    ("West", "Eve", 140.0),
    ("West", "Frank", 130.0),
    ("West", "Grace", 130.0), # Tied with Frank
    ("West", "Heidi", 100.0)
]

schema = "region STRING, rep_name STRING, quota_pct DOUBLE"
df_reps = spark.createDataFrame(reps_data, schema)
df_reps.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Executing All Four Ranking Functions Simultaneously
# MAGIC Observe behavior on tied quota scores.

# COMMAND ----------

window_spec = Window.partitionBy("region").orderBy(F.col("quota_pct").desc())

df_ranked = df_reps.withColumn(
    "row_num", F.row_number().over(window_spec)
).withColumn(
    "rank", F.rank().over(window_spec)
).withColumn(
    "dense_rank", F.dense_rank().over(window_spec)
).withColumn(
    "percent_rank", F.round(F.percent_rank().over(window_spec), 2)
)

print("Comprehensive Ranking Comparison:")
df_ranked.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Top-2 Performers per Region Filtering
# MAGIC Filter for top performers using dense_rank to include all tied runners-up.

# COMMAND ----------

# Using dense_rank ensures ties at rank 2 are both included
df_top2 = df_ranked.filter(F.col("dense_rank") <= 2).select("region", "rep_name", "quota_pct", "dense_rank")
print("Top 2 Performers per Region (Preserving Ties):")
df_top2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`row_number()` assigns strictly unique sequential integers regardless of ties.**
# MAGIC - **`rank()` produces duplicate ranks on ties and skips subsequent numbers (e.g. 1, 2, 2, 4).**
# MAGIC - **`dense_rank()` produces duplicate ranks on ties without skipping (e.g. 1, 2, 2, 3).**
# MAGIC - **Never call `.over(Window.orderBy())` without `partitionBy()` on large tables, as it moves the entire dataset into one partition.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `rank()` and `dense_rank()` when multiple rows have identical values?
# MAGIC 2. What happens to cluster resources if you define a Window without a `partitionBy` clause?
# MAGIC 3. How does `percent_rank()` calculate its value mathematically?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
