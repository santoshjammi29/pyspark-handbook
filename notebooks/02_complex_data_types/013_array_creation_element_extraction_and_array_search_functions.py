# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 013: Array Creation, Element Extraction and Array Search Functions
# MAGIC
# MAGIC **Module:** Module 02: Complex Data Types & Nested Structures Handling
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A digital streaming platform logs user engagement tags, genre preferences, and content playlists as arrays. Marketing analysts need to inspect array length, test for specific genre membership, and extract primary tags.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master array operations: create arrays from multiple columns (`array()`), test element membership (`array_contains()`), retrieve element by index with 1-based and negative indexing (`element_at`), sort array elements (`array_sort`), and compute array sizes.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Spark represents ArrayType as native columnar array buffers in Tungsten memory. `element_at` uses 1-based indexing for positive indices (1 = first element) and -1 for the last element, differing from standard Python 0-based indexing.

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
        .appName("UseCase_013_Array_Creation_Element_Extraction_and_Array_Search_Functions")
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
# MAGIC ### Step 1: Generating Streaming User Preferences
# MAGIC Create mock streaming user records with tags and category arrays.

# COMMAND ----------

user_media = [
    ("U-101", ["Sci-Fi", "Action", "Thriller"], ["HD", "Dolby Atmos", "4K"]),
    ("U-102", ["Romance", "Comedy"], ["SD"]),
    ("U-103", ["Documentary"], ["HD", "Subtitles"]),
    ("U-104", ["Action", "Anime", "Fantasy", "Adventure"], ["4K", "HDR10"])
]

schema = "user_id STRING, genres ARRAY<STRING>, stream_features ARRAY<STRING>"
df_users = spark.createDataFrame(user_media, schema)
df_users.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Array Membership, Length and Extraction
# MAGIC Inspect arrays with array_contains, size, element_at, and array_sort.

# COMMAND ----------

df_array_ops = df_users.withColumn(
    "genre_count", F.size(F.col("genres"))
).withColumn(
    "likes_action", F.array_contains(F.col("genres"), "Action")
).withColumn(
    "primary_genre", F.element_at(F.col("genres"), 1)  # 1-based index: first
).withColumn(
    "last_genre", F.element_at(F.col("genres"), -1)     # negative index: last
).withColumn(
    "sorted_genres", F.array_sort(F.col("genres"))
)

df_array_ops.select("user_id", "genre_count", "likes_action", "primary_genre", "last_genre", "sorted_genres").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Array Set Operations: Union, Intersection & Distinct
# MAGIC Compute combined tags across multiple arrays using array_union, array_intersect, and array_distinct.

# COMMAND ----------

# Simulate secondary recommendations array
df_with_recs = df_users.withColumn("recommended_genres", F.array(F.lit("Action"), F.lit("Mystery"), F.lit("Sci-Fi")))

df_sets = df_with_recs.withColumn(
    "all_genres_union", F.array_distinct(F.array_union(F.col("genres"), F.col("recommended_genres")))
).withColumn(
    "common_genres", F.array_intersect(F.col("genres"), F.col("recommended_genres"))
)

df_sets.select("user_id", "all_genres_union", "common_genres").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`element_at(col, 1)` uses 1-based indexing for positive numbers and -1 for the last element.**
# MAGIC - **`array_contains()` is optimized for fast binary scans across Tungsten array representations.**
# MAGIC - **Use `array_union` paired with `array_distinct` to combine distinct sets without exploding into rows.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How does `element_at` handle out-of-bounds indices in Spark SQL?
# MAGIC 2. What is the difference between `array_distinct` and exploding an array followed by `distinct()`?
# MAGIC 3. How does Spark's Tungsten memory format store nested ArrayType values internally?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
