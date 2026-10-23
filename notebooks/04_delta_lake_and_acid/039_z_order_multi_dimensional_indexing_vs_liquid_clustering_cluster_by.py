# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 039: Z-Order Multi-Dimensional Indexing vs Liquid Clustering (CLUSTER BY)
# MAGIC
# MAGIC **Module:** Module 04: Delta Lake Architecture, ACID Operations & Time Travel  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC An ad-tech analytics platform runs ad-hoc queries filtering on combinations of `customer_id`, `event_date`, and `campaign_id`. Traditional Hive partitioning on all three columns causes over-partitioning, while single-column sorting leaves other query predicates unoptimized.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Compare multi-dimensional indexing techniques: implement `OPTIMIZE ... ZORDER BY (col1, col2)` to co-locate multidimensional data along space-filling Hilbert/Morton curves, and contrast with modern Databricks Liquid Clustering (`CLUSTER BY`) which supports incremental re-clustering without full rewrites.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Z-Ordering maps multi-dimensional data onto a 1D space-filling curve, preserving spatial locality so queries filtering on any subset of Z-ordered columns skip unneeded Parquet files. Liquid Clustering (Databricks Runtime 13.3+) replaces partitioning and Z-order with dynamic clustering that scales incrementally.

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
    .appName("UseCase_039_Z-Order_Multi-Dimensional_Indexing_vs_Liquid_Clustering_(CLUSTER_BY)")
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
# MAGIC ## 5. Generating Multi-Dimensional Telemetry Dataset
# MAGIC Create dataset queried across multiple filter dimensions.

# COMMAND ----------

delta_zorder_path = "/tmp/delta_zorder_039"
if Path(delta_zorder_path).exists():
    shutil.rmtree(delta_zorder_path)

import random
random.seed(42)

telemetry = [
    (f"DEV-{random.randint(1, 100):03d}", f"CAMP-{random.randint(1, 20):02d}", f"2023-11-{random.randint(1, 10):02d}", round(random.uniform(10.0, 500.0), 2))
    for _ in range(1000)
]

schema = "device_id STRING, campaign_id STRING, event_date STRING, metric_value DOUBLE"
df_telemetry = spark.createDataFrame(telemetry, schema)
df_telemetry.write.format("delta").mode("overwrite").save(delta_zorder_path)

print("Telemetry Delta table written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Applying Z-ORDER BY Multi-Dimensional Compaction
# MAGIC Run OPTIMIZE with ZORDER BY on multiple high-cardinality query columns.

# COMMAND ----------

# Z-Order on device_id and campaign_id
df_zorder_res = spark.sql(f"OPTIMIZE delta.`{delta_zorder_path}` ZORDER BY (device_id, campaign_id)")
df_zorder_res.select("path", "metrics.numFilesAdded", "metrics.numFilesRemoved").show(truncate=False)

print("Table successfully Z-Ordered for fast multi-column data skipping!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Architectural Deep Dive: Liquid Clustering (CLUSTER BY)
# MAGIC Explain Databricks Liquid Clustering syntax and operational advantages.

# COMMAND ----------

# In Databricks Runtime 13.3+ / Unity Catalog, Liquid Clustering is declared via:
# CREATE TABLE events (
#     device_id STRING,
#     campaign_id STRING,
#     event_date DATE,
#     metric_value DOUBLE
# ) USING DELTA
# CLUSTER BY (device_id, campaign_id);

# To re-cluster incrementally:
# OPTIMIZE events;  -- Only clusters newly appended data without rewriting entire table!

print("Liquid Clustering advantages: no partition lock-in, non-destructive column changes, and incremental re-clustering!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Z-Ordering arranges multi-dimensional data along a Morton space-filling curve for optimal file skipping.**
# MAGIC - **Z-Order requires periodic full rewrites of data files; Liquid Clustering (`CLUSTER BY`) enables incremental clustering.**
# MAGIC - **Choose clustering columns that appear frequently in query WHERE clause filters and possess high cardinality.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How does Z-Ordering achieve file skipping across multiple columns simultaneously without creating Hive partition directories?
# MAGIC 2. What is the main limitation of traditional Hive partitioning that Liquid Clustering solves?
# MAGIC 3. Why should you limit Z-Order to 2-4 columns rather than Z-ordering 10+ columns?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
