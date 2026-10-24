# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 078: Bucketing for Pre-Sorted Join Optimization and Shuffle Elimination
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC Two multi-billion-row fact tables (Orders and Shipments) are joined hourly in a critical financial pipeline. Because both tables are huge, broadcast joins are impossible. Every run shuffles 5TB of data across the network, consuming 40 minutes of compute.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Eliminate shuffle using Spark Bucketing: create bucketed tables with `bucketBy()` and `sortBy()`, configure matching bucket counts on both tables, and verify the total elimination of the Exchange (shuffle) operator from Sort-Merge Joins.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Bucketing pre-shuffles and pre-sorts data on write according to a hash function of the bucket column. When two tables bucketed on the same key with identical (or multiple) bucket counts are joined, Spark reads matching bucket files directly without triggering any network shuffle (`Exchange` operator disappears).

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
        .appName("UseCase_078_Bucketing_for_Pre_Sorted_Join_Optimization_and_Shuffle_Elimination")
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
# MAGIC ### Step 1: Creating Bucketed Orders and Shipments Tables
# MAGIC Write tables bucketed into 4 buckets on join key 'order_id'.

# COMMAND ----------

# Ensure Hive support or managed catalog enables bucketing
spark.sql("CREATE DATABASE IF NOT EXISTS bucket_db_078")
spark.sql("USE bucket_db_078")

# Create bucketed orders table
orders = [(f"ORD-{i:03d}", 100.0 + i) for i in range(1, 21)]
df_ord = spark.createDataFrame(orders, ["order_id", "amount"])

df_ord.write.format("parquet")\
    .bucketBy(4, "order_id")\
    .sortBy("order_id")\
    .mode("overwrite")\
    .saveAsTable("bucket_db_078.t_orders")

# Create bucketed shipments table with identical bucket count (4)
shipments = [(f"ORD-{i:03d}", f"CARRIER-{i%3}") for i in range(1, 21)]
df_ship = spark.createDataFrame(shipments, ["order_id", "carrier"])

df_ship.write.format("parquet")\
    .bucketBy(4, "order_id")\
    .sortBy("order_id")\
    .mode("overwrite")\
    .saveAsTable("bucket_db_078.t_shipments")

print("Both tables bucketed into 4 buckets and sorted on order_id.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Verifying Shuffle Elimination in Physical Join Plan
# MAGIC Join bucketed tables and inspect physical plan.

# COMMAND ----------

# Enable bucketing optimization
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")

df_b1 = spark.table("bucket_db_078.t_orders")
df_b2 = spark.table("bucket_db_078.t_shipments")

df_joined_bucket = df_b1.join(df_b2, on="order_id", how="inner")

print("=== PHYSICAL PLAN (Notice absence of 'Exchange hashpartitioning' shuffle!) ===")
df_joined_bucket.explain(mode="simple")
df_joined_bucket.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Clean Up Database
# MAGIC Drop temporary bucketed tables.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS bucket_db_078.t_orders")
spark.sql("DROP TABLE IF EXISTS bucket_db_078.t_shipments")
print("Cleaned up temporary tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Bucketing pre-sorts and pre-distributes data on write, eliminating network shuffle during downstream joins.**
# MAGIC - **Both tables must be bucketed on the exact same join key and have matching or multiple bucket counts.**
# MAGIC - **Bucketing requires managed tables (`saveAsTable`); it does not apply to unmanaged path-based writes.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How does bucketing eliminate the `Exchange` operator in a Sort-Merge Join physical plan?
# MAGIC 2. What happens if two bucketed tables are joined, but Table A has 8 buckets and Table B has 16 buckets?
# MAGIC 3. Why is bucketing less common in modern Databricks compared to Z-Ordering and Liquid Clustering?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
