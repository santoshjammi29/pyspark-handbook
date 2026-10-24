# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 047: JDBC Parallel Ingestion: Partitioning by Column, fetchsize and Connection Sizing
# MAGIC
# MAGIC **Module:** Module 05: Data Ingestion Patterns, Auto Loader & File Formats
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A night batch pipeline extracts 50 million sales transaction records from an operational PostgreSQL production database into the Lakehouse. A single-threaded JDBC query takes 6 hours and causes database connection timeouts.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Optimize Spark JDBC ingestion: configure parallel partition extraction using `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions`. Tune `fetchsize` to balance executor memory vs network round-trips, and avoid database connection exhaustion.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC By default, `spark.read.jdbc()` executes a single query on a single executor task (`SELECT * FROM table`), creating a severe driver bottleneck. By specifying partition parameters, Spark splits the query into $N$ parallel slice queries (`WHERE col >= lower AND col < upper`) distributed across executor workers.

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
        .appName("UseCase_047_JDBC_Parallel_Ingestion_Partitioning_by_Column_fetchsize_and_Connection_Sizing")
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
# MAGIC ### Step 1: Reviewing Default vs Partitioned JDBC Architecture
# MAGIC Illustrate how Spark generates parallel SQL queries across partitions.

# COMMAND ----------

# Architectural Blueprint:
# Spark creates N independent tasks, each executing a sliced query:
# Task 1: SELECT * FROM sales WHERE id >= 1 AND id < 2500000
# Task 2: SELECT * FROM sales WHERE id >= 2500000 AND id < 5000000
# Task 3: SELECT * FROM sales WHERE id >= 5000000 AND id < 7500000
# Task 4: SELECT * FROM sales WHERE id >= 7500000 AND id <= 10000000

print("Parallel JDBC partitioning architecture mapped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Constructing Resilient JDBC Ingestion Configuration
# MAGIC Build production-grade JDBC parameter dictionary.

# COMMAND ----------

jdbc_options = {
    "url": "jdbc:postgresql://db-host.internal:5432/retail_db",
    "dbtable": "public.orders",
    "user": "etl_reader",
    "password": "redacted_secret",
    "driver": "org.postgresql.Driver",
    # Crucial parallel parameters:
    "partitionColumn": "order_id",
    "lowerBound": "1",
    "upperBound": "10000000",
    "numPartitions": "16",       # 16 parallel tasks reading simultaneously
    "fetchsize": "10000"         # Read 10,000 rows per network roundtrip (prevents OOM)
}

print("Production JDBC configuration dictionary:")
for k, v in jdbc_options.items():
    if k != "password":
        print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Simulating Parallel Ingestion via Sliced Partitions Locally
# MAGIC Demonstrate the mathematical partition slice generator in PySpark.

# COMMAND ----------

# Simulate 4 parallel partitions over order_id range 1 to 100
df_simulated_db = spark.range(1, 101, 1, 4).withColumnRenamed("id", "order_id").withColumn(
    "order_amount", F.round(F.rand(seed=42) * 500 + 10, 2)
)

print(f"Number of partitions: {df_simulated_db.rdd.getNumPartitions()}")
print("Row distribution per partition:")
df_simulated_db.withColumn("partition_id", F.spark_partition_id()).groupBy("partition_id").agg(
    F.min("order_id").alias("min_id"),
    F.max("order_id").alias("max_id"),
    F.count("order_id").alias("rows")
).orderBy("partition_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Always supply `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` when reading large JDBC tables.**
# MAGIC - **The `partitionColumn` MUST be numeric, indexed, and evenly distributed (e.g. auto-incrementing primary key).**
# MAGIC - **Tune `fetchsize` (e.g. 5,000 to 50,000) to avoid default single-row roundtrips or massive memory spikes.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What happens if you run `spark.read.jdbc()` without specifying `partitionColumn`?
# MAGIC 2. What happens if the `partitionColumn` in a JDBC read has severe data skew (e.g. 90% of rows have id=1)?
# MAGIC 3. How does `fetchsize` impact both the source database memory and the Spark executor memory?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
