# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 076: Data Skew Detection and Mitigation Using Salting and Two-Stage Joins
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A national e-commerce platform joins website clicks with user accounts. Out of 100 million clicks, 40% belong to guest user ID `GUEST_000` (null/anonymous user). The join stalls at 99% for 45 minutes because a single executor task processes all 40 million guest clicks, causing shuffle spill and OOM.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Detect and mitigate severe data skew: quantify partition skew using `spark_partition_id()`, implement the Salting technique (adding random integer suffixes to the skewed key on the large table and exploding matching suffixes on the dimension table), and measure performance improvements.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Data skew occurs when one partition contains disproportionately more records than others. In SortMergeJoin, rows with identical keys hash to the same reducer task. Salting appends a random salt $0 \dots (K-1)$ to the skewed key, distributing the single hot key evenly across $K$ reducer partitions.

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
        .appName("UseCase_076_Data_Skew_Detection_and_Mitigation_Using_Salting_and_Two_Stage_Joins")
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
# MAGIC ### Step 1: Simulating Severely Skewed Transaction Dataset
# MAGIC Create dataset where 80% of rows have the identical customer_id 'HOT_KEY'.

# COMMAND ----------

import random
random.seed(42)

# Generate 10,000 transactions: 8,000 belong to HOT_KEY, 2,000 distributed normally
skewed_txns = [("HOT_KEY", 10.0 + i) for i in range(8000)] + [
    (f"CUST_{random.randint(1, 500):03d}", 50.0 + i) for i in range(2000)
]

df_skewed = spark.createDataFrame(skewed_txns, ["customer_id", "amount"]).repartition(4, "customer_id")

print("Skew Distribution Across Partitions:")
df_skewed.withColumn("pid", F.spark_partition_id()).groupBy("pid").count().orderBy("pid").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Adding Salt to Skewed Table
# MAGIC Append a random salt integer between 0 and 3 to customer_id.

# COMMAND ----------

SALT_FACTOR = 4

# Append random salt to large skewed table
df_salted = df_skewed.withColumn(
    "salt",
    F.when(F.col("customer_id") == "HOT_KEY", F.floor(F.rand(seed=42) * SALT_FACTOR)).otherwise(F.lit(0))
).withColumn(
    "salted_key",
    F.concat(F.col("customer_id"), F.lit("_SALT_"), F.col("salt"))
)

print("Salted Large Table (HOT_KEY split into 4 distinct keys):")
df_salted.filter(F.col("customer_id") == "HOT_KEY").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Exploding Customer Lookup Table and Executing Balanced Join
# MAGIC Replicate dimension keys across all salt values and execute join with even distribution.

# COMMAND ----------

# Small customer dimension table
cust_dim = [("HOT_KEY", "Guest Shopper"), ("CUST_001", "Regular Shopper")]
df_dim = spark.createDataFrame(cust_dim, ["customer_id", "profile"])

# Replicate dimension keys across all possible salt values [0, 1, 2, 3]
df_dim_exploded = (
    df_dim
    .withColumn("salt_array", F.array([F.lit(i) for i in range(SALT_FACTOR)]))
    .select("customer_id", "profile", F.explode("salt_array").alias("salt"))
    .withColumn("salted_key", F.concat(F.col("customer_id"), F.lit("_SALT_"), F.col("salt")))
)

# Execute join on salted_key
df_balanced_join = df_salted.join(df_dim_exploded, on="salted_key", how="inner")

print("Post-Join Partition Distribution (Notice even distribution across partitions):")
df_balanced_join.withColumn("pid", F.spark_partition_id()).groupBy("pid").count().orderBy("pid").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Data skew causes the notorious '99% task stuck' problem where one executor handles massive key volume.**
# MAGIC - **Salting appends random integers to the hot key on the fact table and replicates dimension rows with matching salts.**
# MAGIC - **In Spark 3.0+, enable AQE (`spark.sql.adaptive.skewJoin.enabled = true`) to handle skew automatically without manual code salting.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What are the symptoms of data skew in the Spark UI (Task Duration, GC Time, Shuffle Read Size)?
# MAGIC 2. How does the Salting technique redistribute a single hot join key across multiple partitions?
# MAGIC 3. What is the difference between manual Salting and AQE Dynamic Skew Join?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
