# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 008: Sorting, Ordering Semantics & Stratified Sampling
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API  
# MAGIC **Difficulty:** `Beginner`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A financial auditing team must sample transaction logs for regulatory compliance. The sample must be stratified across risk tiers (Low, Medium, High) to reflect true operational risk distribution without skewing toward high-volume low-risk transactions.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement ordering with explicit null placement (`asc_nulls_first`, `desc_nulls_last`). Compare `sample()` vs `sampleBy()` for stratified sampling across categorical groups, and evaluate execution performance.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC `orderBy()` and `sort()` trigger a global RangePartitioner shuffle across all executor partitions, which can be expensive on multi-terabyte datasets. `sortWithinPartitions()` sorts locally inside each partition without shuffling.

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
    .appName("UseCase_008_Sorting,_Ordering_Semantics_and_Stratified_Sampling")
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
# MAGIC ## 5. Creating Auditing Dataset
# MAGIC Generate audit logs with risk tiers and financial values.

# COMMAND ----------

import random

# Seed for reproducible mock data
random.seed(42)
risk_tiers = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]

records = [
    (
        f"AUD-{i:04d}",
        random.choice(risk_tiers),
        round(random.uniform(10.0, 50000.0), 2) if i % 10 != 0 else None,
        f"ORG-{random.randint(1, 5)}"
    )
    for i in range(1, 101)
]

df_audit_logs = spark.createDataFrame(records, ["audit_id", "risk_tier", "amount", "org_id"])
df_audit_logs.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Multi-Column Sorting with Explicit Null Placement
# MAGIC Sort by risk tier and amount, ensuring null amounts are placed at the end.

# COMMAND ----------

df_sorted = df_audit_logs.orderBy(
    F.col("risk_tier").asc(),
    F.col("amount").desc_nulls_last()
)

print("Top 10 Sorted Records:")
df_sorted.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Stratified Sampling via sampleBy
# MAGIC Extract representative samples per risk tier using exact fractions.

# COMMAND ----------

# Define stratification fractions: sample 10% of LOW, 30% of MEDIUM, 80% of HIGH, 100% of CRITICAL
fractions = {
    "LOW": 0.10,
    "MEDIUM": 0.30,
    "HIGH": 0.80,
    "CRITICAL": 1.00
}

df_stratified = df_audit_logs.sampleBy("risk_tier", fractions=fractions, seed=42)

print("Stratified Sample Distribution:")
df_stratified.groupBy("risk_tier").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`orderBy` performs a global sort via RangePartitioner shuffle, while `sortWithinPartitions` performs local memory sorts without shuffle.**
# MAGIC - **`sampleBy()` performs stratified sampling without requiring manual filtering and union steps.**
# MAGIC - **Always specify `seed` in sampling calls when building deterministic test suites or reproducible ML splits.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `sort()` and `sortWithinPartitions()` in PySpark?
# MAGIC 2. How does Spark's RangePartitioner sample keys to determine partition boundaries during a global sort?
# MAGIC 3. Why can `df.sample(withReplacement=False, fraction=0.1)` produce slightly more or less than 10% of total rows?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
