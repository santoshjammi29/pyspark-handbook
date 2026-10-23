# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 070: Slowly Changing Dimension Type 3 and Type 4 Multi-Tier Implementation
# MAGIC
# MAGIC **Module:** Module 07: Lakehouse Data Modeling, CDC & SCD Implementation  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A sales commission system tracks territory reassignments. Sales reps occasionally switch regions. Compensation analysts only need to compare sales under the 'current territory' vs 'immediately previous territory', while enterprise audit requires an external historical archive.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement SCD Type 3 (adding `current_territory` and `previous_territory` columns in the active table) and SCD Type 4 (maintaining a fast current-state table paired with a separate append-only historical audit archive table).
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC SCD Type 3 stores previous and current attribute values side-by-side in the same row, simplifying year-over-year territory comparisons. SCD Type 4 splits storage: the main dimension holds only current rows for fast lookups, while an external history table records all historical mutation events.

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
    .appName("UseCase_070_Slowly_Changing_Dimension_Type_3_and_Type_4_Multi-Tier_Implementation")
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
# MAGIC ## 5. Implementing SCD Type 3 (Previous vs Current Columns)
# MAGIC Create sales rep table with current and prior territory columns.

# COMMAND ----------

scd3_data = [
    ("REP-01", "Alice", "West", None, "2023-01-01"),
    ("REP-02", "Bob", "East", None, "2023-01-01")
]

schema3 = "rep_id STRING, rep_name STRING, current_territory STRING, previous_territory STRING, last_reassigned STRING"
df_scd3 = spark.createDataFrame(scd3_data, schema3)
print("Initial SCD Type 3 Dimension Table:")
df_scd3.show()

# Alice moves from West to North territory
df_scd3_updated = df_scd3.withColumn(
    "previous_territory",
    F.when(F.col("rep_id") == "REP-01", F.col("current_territory")).otherwise(F.col("previous_territory"))
).withColumn(
    "current_territory",
    F.when(F.col("rep_id") == "REP-01", F.lit("North")).otherwise(F.col("current_territory"))
).withColumn(
    "last_reassigned",
    F.when(F.col("rep_id") == "REP-01", F.lit("2023-11-20")).otherwise(F.col("last_reassigned"))
)

print("Updated SCD Type 3 Dimension (Comparing Current vs Previous):")
df_scd3_updated.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Implementing SCD Type 4 (Current Table + Separate Historical Archive)
# MAGIC Maintain a fast active table and append modifications to history table.

# COMMAND ----------

# Active Table (Current state only -> ultra-fast lookups)
active_table = [("REP-01", "Alice", "North"), ("REP-02", "Bob", "East")]
df_active = spark.createDataFrame(active_table, ["rep_id", "name", "current_territory"])

# Historical Archive Table (Append-only audit trail)
archive_table = [
    ("REP-01", "West", "2023-01-01", "2023-11-20"),
    ("REP-01", "North", "2023-11-20", "9999-12-31"),
    ("REP-02", "East", "2023-01-01", "9999-12-31")
]
df_archive = spark.createDataFrame(archive_table, ["rep_id", "territory", "effective_from", "effective_to"])

print("SCD Type 4 Active Dimension:")
df_active.show()
print("SCD Type 4 Historical Archive Table:")
df_archive.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **SCD Type 3 tracks immediate prior state by adding a `previous_value` column to the same row.**
# MAGIC - **SCD Type 4 maintains two physical tables: a fast current-state table and a separate historical audit log.**
# MAGIC - **SCD Type 4 maximizes query performance on current records while retaining full compliance history.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. When is SCD Type 3 preferred over SCD Type 2 in dimensional modeling?
# MAGIC 2. What are the operational advantages of SCD Type 4 for high-frequency update tables?
# MAGIC 3. How do fact tables join against an SCD Type 4 architecture?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
