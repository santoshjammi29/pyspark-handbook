# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 041: Delta Schema Evolution (mergeSchema) and Schema Enforcement
# MAGIC
# MAGIC **Module:** Module 04: Delta Lake Architecture, ACID Operations & Time Travel
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A product catalog service frequently releases new product attributes (e.g. `warranty_months`, `energy_rating`). When downstream batch writers encounter unexpected new columns, default Spark writes abort with schema mismatch errors.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Demonstrate Delta Lake schema enforcement: observe default write failures when extra columns are introduced, and safely evolve the table schema using the `.option('mergeSchema', 'true')` write flag and automatic schema evolution.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Delta Lake enforces schema on write to prevent bad data from silently corrupting tables. When `mergeSchema` is enabled, Delta updates the table metadata in `_delta_log` to include new columns as nullable, filling existing historical rows with NULLs upon reading.

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
        .appName("UseCase_041_Delta_Schema_Evolution_mergeSchema_and_Schema_Enforcement")
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
# MAGIC ### Step 1: Creating Initial Baseline Delta Table
# MAGIC Create base product schema with 3 columns.

# COMMAND ----------

delta_evolve_path = "/tmp/delta_schema_041"
if Path(delta_evolve_path).exists():
    shutil.rmtree(delta_evolve_path)

initial_products = [("P-100", "Laptop", 899.99)]
spark.createDataFrame(initial_products, ["product_id", "name", "price"])    .write.format("delta").mode("overwrite").save(delta_evolve_path)

print("Initial Table Schema:")
spark.read.format("delta").load(delta_evolve_path).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Demonstrating Schema Enforcement Protection
# MAGIC Observe that appending data with extra columns throws a DeltaAnalysisException.

# COMMAND ----------

new_batch_with_extra_cols = [("P-200", "Smart TV", 450.00, 24, "A++")]
df_extra = spark.createDataFrame(new_batch_with_extra_cols, ["product_id", "name", "price", "warranty_months", "energy_rating"])

try:
    # Attempting to write without mergeSchema fails safely
    df_extra.write.format("delta").mode("append").save(delta_evolve_path)
    print("Write succeeded unexpectedly!")
except Exception as e:
    print("Schema Enforcement Protected Table! Error caught:")
    print(str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Safe Schema Evolution via mergeSchema
# MAGIC Enable mergeSchema to automatically adopt new columns.

# COMMAND ----------

# Append with mergeSchema enabled
df_extra.write.format("delta")    .option("mergeSchema", "true")    .mode("append")    .save(delta_evolve_path)

print("Table Schema Successfully Evolved:")
df_evolved = spark.read.format("delta").load(delta_evolve_path)
df_evolved.printSchema()
df_evolved.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Delta Lake enforces strict schema on write by default, preventing accidental schema corruption.**
# MAGIC - **Set `.option('mergeSchema', 'true')` to allow automatic schema evolution when new columns appear.**
# MAGIC - **Evolving schema updates table metadata atomically in `_delta_log`; existing Parquet data files do not need to be rewritten.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between Schema Enforcement and Schema Evolution in Delta Lake?
# MAGIC 2. Can `mergeSchema` change the data type of an existing column (e.g. from String to Integer)?
# MAGIC 3. How does Delta Lake handle NULL values for newly added columns when querying historical data?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
