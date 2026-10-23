# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 045: Parquet and ORC Columnar Ingestion: Predicate Pushdown and Dictionary Encoding
# MAGIC
# MAGIC **Module:** Module 05: Data Ingestion Patterns, Auto Loader & File Formats  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A petabyte-scale telemetry lakehouse queries 500 million vehicle diagnostic events daily. Analyst queries typically filter on single vehicle IDs and read only 3 of the 80 columns. Inefficient readers trigger multi-terabyte disk reads and network bottlenecks.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Demonstrate the power of columnar formats: compare Parquet and ORC file formats, observe Column Projection (reading only requested columns), and inspect Catalyst physical plans to verify Predicate Pushdown (file and row-group skipping via min/max statistics).
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Columnar storage arranges data by column rather than row. Row groups contain metadata statistics (min, max, null count). Spark's Parquet reader evaluates query predicates against row-group metadata before reading page data from disk, skipping entire blocks that cannot contain matching rows.

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
    .appName("UseCase_045_Parquet_and_ORC_Columnar_Ingestion_Predicate_Pushdown_and_Dictionary_Encoding")
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
# MAGIC ## 5. Generating Wide Diagnostic Telemetry Dataset
# MAGIC Create dataset with multiple columns and save as Parquet.

# COMMAND ----------

parquet_path = "/tmp/parquet_telemetry_045"
if Path(parquet_path).exists():
    shutil.rmtree(parquet_path)

import random
random.seed(42)

# Generate 50,000 telemetry readings with 6 columns
rows = [
    (i, f"V-{random.randint(1, 100):03d}", round(random.uniform(50.0, 120.0), 2), round(random.uniform(20.0, 45.0), 2), f"FW-v{random.randint(1, 4)}.0", "OK")
    for i in range(1, 50001)
]

schema = "reading_id INT, vehicle_id STRING, speed DOUBLE, temp DOUBLE, firmware STRING, status STRING"
df_source = spark.createDataFrame(rows, schema)

# Write as Parquet with Snappy compression
df_source.write.format("parquet").mode("overwrite").save(parquet_path)
print(f"Parquet dataset written with 50,000 rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Demonstrating Column Projection (Zero-Cost Column Pruning)
# MAGIC Select only 2 columns and observe Catalyst projected schema.

# COMMAND ----------

# Read only reading_id and speed
df_pruned = spark.read.parquet(parquet_path).select("reading_id", "speed")

print("=== PHYSICAL PLAN FOR COLUMN PROJECTION ===")
# Notice: 'ReadSchema: struct<reading_id:int,speed:double>' -> Spark skips reading all other 4 columns from disk!
df_pruned.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Demonstrating Predicate Pushdown (Row-Group Skipping)
# MAGIC Filter on vehicle_id and verify PushedFilters in physical plan.

# COMMAND ----------

df_filtered = spark.read.parquet(parquet_path).filter(F.col("vehicle_id") == "V-042").select("vehicle_id", "speed")

print("=== PHYSICAL PLAN FOR PREDICATE PUSHDOWN ===")
# Notice: 'PushedFilters: [IsNotNull(vehicle_id), EqualTo(vehicle_id,V-042)]'
df_filtered.explain(mode="formatted")
df_filtered.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Column Projection reads only required columns from disk, dramatically reducing I/O and network transfer.**
# MAGIC - **Predicate Pushdown pushes WHERE filters directly to the Parquet/ORC file reader to skip row groups using min/max stats.**
# MAGIC - **Dictionary encoding replaces repetitive string values with small integer keys, compressing high-repetition columns efficiently.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the internal structure of a Parquet file (Header, Row Groups, Column Chunks, Pages, Footer)?
# MAGIC 2. Why does wrapping a column in a function (e.g. `WHERE upper(col) == 'VAL'`) disable Parquet predicate pushdown?
# MAGIC 3. How does ORC's Lightweight Index compare to Parquet's Row Group Statistics?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
