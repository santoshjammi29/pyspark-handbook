# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 049: Auto Loader Schema Evolution and Rescued Data Column (_rescued_data)
# MAGIC
# MAGIC **Module:** Module 05: Data Ingestion Patterns, Auto Loader & File Formats  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC Third-party vendors send telemetry files where new columns appear without prior warning, or column types change unexpectedly. If a pipeline drops these unexpected fields, valuable business telemetry is lost permanently.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Configure Auto Loader schema evolution: set `cloudFiles.schemaEvolutionMode = 'addNewColumns'`, and configure the `_rescued_data` column to automatically capture unexpected fields, mismatched data types, or corrupted text lines without halting the pipeline.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC When `schemaEvolutionMode` is enabled, Auto Loader automatically evolves the target schema when new columns arrive. Any values that cannot be parsed into the existing schema (e.g. string arriving in integer column) are automatically diverted into the JSON `_rescued_data` column, ensuring zero data loss.

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
    .appName("UseCase_049_Auto_Loader_Schema_Evolution_and_Rescued_Data_Column_(_rescued_data)")
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
# MAGIC ## 5. Demonstrating Rescued Data Column Mechanics
# MAGIC Simulate schema evolution and rescued data capture.

# COMMAND ----------

# Architectural Concept:
# Incoming Record: {"device_id": "IOT-99", "temp": "INVALID_FLOAT", "new_feature": "GPS_ACTIVE"}
# Target Schema: device_id STRING, temp DOUBLE
#
# Auto Loader Output:
# device_id: "IOT-99"
# temp: NULL
# _rescued_data: '{"temp": "INVALID_FLOAT", "new_feature": "GPS_ACTIVE"}'

print("Auto Loader _rescued_data architecture mapped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Simulating Rescued Data Capture in PySpark
# MAGIC Create an explicit schema with a fallback JSON rescue column.

# COMMAND ----------

# Mock incoming data with mismatched types and extra columns
raw_stream = [
    ("D-1", "25.5", "NORMAL"),
    ("D-2", "CORRUPTED_TEMP", "CRITICAL"),
    ("D-3", "30.0", "NORMAL")
]

df_raw = spark.createDataFrame(raw_stream, ["device_id", "raw_temp", "status"])

# Resilient parsing capturing unparseable values in rescued_data
clean_expr = F.expr("try_cast(raw_temp AS DOUBLE)")

df_rescued = df_raw.withColumn("temp", clean_expr).withColumn(
    "_rescued_data",
    F.when(
        F.col("temp").isNull() & F.col("raw_temp").isNotNull(),
        F.to_json(F.struct(F.col("raw_temp"), F.lit("Type mismatch: expected DOUBLE").alias("error_reason")))
    ).otherwise(None)
)

print("Parsed Records with Rescued Data Capture:")
df_rescued.select("device_id", "temp", "status", "_rescued_data").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Production Auto Loader Schema Evolution Configuration
# MAGIC Show production options for cloudFiles.

# COMMAND ----------

autoloader_options = {
    "cloudFiles.format": "json",
    "cloudFiles.schemaEvolutionMode": "addNewColumns",  # Options: addNewColumns, rescue, failOnNewColumns
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.schemaRescuePath": "/tmp/schema_rescue"
}

print("Production Auto Loader Evolution Settings:")
for k, v in autoloader_options.items():
    print(f"  {k} = '{v}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`cloudFiles.schemaEvolutionMode = 'addNewColumns'` automatically alters target Delta tables to include new columns.**
# MAGIC - **`_rescued_data` captures type mismatches, null violations, and unmapped keys as a JSON string.**
# MAGIC - **Data teams can parse `_rescued_data` later to backfill historical columns without reprocessing raw files.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What are the four schema evolution modes supported by Databricks Auto Loader?
# MAGIC 2. What does the `_rescued_data` column contain and when is it populated?
# MAGIC 3. How does Auto Loader notify users when a schema evolution event occurs?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
