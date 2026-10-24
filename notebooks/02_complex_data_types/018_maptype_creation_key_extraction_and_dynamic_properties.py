# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 018: MapType Creation, Key Extraction and Dynamic Properties
# MAGIC
# MAGIC **Module:** Module 02: Complex Data Types & Nested Structures Handling
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC An advertising technology platform captures custom user ad-tracking attributes (cookies, campaign IDs, UTM tags, experiment variants) as dynamic key-value pairs where keys vary across partners and cannot be fixed into a static schema.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master MapType operations: construct maps from column pairs (`create_map()`), extract keys (`map_keys()`), extract values (`map_values()`), build maps from paired arrays (`map_from_arrays()`), and query dynamic attributes safely without null pointer exceptions.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC MapType represents key-value collections with arbitrary keys of identical data type. Unlike StructType (where field names are fixed in schema metadata), MapType allows dynamic runtime keys, making it ideal for sparse key-value attributes.

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
        .appName("UseCase_018_MapType_Creation_Key_Extraction_and_Dynamic_Properties")
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
# MAGIC ### Step 1: Creating Ad-Tech Campaign Metadata with MapType
# MAGIC Construct maps using create_map and map_from_arrays.

# COMMAND ----------

# Build map using paired key-value lists
campaign_data = [
    ("EV-1", ["utm_source", "utm_campaign", "device"], ["google", "black_friday", "mobile"]),
    ("EV-2", ["utm_source", "experiment_group"], ["facebook", "variant_b"]),
    ("EV-3", ["utm_source", "utm_medium", "utm_campaign"], ["linkedin", "cpc", "q4_enterprise"])
]

df_raw_campaigns = spark.createDataFrame(campaign_data, ["event_id", "param_keys", "param_vals"])

# Convert parallel arrays into MapType
df_mapped = df_raw_campaigns.withColumn(
    "attributes",
    F.map_from_arrays(F.col("param_keys"), F.col("param_vals"))
)

df_mapped.printSchema()
df_mapped.select("event_id", "attributes").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Querying Map Keys, Values & Dynamic Lookup
# MAGIC Access map values by key name using bracket notation and inspect key catalogs.

# COMMAND ----------

# Querying specific keys
df_extracted = df_mapped.withColumn(
    "source", F.col("attributes")["utm_source"]
).withColumn(
    "campaign", F.coalesce(F.col("attributes")["utm_campaign"], F.lit("ORGANIC"))
).withColumn(
    "all_keys", F.map_keys(F.col("attributes"))
).withColumn(
    "all_values", F.map_values(F.col("attributes"))
)

df_extracted.select("event_id", "source", "campaign", "all_keys").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Exploding MapType into Relational Key-Value Rows
# MAGIC Convert sparse key-value pairs into normalized relational rows.

# COMMAND ----------

df_kv_pairs = df_mapped.select(
    "event_id",
    F.explode("attributes").alias("attr_key", "attr_value")
)

df_kv_pairs.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Use `MapType` for semi-structured data where keys are dynamic, variable, or unpredictable.**
# MAGIC - **`map_from_arrays(keys_col, values_col)` builds maps from two parallel array columns.**
# MAGIC - **Accessing non-existent map keys returns `NULL` safely rather than raising runtime errors.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `MapType` and `StructType` in PySpark?
# MAGIC 2. When should you choose `MapType` over a normalized child table in Lakehouse architecture?
# MAGIC 3. How does `explode(map_col)` generate key and value columns simultaneously?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
