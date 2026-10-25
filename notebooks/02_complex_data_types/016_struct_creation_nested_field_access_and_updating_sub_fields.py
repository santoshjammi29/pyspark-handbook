# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 016: Struct Creation, Nested Field Access and Updating Sub-Fields
# MAGIC
# MAGIC **Module:** Module 02: Complex Data Types & Nested Structures Handling
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A customer identity platform groups personal attributes, contact points, and address details into nested structs for clean schema organization. Downstream systems need to query specific sub-fields and update an address without reconstructing the entire object.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Create nested `StructType` columns using `struct()`. Access inner fields via dot notation and `col.getField()`. Demonstrate updating a single nested field inside a struct in PySpark 3.1+ without manually re-specifying all sibling fields.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC StructType is a compound type that stores multiple typed named fields together in memory. In earlier Spark versions, updating one nested field required re-creating the entire struct. Spark 3.1+ introduced `withField()` and `dropFields()` on Struct columns to manipulate nested fields cleanly.

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
        .appName("UseCase_016_Struct_Creation_Nested_Field_Access_and_Updating_Sub_Fields")
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
# MAGIC ### Step 1: Creating Nested Struct Records
# MAGIC Package flat customer columns into organized nested structs.

# COMMAND ----------

customers_flat = [
    ("C-01", "Alice", "Smith", "123 Elm St", "Austin", "TX", "78701", 85000),
    ("C-02", "Bob", "Jones", "456 Oak Ave", "Seattle", "WA", "98101", 92000)
]

cols = ["id", "first_name", "last_name", "street", "city", "state", "zip", "salary"]
df_flat = spark.createDataFrame(customers_flat, cols)

# Bundle into nested structs
df_nested = df_flat.select(
    F.col("id"),
    F.struct(F.col("first_name"), F.col("last_name")).alias("name"),
    F.struct(F.col("street"), F.col("city"), F.col("state"), F.col("zip")).alias("address"),
    F.col("salary")
)

df_nested.printSchema()
df_nested.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Querying Nested Fields via Dot Notation & getField
# MAGIC Select specific sub-fields directly.

# COMMAND ----------

# Accessing nested fields
df_selected = df_nested.select(
    "id",
    "name.last_name",
    F.col("address.city").alias("home_city"),
    F.col("address").getField("state").alias("home_state")
)

df_selected.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Updating Single Nested Fields using withField (Spark 3.1+)
# MAGIC Modify address.city without rewriting the entire struct definition.

# COMMAND ----------

# Update city to uppercase and add a new nested field country
df_updated_struct = df_nested.withColumn(
    "address",
    F.col("address")
     .withField("city", F.upper(F.col("address.city")))
     .withField("country", F.lit("USA"))
)

df_updated_struct.printSchema()
df_updated_struct.select("id", "address").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Use `struct(*cols)` to group related columns into a compound object without network shuffle.**
# MAGIC - **`col.withField('subfield', expr)` (Spark 3.1+) updates or adds a nested field inside an existing struct.**
# MAGIC - **`col.dropFields('subfield')` removes specific nested attributes from a struct.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How does `withField()` simplify nested struct manipulation compared to older Spark versions?
# MAGIC 2. What is the difference between dot notation `col('a.b')` and `col('a').getField('b')`?
# MAGIC 3. Can Parquet push down column projection into individual fields of a nested StructType?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
