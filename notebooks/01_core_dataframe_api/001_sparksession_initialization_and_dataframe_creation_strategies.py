# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 001: SparkSession Initialization & DataFrame Creation Strategies
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API  
# MAGIC **Difficulty:** `Beginner`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A digital commerce platform needs to ingest customer profile extracts from multiple legacy sources (in-memory lists, Python dictionaries, Pandas exports, and RDDs) into a unified PySpark DataFrame with strict schema validation.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Demonstrate the creation of a resilient SparkSession, configuring custom execution properties, and creating DataFrames using: (1) Programmatic RDD with StructType schema, (2) Python dictionaries/tuples with DDL string schema, (3) Pandas DataFrame conversion with PyArrow optimization, and (4) Empty DataFrames with explicit schemas for pipeline initialization.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC SparkSession serves as the unified entry point introduced in Spark 2.0, encapsulating SparkContext, SQLContext, and HiveContext. In Databricks, the SparkSession variable `spark` is pre-instantiated. When creating DataFrames from Python collections, PySpark serializes Python objects to Java Virtual Machine (JVM) objects via Py4J. Enabling Arrow (`spark.sql.execution.arrow.pyspark.enabled`) accelerates Pandas-to-Spark conversions by eliminating per-row serialization overhead.

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
    .appName("UseCase_001_SparkSession_Initialization_and_DataFrame_Creation_Strategies")
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
# MAGIC ## 5. Strategy 1: Programmatic Schema with StructType and Tuples
# MAGIC Explicit StructType schemas prevent costly schema inference passes over distributed data and ensure strict type validation.

# COMMAND ----------

# 1. Define explicit StructType schema
customer_schema = T.StructType([
    T.StructField("customer_id", T.StringType(), nullable=False),
    T.StructField("full_name", T.StringType(), nullable=False),
    T.StructField("signup_date", T.StringType(), nullable=True),
    T.StructField("loyalty_tier", T.StringType(), nullable=False),
    T.StructField("account_balance", T.DoubleType(), nullable=False)
])

# 2. Source raw tuples (representing incoming batch extract)
raw_customers = [
    ("CUST-1001", "Alice Johnson", "2023-01-15", "Gold", 4520.50),
    ("CUST-1002", "Bob Smith", "2023-02-20", "Silver", 1250.00),
    ("CUST-1003", "Charlie Brown", "2023-03-10", "Bronze", 340.25),
    ("CUST-1004", "Diana Prince", "2023-04-05", "Platinum", 15890.00),
    ("CUST-1005", "Evan Wright", "2023-05-18", "Silver", 820.75)
]

# 3. Create DataFrame with explicit schema
df_customers = spark.createDataFrame(data=raw_customers, schema=customer_schema)
df_customers.printSchema()
df_customers.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Strategy 2: DDL-Formatted Schema String
# MAGIC DDL strings provide a concise, readable alternative to nested StructType declarations while retaining full type safety.

# COMMAND ----------

# Define schema using SQL DDL syntax
ddl_schema = "order_id STRING, customer_id STRING, order_amount DOUBLE, order_status STRING, item_count INT"

raw_orders = [
    ("ORD-901", "CUST-1001", 145.20, "COMPLETED", 3),
    ("ORD-902", "CUST-1002", 49.99, "PENDING", 1),
    ("ORD-903", "CUST-1001", 310.00, "COMPLETED", 5),
    ("ORD-904", "CUST-1004", 1250.40, "SHIPPED", 8)
]

df_orders = spark.createDataFrame(raw_orders, schema=ddl_schema)
df_orders.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Strategy 3: High-Performance Pandas Conversion via PyArrow
# MAGIC When working with data scientist outputs or REST API responses, converting Pandas DataFrames using Apache Arrow offers dramatic speedups.

# COMMAND ----------

import pandas as pd
import numpy as np

# Enable PyArrow optimization
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

pdf = pd.DataFrame({
    "product_id": [f"PROD-{i}" for i in range(101, 106)],
    "category": ["Electronics", "Home", "Electronics", "Books", "Fashion"],
    "unit_price": [299.99, 45.50, 899.00, 19.95, 75.00],
    "in_stock": [True, True, False, True, True]
})

df_products = spark.createDataFrame(pdf)
print(f"Created Spark DataFrame from Pandas with {df_products.count()} rows")
df_products.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification & Pipeline Readiness Check
# MAGIC Assert non-empty DataFrames, column integrity, and schema compliance before feeding downstream medallion pipelines.

# COMMAND ----------

# Verification assertions
assert df_customers.count() == 5, "Customer count mismatch"
assert "loyalty_tier" in df_customers.columns, "Missing loyalty_tier column"
assert df_orders.filter(F.col("order_status") == "COMPLETED").count() == 2, "Completed orders mismatch"

print("All DataFrame creation strategies and assertions verified successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Always supply explicit StructType or DDL schemas in production to avoid schema inference performance penalties.**
# MAGIC - **Enable Arrow (`spark.sql.execution.arrow.pyspark.enabled`) for zero-copy memory transfers between Pandas and PySpark.**
# MAGIC - **Spark DataFrames are lazily evaluated representations of Catalyst logical plans—`createDataFrame` only builds the plan until an action is triggered.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What happens under the hood when you omit a schema in `spark.createDataFrame(data)`?
# MAGIC 2. How does Apache Arrow optimize PySpark-to-Pandas serialization compared to default Py4J pickling?
# MAGIC 3. What is the difference between `master('local[*]')` and cluster mode execution when configuring a SparkSession?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
