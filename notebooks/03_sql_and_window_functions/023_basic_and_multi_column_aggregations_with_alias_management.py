# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 023: Basic and Multi-Column Aggregations with Alias Management
# MAGIC
# MAGIC **Module:** Module 03: Advanced SQL, Aggregations & Analytical Window Functions  
# MAGIC **Difficulty:** `Beginner`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A retail analytics engine calculates daily regional performance metrics across store departments: total gross revenue, average transaction value, maximum discount applied, and distinct active shoppers.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Perform multi-column aggregations using `groupBy()` paired with `agg()`. Demonstrate explicit column aliasing, combining multiple statistical functions (`sum`, `avg`, `min`, `max`, `countDistinct`, `approx_count_distinct`), and using dictionary-based aggregation syntax.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Spark executes aggregations via HashAggregate (when data types are mutable) or SortAggregate. In two-phase aggregation, Spark first performs partial local aggregation on each partition before shuffling partial sums across the network, reducing network traffic.

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
    .appName("UseCase_023_Basic_and_Multi-Column_Aggregations_with_Alias_Management")
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
# MAGIC ## 5. Generating Store Sales Transactions
# MAGIC Create a dataset of store transactions with departments and regions.

# COMMAND ----------

sales = [
    ("TXN-1", "West", "Electronics", 450.00, 0.10, "C-01"),
    ("TXN-2", "West", "Electronics", 1200.00, 0.15, "C-02"),
    ("TXN-3", "West", "Groceries", 65.50, 0.00, "C-01"),
    ("TXN-4", "East", "Electronics", 800.00, 0.05, "C-03"),
    ("TXN-5", "East", "Fashion", 150.00, 0.20, "C-04"),
    ("TXN-6", "East", "Fashion", 95.00, 0.10, "C-04"),
    ("TXN-7", "North", "Groceries", 45.00, 0.00, "C-05"),
    ("TXN-8", "North", "Groceries", 120.00, 0.05, "C-06")
]

schema = "txn_id STRING, region STRING, department STRING, amount DOUBLE, discount DOUBLE, customer_id STRING"
df_sales = spark.createDataFrame(sales, schema)
df_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Multi-Column Aggregations with Explicit Aliasing
# MAGIC Calculate comprehensive statistical metrics per region and department.

# COMMAND ----------

df_regional_metrics = df_sales.groupBy("region", "department").agg(
    F.count("txn_id").alias("total_transactions"),
    F.round(F.sum("amount"), 2).alias("gross_revenue"),
    F.round(F.avg("amount"), 2).alias("avg_basket_size"),
    F.max("amount").alias("max_single_sale"),
    F.round(F.max("discount") * 100, 1).alias("max_discount_pct"),
    F.countDistinct("customer_id").alias("unique_shoppers")
).orderBy("region", F.desc("gross_revenue"))

df_regional_metrics.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Performance Optimization: approx_count_distinct for HyperLogLog Scaling
# MAGIC Compare exact distinct count with approximate distinct count for massive datasets.

# COMMAND ----------

# On multi-billion row tables, countDistinct forces massive shuffles.
# approx_count_distinct uses HyperLogLog++ to compute cardinality within 5% error using minimal memory.
df_approx = df_sales.groupBy("region").agg(
    F.countDistinct("customer_id").alias("exact_distinct_shoppers"),
    F.approx_count_distinct("customer_id", rsd=0.05).alias("approx_distinct_shoppers")
)

df_approx.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Always alias aggregated expressions immediately using `.alias()` to prevent messy default column names.**
# MAGIC - **Spark automatically leverages two-phase aggregation: partial aggregate on executors followed by final aggregate after shuffle.**
# MAGIC - **Use `approx_count_distinct` for high-cardinality distinct counts on big data to eliminate severe shuffle spills.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How does Spark's two-phase HashAggregate optimize network transfer during `groupBy()`?
# MAGIC 2. What algorithm powers `approx_count_distinct` and when is it acceptable in business reporting?
# MAGIC 3. What is the difference between HashAggregate and SortAggregate in Spark Catalyst execution plans?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
