# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 024: Advanced Multidimensional OLAP: Rollup, Cube and Grouping Sets
# MAGIC
# MAGIC **Module:** Module 03: Advanced SQL, Aggregations & Analytical Window Functions  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A financial controller needs a single unified sales reporting view that includes: (1) Detailed breakdown by Year, Quarter, and Product, (2) Subtotals by Year and Quarter, (3) Subtotals by Year alone, and (4) The grand total across the entire enterprise.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement multidimensional OLAP aggregations without writing multiple queries and unions. Compare `rollup()` (hierarchical subtotals), `cube()` (all possible combinations of dimensions), and explicit `GROUPING SETS`. Use `grouping_id()` to identify aggregate levels.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC `rollup()` generates $N + 1$ grouping levels for $N$ dimensions in hierarchical order. `cube()` computes $2^N$ grouping combinations. Spark optimizes these into a single Pass using the Expand operator, avoiding multiple scans of the source table.

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
    .appName("UseCase_024_Advanced_Multidimensional_OLAP_Rollup,_Cube_and_Grouping_Sets")
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
# MAGIC ## 5. Generating Financial Revenue Records
# MAGIC Create multi-year revenue transactions.

# COMMAND ----------

revenue_data = [
    (2022, "Q1", "Software", 50000.0),
    (2022, "Q1", "Hardware", 80000.0),
    (2022, "Q2", "Software", 60000.0),
    (2022, "Q2", "Hardware", 75000.0),
    (2023, "Q1", "Software", 70000.0),
    (2023, "Q1", "Hardware", 90000.0),
    (2023, "Q2", "Software", 85000.0),
    (2023, "Q2", "Hardware", 95000.0)
]

schema = "fiscal_year INT, fiscal_quarter STRING, product_line STRING, revenue DOUBLE"
df_rev = spark.createDataFrame(revenue_data, schema)
df_rev.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Hierarchical Rollup for Subtotals and Grand Total
# MAGIC Compute hierarchical aggregates from finest grain to grand total.

# COMMAND ----------

df_rollup = df_rev.rollup("fiscal_year", "fiscal_quarter", "product_line").agg(
    F.sum("revenue").alias("total_revenue"),
    F.grouping_id().alias("grouping_level")
).orderBy(F.col("fiscal_year").asc_nulls_last(), F.col("fiscal_quarter").asc_nulls_last(), F.col("product_line").asc_nulls_last())

print("Hierarchical Rollup (Notice Subtotals and Grand Total when dimensions are null):")
df_rollup.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Full Multidimensional Cube Aggregation
# MAGIC Generate all 2^3 = 8 combinations of grouping dimensions.

# COMMAND ----------

df_cube = df_rev.cube("fiscal_year", "product_line").agg(
    F.sum("revenue").alias("total_revenue")
).orderBy("fiscal_year", "product_line")

print("Cube: All Combinations of Fiscal Year and Product Line:")
df_cube.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`rollup()` computes hierarchical subtotals along a defined drill-down path (Year -> Quarter -> Product).**
# MAGIC - **`cube()` calculates all possible cross-dimensional combinations ($2^N$).**
# MAGIC - **The `grouping_id()` bitmask column identifies which dimensions are aggregated in each result row.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How does Catalyst Optimizer's `Expand` operator implement `cube()` and `rollup()` under the hood?
# MAGIC 2. What is the mathematical difference in row count between `rollup(A, B, C)` and `cube(A, B, C)`?
# MAGIC 3. How do you distinguish between a real NULL value in the data versus a NULL produced by a rollup subtotal?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
