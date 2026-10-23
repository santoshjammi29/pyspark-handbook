# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 025: Pivoting and Unpivoting Data: pivot and stack Expression Mastery
# MAGIC
# MAGIC **Module:** Module 03: Advanced SQL, Aggregations & Analytical Window Functions  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A business intelligence dashboard requires monthly revenue pivoted into wide columns (Jan, Feb, Mar...) for tabular display. Conversely, a data science model requires wide survey responses unpivoted (melted) back into long tidy format.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Master tabular reshaping: (1) Pivot narrow transactional rows into wide summary columns using `groupBy().pivot()`, optimizing performance by passing explicit pivot values. (2) Unpivot wide multi-column tables back into narrow key-value rows using Spark's `stack()` expression.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Calling `.pivot('col')` without specifying distinct values forces an extra, expensive distributed pass over the data to discover all unique column values. Always pass the pre-defined list of distinct values: `.pivot('col', ['Val1', 'Val2'])` to eliminate this extra scan.

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
    .appName("UseCase_025_Pivoting_and_Unpivoting_Data_pivot_and_stack_Expression_Mastery")
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
# MAGIC ## 5. Generating Monthly Department Sales (Narrow Format)
# MAGIC Create transactional sales records across months.

# COMMAND ----------

monthly_sales = [
    ("Electronics", "Jan", 12000.0),
    ("Electronics", "Feb", 14500.0),
    ("Electronics", "Mar", 16000.0),
    ("Groceries", "Jan", 8500.0),
    ("Groceries", "Feb", 9200.0),
    ("Groceries", "Mar", 8900.0),
    ("Apparel", "Jan", 4500.0),
    ("Apparel", "Feb", 5100.0),
    ("Apparel", "Mar", 6300.0)
]

df_narrow = spark.createDataFrame(monthly_sales, ["dept", "month", "sales"])
df_narrow.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optimized Pivoting with Explicit Pivot Values
# MAGIC Reshape months into columns, passing explicit month list to skip extra scan.

# COMMAND ----------

months_list = ["Jan", "Feb", "Mar"]

# Highly optimized pivot with explicit values passed
df_pivoted = df_narrow.groupBy("dept").pivot("month", months_list).agg(
    F.coalesce(F.sum("sales"), F.lit(0.0))
)

print("Pivoted Wide Table:")
df_pivoted.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Unpivoting (Melting) Wide Table back to Narrow with stack()
# MAGIC Use the SQL stack expression to transform wide columns back into rows.

# COMMAND ----------

# stack(n, 'col1_name', col1, 'col2_name', col2, ...)
unpivot_expr = "stack(3, 'Jan', Jan, 'Feb', Feb, 'Mar', Mar) as (month, sales)"

df_unpivoted = df_pivoted.select("dept", F.expr(unpivot_expr))
print("Unpivoted Back to Tidy Narrow Format:")
df_unpivoted.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Always provide an explicit list of pivot values to `pivot(col, values)` to eliminate an expensive full-table discovery scan.**
# MAGIC - **Unpivoting in PySpark is achieved natively using `F.expr('stack(N, ...)')` without requiring external libraries.**
# MAGIC - **Pivoting creates wide schemas; excessive pivot columns can impact Catalyst compilation and memory.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What happens internally when you call `df.groupBy().pivot()` without providing the list of distinct values?
# MAGIC 2. How does the `stack()` expression work in Spark SQL to perform table melting?
# MAGIC 3. What are the alternatives to `pivot()` if you have 1,000+ distinct dynamic categories?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
