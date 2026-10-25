# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 011: Custom PySpark UDFs vs Vectorized Pandas UDFs (PyArrow)
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A credit scoring model applies complex non-linear mathematical risk adjustments to customer accounts. The current implementation uses standard Python UDFs and causes severe CPU throttling and worker memory spikes.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Implement the calculation using: (1) Standard Python UDF (`@udf`), (2) Native Spark SQL built-in expressions, and (3) Vectorized Pandas UDF (`@pandas_udf`). Compare execution time, serialization behavior, and memory footprint.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Standard Python UDFs serialize JVM row data to a Python worker process row-by-row via Py4J, incurring massive serialization and deserialization overhead and disabling Catalyst optimizations. Vectorized Pandas UDFs use Apache Arrow to transfer columnar batches into Pandas Series, achieving near-native C-speed vectorization.

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
        .appName("UseCase_011_Custom_PySpark_UDFs_vs_Vectorized_Pandas_UDFs_PyArrow")
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
# MAGIC ### Step 1: Creating Benchmark Financial Dataset
# MAGIC Prepare 50,000 accounts for algorithmic risk scoring.

# COMMAND ----------

df_accounts = spark.range(1, 50001, 1, 4).select(
    F.col("id").alias("account_id"),
    (F.rand(seed=42) * 800 + 300).alias("credit_score"),
    (F.rand(seed=101) * 50000 + 1000).alias("debt_amount"),
    (F.rand(seed=202) * 100000 + 20000).alias("annual_income")
)

df_accounts.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Method 1: Standard Python UDF (Row-by-Row Serialization)
# MAGIC Define a standard Python function and wrap it with `@F.udf`.

# COMMAND ----------

@F.udf(returnType=T.DoubleType())
def calculate_risk_standard(credit_score, debt, income):
    if income == 0 or credit_score is None:
        return 0.0
    dti = debt / income
    score_factor = (850 - credit_score) / 550.0
    return round((dti * 0.6) + (score_factor * 0.4), 4)

import time
t0 = time.time()
df_res1 = df_accounts.withColumn("risk_std", calculate_risk_standard("credit_score", "debt_amount", "annual_income"))
c1 = df_res1.filter(F.col("risk_std") > 0.5).count()
t1 = time.time() - t0
print(f"Standard UDF Count: {c1} in {t1:.3f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Method 2: Vectorized Pandas UDF via PyArrow
# MAGIC Define a vectorized Arrow function operating over Pandas Series batches.

# COMMAND ----------

import pandas as pd

@F.pandas_udf(T.DoubleType())
def calculate_risk_vectorized(credit_score: pd.Series, debt: pd.Series, income: pd.Series) -> pd.Series:
    dti = debt / income.replace(0, 1.0)
    score_factor = (850.0 - credit_score) / 550.0
    return ((dti * 0.6) + (score_factor * 0.4)).round(4)

t0 = time.time()
df_res2 = df_accounts.withColumn("risk_vec", calculate_risk_vectorized("credit_score", "debt_amount", "annual_income"))
c2 = df_res2.filter(F.col("risk_vec") > 0.5).count()
t2 = time.time() - t0
print(f"Vectorized Pandas UDF Count: {c2} in {t2:.3f} seconds (Speedup factor: {t1/max(t2, 0.001):.1f}x)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Always favor native Spark SQL functions first before writing custom functions.**
# MAGIC - **If custom Python logic is unavoidable, always use `@pandas_udf` instead of standard `@udf`.**
# MAGIC - **Standard UDFs serialize data row-by-row through a Python subprocess, while Pandas UDFs batch data via shared Arrow memory.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why are standard Python UDFs considered a major performance bottleneck in PySpark?
# MAGIC 2. How does Apache Arrow enable zero-copy data transfer in Pandas UDFs?
# MAGIC 3. Can Catalyst Optimizer optimize or push down predicates past a Python UDF?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
