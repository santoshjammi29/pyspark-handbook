# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 027: Aggregate & Cumulative Window Functions: Running Totals and Moving Averages
# MAGIC
# MAGIC **Module:** Module 03: Advanced SQL, Aggregations & Analytical Window Functions
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A fintech wealth management app displays account balance trajectories and 7-day smoothed moving averages of daily expenditures to help customers track spending habits.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Implement cumulative sums (running totals), moving averages, and frame boundary specifications: compare physical row-based frames (`rowsBetween`) vs logical value-based frames (`rangeBetween`). Calculate moving averages over preceding windows.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC The default window frame for an ordered window is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. This can cause subtle bugs when duplicate order keys exist, as Spark aggregates all rows with the identical key together. Using `ROWS BETWEEN` guarantees explicit physical row framing.

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
        .appName("UseCase_027_Aggregate_and_Cumulative_Window_Functions_Running_Totals_and_Moving_Averages")
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
# MAGIC ### Step 1: Generating Daily Spending Feed
# MAGIC Create daily customer transactions.

# COMMAND ----------

spending = [
    ("ACC-1", "2023-11-01", 120.0),
    ("ACC-1", "2023-11-02", 45.0),
    ("ACC-1", "2023-11-03", 210.0),
    ("ACC-1", "2023-11-04", 80.0),
    ("ACC-1", "2023-11-05", 15.0),
    ("ACC-1", "2023-11-06", 350.0),
    ("ACC-1", "2023-11-07", 90.0)
]

schema = "account_id STRING, txn_date STRING, amount DOUBLE"
df_spend = spark.createDataFrame(spending, schema)
df_spend.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Cumulative Running Total & 3-Day Moving Average
# MAGIC Apply rowsBetween frames for physical sliding windows.

# COMMAND ----------

# Cumulative window from start to current row
cum_window = (
    Window.partitionBy("account_id")
    .orderBy("txn_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

# 3-day moving window: 2 preceding rows plus current row
moving_window = (
    Window.partitionBy("account_id")
    .orderBy("txn_date")
    .rowsBetween(-2, Window.currentRow)
)

df_analytics = df_spend.withColumn(
    "cumulative_spend", F.round(F.sum("amount").over(cum_window), 2)
).withColumn(
    "moving_avg_3day", F.round(F.avg("amount").over(moving_window), 2)
)

print("Cumulative Spending & Moving Average Analysis:")
df_analytics.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Comparing ROWS BETWEEN vs RANGE BETWEEN with Duplicate Keys
# MAGIC Illustrate how duplicate keys affect RANGE frames vs ROWS frames.

# COMMAND ----------

dup_data = [
    ("A", 1, 10.0),
    ("A", 2, 20.0),
    ("A", 2, 30.0),  # Duplicate order key
    ("A", 3, 40.0)
]
df_dup = spark.createDataFrame(dup_data, ["id", "val", "amt"])

# ROWS vs RANGE
w_rows = Window.partitionBy("id").orderBy("val").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_range = Window.partitionBy("id").orderBy("val").rangeBetween(Window.unboundedPreceding, Window.currentRow)

df_comp = df_dup.withColumn("sum_rows", F.sum("amt").over(w_rows))                .withColumn("sum_range", F.sum("amt").over(w_range))

print("Notice sum_range evaluates tied val=2 together into 60.0, while sum_rows evaluates row-by-row:")
df_comp.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`ROWS BETWEEN` defines physical row count boundaries (e.g. 2 preceding rows).**
# MAGIC - **`RANGE BETWEEN` defines logical value offsets based on the order column.**
# MAGIC - **Default window frame with `orderBy` is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, which aggregates identical order keys together.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `rowsBetween` and `rangeBetween` in Spark Window specifications?
# MAGIC 2. What is the default frame specification when an `orderBy` clause is present in a Window?
# MAGIC 3. How do you compute a rolling 30-day time-based window on irregular transaction dates?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
