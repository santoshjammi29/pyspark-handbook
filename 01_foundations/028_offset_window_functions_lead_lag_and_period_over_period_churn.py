# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 028: Offset Window Functions: lead, lag and Period-over-Period Churn
# MAGIC
# MAGIC **Module:** Module 03: Advanced SQL, Aggregations & Analytical Window Functions
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A SaaS subscription billing service analyzes customer MRR (Monthly Recurring Revenue). Finance requires month-over-month revenue variance, detection of plan upgrades/downgrades, and churn identification when an active account drops subscription renewal.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master offset window functions: implement `lag()` to retrieve previous month revenue and `lead()` to inspect subsequent renewal state. Calculate absolute and percentage period-over-period differences, and handle default fallback values on boundary rows.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Offset functions fetch values from neighboring rows within the same executor partition buffer without self-joining the table. Self-joins require a full Cartesian shuffle, whereas `lag()` operates in single-pass linear time $O(N)$ inside the partition.

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
        .appName("UseCase_028_Offset_Window_Functions_lead_lag_and_Period_over_Period_Churn")
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
# MAGIC ### Step 1: Generating Monthly SaaS Subscription History
# MAGIC Create monthly subscription charges per customer account.

# COMMAND ----------

subscription_history = [
    ("SUB-1", "2023-01", 100.0),
    ("SUB-1", "2023-02", 100.0),
    ("SUB-1", "2023-03", 150.0), # Upgrade
    ("SUB-1", "2023-04", 120.0), # Downgrade
    ("SUB-2", "2023-01", 50.0),
    ("SUB-2", "2023-02", 50.0),
    ("SUB-2", "2023-03", 0.0)    # Churn
]

schema = "sub_id STRING, billing_month STRING, mrr DOUBLE"
df_subs = spark.createDataFrame(subscription_history, schema)
df_subs.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Applying lag() and lead() with Default Values
# MAGIC Fetch previous month and next month MRR.

# COMMAND ----------

window_sub = Window.partitionBy("sub_id").orderBy("billing_month")

df_lag_lead = df_subs.withColumn(
    "prev_mrr", F.lag("mrr", 1, default=0.0).over(window_sub)
).withColumn(
    "next_mrr", F.lead("mrr", 1, default=0.0).over(window_sub)
)

df_lag_lead.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Calculating Month-over-Month Growth & Change Classification
# MAGIC Compute variance and classify subscription status.

# COMMAND ----------

df_variance = df_lag_lead.withColumn(
    "mrr_change", F.round(F.col("mrr") - F.col("prev_mrr"), 2)
).withColumn(
    "pct_change",
    F.when(F.col("prev_mrr") == 0, None)
     .otherwise(F.round(((F.col("mrr") - F.col("prev_mrr")) / F.col("prev_mrr")) * 100, 1))
).withColumn(
    "status_tag",
    F.when(F.col("prev_mrr") == 0, "NEW")
     .when(F.col("mrr") > F.col("prev_mrr"), "EXPANSION")
     .when((F.col("mrr") < F.col("prev_mrr")) & (F.col("mrr") > 0), "CONTRACTION")
     .when(F.col("mrr") == 0, "CHURNED")
     .otherwise("RETAINED")
)

df_variance.select("sub_id", "billing_month", "prev_mrr", "mrr", "mrr_change", "pct_change", "status_tag").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`lag(col, offset, default)` accesses preceding rows without requiring self-joins.**
# MAGIC - **`lead(col, offset, default)` peeks forward into subsequent rows in the partition.**
# MAGIC - **Always provide sensible `default` values to avoid propagating accidental nulls on boundary rows.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why is `lag()` significantly more efficient than joining a DataFrame to itself on `t.date = t_prev.date + 1`?
# MAGIC 2. How do you calculate year-over-year growth for missing months using window functions?
# MAGIC 3. What is the maximum offset value you can safely supply to `lag()` in PySpark?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
