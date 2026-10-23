# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 074: Catalyst Optimizer Deep Dive: Parsed, Analyzed, Optimized Plans and Code Generation
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A lead data architect conducts code reviews for data pipelines. Junior engineers write complex nested SQL and DataFrame code without understanding whether Catalyst collapses filters or pushes down projections. The team needs to inspect execution plans programmatically.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Master Apache Spark Catalyst Optimizer: analyze the 4 compilation phases (Parsed Logical Plan, Analyzed Logical Plan, Optimized Logical Plan, Physical Plan), inspect Whole-Stage Code Generation (`*` in physical plan), and compare `df.explain(True)` with `df.explain('formatted')`.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Catalyst compiles queries through 4 stages: (1) Analysis via Catalog lookup, (2) Logical Optimization (predicate pushdown, constant folding, projection pruning), (3) Physical Planning (choosing join algorithms, e.g. BroadcastHashJoin vs SortMergeJoin), and (4) Whole-Stage Code Generation into optimized Java bytecode via Janino.

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
    .appName("UseCase_074_Catalyst_Optimizer_Deep_Dive_Parsed,_Analyzed,_Optimized_Plans_and_Code_Generation")
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
# MAGIC ## 5. Constructing Multi-Stage Query for Plan Inspection
# MAGIC Create an unoptimized query with redundant filters and transformations.

# COMMAND ----------

# Create mock orders dataset
df_orders = spark.range(1, 10001, 1, 4).select(
    F.col("id").alias("order_id"),
    (F.col("id") % 10).alias("customer_id"),
    (F.col("id") * 1.5).alias("amount"),
    F.lit("ACTIVE").alias("status")
)

# Unoptimized query with redundant filters and constant math
df_plan_query = (
    df_orders
    .filter(F.col("status") == "ACTIVE")
    .filter(F.col("amount") > (100.0 * 2.0)) # Constant folding opportunity: 100 * 2 = 200
    .groupBy("customer_id")
    .agg(F.sum("amount").alias("total_spend"))
    .filter(F.col("total_spend") > 500)
)

print("Query constructed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inspecting Full Catalyst Lifecycle Plans via explain(True)
# MAGIC Print all 4 internal compilation stages of Catalyst.

# COMMAND ----------

print("=== COMPLETE CATALYST COMPILATION PHASES ===")
df_plan_query.explain(extended=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Analyzing Whole-Stage Code Generation (Tungsten)
# MAGIC Identify the asterisk (*) symbol indicating Whole-Stage CodeGen in the physical plan.

# COMMAND ----------

print("=== FORMATTED PHYSICAL PLAN (Notice WholeStageCodegen operators) ===")
df_plan_query.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Catalyst translates declarative logic into optimized Java bytecode through 4 formal compilation phases.**
# MAGIC - **Constant folding and predicate pushdown occur in the Optimized Logical Plan stage.**
# MAGIC - **The asterisk `*` next to operator names in physical plans signifies Whole-Stage Code Generation (Tungsten execution).**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What are the 4 main phases of the Spark Catalyst Optimizer?
# MAGIC 2. What is Whole-Stage Code Generation and how does it eliminate virtual function call overhead in Java?
# MAGIC 3. How does the Cost-Based Optimizer (CBO) use table statistics to improve physical join planning?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
