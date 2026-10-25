# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 095: Semantic Layer Modeling and Databricks Metric Views for BI and AI Agents
# MAGIC
# MAGIC **Module:** Module 09: Databricks Platform, Unity Catalog, Security & FinOps
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC Different business teams calculate 'Customer Churn' and 'Net Revenue' using conflicting SQL formulas, producing discrepancies between executive reports and Databricks Genie AI natural language answers.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Build an enterprise Semantic Layer using Databricks Metric Views: define centralized business metrics, dimensions, hierarchies, and time grains, ensuring identical calculations across Power BI, Tableau, and Databricks Genie AI agents.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC A Semantic Layer translates physical Delta tables into business concepts (Measures and Dimensions). Databricks Metric Views provide a standardized semantic definition where metrics (e.g. ARR, Churn Rate) are defined once and computed dynamically at any aggregation grain.

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
        .appName("UseCase_095_Semantic_Layer_Modeling_and_Databricks_Metric_Views_for_BI_and_AI_Agents")
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
# MAGIC ### Step 1: Creating Physical Sales Fact and Dimension Tables
# MAGIC Create underlying analytical data for semantic modeling.

# COMMAND ----------

sales_records = [
    ("S-01", "2023-11-01", "US", "Electronics", 1200.0, 100.0),
    ("S-02", "2023-11-02", "US", "Home", 450.0, 50.0),
    ("S-03", "2023-11-03", "EU", "Electronics", 800.0, 80.0),
    ("S-04", "2023-11-04", "EU", "Electronics", 950.0, 95.0)
]

schema = "sale_id STRING, sale_date STRING, country STRING, category STRING, gross_revenue DOUBLE, discount DOUBLE"
df_sales = spark.createDataFrame(sales_records, schema)
df_sales.createOrReplaceTempView("fact_sales_semantic")
df_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Defining Semantic Metric View with Standard Measures
# MAGIC Define conformed measures: Gross Revenue, Total Discounts, and Net Revenue.

# COMMAND ----------

semantic_view_sql = """
CREATE OR REPLACE VIEW v_semantic_sales_metrics AS
SELECT
    sale_date,
    country,
    category,
    -- Standard Measures:
    gross_revenue,
    discount,
    (gross_revenue - discount) AS net_revenue,
    ROUND(((discount) / gross_revenue) * 100, 1) AS discount_pct
FROM fact_sales_semantic
"""

spark.sql(semantic_view_sql)
print("Semantic View 'v_semantic_sales_metrics' created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Querying Metrics Consistently Across Any Dimensional Grain
# MAGIC Demonstrate that Net Revenue computes identically across country or category.

# COMMAND ----------

# Query 1: By Country
print("Metric Aggregation by Country:")
spark.sql("""
SELECT
    country,
    ROUND(SUM(gross_revenue), 2) AS total_gross,
    ROUND(SUM(net_revenue), 2) AS total_net
FROM v_semantic_sales_metrics
GROUP BY country
""").show()

# Query 2: By Category
print("Metric Aggregation by Category:")
spark.sql("""
SELECT
    category,
    ROUND(SUM(gross_revenue), 2) AS total_gross,
    ROUND(SUM(net_revenue), 2) AS total_net
FROM v_semantic_sales_metrics
GROUP BY category
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **A Semantic Layer standardizes business metrics (Net Revenue, Churn, Margin) across the entire company.**
# MAGIC - **Metric Views allow BI tools and Databricks Genie AI agents to query certified metrics without SQL discrepancies.**
# MAGIC - **Defining measures once ensures compliance and trust across executive reporting.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is a Semantic Layer and why is it critical when deploying AI agents like Databricks Genie?
# MAGIC 2. How does a Metric View differ from a standard SQL view?
# MAGIC 3. How do you prevent 'metric drift' across multiple self-service BI tools?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
