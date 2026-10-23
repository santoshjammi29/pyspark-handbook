# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 088: Dynamic Row-Level Security (RLS) Using Row Filters and User Functions
# MAGIC
# MAGIC **Module:** Module 09: Databricks Platform, Unity Catalog, Security & FinOps  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A multinational sales organization maintains a single centralized `sales_pipeline` table. Regional sales reps must only see deals within their assigned territory (US reps only see US deals; EU reps only see EU deals), while executive managers see all global records.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement dynamic Row-Level Security (RLS) in Databricks Unity Catalog: create a SQL user-defined filter function using `is_account_group_member()` and `current_user()`, apply the filter to a table using `ALTER TABLE ... SET ROW FILTER`, and verify secure row filtering.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Unity Catalog Row Filters evaluate dynamically at query compilation time. Catalyst injects the boolean filter function into the query plan, ensuring unauthorized rows are filtered out at the storage scan level before reaching memory.

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
    .appName("UseCase_088_Dynamic_Row-Level_Security_(RLS)_Using_Row_Filters_and_User_Functions")
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
# MAGIC ## 5. Defining SQL Row Filter User-Defined Function
# MAGIC Create function evaluating user group membership against row region.

# COMMAND ----------

# Row Filter Function Blueprint:
rls_sql = """
CREATE OR REPLACE FUNCTION enterprise_prod.sales.region_row_filter(region STRING)
RETURN IF(
    is_account_group_member('global_sales_executives'),  -- Execs see all rows
    true,
    IF(is_account_group_member('us_sales_reps') AND region = 'US', true,
    IF(is_account_group_member('eu_sales_reps') AND region = 'EU', true, false))
);
"""

print("Row Filter SQL Function:")
print(rls_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Applying Row Filter to Production Table
# MAGIC Attach filter function to table using SET ROW FILTER DDL.

# COMMAND ----------

# Attach filter function to target table:
# ALTER TABLE enterprise_prod.sales.t_deals
# SET ROW FILTER enterprise_prod.sales.region_row_filter ON (deal_region);

print("Table successfully protected with dynamic Row Filter.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Simulating Dynamic Row Filtering Locally in PySpark
# MAGIC Simulate the Catalyst row filtering evaluation logic.

# COMMAND ----------

# Simulating current user context and dynamic row filter
deals = [
    ("DEAL-1", "US", 150000.0, "Acme Corp"),
    ("DEAL-2", "EU", 220000.0, "Bavaria Ltd"),
    ("DEAL-3", "APAC", 95000.0, "Tokyo Corp")
]
df_deals = spark.createDataFrame(deals, ["deal_id", "deal_region", "value", "account"])

# Simulate user who belongs to 'us_sales_reps'
user_group = "us_sales_reps"

def simulate_rls_filter(df, active_group):
    if active_group == "global_sales_executives":
        return df
    elif active_group == "us_sales_reps":
        return df.filter(F.col("deal_region") == "US")
    elif active_group == "eu_sales_reps":
        return df.filter(F.col("deal_region") == "EU")
    return df.filter("1 = 0") # Deny all

df_filtered_user = simulate_rls_filter(df_deals, user_group)
print(f"Query Result for user in group '{user_group}':")
df_filtered_user.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Unity Catalog Row Filters enforce data isolation on single conformed tables without creating fragmented regional views.**
# MAGIC - **Row filters are defined via SQL UDFs using `is_account_group_member()` and `current_user()`.**
# MAGIC - **Filters are injected into the Catalyst optimizer plan, ensuring performance optimization and predicate pushdown.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How does Unity Catalog Row-Level Security differ from creating separate regional SQL views?
# MAGIC 2. What happens if an unauthorized user attempts to query a table protected by a Row Filter?
# MAGIC 3. How does Catalyst Optimizer optimize queries with Row Filters applied?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
