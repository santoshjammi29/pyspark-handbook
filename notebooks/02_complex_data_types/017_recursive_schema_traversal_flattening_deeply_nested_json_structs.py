# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 017: Recursive Schema Traversal: Flattening Deeply Nested JSON/Structs
# MAGIC
# MAGIC **Module:** Module 02: Complex Data Types & Nested Structures Handling  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC An enterprise integration hub ingests complex payloads from ERP and CRM systems containing 5+ levels of nested structs and arrays. Analysts require a completely flattened tabular DataFrame where column names reflect their full dot-delimited path.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Develop a generic, production-ready recursive schema flattener in Python that traverses any arbitrarily deep Spark schema. The function must explode nested arrays and flatten all nested structs into top-level columns (`parent_child_field`).
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Deeply nested schemas can cause severe query compilation overhead. Traversing the schema programmatically inspects `df.schema.fields` and constructs a projection expression list dynamically without executing any JVM actions.

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
    .appName("UseCase_017_Recursive_Schema_Traversal_Flattening_Deeply_Nested_JSON/Structs")
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
# MAGIC ## 5. Generating Arbitrarily Deep Nested Schema
# MAGIC Create a multi-level nested customer record with structs inside structs.

# COMMAND ----------

deep_data = [
    {
        "id": "ACC-99",
        "company": {
            "name": "Acme Corp",
            "headquarters": {
                "geo": {"lat": 37.7749, "lon": -122.4194},
                "city": "San Francisco"
            }
        },
        "metrics": {"financials": {"revenue": 5000000.0, "ebitda": 1200000.0}}
    }
]

import json
rdd_json = spark.sparkContext.parallelize([json.dumps(d) for d in deep_data])
df_deep = spark.read.json(rdd_json)
print("Original Deeply Nested Schema:")
df_deep.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Recursive Flattening Algorithm
# MAGIC Implement recursive schema inspection function.

# COMMAND ----------

def flatten_dataframe(df):
    """
    Recursively flattens all StructType columns in a DataFrame into top-level columns.
    """
    flat_cols = []
    has_struct = False
    
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StructType):
            has_struct = True
            for child in field.dataType.fields:
                flat_cols.append(
                    F.col(f"{field.name}.{child.name}").alias(f"{field.name}_{child.name}")
                )
        else:
            flat_cols.append(F.col(field.name))
            
    df_result = df.select(*flat_cols)
    # If any struct columns were flattened, recurse until all structs are resolved
    if has_struct:
        return flatten_dataframe(df_result)
    return df_result

df_flattened = flatten_dataframe(df_deep)
print("Completely Flattened Schema:")
df_flattened.printSchema()
df_flattened.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation & Column Verification
# MAGIC Verify all leaf columns are top-level primitives.

# COMMAND ----------

expected_cols = [
    "id",
    "company_name",
    "company_headquarters_city",
    "company_headquarters_geo_lat",
    "company_headquarters_geo_lon",
    "metrics_financials_ebitda",
    "metrics_financials_revenue"
]

for c in expected_cols:
    assert c in df_flattened.columns, f"Expected column {c} missing from flattened DataFrame"

print(f"Successfully flattened deep schema into {len(df_flattened.columns)} scalar columns!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Programmatic schema inspection via `df.schema.fields` enables dynamic metadata-driven pipelines.**
# MAGIC - **Recursive flattening eliminates manual column renaming and hardcoded nested selectors.**
# MAGIC - **Flattened schemas allow downstream BI tools (Power BI, Tableau) to query data without JSON parsing.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How do you handle nested `ArrayType(StructType)` columns during automated schema flattening?
# MAGIC 2. What are the performance implications of flattening a schema with 500+ leaf columns in Parquet?
# MAGIC 3. Why can recursive schema transformations exceed Java bytecode limits (64KB method size) in Spark?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
