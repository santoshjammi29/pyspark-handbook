# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 007: Handling Nulls, NaNs & Imputation Strategies
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API  
# MAGIC **Difficulty:** `Beginner`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A medical diagnostics platform aggregates clinical lab test results. Missing biomarker readings occur due to machine sensor dropouts or patient cancellations. The pipeline must differentiate between true NULLs, NaN values, and impute baseline values.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Detect and quantify missing values across all columns. Compare `coalesce`, `fillna`/`na.fill`, and `dropna`/`na.drop` with thresholds. Implement statistical imputation (replacing missing numerical values with category medians).
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC In PySpark, `NULL` (unknown value) and `NaN` (IEEE 754 Not-a-Number float) have distinct semantics. `isnan()` only applies to Double/Float columns, while `isNull()` applies to all types. Functions like `count()` ignore NULLs, but `count(*)` counts all rows.

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
    .appName("UseCase_007_Handling_Nulls,_NaNs_and_Imputation_Strategies")
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
# MAGIC ## 5. Creating Clinical Dataset with Nulls and NaNs
# MAGIC Create sample medical records with missing patient readings.

# COMMAND ----------

clinical_data = [
    ("P-01", "Metabolic", 98.6, 120.0, "NORMAL"),
    ("P-02", "Cardiac", float("nan"), 145.0, "ELEVATED"),
    ("P-03", "Metabolic", None, None, None),
    ("P-04", "Cardiac", 101.2, float("nan"), "CRITICAL"),
    ("P-05", "Metabolic", 97.9, 115.0, "NORMAL"),
    ("P-06", "Neurology", None, 130.0, None)
]

schema = "patient_id STRING, dept STRING, body_temp DOUBLE, systolic_bp DOUBLE, assessment STRING"
df_clinical = spark.createDataFrame(clinical_data, schema)
df_clinical.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Auditing Missing Values Across All Columns
# MAGIC Compute a comprehensive null and NaN count audit in a single aggregation pass.

# COMMAND ----------

# Build expressions dynamically for all columns
null_audit_exprs = [
    F.count(
        F.when(F.col(c).isNull() | (F.isnan(F.col(c)) if t in ["double", "float"] else F.lit(False)), c)
    ).alias(f"{c}_missing_count")
    for c, t in df_clinical.dtypes
]

df_audit = df_clinical.agg(*null_audit_exprs)
df_audit.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Threshold Dropping and Imputation
# MAGIC Drop records with too many missing values, then impute departmental averages for remaining gaps.

# COMMAND ----------

# 1. Drop rows with at least 3 null fields
df_filtered = df_clinical.dropna(thresh=3)

# 2. Compute departmental average systolic_bp for imputation
dept_avgs = df_filtered.groupBy("dept").agg(F.round(F.avg("systolic_bp"), 1).alias("avg_bp"))

# 3. Join and coalesce missing readings
df_imputed = df_filtered.join(dept_avgs, on="dept", how="left").withColumn(
    "systolic_bp_imputed",
    F.coalesce(
        F.when(F.isnan(F.col("systolic_bp")), None).otherwise(F.col("systolic_bp")),
        F.col("avg_bp"),
        F.lit(120.0) # default fallback
    )
).withColumn(
    "assessment_clean",
    F.coalesce(F.col("assessment"), F.lit("PENDING_REVIEW"))
)

df_imputed.select("patient_id", "dept", "systolic_bp", "systolic_bp_imputed", "assessment_clean").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`F.isnan()` applies strictly to Float/Double types, while `F.isNull()` applies universally.**
# MAGIC - **Use `dropna(thresh=k)` to enforce a minimum number of valid non-null columns per row.**
# MAGIC - **`F.coalesce(*cols)` returns the first non-null expression in evaluation order.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `NULL` and `NaN` in Spark SQL, and how does `count(col)` treat them?
# MAGIC 2. How does `F.coalesce()` differ from `DataFrame.coalesce()` in PySpark?
# MAGIC 3. How do you implement categorical mode imputation in a single Spark transformation pass?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
