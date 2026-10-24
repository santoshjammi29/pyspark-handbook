# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 005: String Operations, Regex Extraction & Data Cleansing
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API
# MAGIC **Difficulty:** `Beginner`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A marketing automation system ingests unstructured customer lead notes containing messy phone numbers, email addresses with inconsistent cases, and unformatted address strings.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Apply string manipulation functions: trimming whitespace, case normalization, regex value extraction (`regexp_extract`), masking sensitive values with `regexp_replace`, and parsing delimited tokens into structured columns.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Spark SQL string functions are backed by native UTF-8 binary representations in Apache Spark's Tungsten project, avoiding Java String object allocation overhead.

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
        .appName("UseCase_005_String_Operations_Regex_Extraction_and_Data_Cleansing")
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
# MAGIC ### Step 1: Generating Unclean Lead Data
# MAGIC Simulate raw, unstandardized user inputs.

# COMMAND ----------

leads = [
    ("LEAD-1", "  JOHN.DOE@GMAIL.COM  ", "Phone: (555) 123-4567 | City: New York, NY"),
    ("LEAD-2", "SARAH_CONNOR@skyNET.io", "Cell: 555.987.6543 | City: Los Angeles, CA"),
    ("LEAD-3", "unknown_user", "No contact provided | City: Chicago, IL"),
    ("LEAD-4", "bruce.wayne@WayneEnt.com ", "Direct: +1-555-444-3333 | City: Gotham, NJ")
]

df_leads = spark.createDataFrame(leads, ["lead_id", "raw_email", "raw_contact_str"])
df_leads.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Sanitizing Emails & Extracting Structured Fields via Regex
# MAGIC Normalize text and extract patterns using regular expressions.

# COMMAND ----------

# 1. Clean email: trim whitespace and lower case
df_clean_email = df_leads.withColumn("clean_email", F.lower(F.trim(F.col("raw_email"))))

# 2. Extract 10-digit phone number pattern
# Matches (xxx) xxx-xxxx, xxx.xxx.xxxx, or xxx-xxx-xxxx
phone_regex = r"(\d{3})[^\d]?(\d{3})[^\d]?(\d{4})"

df_extracted = df_clean_email.withColumn(
    "phone_digits",
    F.regexp_replace(
        F.regexp_extract(F.col("raw_contact_str"), phone_regex, 0),
        r"[^\d]",
        ""
    )
).withColumn(
    "city",
    F.regexp_extract(F.col("raw_contact_str"), r"City:\s*([A-Za-z\s]+),", 1)
).withColumn(
    "state",
    F.regexp_extract(F.col("raw_contact_str"), r",\s*([A-Z]{2})", 1)
)

df_extracted.select("lead_id", "clean_email", "phone_digits", "city", "state").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: PII Masking on Email & Phone
# MAGIC Mask phone numbers and email usernames for compliance before landing in analytics tables.

# COMMAND ----------

# Mask email username: j***e@domain.com
df_masked = df_extracted.withColumn(
    "masked_email",
    F.regexp_replace(F.col("clean_email"), r"(^.).*(@.*$)", "$1***$2")
).withColumn(
    "masked_phone",
    F.when(
        F.length(F.col("phone_digits")) == 10,
        F.concat(F.lit("***-***-"), F.substring(F.col("phone_digits"), 7, 4))
    ).otherwise(F.lit("UNAVAILABLE"))
)

df_masked.select("lead_id", "masked_email", "masked_phone", "city", "state").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`regexp_extract(col, pattern, group_index)` extracts capturing groups where group 0 is the full match.**
# MAGIC - **Tungsten's native UTF8String implementation avoids garbage collection overhead during large-scale string operations.**
# MAGIC - **Combine regex cleaning with conditional `when/otherwise` masks to ensure privacy compliance.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How does Spark's `regexp_extract` handle missing or non-matching patterns?
# MAGIC 2. What is the performance difference between built-in SQL string functions and a custom Python regex UDF?
# MAGIC 3. How does Tungsten memory management optimize string representations in Spark?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
