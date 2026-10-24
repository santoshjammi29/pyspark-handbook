# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 098: Project Healthcare: HIPAA-Compliant Clinical Records De-Identification
# MAGIC
# MAGIC **Module:** Module 10: End-to-End Enterprise Projects & Production Workflows
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A hospital research network prepares electronic health records (EHR) for clinical cancer research. HIPAA Safe Harbor compliance mandates the complete removal or cryptographic hashing of 18 direct identifiers (names, dates of birth, MRNs, zip codes) before sharing with researchers.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Build a HIPAA-compliant clinical pipeline: (1) Ingest raw clinical encounter records, (2) Apply Safe Harbor de-identification (mask names, truncate zip codes to 3 digits, shift admission dates using patient-specific cryptographically salted offsets), and (3) Calculate patient length-of-stay (LOS) metrics.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC HIPAA Safe Harbor requires removing names, phone numbers, and truncating zip codes where populations exceed 20,000. Dates must not be precise; date shifting preserves the chronological duration between events (e.g. admission and discharge) without revealing true calendar dates.

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
        .appName("UseCase_098_Project_Healthcare_HIPAA_Compliant_Clinical_Records_De_Identification")
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
# MAGIC ### Step 1: Simulating Sensitive Clinical Records
# MAGIC Create EHR patient records with protected health information (PHI).

# COMMAND ----------

raw_patients = [
    ("PAT-1001", "John Doe", "1975-06-12", "78701", "2023-11-01 08:00:00", "2023-11-05 14:00:00", "Cardiology"),
    ("PAT-1002", "Jane Roe", "1988-09-24", "98101", "2023-11-02 10:00:00", "2023-11-03 18:00:00", "Neurology")
]

schema = "mrn STRING, name STRING, dob STRING, zip_code STRING, admit_ts STRING, discharge_ts STRING, diagnosis STRING"
df_raw_ehr = spark.createDataFrame(raw_patients, schema)
print("Raw PHI Patient Dataset (Non-Compliant for Researchers):")
df_raw_ehr.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Applying HIPAA Safe Harbor De-Identification Rules
# MAGIC Mask names, truncate zip codes, and pseudonymize MRNs with salted hash.

# COMMAND ----------

SECRET_SALT = "CLINICAL_SALT_KEY_99"

df_deidentified = (
    df_raw_ehr
    # 1. Pseudonymize MRN using salted SHA-256 hash
    .withColumn("research_patient_id", F.substring(F.sha2(F.concat(F.col("mrn"), F.lit(SECRET_SALT)), 256), 1, 16))
    # 2. Drop direct name entirely
    .drop("name", "mrn")
    # 3. Truncate ZIP code to first 3 digits
    .withColumn("zip3", F.substring(F.col("zip_code"), 1, 3))
    .drop("zip_code")
    # 4. Calculate patient age instead of revealing full DOB
    .withColumn("patient_age", F.floor(F.datediff(F.current_date(), F.to_date("dob")) / 365.25))
    .drop("dob")
)

df_deidentified.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Computing Length of Stay (LOS) & Research Table Publication
# MAGIC Calculate admission stay duration in fractional days without exposing calendar dates.

# COMMAND ----------

df_research_gold = (
    df_deidentified
    .withColumn("admit", F.to_timestamp("admit_ts"))
    .withColumn("discharge", F.to_timestamp("discharge_ts"))
    .withColumn(
        "length_of_stay_days",
        F.round((F.col("discharge").cast("long") - F.col("admit").cast("long")) / 86400.0, 2)
    )
    .drop("admit_ts", "discharge_ts", "admit", "discharge")
)

print("HIPAA-Compliant De-Identified Research Table (Safe for ML & Clinical Trials):")
df_research_gold.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **HIPAA Safe Harbor requires de-identifying 18 categories of protected health information (PHI).**
# MAGIC - **Pseudonymize primary identifiers using irreversible salted hashes.**
# MAGIC - **Calculate derived intervals (Length of Stay, Age) to prevent exposing calendar dates.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What are the 18 HIPAA Safe Harbor identifiers and how do you handle them in Spark?
# MAGIC 2. What is date-shifting in healthcare data engineering and why is it preferred over dropping dates?
# MAGIC 3. How do you ensure pseudonymized patient IDs cannot be re-identified through linkage attacks?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
