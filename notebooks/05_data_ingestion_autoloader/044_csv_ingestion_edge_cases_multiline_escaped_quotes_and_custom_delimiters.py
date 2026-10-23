# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 044: CSV Ingestion Edge Cases: Multiline, Escaped Quotes and Custom Delimiters
# MAGIC
# MAGIC **Module:** Module 05: Data Ingestion Patterns, Auto Loader & File Formats  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A financial auditing pipeline imports legacy bank statement extracts. The files contain multiline transaction descriptions with embedded newlines, pipe delimiters (`|`), quotes with backslash escapes, and corrupted header rows.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Configure robust CSV reader options in PySpark: handle multiline records (`multiLine=True`), custom delimiters (`sep='|'`), escape characters (`escape='\\'`), quote handling, explicit datetime inference, and graceful handling of malformed lines.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC By default, Spark splits CSV files across byte boundaries for parallel reading, which breaks if records contain embedded newlines. Setting `multiLine=true` disables byte-offset splitting for that file, forcing a single thread per file to parse line boundaries safely.

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
    .appName("UseCase_044_CSV_Ingestion_Edge_Cases_Multiline,_Escaped_Quotes_and_Custom_Delimiters")
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
# MAGIC ## 5. Creating Edge-Case CSV File on Disk
# MAGIC Write raw CSV data with embedded newlines, pipe delimiter, and escaped quotes.

# COMMAND ----------

import shutil
from pathlib import Path

csv_dir = Path("/tmp/csv_edge_cases_044")
if csv_dir.exists():
    shutil.rmtree(csv_dir)
csv_dir.mkdir(parents=True, exist_ok=True)

csv_content = """id|customer|amount|notes|txn_date
101|Alice Corp|1500.50|Invoice #1234\nPayment split across accounts|2023-11-01
102|Bob & Co|450.00|Standard payment for \"consulting services\"|2023-11-02
103|Charlie Ltd|8900.25|Rush order\nDirect wire transfer|2023-11-03"""

file_path = csv_dir / "transactions.csv"
with open(file_path, "w") as f:
    f.write(csv_content)

print(f"Sample edge-case CSV created at {file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Configuring Resilient PySpark CSV Reader
# MAGIC Apply multiLine, sep, escape, and quote options.

# COMMAND ----------

# Configure options to handle all edge cases
df_csv = (
    spark.read
    .option("header", "true")
    .option("sep", "|")
    .option("multiLine", "true")
    .option("quote", """)
    .option("escape", "\\")
    .option("inferSchema", "true")
    .csv(str(csv_dir))
)

print("Parsed Schema:")
df_csv.printSchema()

print("Parsed Data (Notice multi-line notes preserved within rows):")
df_csv.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verifying Row Integrity Assertions
# MAGIC Verify exactly 3 records were ingested without split-line corruption.

# COMMAND ----------

assert df_csv.count() == 3, f"Expected 3 rows, found {df_csv.count()} (Multiline issue!)"
assert "\n" in df_csv.filter(F.col("id") == 101).select("notes").first()[0], "Embedded newline lost"
print("All CSV edge-case ingestion assertions passed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`multiLine=true` is required when fields contain embedded newlines; note that it disables file splitting.**
# MAGIC - **Always specify explicit `escape` and `quote` characters when ingesting non-standard CSV extracts.**
# MAGIC - **For production pipelines, prefer Parquet/Delta over CSV to eliminate textual parsing ambiguities.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How does `multiLine=true` affect the parallelism and performance of reading large CSV files in Spark?
# MAGIC 2. What is the difference between `inferSchema=true` and providing an explicit StructType on CSV ingestion?
# MAGIC 3. How do you handle CSV files where the header appears periodically throughout the file (concatenated extracts)?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
