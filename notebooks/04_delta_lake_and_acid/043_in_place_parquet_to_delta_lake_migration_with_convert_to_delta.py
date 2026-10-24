# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 043: In-Place Parquet to Delta Lake Migration with CONVERT TO DELTA
# MAGIC
# MAGIC **Module:** Module 04: Delta Lake Architecture, ACID Operations & Time Travel
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC An enterprise data platform has 100TB of historical data stored across partitioned Parquet directories in Amazon S3 or Azure ADLS. Re-writing all Parquet files into Delta would take days and cost thousands of dollars in cloud compute.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Perform a zero-copy, in-place migration of an existing Parquet table to Delta Lake using `CONVERT TO DELTA`. Preserve existing partition directory hierarchies and verify instant ACID transaction log generation without moving data files.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC `CONVERT TO DELTA` reads the file metadata and directory structure of an existing Parquet directory and synthesizes commit `000000.json` in `_delta_log/` in seconds. It does not rewrite or copy underlying Parquet data files, making it a zero-copy instant operation.

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
        .appName("UseCase_043_In_Place_Parquet_to_Delta_Lake_Migration_with_CONVERT_TO_DELTA")
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
# MAGIC ### Step 1: Generating Partitioned Parquet Table
# MAGIC Create an unmanaged Parquet table partitioned by department.

# COMMAND ----------

parquet_base_path = "/tmp/parquet_legacy_043"
if Path(parquet_base_path).exists():
    shutil.rmtree(parquet_base_path)

legacy_records = [
    ("EMP-01", "Alice", 90000.0, "HR"),
    ("EMP-02", "Bob", 110000.0, "Engineering"),
    ("EMP-03", "Charlie", 95000.0, "HR"),
    ("EMP-04", "Diana", 140000.0, "Engineering")
]

# Write as standard Parquet partitioned by department
spark.createDataFrame(legacy_records, ["emp_id", "name", "salary", "department"])    .write.format("parquet")    .partitionBy("department")    .mode("overwrite")    .save(parquet_base_path)

print(f"Legacy partitioned Parquet created at: {parquet_base_path}")
print(f"Subdirectories: {[d.name for d in Path(parquet_base_path).iterdir() if d.is_dir()]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Executing CONVERT TO DELTA In-Place
# MAGIC Convert Parquet directory to Delta Lake with zero file copies.

# COMMAND ----------

# Execute CONVERT TO DELTA with partition specification
spark.sql(f"CONVERT TO DELTA parquet.`{parquet_base_path}` PARTITIONED BY (department STRING)")

print("CONVERT TO DELTA completed!")
print(f"Has _delta_log now: {(Path(parquet_base_path) / '_delta_log').exists()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Verifying ACID Capabilities on Converted Table
# MAGIC Verify that UPDATE and ACID operations now function on the converted table.

# COMMAND ----------

from delta.tables import DeltaTable

dt_converted = DeltaTable.forPath(spark, parquet_base_path)

# Execute an ACID update on the newly converted table
dt_converted.update(condition=F.col("emp_id") == "EMP-01", set={"salary": F.lit(98000.0)})

print("Converted Table after ACID Update:")
dt_converted.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`CONVERT TO DELTA` is a zero-copy metadata-only operation that constructs `_delta_log` instantly.**
# MAGIC - **For partitioned tables, always specify `PARTITIONED BY (col TYPE)` matching the physical directory structure.**
# MAGIC - **After conversion, you gain full ACID support, Time Travel, and MERGE capabilities without moving files.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Does `CONVERT TO DELTA` duplicate or move any underlying Parquet data files?
# MAGIC 2. What happens if an external system writes new Parquet files into the directory after conversion without going through Delta?
# MAGIC 3. Can you run `CONVERT TO DELTA` on CSV or JSON files?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
