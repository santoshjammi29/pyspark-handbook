# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 042: Delta Table Constraints: CHECK and NOT NULL Data Quality Rules
# MAGIC
# MAGIC **Module:** Module 04: Delta Lake Architecture, ACID Operations & Time Travel  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A banking compliance application requires strict data quality invariants on account records: account balances must never drop below -$500, customer email addresses must contain '@', and account IDs must never be null.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Apply data quality constraints to Delta tables: configure `NOT NULL` constraints, add boolean `CHECK` constraints using `ALTER TABLE ADD CONSTRAINT`, verify constraint enforcement on writes, and handle constraint violation exceptions.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Delta constraints are stored directly in table metadata properties (`_delta_log`). Any write operation that violates a constraint is rejected atomically before writing to disk, ensuring zero bad data lands in Gold or Silver tables.

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
    .appName("UseCase_042_Delta_Table_Constraints_CHECK_and_NOT_NULL_Data_Quality_Rules")
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
# MAGIC ## 5. Creating Delta Table with Constraints
# MAGIC Create table and apply NOT NULL and CHECK constraints.

# COMMAND ----------

delta_cons_path = "/tmp/delta_constraints_042"
if Path(delta_cons_path).exists():
    shutil.rmtree(delta_cons_path)

init_accounts = [("ACC-1", "user1@bank.com", 250.0)]
spark.createDataFrame(init_accounts, ["acc_id", "email", "balance"])    .write.format("delta").mode("overwrite").save(delta_cons_path)

# Register as temp view to run ALTER TABLE DDL
spark.sql(f"CREATE TABLE delta_accounts_042 USING DELTA LOCATION '{delta_cons_path}'")

# Add NOT NULL and CHECK constraints
spark.sql("ALTER TABLE delta_accounts_042 ALTER COLUMN acc_id SET NOT NULL")
spark.sql("ALTER TABLE delta_accounts_042 ADD CONSTRAINT valid_email CHECK (email LIKE '%@%')")
spark.sql("ALTER TABLE delta_accounts_042 ADD CONSTRAINT min_overdraft CHECK (balance >= -500.0)")

print("Constraints successfully applied to Delta table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Testing Constraint Violations & Atomic Rejection
# MAGIC Attempt to insert a record violating the minimum overdraft check.

# COMMAND ----------

# Attempting to insert an account with balance -1000 (Violates min_overdraft >= -500.0)
bad_account = [("ACC-2", "user2@bank.com", -1000.0)]
df_bad = spark.createDataFrame(bad_account, ["acc_id", "email", "balance"])

try:
    df_bad.write.format("delta").mode("append").save(delta_cons_path)
    print("Write succeeded unexpectedly!")
except Exception as e:
    print("Constraint Enforcement Blocked Bad Data! Error captured:")
    print(str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validating Safe Writes
# MAGIC Insert valid accounts and verify table state.

# COMMAND ----------

valid_accounts = [
    ("ACC-2", "user2@bank.com", 500.0),
    ("ACC-3", "user3@bank.com", -150.0) # Allowed overdraft
]
spark.createDataFrame(valid_accounts, ["acc_id", "email", "balance"])    .write.format("delta").mode("append").save(delta_cons_path)

print("Current Clean Account Table:")
spark.read.format("delta").load(delta_cons_path).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Delta CHECK constraints enforce data quality at write time directly inside the storage engine.**
# MAGIC - **If a single row in an incoming write batch violates a constraint, the entire transaction is rolled back.**
# MAGIC - **Inspect active constraints using `DESCRIBE DETAIL table_name` or `SHOW TBLPROPERTIES`.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How do Delta Lake `CHECK` constraints compare to Delta Live Tables (DLT) expectations?
# MAGIC 2. What happens if you add a CHECK constraint to a table that already contains existing violating rows?
# MAGIC 3. Can CHECK constraints call external Python UDFs or subqueries?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
