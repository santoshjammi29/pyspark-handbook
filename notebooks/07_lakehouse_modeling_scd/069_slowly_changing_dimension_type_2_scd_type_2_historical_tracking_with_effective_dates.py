# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 069: Slowly Changing Dimension Type 2 (SCD Type 2) Historical Tracking with Effective Dates
# MAGIC
# MAGIC **Module:** Module 07: Lakehouse Data Modeling, CDC & SCD Implementation  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC An insurance underwriting platform tracks policyholder residential address history. Premium risk calculations depend strictly on the policyholder's address at the time a claim occurred, requiring complete historical tracking of address changes.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement Slowly Changing Dimension Type 2 (SCD Type 2) in PySpark: maintain `effective_start_date`, `effective_end_date`, and `is_current` active flag columns. When an attribute changes, expire the current record by setting `effective_end_date` and insert a new active record.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC SCD Type 2 preserves the full history of attribute changes over time. Delta Lake implements SCD2 using a two-pass MERGE pattern or Union technique: (1) Update existing current records to close the effective date window (`is_current = false`), and (2) Insert the new state with `is_current = true` and `effective_end_date = NULL` (or 9999-12-31).

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
    .appName("UseCase_069_Slowly_Changing_Dimension_Type_2_(SCD_Type_2)_Historical_Tracking_with_Effective_Dates")
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
# MAGIC ## 5. Initializing SCD Type 2 Customer Table in Delta
# MAGIC Create target dimension table with surrogate keys and validity dates.

# COMMAND ----------

from delta.tables import DeltaTable

scd2_path = "/tmp/delta_dim_customer_scd2_069"
if Path(scd2_path).exists():
    shutil.rmtree(scd2_path)

initial_scd2 = [
    (1, "C-101", "Alice", "Austin", "2023-01-01", "9999-12-31", True),
    (2, "C-102", "Bob", "Seattle", "2023-01-01", "9999-12-31", True)
]

schema = "sk_id LONG, cust_id STRING, name STRING, city STRING, start_date STRING, end_date STRING, is_current BOOLEAN"
spark.createDataFrame(initial_scd2, schema).write.format("delta").mode("overwrite").save(scd2_path)

dt_scd2 = DeltaTable.forPath(spark, scd2_path)
print("Initial Active SCD Type 2 Dimension:")
dt_scd2.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Preparing Inbound Update (Alice moves from Austin to Denver)
# MAGIC Alice moves to Denver on 2023-11-20; Bob remains unchanged.

# COMMAND ----------

new_cdc_feed = [
    ("C-101", "Alice", "Denver", "2023-11-20")
]

df_incoming = spark.createDataFrame(new_cdc_feed, ["cust_id", "name", "city", "change_date"])
df_incoming.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Executing 2-Stage SCD Type 2 Delta Merge
# MAGIC Expire the previous version and insert the new active record.

# COMMAND ----------

# Step 1: Identify existing records that need expiration
df_current = dt_scd2.toDF().filter(F.col("is_current") == True)

df_to_expire = df_incoming.join(
    df_current,
    (df_incoming["cust_id"] == df_current["cust_id"]) & (df_incoming["city"] != df_current["city"]),
    how="inner"
).select(df_current["sk_id"], df_incoming["change_date"])

# Expire old records: set end_date and is_current = False
for row in df_to_expire.collect():
    dt_scd2.update(
        condition=F.col("sk_id") == row.sk_id,
        set={"end_date": F.lit(row.change_date), "is_current": F.lit(False)}
    )

# Step 2: Insert new active record
max_sk = dt_scd2.toDF().agg(F.max("sk_id")).first()[0] or 0

df_new_inserts = df_incoming.select(
    (F.lit(max_sk) + F.monotonically_increasing_id() + 1).alias("sk_id"),
    "cust_id",
    "name",
    "city",
    F.col("change_date").alias("start_date"),
    F.lit("9999-12-31").alias("end_date"),
    F.lit(True).alias("is_current")
)

df_new_inserts.write.format("delta").mode("append").save(scd2_path)

print("Final SCD Type 2 Dimension Table (Notice Alice's historical record preserved!):")
dt_scd2.toDF().orderBy("cust_id", "start_date").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **SCD Type 2 preserves complete historical lineage using effective start date, end date, and `is_current` flags.**
# MAGIC - **Surrogate keys (not natural keys) are mandatory because the same natural key appears across multiple rows.**
# MAGIC - **Queries join fact tables to SCD2 dimensions using point-in-time condition: `fact.date BETWEEN dim.start_date AND dim.end_date`.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between SCD Type 1, SCD Type 2, and SCD Type 3?
# MAGIC 2. How do point-in-time queries join transaction facts with SCD Type 2 dimension tables?
# MAGIC 3. Why can natural keys NOT serve as primary keys in an SCD Type 2 dimension table?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
