# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 071: Change Data Capture (CDC) Ingestion from Debezium/Kafka into Lakehouse
# MAGIC
# MAGIC **Module:** Module 07: Lakehouse Data Modeling, CDC & SCD Implementation  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A transactional MySQL inventory database streams row-level changes into Kafka via Debezium CDC. Debezium publishes events containing `before`, `after`, and `op` flags (`c` for create, `u` for update, `d` for delete). The lakehouse must reconstruct current database state.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Ingest and process Debezium CDC streams in PySpark: parse the nested JSON envelope, handle out-of-order CDC events using sequence numbers/timestamps, execute conditional MERGE into target Delta tables, and handle hard deletes gracefully.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Debezium CDC events contain metadata fields: `op` (operation: c=create, u=update, d=delete, r=snapshot read) and `ts_ms` (source commit timestamp). Applying CDC records requires deduplicating within each micro-batch by taking the latest `ts_ms` before merging.

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
    .appName("UseCase_071_Change_Data_Capture_(CDC)_Ingestion_from_Debezium/Kafka_into_Lakehouse")
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
# MAGIC ## 5. Generating Debezium CDC JSON Payloads
# MAGIC Create representative Debezium change events including create, update, and delete.

# COMMAND ----------

cdc_raw_events = [
    # Insert SKU-101
    ('{"before": null, "after": {"sku": "SKU-101", "stock": 100, "price": 20.0}, "op": "c", "ts_ms": 1700560000}'),
    # Update SKU-101 stock to 80
    ('{"before": {"sku": "SKU-101", "stock": 100, "price": 20.0}, "after": {"sku": "SKU-101", "stock": 80, "price": 20.0}, "op": "u", "ts_ms": 1700560500}'),
    # Insert SKU-102
    ('{"before": null, "after": {"sku": "SKU-102", "stock": 50, "price": 45.0}, "op": "c", "ts_ms": 1700560100}'),
    # Delete SKU-102
    ('{"before": {"sku": "SKU-102", "stock": 50, "price": 45.0}, "after": null, "op": "d", "ts_ms": 1700560800}')
]

rdd_cdc = spark.sparkContext.parallelize(cdc_raw_events)
df_raw_cdc = spark.read.json(rdd_cdc)
df_raw_cdc.printSchema()
df_raw_cdc.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Flattening CDC Payload & Filtering Latest Event per Primary Key
# MAGIC Extract primary keys, payload attributes, and deduplicate by timestamp.

# COMMAND ----------

# Extract target attributes from after struct (or before struct for deletes)
df_flattened_cdc = df_raw_cdc.select(
    F.coalesce(F.col("after.sku"), F.col("before.sku")).alias("sku"),
    F.col("after.stock").alias("stock"),
    F.col("after.price").alias("price"),
    F.col("op"),
    F.col("ts_ms")
)

# Deduplicate to find the latest operation per SKU
win_latest = Window.partitionBy("sku").orderBy(F.col("ts_ms").desc())
df_latest_cdc = (
    df_flattened_cdc
    .withColumn("rn", F.row_number().over(win_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print("Deduplicated Latest CDC State:")
df_latest_cdc.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Merging CDC State into Target Delta Lakehouse Table
# MAGIC Apply conditional update, insert, and delete using Delta merge.

# COMMAND ----------

from delta.tables import DeltaTable

delta_cdc_target = "/tmp/delta_inventory_cdc_071"
if Path(delta_cdc_target).exists():
    shutil.rmtree(delta_cdc_target)

# Initialize target table
init_schema = "sku STRING, stock INT, price DOUBLE, last_ts LONG"
spark.createDataFrame([], init_schema).write.format("delta").mode("overwrite").save(delta_cdc_target)

target_tbl = DeltaTable.forPath(spark, delta_cdc_target)

(
    target_tbl.alias("target")
    .merge(
        source=df_latest_cdc.alias("source"),
        condition="target.sku = source.sku"
    )
    .whenMatchedDelete(
        condition="source.op = 'd'"
    )
    .whenMatchedUpdate(
        set={"stock": F.col("source.stock"), "price": F.col("source.price"), "last_ts": F.col("source.ts_ms")}
    )
    .whenNotMatchedInsert(
        condition="source.op != 'd'",
        values={"sku": F.col("source.sku"), "stock": F.col("source.stock"), "price": F.col("source.price"), "last_ts": F.col("source.ts_ms")}
    )
    .execute()
)

print("Target Delta Inventory Table after CDC Replay (SKU-101 updated, SKU-102 deleted):")
target_tbl.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Debezium CDC envelopes provide `before`, `after`, `op`, and commit sequence timestamps.**
# MAGIC - **Always deduplicate the incoming CDC micro-batch by primary key ordering on `ts_ms` before calling Delta MERGE.**
# MAGIC - **Handle delete operations explicitly with `whenMatchedDelete(condition='source.op == "d"')`.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How do you handle out-of-order CDC events arriving from Kafka in a Spark Structured Streaming job?
# MAGIC 2. What is the difference between soft deletes and hard deletes in Lakehouse CDC architectures?
# MAGIC 3. How does Delta Lake's Change Data Feed (CDF) compare with external CDC sources like Debezium?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
