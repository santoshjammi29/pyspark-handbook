# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 021: XML Data Ingestion, Hierarchical Parsing and Tag Structuring
# MAGIC
# MAGIC **Module:** Module 02: Complex Data Types & Nested Structures Handling  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A legacy healthcare exchange transmits insurance claims in XML format. Claims contain nested patient information, diagnosis codes, and billing line items that must be parsed into structured Spark tables.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Ingest hierarchical XML records in PySpark: parse nested XML tags into StructType and ArrayType hierarchies, configure row tags, and handle optional missing tags without schema mismatches.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Native XML parsing in Spark utilizes the `spark-xml` library (or Databricks built-in XML reader in DBR 14+). In standard PySpark, XML can be processed via the XML data source format or parsed using Python/XPath helper functions for lightweight payloads.

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
    .appName("UseCase_021_XML_Data_Ingestion,_Hierarchical_Parsing_and_Tag_Structuring")
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
# MAGIC ## 5. Generating Sample Healthcare XML Claims
# MAGIC Create multi-level XML claim documents.

# COMMAND ----------

xml_payloads = [
    ("""<claim id="CLM-001">
        <patient id="P-10">
            <name>John Doe</name>
            <dob>1980-05-12</dob>
        </patient>
        <items>
            <item code="99213" amount="150.00"/>
            <item code="80053" amount="75.50"/>
        </items>
    </claim>""",),
    ("""<claim id="CLM-002">
        <patient id="P-20">
            <name>Jane Smith</name>
            <dob>1992-11-23</dob>
        </patient>
        <items>
            <item code="99214" amount="220.00"/>
        </items>
    </claim>""",)
]

df_raw_xml = spark.createDataFrame(xml_payloads, ["raw_xml"])
df_raw_xml.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Parsing XML Payloads via Built-in Regex & XPath Expressions
# MAGIC Extract attributes and tags using xpath_string and xpath_double expressions.

# COMMAND ----------

df_parsed_claims = df_raw_xml.withColumn(
    "claim_id", F.expr("xpath_string(raw_xml, 'claim/@id')")
).withColumn(
    "patient_id", F.expr("xpath_string(raw_xml, 'claim/patient/@id')")
).withColumn(
    "patient_name", F.expr("xpath_string(raw_xml, 'claim/patient/name')")
).withColumn(
    "patient_dob", F.expr("xpath_string(raw_xml, 'claim/patient/dob')")
).withColumn(
    "primary_item_code", F.expr("xpath_string(raw_xml, 'claim/items/item[1]/@code')")
).withColumn(
    "primary_item_amount", F.expr("xpath_double(raw_xml, 'claim/items/item[1]/@amount')")
)

df_parsed_claims.select("claim_id", "patient_id", "patient_name", "primary_item_code", "primary_item_amount").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation & Structural Integrity Check
# MAGIC Verify that all claims were parsed with valid IDs and positive amounts.

# COMMAND ----------

assert df_parsed_claims.filter(F.col("claim_id").isNull()).count() == 0, "Missing claim IDs"
assert df_parsed_claims.filter(F.col("primary_item_amount") <= 0).count() == 0, "Invalid item amounts"

print("XML extraction and schema validation completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Spark SQL provides built-in XPath expressions (`xpath_string`, `xpath_double`, `xpath_int`) for lightweight XML parsing without external dependencies.**
# MAGIC - **For heavy multi-gigabyte XML datasets, use `spark.read.format('xml').option('rowTag', 'tag_name')`.**
# MAGIC - **Always validate attribute extraction to ensure mandatory business keys are populated.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between parsing XML via XPath expressions versus using the `spark-xml` library?
# MAGIC 2. How does the `rowTag` option dictate partition splitting in large XML files?
# MAGIC 3. Why is XML typically less efficient than Parquet or Avro in distributed big data pipelines?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
