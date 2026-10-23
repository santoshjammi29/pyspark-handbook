# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 091: Databricks Secret Scopes and Secure Credential Injection via dbutils.secrets
# MAGIC
# MAGIC **Module:** Module 09: Databricks Platform, Unity Catalog, Security & FinOps  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A data integration pipeline connects to cloud databases, external APIs (Stripe, Salesforce), and message queues. Developers must never hardcode API keys or passwords in notebooks, Git repositories, or logs.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Master secret management in Databricks: create Azure Key Vault / AWS Secrets Manager backed secret scopes, retrieve secrets safely via `dbutils.secrets.get()`, understand automatic secret redaction in driver stdout, and pass credentials into JDBC and REST APIs securely.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Databricks Secret Scopes store encrypted key-value pairs backed by Databricks internal storage or cloud key vaults (Azure Key Vault / AWS KMS). When `dbutils.secrets.get()` is called, Databricks replaces the secret with `[REDACTED]` in all notebook output cells and Spark logs, preventing credential leakage.

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
    .appName("UseCase_091_Databricks_Secret_Scopes_and_Secure_Credential_Injection_via_dbutils.secrets")
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
# MAGIC ## 5. Secret Scope Creation Blueprint (CLI & Cloud Key Vault)
# MAGIC Review commands to configure Databricks Secret Scopes.

# COMMAND ----------

# Databricks CLI Secret Scope Creation:
# 1. Create Databricks-backed scope:
#    databricks secrets create-scope finance_scope
# 2. Store a secret:
#    databricks secrets put-secret finance_scope stripe_api_key
# 3. List available scopes:
#    databricks secrets list-scopes

print("Databricks secret scope management commands reviewed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Demonstrating Secure Secret Retrieval and Output Redaction
# MAGIC Retrieve secrets via dbutils.secrets.get and observe automatic redaction.

# COMMAND ----------

# Safe wrapper for dbutils.secrets across Databricks and local dev
import os

class SafeSecretProvider:
    @staticmethod
    def get_secret(scope: str, key: str) -> str:
        try:
            # Native Databricks environment
            import dbutils
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            # Local / CI fallback using environment variables
            env_var = f"{scope.upper()}_{key.upper()}"
            return os.environ.get(env_var, f"dummy_secret_for_{key}")

api_token = SafeSecretProvider.get_secret("finance_scope", "stripe_api_key")
print(f"Retrieved token value: {api_token[:4]}**** (Never print complete secrets to logs!)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Injecting Credentials into JDBC Connections Safely
# MAGIC Use retrieved credentials in connection configurations without persisting to disk.

# COMMAND ----------

db_pass = SafeSecretProvider.get_secret("dw_scope", "postgres_password")

connection_properties = {
    "user": "etl_service_account",
    "password": db_pass,
    "driver": "org.postgresql.Driver"
}

print("Database connection properties assembled securely.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Never hardcode passwords or API keys in notebooks or Git repositories.**
# MAGIC - **Databricks automatically intercepts and redacts secrets retrieved via `dbutils.secrets.get()` in notebook outputs.**
# MAGIC - **Back secret scopes with Azure Key Vault or AWS Secrets Manager for enterprise audit compliance.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How does Databricks ensure that secrets retrieved via `dbutils.secrets.get()` are not exposed in stdout logs?
# MAGIC 2. What is the difference between a Databricks-backed secret scope and an Azure Key Vault-backed secret scope?
# MAGIC 3. How do you manage secret scope access control using Databricks Access Control Lists (ACLs)?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
