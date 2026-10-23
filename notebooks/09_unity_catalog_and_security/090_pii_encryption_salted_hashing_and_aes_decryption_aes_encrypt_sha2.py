# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 090: PII Encryption, Salted Hashing and AES Decryption (aes_encrypt, sha2)
# MAGIC
# MAGIC **Module:** Module 09: Databricks Platform, Unity Catalog, Security & FinOps  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A payment processing platform stores credit card Primary Account Numbers (PANs). PCI-DSS regulatory standards mandate that card numbers must be cryptographically encrypted at rest using AES-256 with managed secret keys and salted hashes for tokenization.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Master cryptographic security in PySpark: encrypt sensitive columns using `aes_encrypt()`, decrypt using `aes_decrypt()`, implement irreversible salted hashing with `sha2()`, and manage cryptographic keys securely.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Spark SQL built-in `aes_encrypt(expr, key, mode, padding)` provides hardware-accelerated AES encryption. Using a secret cryptographic salt prevents rainbow table attacks on hashed columns.

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
    .appName("UseCase_090_PII_Encryption,_Salted_Hashing_and_AES_Decryption_(aes_encrypt,_sha2)")
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
# MAGIC ## 5. Generating Credit Card Transactions
# MAGIC Create sensitive payment records requiring PCI-DSS encryption.

# COMMAND ----------

cards = [
    ("TXN-1", "4111111111111234", 150.00),
    ("TXN-2", "5500000000005678", 320.50)
]

schema = "txn_id STRING, card_number STRING, amount DOUBLE"
df_cards = spark.createDataFrame(cards, schema)
df_cards.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. AES Encryption and Salted Hashing
# MAGIC Encrypt card numbers with AES-128/256 and generate tokenized hash.

# COMMAND ----------

# 16-byte key for AES-128 (in production, retrieved via dbutils.secrets.get)
aes_key = "1234567890123456"
salt_token = "SECURE_ENTERPRISE_SALT_2023"

# 1. AES Encrypt (produces binary ciphertext)
# 2. Salted SHA-256 Tokenization (irreversible pseudonymization)
df_secured = (
    df_cards
    .withColumn(
        "encrypted_card_bin",
        F.expr(f"aes_encrypt(card_number, '{aes_key}')")
    )
    .withColumn(
        "tokenized_card_hash",
        F.sha2(F.concat(F.col("card_number"), F.lit(salt_token)), 256)
    )
    .drop("card_number") # Drop cleartext immediately!
)

print("Secured Payment Table (Cleartext PAN eliminated):")
df_secured.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Decryption for Authorized Settlement Workflow
# MAGIC Decrypt ciphertext back to cleartext using aes_decrypt in authorized settlement service.

# COMMAND ----------

# Decrypt binary ciphertext back to string
df_decrypted = df_secured.withColumn(
    "decrypted_card_number",
    F.expr(f"cast(aes_decrypt(encrypted_card_bin, '{aes_key}') AS STRING)")
)

print("Decrypted Payment Records (Authorized Session):")
df_decrypted.select("txn_id", "decrypted_card_number", "amount", "tokenized_card_hash").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`aes_encrypt` and `aes_decrypt` provide native, high-speed cryptographic protection directly in Spark SQL.**
# MAGIC - **Salted hashing (`sha2(concat(col, salt))`) enables irreversible tokenization resistant to rainbow table attacks.**
# MAGIC - **Never store encryption keys in notebook code—always inject them dynamically via secret scopes.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between reversible encryption (AES) and irreversible tokenization (SHA-256)?
# MAGIC 2. Why is salting mandatory when hashing identifiers like emails or credit card numbers?
# MAGIC 3. How do you rotate AES encryption keys on multi-terabyte historical Delta tables?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
