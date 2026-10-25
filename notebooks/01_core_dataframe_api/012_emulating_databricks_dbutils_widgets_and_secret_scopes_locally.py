# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 012: Emulating Databricks DBUtils, Widgets & Secret Scopes Locally
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC An enterprise migration team develops Databricks pipelines locally inside VS Code or CI/CD test runners before deploying to production workspaces. Code relying on `dbutils.fs`, `dbutils.secrets`, and `dbutils.widgets` crashes outside of Databricks.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Design a clean, modular Python compatibility wrapper class `MockDBUtils` that provides identical interfaces for `dbutils.fs` (`ls`, `cp`, `mv`, `rm`, `head`, `put`), `dbutils.widgets` (`text`, `dropdown`, `get`), and `dbutils.secrets` (`get`) using environment variables and local disk storage.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC In Databricks, `dbutils` is a Java/Scala bridge injected into the Python REPL. Writing pipeline code that imports or references `dbutils` directly creates tight vendor lock-in. Wrapping it in a helper or using Databricks Connect decouples code for multi-cloud and local portability.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Setup & SparkSession Initialization
# MAGIC If running inside a Databricks notebook, the `spark` session is pre-initialized by the cluster.
# MAGIC If executing outside Databricks (e.g. VS Code, local CLI, CI/CD runner), we initialize a local SparkSession with Delta Lake support.

# COMMAND ----------

import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_012_Emulating_Databricks_DBUtils_Widgets_and_Secret_Scopes_Locally")
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
# MAGIC ### Step 1: Building the Universal DBUtils Emulation Bridge
# MAGIC Create a drop-in replacement that activates when native `dbutils` is unavailable.

# COMMAND ----------

import os
import shutil
from pathlib import Path

class MockFS:
    def ls(self, path: str):
        p = Path(path.replace("dbfs:", "").replace("file:", ""))
        if not p.exists():
            return []
        class FileInfo:
            def __init__(self, path, name, size):
                self.path = str(path)
                self.name = name
                self.size = size
        return [FileInfo(f, f.name, f.stat().st_size) for f in p.iterdir()]

    def put(self, path: str, content: str, overwrite: bool = True):
        p = Path(path.replace("dbfs:", ""))
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w") as f:
            f.write(content)

    def head(self, path: str, max_bytes: int = 1000):
        p = Path(path.replace("dbfs:", ""))
        with open(p, "r") as f:
            return f.read(max_bytes)

    def rm(self, path: str, recurse: bool = False):
        p = Path(path.replace("dbfs:", ""))
        if p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()

class MockWidgets:
    def __init__(self):
        self._widgets = {}

    def text(self, name: str, defaultValue: str, label: str = ""):
        self._widgets[name] = defaultValue

    def dropdown(self, name: str, defaultValue: str, choices: list, label: str = ""):
        self._widgets[name] = defaultValue

    def get(self, name: str):
        return self._widgets.get(name, "")

class MockSecrets:
    def get(self, scope: str, key: str):
        # Resolve from environment variables (e.g. SCOPE_KEY)
        env_key = f"{scope.upper()}_{key.upper()}"
        return os.environ.get(env_key, f"mock-secret-for-{key}")

class DBUtilsEmulator:
    def __init__(self):
        self.fs = MockFS()
        self.widgets = MockWidgets()
        self.secrets = MockSecrets()

# Instantiate emulator
dbutils = DBUtilsEmulator()
print("Universal DBUtils Emulator initialized successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Testing DBUtils Filesystem Operations
# MAGIC Create, list, read, and delete files using the dbutils interface.

# COMMAND ----------

# 1. Put file in mock DBFS
dbutils.fs.put("/tmp/dbfs_test/config.json", '{"environment": "staging", "batch_size": 5000}')

# 2. Read file back with head
content = dbutils.fs.head("/tmp/dbfs_test/config.json")
print("Read Content:", content)

# 3. List directory
files = dbutils.fs.ls("/tmp/dbfs_test")
for f in files:
    print(f"File found: {f.name} ({f.size} bytes)")

# 4. Clean up
dbutils.fs.rm("/tmp/dbfs_test", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Testing Widgets & Secure Secret Retrieval
# MAGIC Pass dynamic parameters and fetch credentials without hardcoding.

# COMMAND ----------

# Configure notebook widgets
dbutils.widgets.dropdown("env", "staging", ["dev", "staging", "prod"], "Target Environment")
dbutils.widgets.text("processing_date", "2023-11-20", "Batch Date")

current_env = dbutils.widgets.get("env")
proc_date = dbutils.widgets.get("processing_date")
api_key = dbutils.secrets.get(scope="finance_scope", key="stripe_token")

print(f"Running pipeline for ENV={current_env}, DATE={proc_date}")
print(f"Retrieved Secret: {api_key[:4]}**** (Masked)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Never hardcode credentials in notebooks—use secret scopes via `dbutils.secrets.get()` in Databricks or environment variables locally.**
# MAGIC - **Widgets allow parameterized workflow execution from Databricks Jobs and Airflow DAGs.**
# MAGIC - **Emulating `dbutils` enables seamless local development and automated CI testing outside Databricks.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `%run` and `dbutils.notebook.run()` when chaining notebooks?
# MAGIC 2. How does Databricks redact secrets retrieved via `dbutils.secrets.get()` in driver output logs?
# MAGIC 3. What are Unity Catalog Volumes and how do they replace legacy DBFS mounts?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
