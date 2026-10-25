# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 094: Databricks Asset Bundles (DABs) for Multi-Environment CI/CD Automation
# MAGIC
# MAGIC **Module:** Module 09: Databricks Platform, Unity Catalog, Security & FinOps
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC An enterprise development team deploys 50 Databricks workflows and pipelines across development, staging, and production workspaces. Manual UI deployments cause configuration drift, broken paths, and deployment bugs.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master Databricks Asset Bundles (DABs): configure `databricks.yml`, define multi-environment deployment targets (dev, staging, prod), configure parameterized workflows, and automate CI/CD validation (`bundle validate`, `bundle deploy`, `bundle run`).
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Databricks Asset Bundles (DABs) provide Infrastructure-as-Code (IaC) for Databricks projects. A YAML bundle definition (`databricks.yml`) manages notebooks, pipelines, jobs, and permissions, allowing automated deployments via GitHub Actions or GitLab CI.

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
        .appName("UseCase_094_Databricks_Asset_Bundles_DABs_for_Multi_Environment_CICD_Automation")
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
# MAGIC ### Step 1: Designing databricks.yml Bundle Configuration
# MAGIC Create production-ready YAML specification for multi-environment deployment.

# COMMAND ----------

bundle_yaml_content = """
bundle:
  name: enterprise_spark_masterclass

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-dev.azuredatabricks.net
      root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}

  staging:
    mode: production
    workspace:
      host: https://adb-staging.azuredatabricks.net
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}

  prod:
    mode: production
    workspace:
      host: https://adb-prod.azuredatabricks.net
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
"""

print("databricks.yml configuration structure:")
print(bundle_yaml_content)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Defining Workflow Job Resource in Bundle
# MAGIC Declare jobs, schedules, and cluster settings in resources/jobs.yml.

# COMMAND ----------

job_resource_yaml = """
resources:
  jobs:
    nightly_etl_pipeline:
      name: "[${bundle.target}] Nightly Financial ETL"
      tasks:
        - task_key: run_ingestion
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ../notebooks/01_ingest.py
        - task_key: run_transformation
          depends_on:
            - task_key: run_ingestion
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ../notebooks/02_transform.py
      job_clusters:
        - job_cluster_key: etl_cluster
          new_cluster:
            spark_version: 14.3.x-scala2.12
            node_type_id: Standard_D4ds_v5
            num_workers: 2
"""

print("resources/jobs.yml definition structure:")
print(job_resource_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: DABs CLI Command Lifecycle Cheatsheet
# MAGIC Review developer CLI workflows for DABs.

# COMMAND ----------

commands = [
    ("databricks bundle init", "Initializes a new bundle template project."),
    ("databricks bundle validate", "Validates YAML syntax, resource references, and paths."),
    ("databricks bundle deploy -t dev", "Deploys resources to target environment (dev sandbox)."),
    ("databricks bundle run -t dev nightly_etl_pipeline", "Triggers immediate run of the deployed workflow in dev.")
]

for cmd, desc in commands:
    print(f" $ {cmd}\n   -> {desc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Databricks Asset Bundles (DABs) implement Infrastructure-as-Code (IaC) for Databricks projects.**
# MAGIC - **Target environments (dev, staging, prod) enable seamless promotion via GitHub Actions CI/CD.**
# MAGIC - **DABs eliminate manual workspace UI changes, preventing configuration drift.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is a Databricks Asset Bundle (DAB) and what problem does it solve in enterprise MLOps/DataOps?
# MAGIC 2. What is the difference between `mode: development` and `mode: production` in `databricks.yml`?
# MAGIC 3. How do you parameterize cluster configurations across dev and prod targets in DABs?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
