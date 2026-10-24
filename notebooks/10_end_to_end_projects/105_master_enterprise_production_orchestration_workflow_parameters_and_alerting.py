# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 105: Master Enterprise Production Orchestration, Workflow Parameters and Alerting
# MAGIC
# MAGIC **Module:** Module 10: End-to-End Enterprise Projects & Production Workflows
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A mission-critical enterprise financial pipeline executes 15 dependent tasks every night. The workflow must accept dynamic run parameters, retry failed tasks with exponential backoff, send Slack/Teams webhook alerts on failure, and write audit run logs.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Design an enterprise orchestration harness in PySpark: implement parameterized workflow execution, simulate task dependency graphs (DAGs), capture task execution durations, catch runtime exceptions, and dispatch webhook failure alerts.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Databricks Workflows orchestrates complex multi-task DAGs. Using parameters (`dbutils.widgets`), tasks can be parameterized across environments. Implementing centralized audit logging inside tasks ensures compliance and accelerates root cause analysis during pipeline failures.

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
        .appName("UseCase_105_Master_Enterprise_Production_Orchestration_Workflow_Parameters_and_Alerting")
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
# MAGIC ### Step 1: Designing Parameterized Task Execution Harness
# MAGIC Create an orchestration class that executes pipeline tasks with timing and error handling.

# COMMAND ----------

import time
import traceback
from datetime import datetime

class EnterpriseTaskRunner:
    def __init__(self, run_env: str, batch_date: str):
        self.run_env = run_env
        self.batch_date = batch_date
        self.audit_log = []

    def execute_task(self, task_name: str, task_func):
        print(f"=== [START] Task: {task_name} | Env: {self.run_env} | Date: {self.batch_date} ===")
        start_time = time.time()
        status = "SUCCESS"
        error_msg = None

        try:
            task_func()
        except Exception as e:
            status = "FAILED"
            error_msg = str(e)
            print(f"!!! [ERROR] Task {task_name} failed: {error_msg}")
            self.dispatch_alert(task_name, error_msg)
        finally:
            duration = round(time.time() - start_time, 3)
            self.audit_log.append({
                "task_name": task_name,
                "status": status,
                "duration_seconds": duration,
                "error_message": error_msg,
                "executed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            })
            print(f"=== [END] Task: {task_name} | Status: {status} | Duration: {duration}s ===\n")

    def dispatch_alert(self, task_name: str, error_msg: str):
        # Simulate Slack / PagerDuty webhook dispatch
        print(f"--> [WEBHOOK DISPATCH] Paging On-Call: Task '{task_name}' failed in {self.run_env}! Error: {error_msg}")

print("EnterpriseTaskRunner class initialized.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Executing Multi-Task Orchestration Sequence
# MAGIC Run tasks representing Ingestion, Transformation, and Quality Checks.

# COMMAND ----------

# Define individual pipeline task functions
def task_ingest():
    time.sleep(0.05)
    print("Ingestion complete: 15,000 records processed.")

def task_transform():
    time.sleep(0.08)
    print("Transformation complete: Cleaned and conformed Silver records.")

def task_quality_check():
    time.sleep(0.02)
    # Simulate data quality failure under bad conditions
    print("Quality check passed: 100% compliance.")

runner = EnterpriseTaskRunner(run_env="production", batch_date="2023-11-20")

runner.execute_task("Task_1_Bronze_Ingestion", task_ingest)
runner.execute_task("Task_2_Silver_Transformation", task_transform)
runner.execute_task("Task_3_Data_Quality_Validation", task_quality_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Publishing Master Workflow Audit Log to Delta Table
# MAGIC Persist run execution telemetry for FinOps and SLA monitoring.

# COMMAND ----------

df_audit = spark.createDataFrame(runner.audit_log)
print("Master Workflow Execution Audit Log:")
df_audit.show(truncate=False)

# In production, save to system audit table:
# df_audit.write.format("delta").mode("append").save("/Volumes/enterprise/audit/workflow_runs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Production pipelines require parameterized orchestration across environments (dev, test, prod).**
# MAGIC - **Always capture start time, duration, status, and error stack traces in a centralized audit Delta table.**
# MAGIC - **Automate webhook alerts to Slack or PagerDuty on task failure to meet enterprise SLAs.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How do you pass dynamic parameters and return values between tasks in Databricks Workflows?
# MAGIC 2. What is the difference between task-level retries and job-level retries in orchestration DAGs?
# MAGIC 3. How do you design an audit logging architecture that captures pipeline execution metrics across 500+ jobs?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
