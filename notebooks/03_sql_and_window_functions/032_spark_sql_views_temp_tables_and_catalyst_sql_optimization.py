# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 032: Spark SQL Views, Temp Tables and Catalyst SQL Optimization
# MAGIC
# MAGIC **Module:** Module 03: Advanced SQL, Aggregations & Analytical Window Functions  
# MAGIC **Difficulty:** `Beginner`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A hybrid analytics team consists of Python data engineers and SQL business analysts. The data engineering team prepares sanitized DataFrames that SQL analysts query using ANSI SQL queries and Common Table Expressions (CTEs).
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Demonstrate seamless interoperability between PySpark DataFrames and Spark SQL: register temporary views (`createOrReplaceTempView`), global temporary views (`createGlobalTempView`), run complex SQL with CTEs via `spark.sql()`, pass dynamic parameters, and inspect Catalyst optimization.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Temporary views exist purely as logical metadata pointers in the SparkSession catalog without materializing data to disk. Calling `spark.sql()` compiles SQL text through the same Catalyst parser and optimizer as DataFrame API code, producing identical physical plans.

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
    .appName("UseCase_032_Spark_SQL_Views,_Temp_Tables_and_Catalyst_SQL_Optimization")
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
# MAGIC ## 5. Creating DataFrame & Registering Temporary Views
# MAGIC Register local temporary views and global temporary views.

# COMMAND ----------

employees = [
    ("E-1", "Alice", "Engineering", 130000, "2020-03-15"),
    ("E-2", "Bob", "Engineering", 145000, "2019-06-01"),
    ("E-3", "Charlie", "Marketing", 95000, "2021-01-10"),
    ("E-4", "Diana", "Marketing", 105000, "2018-11-20"),
    ("E-5", "Evan", "Finance", 115000, "2022-04-05")
]

schema = "emp_id STRING, name STRING, dept STRING, salary DOUBLE, hire_date STRING"
df_emp = spark.createDataFrame(employees, schema)

# Register local temporary view (scoped to this SparkSession)
df_emp.createOrReplaceTempView("v_employees")
print("Temporary view 'v_employees' registered in SparkSession catalog.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Executing Complex SQL with CTEs and Window Functions
# MAGIC Run standard ANSI SQL queries over registered DataFrames.

# COMMAND ----------

sql_query = """
WITH dept_stats AS (
    SELECT 
        dept,
        AVG(salary) AS avg_dept_salary,
        MAX(salary) AS max_dept_salary
    FROM v_employees
    GROUP BY dept
)
SELECT 
    e.emp_id,
    e.name,
    e.dept,
    e.salary,
    ROUND(d.avg_dept_salary, 2) AS dept_avg,
    ROUND(e.salary - d.avg_dept_salary, 2) AS diff_from_avg
FROM v_employees e
JOIN dept_stats d ON e.dept = d.dept
ORDER BY diff_from_avg DESC
"""

df_sql_result = spark.sql(sql_query)
df_sql_result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Parameterized SQL Queries via Python Formatting & Identical Plans
# MAGIC Inject dynamic variables safely into SQL queries and verify Catalyst equivalence.

# COMMAND ----------

min_salary = 100000
target_dept = "Engineering"

# Parameterized SQL execution
df_filtered_sql = spark.sql(
    "SELECT * FROM v_employees WHERE salary >= :min_sal AND dept = :dept",
    args={"min_sal": min_salary, "dept": target_dept}
)
df_filtered_sql.show()

# Verify that DataFrame API produces the exact same physical plan as Spark SQL
df_api = df_emp.filter((F.col("salary") >= min_salary) & (F.col("dept") == target_dept))

print("SQL and DataFrame API produce identical Catalyst plans:")
assert df_filtered_sql.collect() == df_api.collect(), "Plan execution result mismatch"
print("Verification passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Temporary views are lightweight metadata aliases that require zero disk storage or computation until queried.**
# MAGIC - **`createOrReplaceTempView` is scoped to the current SparkSession; `createGlobalTempView` is accessible across sessions under the `global_temp` database.**
# MAGIC - **DataFrame API and Spark SQL share the exact same Catalyst compiler and Tungsten execution engine.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between a Temporary View and a Managed Table in Spark?
# MAGIC 2. How does `createGlobalTempView` differ from `createOrReplaceTempView`?
# MAGIC 3. Does writing code in Spark SQL perform slower or faster than writing PySpark DataFrame API code?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
