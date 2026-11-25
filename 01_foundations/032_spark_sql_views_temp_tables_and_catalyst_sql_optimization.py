"""Scenario & Scale: Hybrid Python/SQL analytics team (50M HR & payroll records/day).

Business Context:
Expose sanitized DataFrames via session-scoped temporary views, execute parameterized
SQL CTE queries safely without SQL injection, and verify Catalyst plan equivalence.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "032_Spark_SQL_Views_Parameterized") -> SparkSession:
    """Create or reuse a portable SparkSession with ANSI mode enabled."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 1. Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice:
#   spark.sql(f"SELECT * FROM v_employees WHERE dept = '{user_input}' AND salary >= {min_sal}")
# Otherwise:
# - String-interpolated SQL queries allow SQL injection (`' OR '1'='1`), break on
#   embedded quotes, and pollute Catalyst's query plan cache with unique literal strings.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: `createOrReplaceTempView` + parameterized `spark.sql(..., args={...})`.
EMPLOYEE_SCHEMA = T.StructType(
    [
        T.StructField("emp_id", T.StringType(), nullable=False),
        T.StructField("name", T.StringType(), nullable=False),
        T.StructField("dept", T.StringType(), nullable=False),
        T.StructField("salary", T.DoubleType(), nullable=False),
        T.StructField("hire_date", T.StringType(), nullable=False),
    ]
)

DEPT_BENCHMARK_SQL = """
WITH dept_stats AS (
    SELECT
        dept,
        AVG(salary) AS avg_dept_salary
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


def filter_employees_parameterized(
    spark: SparkSession,
    min_salary: float,
    target_dept: str,
) -> DataFrame:
    """Execute a parameterized Spark SQL query safely against a registered temp view."""
    return spark.sql(
        "SELECT * FROM v_employees WHERE salary >= :min_sal AND dept = :dept",
        args={"min_sal": min_salary, "dept": target_dept},
    )


def main() -> None:
    spark = build_spark()

    employees = [
        ("E-1", "Alice", "Engineering", 130000.0, "2020-03-15"),
        ("E-2", "Bob", "Engineering", 145000.0, "2019-06-01"),
        ("E-3", "Charlie", "Marketing", 95000.0, "2021-01-10"),
        ("E-4", "Diana", "Marketing", 105000.0, "2018-11-20"),
        ("E-5", "Evan", "Finance", 115000.0, "2022-04-05"),
    ]
    df_emp = spark.createDataFrame(employees, schema=EMPLOYEE_SCHEMA)
    df_emp.createOrReplaceTempView("v_employees")

    df_cte = spark.sql(DEPT_BENCHMARK_SQL)
    df_sql = filter_employees_parameterized(spark, min_salary=100000.0, target_dept="Engineering")
    df_api = df_emp.filter((F.col("salary") >= 100000.0) & (F.col("dept") == "Engineering"))

    assert df_cte.count() == 5
    assert df_sql.collect() == df_api.collect()


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - Comparing `df_sql.explain("formatted")` and `df_api.explain("formatted")` yields
#   identical `LocalTableScan` + `Filter` physical operators—SQL and DataFrame API
#   compile through the exact same Catalyst analyzer, optimizer, and Tungsten engine.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `createOrReplaceTempView` is scoped to a single `SparkSession` (zero disk storage);
#    `createGlobalTempView` registers under `global_temp.<view>` across sessions.
# 2. Parameterized `spark.sql(..., args={...})` (Spark 3.4+) binds parameters safely
#    inside Catalyst without string concatenation or SQL injection risk.
# 3. Temp views lazily re-evaluate their upstream DataFrame on every query unless
#    explicitly cached (`spark.catalog.cacheTable("v_employees")`).

if __name__ == "__main__":
    main()
