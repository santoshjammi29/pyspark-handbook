"""
Module 04 / 006: Spark 4.0 SQL Pipe Syntax & Parameterized SQL Safety

Scenario & Scale:
    An enterprise BI & feature-engineering service runs 1,500+ dynamic SQL
    analytics templates over 20 TB curated tables. Legacy queries suffer from
    deeply nested 6-level subqueries and Python f-string interpolation bugs that
    expose SQL injection risks and thrash Catalyst's plan cache.

Goal:
    Author readable, left-to-right Spark 4.0 SQL Pipe pipelines (`|> WHERE
    |> EXTEND |> AGGREGATE |> ORDER BY`) combined with parameterized
    `spark.sql(..., args=...)` and `IDENTIFIER(:table)` for zero-injection,
    plan-cached execution.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def build_spark() -> SparkSession:
    """Build an ANSI-enabled Spark 4.0+ session."""
    return (
        SparkSession.builder.appName("006_SQL_Pipe_And_Parameterized_SQL")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# (2) Anti-Pattern: Python f-string SQL injection + nested subquery spaghetti
# ---------------------------------------------------------------------------
def anti_pattern_fstring_nested_sql(
    spark: SparkSession, table_name: str, region: str, min_amount: float
) -> DataFrame:
    # ❌ Bad Practice: Interpolating user parameters via f-strings inside deeply
    # nested SQL subqueries.
    # Otherwise: A malicious `region` value (`"EMEA' OR '1'='1"`) triggers SQL injection,
    # quotes/decimals break SQL parsing, and every unique literal forces Catalyst to
    # re-parse and re-compile an independent logical plan from scratch.
    unsafe_sql = f"""
        SELECT region, SUM(net_amount) AS total_net
        FROM (
            SELECT region, (gross_amount - discount) AS net_amount
            FROM {table_name}
            WHERE region = '{region}' AND gross_amount >= {min_amount}
        ) t
        GROUP BY region
        ORDER BY total_net DESC
    """
    return spark.sql(unsafe_sql)


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: SQL Pipe Syntax + Parameterized `args=`
# ---------------------------------------------------------------------------
def good_practice_pipe_and_params(
    spark: SparkSession, table_name: str, region: str, min_amount: float
) -> DataFrame:
    # ✅ Good Practice: Compose linear Spark 4.0 SQL Pipe operators (`FROM tbl |> ...`)
    # and bind values safely with `:param` + `IDENTIFIER(:tbl)` via `args={...}`.
    pipe_sql = """
        FROM IDENTIFIER(:target_table)
        |> WHERE region = :region_filter AND gross_amount >= :min_gross
        |> EXTEND (gross_amount - discount) AS net_amount
        |> AGGREGATE COUNT(*) AS order_cnt, SUM(net_amount) AS total_net
           GROUP BY region
        |> ORDER BY total_net DESC
        |> LIMIT :max_rows
    """
    return spark.sql(
        pipe_sql,
        args={
            "target_table": table_name,
            "region_filter": region,
            "min_gross": min_amount,
            "max_rows": 50,
        },
    )


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - In `explain("formatted")`, Spark 4.0 SQL Pipe syntax compiles down to the exact same
#   optimized Catalyst physical plan (`Sort` -> `HashAggregate` -> `Exchange` -> `Filter`
#   -> `Scan`) as standard SQL—pipe operators introduce zero runtime overhead.
# - Parameterized values from `args={...}` are bound as typed `Literal` nodes inside
#   the unresolved logical plan before optimization, enabling predicate pushdown into
#   Parquet/Delta `PushedFilters` (`region = 'EMEA'`, `gross_amount >= 100.0`).
# - In Spark UI -> SQL tab, the query description preserves clean parameterized SQL
#   without leaking raw concatenated credentials or breaking query plan fingerprints.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `EXTEND` vs `SET` vs `SELECT` in Pipe Syntax: `|> EXTEND expr AS col` appends a new
#    column while retaining all upstream columns; `|> SET col = expr` overwrites an
#    existing column in place; `|> SELECT ...` drops unlisted columns.
# 2. Never Quote `:param` Markers: Writing `WHERE region = ':region_filter'` turns the
#    placeholder into a literal 14-character string instead of a bind parameter; always
#    write `WHERE region = :region_filter`.
# 3. `IDENTIFIER(:name)` for Dynamic Objects: `:param` binds scalar values only; use
#    `IDENTIFIER(:target_table)` (or `IDENTIFIER(:col_name)`) to safely parameterize
#    catalog/schema/table or column identifiers without f-strings.
# 4. ANSI Cast Safety in `args=`: Python floats/ints passed in `args` infer exact SQL
#    types; cast explicitly (e.g., `CAST(:min_gross AS DECIMAL(12,2))`) when matching
#    strict DECIMAL columns under ANSI mode.


if __name__ == "__main__":
    spark = build_spark()
    try:
        orders = [
            ("EMEA", 150.0, 10.0),
            ("EMEA", 250.0, 25.0),
            ("APAC", 300.0, 0.0),
        ]
        df_orders = spark.createDataFrame(orders, "region STRING, gross_amount DOUBLE, discount DOUBLE")
        df_orders.createOrReplaceTempView("orders_006")
        good_practice_pipe_and_params(spark, "orders_006", "EMEA", 100.0).show(truncate=False)
    finally:
        spark.stop()
