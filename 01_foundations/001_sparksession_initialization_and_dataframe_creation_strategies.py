"""Scenario & Scale: Digital commerce customer profile & order ingestion (10M+ daily rows, <5m SLA).

Business Context:
Ingest batch customer extracts, order tuples, and small Pandas lookup payloads into
strongly-typed PySpark 4.0+ DataFrames without triggering driver-side schema inference
or relying on non-portable SparkContext/RDD APIs.
"""

from typing import List, Tuple

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "001_SparkSession_DataFrame_Creation") -> SparkSession:
    """Create or reuse a portable SparkSession compatible with Spark Connect."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 1. Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice:
# - Calling `spark.createDataFrame(raw_rows)` without a schema or reading files with
#   `inferSchema=True` and calling `.master("local[*]")` / `spark.sparkContext` APIs.
# - Calling unbounded `df.toPandas()` to inspect or convert distributed tables.
# Otherwise:
# - Schema inference triggers an extra full-data scan, misinfers nullable/numeric
#   types (e.g., ZIP codes as INT), `.sparkContext` fails on Spark Connect, and
#   unbounded `.toPandas()` triggers Driver OutOfMemoryError on multi-GB datasets.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Explicit StructType/DDL schemas, portable SparkSession, Arrow transfer.
CUSTOMER_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.StringType(), nullable=False),
        T.StructField("full_name", T.StringType(), nullable=False),
        T.StructField("signup_date", T.StringType(), nullable=True),
        T.StructField("loyalty_tier", T.StringType(), nullable=False),
        T.StructField("account_balance", T.DoubleType(), nullable=False),
    ]
)

ORDER_DDL_SCHEMA = (
    "order_id STRING, customer_id STRING, order_amount DOUBLE, "
    "order_status STRING, item_count INT"
)


def create_customers_df(
    spark: SparkSession,
    rows: List[Tuple[str, str, str, str, float]],
) -> DataFrame:
    """Build a DataFrame with an explicit StructType schema (zero inference pass)."""
    return spark.createDataFrame(data=rows, schema=CUSTOMER_SCHEMA)


def create_empty_pipeline_df(spark: SparkSession) -> DataFrame:
    """Initialize an empty DataFrame with an explicit schema for union/scaffold steps."""
    return spark.createDataFrame(data=[], schema=CUSTOMER_SCHEMA)


def main() -> None:
    spark = build_spark()

    raw_customers = [
        ("CUST-1001", "Alice Johnson", "2023-01-15", "Gold", 4520.50),
        ("CUST-1002", "Bob Smith", "2023-02-20", "Silver", 1250.00),
        ("CUST-1003", "Charlie Brown", "2023-03-10", "Bronze", 340.25),
        ("CUST-1004", "Diana Prince", "2023-04-05", "Platinum", 15890.00),
        ("CUST-1005", "Evan Wright", "2023-05-18", "Silver", 820.75),
    ]
    df_customers = create_customers_df(spark, raw_customers)

    raw_orders = [
        ("ORD-901", "CUST-1001", 145.20, "COMPLETED", 3),
        ("ORD-902", "CUST-1002", 49.99, "PENDING", 1),
        ("ORD-903", "CUST-1001", 310.00, "COMPLETED", 5),
        ("ORD-904", "CUST-1004", 1250.40, "SHIPPED", 8),
    ]
    df_orders = spark.createDataFrame(raw_orders, schema=ORDER_DDL_SCHEMA)

    # Small Pandas lookup -> Spark DataFrame via Apache Arrow zero-copy buffers
    pdf = pd.DataFrame(
        {
            "product_id": [f"PROD-{i}" for i in range(101, 106)],
            "category": ["Electronics", "Home", "Electronics", "Books", "Fashion"],
            "unit_price": [299.99, 45.50, 899.00, 19.95, 75.00],
            "in_stock": [True, True, False, True, True],
        }
    )
    df_products = spark.createDataFrame(pdf)
    df_empty = create_empty_pipeline_df(spark)

    assert df_customers.count() == 5
    assert df_empty.count() == 0
    assert df_empty.schema == df_customers.schema
    assert df_orders.filter(F.col("order_status") == "COMPLETED").count() == 2
    assert df_products.count() == 5


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_customers.explain("formatted")` shows a `LocalTableScan` (or Arrow-backed scan
#   in Spark Connect) with exact column nullability and types baked into the plan.
# - In Spark UI (SQL tab), verify no unexpected PythonUDF serialization or extra
#   schema-inference jobs appear before the main write stage.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `spark.sparkContext` is unsupported in Spark Connect (Spark 4.0 default client);
#    always configure logging/properties via `spark.conf` or cluster configs instead.
# 2. Omitting `.master(...)` lets `spark-submit` or Spark Connect bind the target cluster
#    automatically instead of silently forcing single-node local execution.
# 3. `spark.sql.execution.arrow.pyspark.enabled=true` uses columnar Arrow batches for
#    small Pandas conversions; never call unbounded `.toPandas()`—always `.limit(N)` first.

if __name__ == "__main__":
    main()
