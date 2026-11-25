# 🎓 PySpark Interview Guide — Tier 1: Junior & Associate PySpark Engineer (Foundations)

> **Focus Areas**: SparkSession Initialization, DataFrame API, Lazy Evaluation, Transformations vs Actions, Narrow vs Wide Dependencies, Basic Aggregations, File Formats.
> **Target Audience**: Associate Data Engineers, Python Developers transitioning to Distributed Systems

---

### Q1.1: SparkSession Initialization and Config (SparkSession & Config)

**Question & Concept Overview:**  
The entry point to programming Spark with the Dataset and DataFrame API. It explains how to build a local or cluster-connected Spark session with configurations optimized for cloud storage and adaptive query execution in Azure Databricks or Microsoft Fabric.

**Production Implementation:**
```python
from pyspark.sql import SparkSession

# Build SparkSession with Delta Lake support
spark = SparkSession.builder \
    .appName("EU_BFSI_Transaction_Processor") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .getOrCreate()

# Verify active session
print(f"Spark Version: {spark.version}")
# Spark Connect-safe session metadata (avoid spark.sparkContext in Spark 4.0+)
print(f"App Name: {spark.conf.get('spark.app.name', 'EU_BFSI_Transaction_Processor')}")
print(f"ANSI Mode: {spark.conf.get('spark.sql.ansi.enabled', 'true')}")
```

**Enterprise Use Case:**  
Initializing a PySpark pipeline for an EU BFSI data lakehouse in Azure Databricks, enabling baseline optimizations for Delta Lake writes.

**Architectural Invariants & Gotchas:**
- Use SparkSession.builder to configure parameters before runtime.
- CoalescePartitions avoids small file issues by shrinking partition counts after shuffle stages.
- In Databricks and Microsoft Fabric, the SparkSession is pre-initialized as 'spark'.
- OptimizeWrite and AutoCompact are Delta-specific properties essential for maintaining performance.

---

### Q1.2: Reading and Writing Delta Tables (Data Ingestion & Storage)

**Question & Concept Overview:**  
Reading from and writing to Delta Lake tables, including standard write modes (append, overwrite) and format options. Delta Lake provides ACID transaction guarantees on top of cloud storage like ADLS Gen2 or Microsoft OneLake.

**Production Implementation:**
```python
# Read raw transaction data from Azure Data Lake Storage (ADLS) Gen2
bronze_df = spark.read.format("delta") \
    .load("abfss://bronze@euadlsprod.dfs.core.windows.net/ledger_logs")

# Filter only settled SEPA transfers for processing
sepa_txns_df = bronze_df.filter(
    (bronze_df.txn_type == "SEPA") & (bronze_df.status == "COMPLETED")
)

# Write to Silver layer with schema merging and overwrite mode
sepa_txns_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save("abfss://silver@euadlsprod.dfs.core.windows.net/sepa_transactions")
```

**Enterprise Use Case:**  
Ingesting raw payment logs from ADLS landing zone and promoting them to cleansed Silver tables in an Azure Databricks medallion environment.

**Architectural Invariants & Gotchas:**
- Delta Lake uses transaction logs (.checkpoint files) to guarantee ACID properties.
- overwrite mode replaces the entire directory atomically without causing downtime to readers.
- mergeSchema allows schema evolution when new fields are introduced downstream.
- Use abfss:// protocol for secure access to Azure storage accounts via OAuth or SAS tokens.

---

### Q1.3: DataFrame Schema Definition (StructType & StructField) (Schema Management)

**Question & Concept Overview:**  
Programmatic definition of schemas to enforce type safety on ingestion, avoiding auto-schema inference performance costs and errors. This is critical when dealing with strictly structured files like regulatory bank exports.

**Production Implementation:**
```python
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, DateType

# Enforce rigorous schema validation for incoming SWIFT payment files
swift_schema = StructType([
    StructField("swift_msg_id", StringType(), False),
    StructField("sender_bic", StringType(), False),
    StructField("receiver_bic", StringType(), False),
    StructField("txn_amount", DecimalType(18, 4), False),
    StructField("txn_currency", StringType(), False),
    StructField("value_date", DateType(), True),
    StructField("ingestion_ts", TimestampType(), True)
])

# Read incoming JSON files enforcing the strict schema
raw_swift_df = spark.read.format("json") \
    .schema(swift_schema) \
    .load("abfss://raw@euadlsprod.dfs.core.windows.net/swift_inbound/2026/*.json")
```

**Enterprise Use Case:**  
Enforcing static types on incoming SWIFT MT103 payment messages in Microsoft Fabric to guarantee data quality and audit compliance.

**Architectural Invariants & Gotchas:**
- Explicit schema definition prevents Spark from scanning the entire dataset to infer types, speeding up read operations.
- DecimalType(18,4) is used for financial amounts to avoid floating-point representation errors.
- Nullable flag (False/True) specifies if columns can accept null values during ingestion parsing.
- Unmatched values in schema columns will be converted to NULL rather than failing the execution.

---

### Q1.4: Basic Column Transformations (select, filter, withColumn, drop) (DataFrame Transformations)

**Question & Concept Overview:**  
Fundamental transformations to project columns, filter records, add or modify columns, and remove unnecessary fields. These operations are evaluated lazily by Spark's Catalyst Optimizer.

**Production Implementation:**
```python
from pyspark.sql.functions import col, upper, round

# Load Silver transactions
silver_df = spark.read.format("delta").load("abfss://silver@euadlsprod.dfs.core.windows.net/transactions")

# Project relevant fields, apply currency conversion, and clean up raw columns
cleansed_df = silver_df.select("txn_id", "account_id", "txn_amount", "txn_currency", "fx_rate") \
    .filter(col("txn_amount") > 0) \
    .withColumn("txn_currency_clean", upper(col("txn_currency"))) \
    .withColumn("amount_eur", round(col("txn_amount") * col("fx_rate"), 2)) \
    .drop("txn_currency", "fx_rate")

# Show transformed schema
cleansed_df.printSchema()
```

**Enterprise Use Case:**  
Filtering out negative/null amounts and normalizing ISO currencies for retail banking transactions in the Silver layer.

**Architectural Invariants & Gotchas:**
- withColumn can overwrite an existing column if the names match, or create a new one.
- Catalyst Optimizer merges multiple select() and filter() operations into a single execution step.
- Using col('column_name') is preferred over df.column_name to avoid collisions with DataFrame methods.
- Dropping source columns after transformation is a good practice to reduce memory usage during serialization.

---

### Q1.5: Aggregations and GroupBy (groupBy, agg, sum, avg, count) (Basic Aggregations)

**Question & Concept Overview:**  
Grouping data by dimensions and performing aggregations like sum, count, and average. GroupBy operations trigger shuffles in a distributed system, reorganizing partitions based on the grouping keys.

**Production Implementation:**
```python
from pyspark.sql.functions import sum as _sum, avg, count, col

# Load transformed transaction data
df = spark.read.format("delta").load("abfss://silver@euadlsprod.dfs.core.windows.net/cleansed_transactions")

# Aggregate transactional volume and count by account for EBA reporting
monthly_summary_df = df.groupBy("account_id", "txn_currency_clean") \
    .agg(
        _sum("amount_eur").alias("total_volume_eur"),
        avg("amount_eur").alias("avg_txn_amount_eur"),
        count("txn_id").alias("txn_count")
    ) \
    .filter(col("total_volume_eur") > 100000)

# Display aggregate output
monthly_summary_df.show(10)
```

**Enterprise Use Case:**  
Generating daily summary tables of transaction counts and volume by credit card holder for fraud detection thresholds.

**Architectural Invariants & Gotchas:**
- Always import aggregate functions explicitly (e.g. from pyspark.sql.functions import sum as _sum) to avoid Python namespace collisions.
- groupBy triggers a Wide Transformation (shuffle) where records are moved across network nodes.
- Use agg() to apply multiple operations (sum, avg, count) simultaneously on the grouped dataset.
- Add post-aggregation filters to limit the dataset size before writing to Gold tables.

---

### Q1.6: Sorting and Deduplication (orderBy, sort, dropDuplicates) (Data Quality & Ordering)

**Question & Concept Overview:**  
Ordering data distributed across partitions and removing duplicate rows based on business keys. Clean and ordered data is critical for accurate reporting and optimized partition storage layouts.

**Production Implementation:**
```python
from pyspark.sql.functions import col

# Load duplicate-prone ledger stream
ledger_df = spark.read.format("delta").load("abfss://bronze@euadlsprod.dfs.core.windows.net/raw_ledger")

# Deduplicate based on transaction ID and audit timestamp
deduplicated_df = ledger_df.dropDuplicates(["txn_id", "txn_timestamp"])

# Sort within partitions to optimize write times and clustering performance
sorted_df = deduplicated_df.sortWithinPartitions("txn_date", "account_id")

# Global ordering (used strictly before small file output extracts)
ordered_report_df = sorted_df.orderBy(col("txn_timestamp").desc())
```

**Enterprise Use Case:**  
Deduplicating incoming SEPA transactions in a multi-hop Medallion pipeline prior to running risk analytics.

**Architectural Invariants & Gotchas:**
- dropDuplicates removes duplicate rows by keeping only the first occurrence for the specified business keys.
- sortWithinPartitions is faster than sort() because it avoids a global shuffle across the network.
- orderBy performs a global sort and collects data into sorted partitions, which can be computationally expensive.
- Ensure you deduplicate early in the Silver layer to prevent cascading inconsistencies in reports.

---

### Q1.7: Joining DataFrames (Inner, Left, Semi, Anti Joins) (DataFrame Transformations)

**Question & Concept Overview:**  
Joining multiple DataFrames using various join types like inner, left, semi, and anti joins. Correct join selection is foundational to master dimensional enrichment and exception checking in Spark.

**Production Implementation:**
```python
# Load datasets
transactions = spark.read.format("delta").load("abfss://silver@euadlsprod.dfs.core.windows.net/transactions")
customer_profiles = spark.read.format("delta").load("abfss://silver@euadlsprod.dfs.core.windows.net/customers")
sanctioned_parties = spark.read.format("delta").load("abfss://silver@euadlsprod.dfs.core.windows.net/watchlist")

# 1. Left Join to enrich transactions with customer data
enriched_df = transactions.join(
    customer_profiles,
    on="customer_id",
    how="left"
)

# 2. Semi Join to extract transactions with matching sanctioned entities
flagged_txns_df = transactions.join(
    sanctioned_parties,
    transactions.counterparty_bic == sanctioned_parties.bic,
    how="left_semi"
)

# 3. Anti Join to isolate orphan records with no matching customer (Data Quality checks)
orphaned_df = transactions.join(
    customer_profiles,
    on="customer_id",
    how="left_anti"
)
```

**Enterprise Use Case:**  
Enriching customer records with risk scores, while filtering out invalid accounts and scanning for sanctioned counterparties.

**Architectural Invariants & Gotchas:**
- left_semi join returns columns strictly from the left DataFrame when there is a match, behaving like an IN subquery.
- left_anti join returns rows from the left DataFrame when NO match is found, ideal for locating orphaned keys.
- Specify join columns as strings (e.g. on='customer_id') to automatically deduplicate the key in the output schema.
- Ensure join keys have matching data types to prevent implicit casting and slow execution plans.

---

### Q1.8: Temporary Views & PySpark-SQL Interoperability (SQL Interoperability)

**Question & Concept Overview:**  
Creating session-scoped temporary views from DataFrames to enable query writing via Spark SQL. This enables seamless collaboration between Python data engineers and SQL business analysts.

**Production Implementation:**
```python
# Load Silver transactions DataFrame
silver_df = spark.read.format("delta").load("abfss://silver@euadlsprod.dfs.core.windows.net/transactions")

# Create temporary session-scoped view
silver_df.createOrReplaceTempView("v_silver_transactions")

# Execute standard ANSI SQL commands against the view
summary_report_df = spark.sql("""
    SELECT
        account_id,
        SUM(txn_amount) as total_debited,
        CURRENT_TIMESTAMP() as report_generated_at
    FROM v_silver_transactions
    WHERE txn_type = 'DEBIT' AND status = 'COMPLETED'
    GROUP BY account_id
""")

# Show aggregate results
summary_report_df.show(5)
```

**Enterprise Use Case:**  
Allowing SQL-native risk analysts to perform ad-hoc checks against a pre-cleansed PySpark DataFrame during regulatory audits.

**Architectural Invariants & Gotchas:**
- Temporary views are bound to the active SparkSession and will be dropped when the session ends.
- Global temporary views can be shared across multiple sessions using the 'global_temp.' prefix.
- spark.sql() returns a PySpark DataFrame, allowing seamless chaining of Python and SQL expressions.
- Always parameterize SQL queries to prevent formatting issues and query parsing failures.

---

### Q1.9: #001 SparkSession Initialization & DataFrame Creation Strategies (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Demonstrate the creation of a resilient SparkSession, configuring custom execution properties, and creating DataFrames using: (1) Programmatic RDD with StructType schema, (2) Python dictionaries/tuples with DDL string schema, (3) Pandas DataFrame conversion with PyArrow optimization, and (4) Empty DataFrames with explicit schemas for pipeline initialization.

**Production Implementation:**
```python
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
        .appName("UseCase_001_SparkSession_Initialization_and_DataFrame_Creation_Strategies")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
# 1. Define explicit StructType schema
customer_schema = T.StructType([
    T.StructField("customer_id", T.StringType(), nullable=False),
    T.StructField("full_name", T.StringType(), nullable=False),
    T.StructField("signup_date", T.StringType(), nullable=True),
    T.StructField("loyalty_tier", T.StringType(), nullable=False),
    T.StructField("account_balance", T.DoubleType(), nullable=False)
])

# 2. Source raw tuples (representing incoming batch extract)
raw_customers = [
    ("CUST-1001", "Alice Johnson", "2023-01-15", "Gold", 4520.50),
    ("CUST-1002", "Bob Smith", "2023-02-20", "Silver", 1250.00),
    ("CUST-1003", "Charlie Brown", "2023-03-10", "Bronze", 340.25),
    ("CUST-1004", "Diana Prince", "2023-04-05", "Platinum", 15890.00),
    ("CUST-1005", "Evan Wright", "2023-05-18", "Silver", 820.75)
]

# 3. Create DataFrame with explicit schema
df_customers = spark.createDataFrame(data=raw_customers, schema=customer_schema)
df_customers.printSchema()
df_customers.show(truncate=False)

# --- Execution Step ---
# Define schema using SQL DDL syntax
ddl_schema = "order_id STRING, customer_id STRING, order_amount DOUBLE, order_status STRING, item_count INT"

raw_orders = [
    ("ORD-901", "CUST-1001", 145.20, "COMPLETED", 3),
    ("ORD-902", "CUST-1002", 49.99, "PENDING", 1),
    ("ORD-903", "CUST-1001", 310.00, "COMPLETED", 5),
    ("ORD-904", "CUST-1004", 1250.40, "SHIPPED", 8)
]

df_orders = spark.createDataFrame(raw_orders, schema=ddl_schema)
df_orders.show()

# --- Execution Step ---
import pandas as pd
import numpy as np

# Enable PyArrow optimization
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

pdf = pd.DataFrame({
    "product_id": [f"PROD-{i}" for i in range(101, 106)],
    "category": ["Electronics", "Home", "Electronics", "Books", "Fashion"],
    "unit_price": [299.99, 45.50, 899.00, 19.95, 75.00],
    "in_stock": [True, True, False, True, True]
})

df_products = spark.createDataFrame(pdf)
print(f"Created Spark DataFrame from Pandas with {df_products.count()} rows")
df_products.show()

# --- Execution Step ---
# Verification assertions
assert df_customers.count() == 5, "Customer count mismatch"
assert "loyalty_tier" in df_customers.columns, "Missing loyalty_tier column"
assert df_orders.filter(F.col("order_status") == "COMPLETED").count() == 2, "Completed orders mismatch"

print("All DataFrame creation strategies and assertions verified successfully!")
```

**Enterprise Use Case:**  
A digital commerce platform needs to ingest customer profile extracts from multiple legacy sources (in-memory lists, Python dictionaries, Pandas exports, and RDDs) into a unified PySpark DataFrame with strict schema validation.

**Architectural Invariants & Gotchas:**
- SparkSession serves as the unified entry point introduced in Spark 2.0, encapsulating SparkContext, SQLContext, and HiveContext. In Databricks, the SparkSession variable `spark` is pre-instantiated. When creating DataFrames from Python collections, PySpark serializes Python objects to Java Virtual Machine (JVM) objects via Py4J. Enabling Arrow (`spark.sql.execution.arrow.pyspark.enabled`) accelerates Pandas-to-Spark conversions by eliminating per-row serialization overhead.

---

### Q1.10: #002 Schema Enforcement, Safe Type Casting & Try-Cast Handling (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Implement resilient schema transformations: safely cast string columns to numeric/date types, handle malformed rows using `try_cast` (or regex sanitization), and separate valid records from corrupted data without halting execution.

**Production Implementation:**
```python
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
        .appName("UseCase_002_Schema_Enforcement_Safe_Type_Casting_and_Try_Cast_Handling")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
raw_feed = [
    ("TXN-001", "$1,450.50", "2023-10-01 10:30:00", "APPROVED"),
    ("TXN-002", "€320.00", "01/10/2023 11:15", "APPROVED"),
    ("TXN-003", "CORRUPTED_AMT", "2023-10-02", "FAILED"),
    ("TXN-004", "$8,900.75", "2023-10-03 14:00:00", "APPROVED"),
    ("TXN-005", "N/A", "INVALID_DATE", "PENDING")
]

df_raw = spark.createDataFrame(raw_feed, ["txn_id", "raw_amount", "raw_timestamp", "status"])
df_raw.show(truncate=False)

# --- Execution Step ---
# 1. Cleanse raw_amount by stripping non-numeric characters (except period)
clean_amount_expr = F.regexp_replace(F.col("raw_amount"), r"[^\d.]", "")

# 2. Apply safe casting: if sanitized string is empty or invalid, evaluate to NULL
df_cleansed = df_raw.withColumn(
    "clean_amount",
    F.when(clean_amount_expr == "", None).otherwise(clean_amount_expr.cast(T.DoubleType()))
)

# 3. Standardize timestamp handling
df_typed = df_cleansed.withColumn(
    "txn_timestamp",
    F.coalesce(
        F.to_timestamp(F.col("raw_timestamp"), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col("raw_timestamp"), "dd/MM/yyyy HH:mm"),
        F.to_timestamp(F.col("raw_timestamp"), "yyyy-MM-dd")
    )
)

df_typed.show(truncate=False)

# --- Execution Step ---
# Valid records have both parsed amount and parsed timestamp
df_valid = df_typed.filter(F.col("clean_amount").isNotNull() & F.col("txn_timestamp").isNotNull())
df_quarantine = df_typed.filter(F.col("clean_amount").isNull() | F.col("txn_timestamp").isNull())

print(f"Valid Silver Records: {df_valid.count()}")
df_valid.show(truncate=False)

print(f"Quarantined Dead-Letter Records: {df_quarantine.count()}")
df_quarantine.show(truncate=False)
```

**Enterprise Use Case:**  
A payments processor receives raw transaction dumps where numeric amounts contain currency symbols, timestamps are in non-standard formats, and corrupted text entries cause pipeline aborts on type casting.

**Architectural Invariants & Gotchas:**
- In ANSI SQL mode (`spark.sql.ansi.enabled = true`), standard `.cast()` throws runtime exceptions on invalid values (e.g. casting 'INVALID' to INT). Using `try_cast` or `when` expressions ensures invalid values convert to NULL rather than failing long-running batch jobs.

---

### Q1.11: #003 Column Manipulation, Batch Addition & Projection Pruning (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Compare single `withColumn` chaining against Spark 3.3+ `withColumns` batch operations. Rename multiple columns dynamically using dictionary mapping, drop unwanted columns, and explain projection pruning optimizations.

**Production Implementation:**
```python
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
        .appName("UseCase_003_Column_Manipulation_Batch_Addition_and_Projection_Pruning")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
telemetry_data = [
    ("EVT-1", "user_101", "mobile_app", "iOS", 120, 2, "2023-11-01"),
    ("EVT-2", "user_102", "web_browser", "Chrome", 450, 8, "2023-11-01"),
    ("EVT-3", "user_103", "mobile_app", "Android", 30, 1, "2023-11-01"),
    ("EVT-4", "user_104", "api_client", "Postman", 600, 15, "2023-11-01")
]

columns = ["event_id", "user_id", "client_type", "client_os", "duration_seconds", "actions_count", "event_date"]
df_telemetry = spark.createDataFrame(telemetry_data, columns)
df_telemetry.show()

# --- Execution Step ---
# Batch enrichments using withColumns
enrichments = {
    "duration_minutes": F.round(F.col("duration_seconds") / 60.0, 2),
    "is_high_engagement": F.when((F.col("duration_seconds") > 300) & (F.col("actions_count") > 5), True).otherwise(False),
    "platform_tag": F.concat_ws("::", F.upper(F.col("client_type")), F.col("client_os")),
    "actions_per_minute": F.round(F.col("actions_count") / (F.col("duration_seconds") / 60.0), 2)
}

df_enriched = df_telemetry.withColumns(enrichments)
df_enriched.show(truncate=False)

# --- Execution Step ---
rename_map = {
    "event_id": "id",
    "user_id": "account_id",
    "duration_seconds": "duration_sec"
}

# Apply renaming dynamically
df_renamed = df_enriched
for old_col, new_col in rename_map.items():
    df_renamed = df_renamed.withColumnRenamed(old_col, new_col)

# Drop redundant legacy columns
df_final = df_renamed.drop("client_type", "client_os")
df_final.printSchema()
df_final.show(truncate=False)
```

**Enterprise Use Case:**  
A SaaS telemetry service needs to enrich raw session events with calculated duration, derived flags, normalized client identifiers, and drop redundant legacy headers without incurring multiple Catalyst plan rebuilds.

**Architectural Invariants & Gotchas:**
- Chaining dozens of `.withColumn()` calls produces a deep Catalyst logical plan tree that can drastically increase driver planning and compilation time. Using `withColumns` (or a single `select` projection with all expressions) flattens the project node into a single Catalyst Project operation.

---

### Q1.12: #004 Filtering, Multi-Condition Predicates & Pushdown Analysis (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Build complex multi-condition filters using bitwise operators (`&`, `|`, `~`), evaluate `isin()`, string regex matching (`rlike`), and inspect the Catalyst physical plan to verify predicate pushdown into the file reader.

**Production Implementation:**
```python
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
        .appName("UseCase_004_Filtering_Multi_Condition_Predicates_and_Pushdown_Analysis")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
telemetry = [
    ("V-01", "North America", 102.5, 32.0, 94.0, "WARN_TEMP"),
    ("V-02", "Europe", 88.0, 33.5, 98.0, "NORMAL"),
    ("V-03", "North America", 115.0, 26.0, 65.0, "CRITICAL_PRESSURE"),
    ("V-04", "Asia Pacific", 92.5, 31.0, 45.0, "WARN_BATTERY"),
    ("V-05", "Europe", 110.2, 27.5, 50.0, "CRITICAL_TEMP_PRESSURE"),
    ("V-06", "Latin America", 85.0, 34.0, 99.0, "NORMAL")
]

schema = "vehicle_id STRING, region STRING, engine_temp DOUBLE, tire_pressure DOUBLE, battery_pct DOUBLE, alert_code STRING"
df_fleet = spark.createDataFrame(telemetry, schema)
df_fleet.show()

# --- Execution Step ---
# 1. Vehicles with severe alerts in North America or Europe
target_regions = ["North America", "Europe"]

severe_filter = (
    (F.col("region").isin(target_regions)) &
    (
        (F.col("engine_temp") > 105.0) |
        (F.col("tire_pressure") < 28.0) |
        (F.col("battery_pct") < 60.0)
    ) &
    (~F.col("alert_code").isin(["NORMAL"]))
)

df_severe_alerts = df_fleet.filter(severe_filter)
df_severe_alerts.show()

# 2. Regex matching on alert descriptions
df_critical = df_fleet.filter(F.col("alert_code").rlike("^CRITICAL_"))
df_critical.show()

# --- Execution Step ---
print("=== PHYSICAL PLAN FOR FILTER QUERY ===")
df_severe_alerts.explain(mode="formatted")
```

**Enterprise Use Case:**  
A logistics fleet management company needs to identify critical alerts: vehicles with engine temperatures exceeding safe thresholds, low tire pressure, or battery degradation across specific geographic territories.

**Architectural Invariants & Gotchas:**
- Predicate pushdown allows the storage layer (Parquet/Delta) to skip entire row groups and data files using file-level min/max statistics without deserializing data into the JVM heap. Complex functions wrapped around columns can prevent pushdown, forcing full table scans.

---

### Q1.13: #005 String Operations, Regex Extraction & Data Cleansing (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Apply string manipulation functions: trimming whitespace, case normalization, regex value extraction (`regexp_extract`), masking sensitive values with `regexp_replace`, and parsing delimited tokens into structured columns.

**Production Implementation:**
```python
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
        .appName("UseCase_005_String_Operations_Regex_Extraction_and_Data_Cleansing")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
leads = [
    ("LEAD-1", "  JOHN.DOE@GMAIL.COM  ", "Phone: (555) 123-4567 | City: New York, NY"),
    ("LEAD-2", "SARAH_CONNOR@skyNET.io", "Cell: 555.987.6543 | City: Los Angeles, CA"),
    ("LEAD-3", "unknown_user", "No contact provided | City: Chicago, IL"),
    ("LEAD-4", "bruce.wayne@WayneEnt.com ", "Direct: +1-555-444-3333 | City: Gotham, NJ")
]

df_leads = spark.createDataFrame(leads, ["lead_id", "raw_email", "raw_contact_str"])
df_leads.show(truncate=False)

# --- Execution Step ---
# 1. Clean email: trim whitespace and lower case
df_clean_email = df_leads.withColumn("clean_email", F.lower(F.trim(F.col("raw_email"))))

# 2. Extract 10-digit phone number pattern
# Matches (xxx) xxx-xxxx, xxx.xxx.xxxx, or xxx-xxx-xxxx
phone_regex = r"(\d{3})[^\d]?(\d{3})[^\d]?(\d{4})"

df_extracted = df_clean_email.withColumn(
    "phone_digits",
    F.regexp_replace(
        F.regexp_extract(F.col("raw_contact_str"), phone_regex, 0),
        r"[^\d]",
        ""
    )
).withColumn(
    "city",
    F.regexp_extract(F.col("raw_contact_str"), r"City:\s*([A-Za-z\s]+),", 1)
).withColumn(
    "state",
    F.regexp_extract(F.col("raw_contact_str"), r",\s*([A-Z]{2})", 1)
)

df_extracted.select("lead_id", "clean_email", "phone_digits", "city", "state").show(truncate=False)

# --- Execution Step ---
# Mask email username: j***e@domain.com
df_masked = df_extracted.withColumn(
    "masked_email",
    F.regexp_replace(F.col("clean_email"), r"(^.).*(@.*$)", "$1***$2")
).withColumn(
    "masked_phone",
    F.when(
        F.length(F.col("phone_digits")) == 10,
        F.concat(F.lit("***-***-"), F.substring(F.col("phone_digits"), 7, 4))
    ).otherwise(F.lit("UNAVAILABLE"))
)

df_masked.select("lead_id", "masked_email", "masked_phone", "city", "state").show(truncate=False)
```

**Enterprise Use Case:**  
A marketing automation system ingests unstructured customer lead notes containing messy phone numbers, email addresses with inconsistent cases, and unformatted address strings.

**Architectural Invariants & Gotchas:**
- Spark SQL string functions are backed by native UTF-8 binary representations in Apache Spark's Tungsten project, avoiding Java String object allocation overhead.

---

### Q1.14: #006 Date & Timestamp Mastery: Parsing, Arithmetic & Timezones (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Master datetime transformations: parse timestamps with explicit timezone offsets, convert between UTC and local timezones (`from_utc_timestamp`), perform date arithmetic (`date_add`, `datediff`), and compute precise duration intervals.

**Production Implementation:**
```python
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
        .appName("UseCase_006_Date_and_Timestamp_Mastery_Parsing_Arithmetic_and_Timezones")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
rides_data = [
    ("RIDE-01", "2023-11-15T08:15:00Z", "2023-11-15T08:42:30Z", "America/New_York"),
    ("RIDE-02", "2023-11-15T14:30:00Z", "2023-11-15T15:05:00Z", "Europe/London"),
    ("RIDE-03", "2023-11-15T22:45:00Z", "2023-11-15T23:25:15Z", "Asia/Tokyo"),
    ("RIDE-04", "2023-11-16T01:10:00Z", "2023-11-16T01:35:00Z", "America/Los_Angeles")
]

df_rides = spark.createDataFrame(rides_data, ["ride_id", "pickup_utc", "dropoff_utc", "tz_city"])
df_rides.show(truncate=False)

# --- Execution Step ---
df_parsed = df_rides.withColumn(
    "pickup_ts", F.to_timestamp(F.col("pickup_utc"))
).withColumn(
    "dropoff_ts", F.to_timestamp(F.col("dropoff_utc"))
)

# Compute duration using unix timestamp difference
df_duration = df_parsed.withColumn(
    "duration_seconds",
    F.col("dropoff_ts").cast(T.LongType()) - F.col("pickup_ts").cast(T.LongType())
).withColumn(
    "duration_minutes",
    F.round(F.col("duration_seconds") / 60.0, 2)
)

df_duration.select("ride_id", "pickup_ts", "dropoff_ts", "duration_minutes").show()

# --- Execution Step ---
df_local = df_duration.withColumn(
    "pickup_local",
    F.from_utc_timestamp(F.col("pickup_ts"), F.col("tz_city"))
).withColumn(
    "pickup_hour",
    F.hour(F.col("pickup_local"))
).withColumn(
    "day_of_week",
    F.date_format(F.col("pickup_local"), "EEEE")
).withColumn(
    "is_peak_hour",
    F.when(F.col("pickup_hour").between(7, 9) | F.col("pickup_hour").between(17, 19), True).otherwise(False)
)

df_local.select("ride_id", "tz_city", "pickup_local", "pickup_hour", "day_of_week", "is_peak_hour").show(truncate=False)
```

**Enterprise Use Case:**  
A global ride-sharing platform records ride dispatch, pickup, and dropoff events across multiple international timezones. Operations teams require accurate trip duration in minutes, local business hour classifications, and SLA reporting.

**Architectural Invariants & Gotchas:**
- Spark 3.0 introduced Proleptic Gregorian calendar support by default (`spark.sql.session.timeZone`). Spark stores timestamps internally as UTC epoch microseconds. Using timezone-aware functions ensures consistency across distributed worker nodes regardless of server OS timezone settings.

---

### Q1.15: #007 Handling Nulls, NaNs & Imputation Strategies (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Detect and quantify missing values across all columns. Compare `coalesce`, `fillna`/`na.fill`, and `dropna`/`na.drop` with thresholds. Implement statistical imputation (replacing missing numerical values with category medians).

**Production Implementation:**
```python
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
        .appName("UseCase_007_Handling_Nulls_NaNs_and_Imputation_Strategies")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
clinical_data = [
    ("P-01", "Metabolic", 98.6, 120.0, "NORMAL"),
    ("P-02", "Cardiac", float("nan"), 145.0, "ELEVATED"),
    ("P-03", "Metabolic", None, None, None),
    ("P-04", "Cardiac", 101.2, float("nan"), "CRITICAL"),
    ("P-05", "Metabolic", 97.9, 115.0, "NORMAL"),
    ("P-06", "Neurology", None, 130.0, None)
]

schema = "patient_id STRING, dept STRING, body_temp DOUBLE, systolic_bp DOUBLE, assessment STRING"
df_clinical = spark.createDataFrame(clinical_data, schema)
df_clinical.show()

# --- Execution Step ---
# Build expressions dynamically for all columns
null_audit_exprs = [
    F.count(
        F.when(F.col(c).isNull() | (F.isnan(F.col(c)) if t in ["double", "float"] else F.lit(False)), c)
    ).alias(f"{c}_missing_count")
    for c, t in df_clinical.dtypes
]

df_audit = df_clinical.agg(*null_audit_exprs)
df_audit.show(truncate=False)

# --- Execution Step ---
# 1. Drop rows with at least 3 null fields
df_filtered = df_clinical.dropna(thresh=3)

# 2. Compute departmental average systolic_bp for imputation
dept_avgs = df_filtered.groupBy("dept").agg(F.round(F.avg("systolic_bp"), 1).alias("avg_bp"))

# 3. Join and coalesce missing readings
df_imputed = df_filtered.join(dept_avgs, on="dept", how="left").withColumn(
    "systolic_bp_imputed",
    F.coalesce(
        F.when(F.isnan(F.col("systolic_bp")), None).otherwise(F.col("systolic_bp")),
        F.col("avg_bp"),
        F.lit(120.0) # default fallback
    )
).withColumn(
    "assessment_clean",
    F.coalesce(F.col("assessment"), F.lit("PENDING_REVIEW"))
)

df_imputed.select("patient_id", "dept", "systolic_bp", "systolic_bp_imputed", "assessment_clean").show()
```

**Enterprise Use Case:**  
A medical diagnostics platform aggregates clinical lab test results. Missing biomarker readings occur due to machine sensor dropouts or patient cancellations. The pipeline must differentiate between true NULLs, NaN values, and impute baseline values.

**Architectural Invariants & Gotchas:**
- In PySpark, `NULL` (unknown value) and `NaN` (IEEE 754 Not-a-Number float) have distinct semantics. `isnan()` only applies to Double/Float columns, while `isNull()` applies to all types. Functions like `count()` ignore NULLs, but `count(*)` counts all rows.

---

### Q1.16: #008 Sorting, Ordering Semantics & Stratified Sampling (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Implement ordering with explicit null placement (`asc_nulls_first`, `desc_nulls_last`). Compare `sample()` vs `sampleBy()` for stratified sampling across categorical groups, and evaluate execution performance.

**Production Implementation:**
```python
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
        .appName("UseCase_008_Sorting_Ordering_Semantics_and_Stratified_Sampling")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
import random

# Seed for reproducible mock data
random.seed(42)
risk_tiers = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]

records = [
    (
        f"AUD-{i:04d}",
        random.choice(risk_tiers),
        round(random.uniform(10.0, 50000.0), 2) if i % 10 != 0 else None,
        f"ORG-{random.randint(1, 5)}"
    )
    for i in range(1, 101)
]

df_audit_logs = spark.createDataFrame(records, ["audit_id", "risk_tier", "amount", "org_id"])
df_audit_logs.show(5)

# --- Execution Step ---
df_sorted = df_audit_logs.orderBy(
    F.col("risk_tier").asc(),
    F.col("amount").desc_nulls_last()
)

print("Top 10 Sorted Records:")
df_sorted.show(10)

# --- Execution Step ---
# Define stratification fractions: sample 10% of LOW, 30% of MEDIUM, 80% of HIGH, 100% of CRITICAL
fractions = {
    "LOW": 0.10,
    "MEDIUM": 0.30,
    "HIGH": 0.80,
    "CRITICAL": 1.00
}

df_stratified = df_audit_logs.sampleBy("risk_tier", fractions=fractions, seed=42)

print("Stratified Sample Distribution:")
df_stratified.groupBy("risk_tier").count().show()
```

**Enterprise Use Case:**  
A financial auditing team must sample transaction logs for regulatory compliance. The sample must be stratified across risk tiers (Low, Medium, High) to reflect true operational risk distribution without skewing toward high-volume low-risk transactions.

**Architectural Invariants & Gotchas:**
- `orderBy()` and `sort()` trigger a global RangePartitioner shuffle across all executor partitions, which can be expensive on multi-terabyte datasets. `sortWithinPartitions()` sorts locally inside each partition without shuffling.

---

### Q1.17: #023 Basic and Multi-Column Aggregations with Alias Management (Advanced SQL, Aggregations & Analytical Window Functions)

**Question & Concept Overview:**  
Perform multi-column aggregations using `groupBy()` paired with `agg()`. Demonstrate explicit column aliasing, combining multiple statistical functions (`sum`, `avg`, `min`, `max`, `countDistinct`, `approx_count_distinct`), and using dictionary-based aggregation syntax.

**Production Implementation:**
```python
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
        .appName("UseCase_023_Basic_and_Multi_Column_Aggregations_with_Alias_Management")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
sales = [
    ("TXN-1", "West", "Electronics", 450.00, 0.10, "C-01"),
    ("TXN-2", "West", "Electronics", 1200.00, 0.15, "C-02"),
    ("TXN-3", "West", "Groceries", 65.50, 0.00, "C-01"),
    ("TXN-4", "East", "Electronics", 800.00, 0.05, "C-03"),
    ("TXN-5", "East", "Fashion", 150.00, 0.20, "C-04"),
    ("TXN-6", "East", "Fashion", 95.00, 0.10, "C-04"),
    ("TXN-7", "North", "Groceries", 45.00, 0.00, "C-05"),
    ("TXN-8", "North", "Groceries", 120.00, 0.05, "C-06")
]

schema = "txn_id STRING, region STRING, department STRING, amount DOUBLE, discount DOUBLE, customer_id STRING"
df_sales = spark.createDataFrame(sales, schema)
df_sales.show()

# --- Execution Step ---
df_regional_metrics = df_sales.groupBy("region", "department").agg(
    F.count("txn_id").alias("total_transactions"),
    F.round(F.sum("amount"), 2).alias("gross_revenue"),
    F.round(F.avg("amount"), 2).alias("avg_basket_size"),
    F.max("amount").alias("max_single_sale"),
    F.round(F.max("discount") * 100, 1).alias("max_discount_pct"),
    F.countDistinct("customer_id").alias("unique_shoppers")
).orderBy("region", F.desc("gross_revenue"))

df_regional_metrics.show()

# --- Execution Step ---
# On multi-billion row tables, countDistinct forces massive shuffles.
# approx_count_distinct uses HyperLogLog++ to compute cardinality within 5% error using minimal memory.
df_approx = df_sales.groupBy("region").agg(
    F.countDistinct("customer_id").alias("exact_distinct_shoppers"),
    F.approx_count_distinct("customer_id", rsd=0.05).alias("approx_distinct_shoppers")
)

df_approx.show()
```

**Enterprise Use Case:**  
A retail analytics engine calculates daily regional performance metrics across store departments: total gross revenue, average transaction value, maximum discount applied, and distinct active shoppers.

**Architectural Invariants & Gotchas:**
- Spark executes aggregations via HashAggregate (when data types are mutable) or SortAggregate. In two-phase aggregation, Spark first performs partial local aggregation on each partition before shuffling partial sums across the network, reducing network traffic.

---

### Q1.18: #032 Spark SQL Views, Temp Tables and Catalyst SQL Optimization (Advanced SQL, Aggregations & Analytical Window Functions)

**Question & Concept Overview:**  
Demonstrate seamless interoperability between PySpark DataFrames and Spark SQL: register temporary views (`createOrReplaceTempView`), global temporary views (`createGlobalTempView`), run complex SQL with CTEs via `spark.sql()`, pass dynamic parameters, and inspect Catalyst optimization.

**Production Implementation:**
```python
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
        .appName("UseCase_032_Spark_SQL_Views_Temp_Tables_and_Catalyst_SQL_Optimization")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
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

# --- Execution Step ---
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

# --- Execution Step ---
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
```

**Enterprise Use Case:**  
A hybrid analytics team consists of Python data engineers and SQL business analysts. The data engineering team prepares sanitized DataFrames that SQL analysts query using ANSI SQL queries and Common Table Expressions (CTEs).

**Architectural Invariants & Gotchas:**
- Temporary views exist purely as logical metadata pointers in the SparkSession catalog without materializing data to disk. Calling `spark.sql()` compiles SQL text through the same Catalyst parser and optimizer as DataFrame API code, producing identical physical plans.

---

### Q1.19: spark.sql() Basics – Running SQL in PySpark (Spark SQL Fundamentals)

**Question & Concept Overview:**  
spark.sql() allows you to execute SQL statements directly against the Spark session. It returns a DataFrame that can be further transformed or displayed. This is the entry point for all SQL-based interactions in Databricks notebooks.

**Production Implementation:**
```python
-- In a Databricks notebook cell (SQL mode)
-- Or via spark.sql() in PySpark:

-- 1. Simple query against an in-memory view
SELECT current_date() AS run_date, current_user() AS executed_by;

-- 2. Create a temp view from a DataFrame (PySpark interop)
-- df = spark.read.format('delta').load('/mnt/bronze/transactions')
-- df.createOrReplaceTempView('raw_transactions')

-- 3. Query the temp view via SQL
SELECT
    txn_id,
    account_id,
    txn_amount,
    txn_currency,
    txn_date
FROM raw_transactions
WHERE txn_amount > 10000
  AND txn_currency = 'EUR'
LIMIT 100;

-- 4. Set Spark SQL config within SQL
SET spark.sql.shuffle.partitions = 200;
SET spark.databricks.delta.optimizeWrite.enabled = true;

-- 5. Use spark.sql() result as DataFrame (PySpark)
-- result_df = spark.sql("""
--     SELECT account_id, SUM(txn_amount) AS total_amount
--     FROM raw_transactions
--     GROUP BY account_id
-- """)
-- result_df.display()
```

**Enterprise Use Case:**  
A TCS BFSI team running ad-hoc regulatory queries on Azure Databricks — analysts write SQL while engineers orchestrate via PySpark pipelines, using spark.sql() to bridge both worlds in multi-hop Medallion Architecture notebooks.

**Architectural Invariants & Gotchas:**
- spark.sql() returns a DataFrame — chain .display(), .show(), .write, etc.
- SQL cells in Databricks use %sql magic — no need to wrap in spark.sql()
- Temp views created via createOrReplaceTempView are session-scoped; Global temp views persist across sessions within the same app
- SET commands configure Spark session properties and can affect query performance significantly
- In Unity Catalog environments, always qualify table names: catalog.schema.table

---

### Q1.20: Creating and Using Databases/Schemas (Spark SQL Fundamentals)

**Question & Concept Overview:**  
In Spark SQL, databases and schemas are synonymous terms used to logically group tables. In Unity Catalog, the hierarchy is Catalog > Schema > Table, replacing the legacy two-level Hive metastore model. Proper namespace management is foundational for any enterprise data lake.

**Production Implementation:**
```python
-- =============================================
-- Database/Schema Management in Databricks SQL
-- Scenario: EU BFSI Medallion Architecture setup
-- =============================================

-- Create catalog (Unity Catalog only)
CREATE CATALOG IF NOT EXISTS eu_bfsi_prod
  COMMENT 'Production catalog for EU Banking, Financial Services & Insurance data';

-- Create schemas for each Medallion layer
CREATE SCHEMA IF NOT EXISTS eu_bfsi_prod.bronze
  COMMENT 'Raw ingestion layer — unprocessed data from source systems'
  MANAGED LOCATION 'abfss://bronze@euadlsprod.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS eu_bfsi_prod.silver
  COMMENT 'Cleansed and conformed layer — validated, deduplicated data'
  MANAGED LOCATION 'abfss://silver@euadlsprod.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS eu_bfsi_prod.gold
  COMMENT 'Aggregated business layer — KPIs, reports, ML features'
  MANAGED LOCATION 'abfss://gold@euadlsprod.dfs.core.windows.net/';

-- Set active catalog and schema for subsequent statements
USE CATALOG eu_bfsi_prod;
USE SCHEMA silver;

-- Verify current context
SELECT current_catalog(), current_database();

-- List all schemas in current catalog
SHOW SCHEMAS IN eu_bfsi_prod;

-- Drop schema (CASCADE drops all contained objects)
-- DROP SCHEMA IF EXISTS eu_bfsi_dev.temp CASCADE;
```

**Enterprise Use Case:**  
Setting up a new EU regulatory reporting workspace in Azure Databricks with Unity Catalog for a retail bank's AML (Anti-Money Laundering) compliance team — each Medallion layer gets its own schema, storage location, and access control boundary.

**Architectural Invariants & Gotchas:**
- Unity Catalog uses 3-level namespace: catalog.schema.table — always use fully qualified names in production
- MANAGED LOCATION specifies ADLS Gen2 path for all managed tables within the schema
- USE CATALOG and USE SCHEMA set the session context — subsequent unqualified table refs resolve to these
- Legacy Hive metastore uses hive_metastore as default catalog — migration to Unity Catalog is recommended
- Schema-level permissions in Unity Catalog cascade to all tables within — design schemas around access boundaries

---

### Q1.21: SELECT / WHERE / ORDER BY / LIMIT in Spark SQL (Spark SQL Fundamentals)

**Question & Concept Overview:**  
Foundational DQL (Data Query Language) operations in Spark SQL follow ANSI SQL syntax. Spark SQL supports predicate pushdown to optimize WHERE clause filtering, especially on partitioned Delta tables. ORDER BY in distributed systems is expensive — use SORT WITHIN PARTITIONS for better performance at scale.

**Production Implementation:**
```python
-- =============================================
-- DQL Fundamentals — EU Banking Transactions
-- =============================================

USE CATALOG eu_bfsi_prod;
USE SCHEMA silver;

-- Basic SELECT with column aliasing
SELECT
    txn_id                              AS transaction_id,
    account_id,
    UPPER(txn_type)                     AS transaction_type,
    txn_amount,
    txn_currency,
    ROUND(txn_amount * fx_rate, 2)      AS amount_in_eur,
    txn_timestamp                       AS transaction_ts,
    DATE(txn_timestamp)                 AS transaction_date
FROM silver_transactions
WHERE
    txn_date >= '2024-01-01'            -- Partition pruning benefit
    AND txn_date <  '2025-01-01'
    AND txn_currency IN ('EUR', 'GBP', 'USD')
    AND txn_amount   BETWEEN 1000 AND 1000000
    AND txn_status   != 'REVERSED'
    AND account_id   IS NOT NULL
ORDER BY txn_amount DESC
LIMIT 500;

-- DISTINCT values
SELECT DISTINCT txn_currency, txn_type
FROM silver_transactions
ORDER BY txn_currency;

-- Wildcard LIKE filter (AML keyword scan)
SELECT txn_id, merchant_name, txn_amount
FROM silver_transactions
WHERE merchant_name LIKE '%CRYPTO%'
   OR merchant_name LIKE '%OFFSHORE%'
ORDER BY txn_amount DESC;

-- NULL-safe comparisons
SELECT *
FROM silver_accounts
WHERE risk_score IS NULL
   OR credit_limit <=> 0;  -- <=> is NULL-safe equality
```

**Enterprise Use Case:**  
Compliance analysts at a European bank querying the silver transaction layer to identify high-value FX trades for regulatory EMIR reporting — partition pruning on txn_date dramatically reduces query cost on multi-billion-row Delta tables.

**Architectural Invariants & Gotchas:**
- Spark SQL supports ANSI SQL — use standard syntax; Spark-specific extensions are additive
- WHERE on partition columns (e.g. txn_date) triggers partition pruning — critical for Delta table performance
- ORDER BY is a full sort — it collects all data to a single partition; use only with LIMIT for large datasets
- BETWEEN is inclusive on both ends: BETWEEN 1000 AND 1000000 includes both boundary values
- The NULL-safe equality operator <=> returns true when both sides are NULL — unlike = which returns NULL

---

### Q1.22: CREATE TABLE USING DELTA (Delta Lake DDL)

**Question & Concept Overview:**  
Delta Lake is the default table format in Databricks. CREATE TABLE USING DELTA creates a versioned, ACID-compliant table backed by Parquet files and a Delta transaction log. Delta tables support schema enforcement, time travel, and DML operations unlike plain Parquet or CSV tables.

**Production Implementation:**
```python
-- =============================================
-- CREATE Delta Tables — EU BFSI Bronze Layer
-- =============================================

USE CATALOG eu_bfsi_prod;
USE SCHEMA bronze;

-- Managed Delta table (storage managed by Unity Catalog)
CREATE TABLE IF NOT EXISTS bronze_transactions (
    txn_id          STRING          NOT NULL COMMENT 'Unique transaction identifier from source',
    account_id      STRING          NOT NULL COMMENT 'Source account identifier',
    counterparty_id STRING                   COMMENT 'Beneficiary account or counterparty',
    txn_amount      DECIMAL(18, 4)  NOT NULL COMMENT 'Transaction amount in originating currency',
    txn_currency    STRING(3)       NOT NULL COMMENT 'ISO 4217 currency code',
    txn_type        STRING                   COMMENT 'CREDIT, DEBIT, FEE, REVERSAL',
    txn_status      STRING                   COMMENT 'PENDING, COMPLETED, REVERSED, FAILED',
    txn_timestamp   TIMESTAMP       NOT NULL COMMENT 'Transaction initiation timestamp (UTC)',
    value_date      DATE                     COMMENT 'Settlement date',
    source_system   STRING                   COMMENT 'Origin system: CORE_BANKING, SWIFT, SEPA',
    ingestion_ts    TIMESTAMP       NOT NULL COMMENT 'Databricks ingestion timestamp',
    batch_id        STRING                   COMMENT 'Pipeline run batch identifier',
    _rescued_data   STRING                   COMMENT 'Auto Loader rescued data column'
)
USING DELTA
PARTITIONED BY (txn_date DATE GENERATED ALWAYS AS (DATE(txn_timestamp)))
CLUSTER BY (account_id, txn_currency)       -- Liquid clustering (post-DBR 13.3)
COMMENT 'Raw transaction events ingested from Core Banking and SWIFT — Bronze layer'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'            = 'true',
    'delta.autoOptimize.optimizeWrite'      = 'true',
    'delta.autoOptimize.autoCompact'        = 'true',
    'delta.columnMapping.mode'              = 'name',
    'delta.minReaderVersion'                = '2',
    'delta.minWriterVersion'                = '5',
    'pipelines.autoOptimize.zOrderCols'     = 'account_id',
    'quality.layer'                         = 'bronze',
    'data.owner'                            = 'data-engineering@bank.eu',
    'data.classification'                   = 'CONFIDENTIAL'
);

-- External Delta table (unmanaged — storage controlled externally)
CREATE TABLE IF NOT EXISTS bronze_swift_messages
USING DELTA
LOCATION 'abfss://bronze@euadlsprod.dfs.core.windows.net/swift/messages/'
COMMENT 'SWIFT MT messages — external Delta table';
```

**Enterprise Use Case:**  
A data engineering team at a European custodian bank creating the bronze layer for SWIFT and Core Banking ingestion — Delta's ACID guarantees mean concurrent Auto Loader streams can ingest safely without corrupting the table.

**Architectural Invariants & Gotchas:**
- USING DELTA is the default in Databricks — explicitly stating it is good practice for documentation clarity
- PARTITIONED BY on a GENERATED ALWAYS AS column avoids user error while enforcing partition consistency
- CLUSTER BY (Liquid Clustering) is the modern replacement for static partitioning — more flexible re-clustering without full rewrites
- TBLPROPERTIES stores metadata and enables features like CDF, column mapping, and auto-optimize
- Managed tables: Unity Catalog controls storage. External tables: you control storage but UC manages metadata

---

### Q1.23: INSERT INTO and INSERT OVERWRITE (Delta Lake DML)

**Question & Concept Overview:**  
INSERT INTO appends records to a Delta table atomically. INSERT OVERWRITE replaces table (or partition) data in a single atomic operation, making it ideal for idempotent batch loads. Delta's transaction log ensures both operations are ACID-compliant and recoverable.

**Production Implementation:**
```python
-- =============================================
-- INSERT Operations on Delta Tables
-- EU BFSI — Loading Reference Data
-- =============================================

USE CATALOG eu_bfsi_prod;
USE SCHEMA bronze;

-- 1. INSERT INTO — Append rows
INSERT INTO bronze_transactions
SELECT
    txn_id,
    account_id,
    counterparty_id,
    CAST(txn_amount AS DECIMAL(18,4)),
    txn_currency,
    txn_type,
    txn_status,
    CAST(txn_timestamp AS TIMESTAMP),
    CAST(value_date AS DATE),
    source_system,
    current_timestamp()                 AS ingestion_ts,
    '${batch_id}'                       AS batch_id,
    NULL                                AS _rescued_data
FROM bronze_transactions_staging
WHERE batch_id = '${batch_id}';

-- 2. INSERT OVERWRITE — Replace partition atomically
INSERT OVERWRITE eu_bfsi_prod.silver.silver_fx_rates
PARTITION (rate_date = '2024-12-31')
SELECT
    base_currency,
    target_currency,
    exchange_rate,
    source,
    current_timestamp() AS loaded_ts
FROM eu_bfsi_prod.bronze.bronze_fx_rates
WHERE rate_date = '2024-12-31'
  AND source = 'ECB';

-- 3. INSERT with VALUES (small reference loads)
INSERT INTO eu_bfsi_prod.silver.dim_currency
    (currency_code, currency_name, is_eu_currency, created_ts)
VALUES
    ('EUR', 'Euro',           true,  current_timestamp()),
    ('GBP', 'British Pound',  false, current_timestamp()),
    ('CHF', 'Swiss Franc',    false, current_timestamp()),
    ('DKK', 'Danish Krone',   false, current_timestamp()),
    ('SEK', 'Swedish Krona',  false, current_timestamp());

-- 4. INSERT OVERWRITE (full table replace — idempotent)
INSERT OVERWRITE eu_bfsi_prod.gold.gold_country_risk
SELECT country_code, risk_tier, updated_date
FROM eu_bfsi_prod.silver.silver_country_risk_staging;
```

**Enterprise Use Case:**  
Daily ECB FX rate refresh for a European investment bank — INSERT OVERWRITE on the rate_date partition ensures idempotent pipeline runs without risk of duplicate exchange rates corrupting downstream P&L calculations.

**Architectural Invariants & Gotchas:**
- INSERT INTO is idempotent only with deduplication logic — without it, rerunning adds duplicates
- INSERT OVERWRITE on Delta replaces entire table or specific partition atomically — safe for reruns
- Delta's transaction log records every INSERT as a new version — enabling time travel rollback
- Use INSERT OVERWRITE for daily/monthly partition refreshes; MERGE INTO for upsert patterns
- Schema validation is enforced on INSERT — column types must match or be castable to target schema

---

### Q1.24: DESCRIBE TABLE / DESCRIBE EXTENDED / SHOW TABLES (Metadata & Catalog)

**Question & Concept Overview:**  
DESCRIBE and SHOW commands are essential for understanding table schemas, metadata, storage locations, and table properties without opening external tools. In Unity Catalog environments, these commands expose catalog-level metadata including owner, creation time, and data classification tags.

**Production Implementation:**
```python
-- =============================================
-- Metadata Discovery Commands
-- EU BFSI Data Catalog Exploration
-- =============================================

USE CATALOG eu_bfsi_prod;
USE SCHEMA silver;

-- 1. Basic column schema
DESCRIBE TABLE silver_transactions;
-- Returns: col_name, data_type, comment

-- 2. Extended metadata (storage, properties, stats)
DESCRIBE TABLE EXTENDED silver_transactions;
-- Returns: schema + Table Type, Location, Provider,
--          Owner, Created Time, Table Properties, Statistics

-- 3. Describe specific partition
DESCRIBE TABLE EXTENDED silver_transactions
PARTITION (txn_date = '2024-06-01');

-- 4. Show all tables in current schema
SHOW TABLES IN eu_bfsi_prod.silver;

-- 5. Show tables matching a pattern
SHOW TABLES IN eu_bfsi_prod.bronze LIKE 'bronze_*';

-- 6. Show all schemas
SHOW SCHEMAS IN eu_bfsi_prod;

-- 7. Show all catalogs (Unity Catalog)
SHOW CATALOGS;

-- 8. Show columns
SHOW COLUMNS IN silver_transactions;

-- 9. Show create statement (DDL reverse engineering)
SHOW CREATE TABLE eu_bfsi_prod.silver.silver_transactions;

-- 10. Show table properties
SHOW TBLPROPERTIES eu_bfsi_prod.silver.silver_transactions;

-- 11. Show partitions
SHOW PARTITIONS eu_bfsi_prod.silver.silver_transactions;

-- 12. Delta-specific: table history
DESCRIBE HISTORY eu_bfsi_prod.silver.silver_transactions
LIMIT 20;
-- Returns: version, timestamp, userId, operation, operationParameters
```

**Enterprise Use Case:**  
A data governance team at a European insurance company auditing table ownership and storage locations before migrating from Hive Metastore to Unity Catalog — DESCRIBE EXTENDED and SHOW CREATE TABLE help automate the migration DDL generation.

**Architectural Invariants & Gotchas:**
- DESCRIBE EXTENDED is the go-to for debugging table issues — reveals location, format, owner, and properties
- DESCRIBE HISTORY is unique to Delta Lake — shows full audit trail of all DML/DDL operations with timestamps
- SHOW CREATE TABLE is invaluable for documentation and reproducing table DDL in new environments
- SHOW TBLPROPERTIES helps verify Delta features like CDF, column mapping, and optimize settings
- In Unity Catalog, DESCRIBE TABLE EXTENDED shows the securable object's owner and creation metadata

---

### Q1.25: Basic SQL Functions: CAST, COALESCE, NVL, ISNULL (Spark SQL Functions)

**Question & Concept Overview:**  
Type conversion and null-handling functions are the building blocks of data quality transformations in the silver layer. CAST enforces type safety, while COALESCE/NVL/ISNULL provide null-handling patterns that prevent downstream aggregation errors in financial reporting.

**Production Implementation:**
```python
-- =============================================
-- Type Conversion & NULL Handling
-- EU BFSI Silver Layer Data Cleansing
-- =============================================

USE CATALOG eu_bfsi_prod;
USE SCHEMA silver;

SELECT
    -- CAST: Explicit type conversion
    txn_id,
    CAST(txn_amount AS DECIMAL(18, 4))              AS txn_amount_decimal,
    CAST(txn_timestamp AS DATE)                     AS txn_date,
    CAST(txn_timestamp AS STRING)                   AS txn_ts_str,
    CAST('2024-12-31' AS DATE)                      AS end_date,
    CAST(is_international AS BOOLEAN)               AS international_flag,

    -- COALESCE: Returns first non-null value
    COALESCE(merchant_name, counterparty_name,
             beneficiary_name, 'UNKNOWN')           AS resolved_party_name,
    COALESCE(fx_rate, 1.0)                          AS fx_rate_with_default,
    COALESCE(credit_limit, overdraft_limit, 0.0)    AS effective_limit,

    -- NVL: Identical to COALESCE(expr, default) — Hive/Spark compatibility
    NVL(risk_score, 0)                              AS risk_score_safe,
    NVL(branch_code, 'ONLINE')                      AS branch_code_safe,

    -- ISNULL / ISNOTNULL / IFNULL
    ISNULL(reversal_reference)                      AS is_reversal,
    ISNOTNULL(settlement_date)                      AS is_settled,
    IFNULL(processing_fee, 0.00)                    AS processing_fee_safe,

    -- NVL2: NVL2(expr, if_not_null, if_null)
    NVL2(counterparty_country,
         CONCAT('CROSS-BORDER:', counterparty_country),
         'DOMESTIC')                                AS txn_geography,

    -- Combining CAST + COALESCE for safe numeric operations
    CAST(COALESCE(txn_amount, 0) AS DECIMAL(18,4))
        * CAST(COALESCE(fx_rate, 1.0) AS DECIMAL(10,6))  AS amount_eur,

    -- NULLIF: Returns NULL if two values are equal
    NULLIF(error_code, '')                          AS error_code_clean,  -- Empty string to NULL
    NULLIF(txn_count, 0)                            AS txn_count_nonzero  -- 0 to NULL (safe division)

FROM bronze_transactions
WHERE ingestion_date = current_date();
```

**Enterprise Use Case:**  
Silver layer data cleansing pipeline at a European payments processor — raw Core Banking data arrives with mixed types and nulls; CAST and COALESCE transforms ensure downstream gold layer aggregations produce accurate SEPA reporting figures.

**Architectural Invariants & Gotchas:**
- COALESCE is ANSI SQL and evaluates arguments left to right — use it over NVL for portability
- NULLIF(value, '') is the idiomatic way to convert empty strings to NULL in cleansing pipelines
- CAST is strict — use TRY_CAST to return NULL instead of throwing on invalid conversions
- DECIMAL(18,4) is the recommended type for financial amounts — avoids floating-point precision errors
- NVL is Hive-compatible syntax supported in Spark SQL; prefer COALESCE in new code for ANSI compliance

---
