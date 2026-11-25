# 🎓 PySpark Interview Guide — Tier 2: Mid-Level Data Engineer (Core Engineering & Lakehouse)

> **Focus Areas**: Complex Data Types, Structs & Arrays, Higher-Order Functions, Analytical Window Functions, PyArrow Pandas UDFs, Delta Lake ACID Operations, Auto Loader Ingestion.
> **Target Audience**: Production Data Engineers, Analytics Engineers, ETL Pipeline Authors

---

### Q2.1: Window Functions for Analytical Queries (Analytical Functions)

**Question & Concept Overview:**  
Performing calculations across a set of rows related to the current row without collapsing them. Window functions are highly optimized by Spark for calculating running totals, rankings, and lagged records in financial workflows.

**Production Implementation:**
```python
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, lag, sum as _sum

# Window partition by account, ordered by time
ranking_window = Window.partitionBy("account_id").orderBy(col("txn_timestamp").desc())
running_total_window = Window.partitionBy("account_id") \
    .orderBy("txn_timestamp") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Add rankings and running balance
enriched_df = transactions \
    .withColumn("txn_recency", row_number().over(ranking_window)) \
    .withColumn("running_total_eur", _sum("amount_eur").over(running_total_window)) \
    .withColumn("prev_txn_amount", lag("amount_eur", 1, 0.0).over(ranking_window))

# Filter to get the top 3 most recent transactions per account
recent_txns_df = enriched_df.filter(col("txn_recency") <= 3)
```

**Enterprise Use Case:**  
Calculating rolling account balances and identifying transaction recency for AML (Anti-Money Laundering) sequence reviews.

**Architectural Invariants & Gotchas:**
- Window functions require partitioning and ordering to determine the partition boundaries and row sequence.
- Use rowsBetween or rangeBetween to define sliding windows (e.g. running totals, moving averages).
- lag and lead fetch rows at a specified offset relative to the current row, defaulting to a custom value (e.g. 0.0) if null.
- Omitting partitionBy triggers a global window, collecting all data onto a single worker node (should be avoided at scale).

---

### Q2.2: Complex Types: Structs, Arrays, and Maps (Complex Data Types)

**Question & Concept Overview:**  
Working with nested schemas, lists, and key-value mapping structures natively in PySpark. This is key for parsing multi-valued JSON structures (like SWIFT/ISO20022 messages) without flattening them prematurely.

**Production Implementation:**
```python
from pyspark.sql.functions import col, struct, array, explode, map_keys

# 1. Nest columns into a Struct (useful for grouping metadata)
struct_df = silver_df.withColumn("audit_meta", struct(
    col("ingestion_ts").alias("loaded_at"),
    col("batch_id").alias("job_run_id")
))

# 2. Extract elements from an array structure using explode (row replication)
exploded_trades_df = raw_trades_df \
    .withColumn("trade_leg", explode("trade_legs")) \
    .select("trade_id", "counterparty", "trade_leg.leg_amount", "trade_leg.currency")

# 3. Work with maps to retrieve configuration properties
config_keys_df = app_configs_df.withColumn("keys", map_keys("property_map"))
```

**Enterprise Use Case:**  
Decomposing nested trade messages containing multiple execution legs into individual rows for capital risk metrics calculation.

**Architectural Invariants & Gotchas:**
- struct creates a single complex column containing sub-fields, accessible via dot-notation (col.subfield).
- explode takes an array or map column and generates a new row for each element, replicating other column values.
- higher-order functions (e.g. filter, transform) can process arrays inline without needing to explode first.
- Maps are perfect for dynamic properties where schema keys change frequently across records.

---

### Q2.3: Built-in Functions & Null Handling (Data Quality)

**Question & Concept Overview:**  
Standard practices for cleaning dirty inputs, using conditional logic, and handling missing values safely. Resolving null values ensures accurate calculations in financial rollup reporting.

**Production Implementation:**
```python
from pyspark.sql.functions import coalesce, col, when, lit

# Clean customer profile details and assign defaults for reporting
sanitized_customers_df = customer_df.withColumn(
    "contact_phone",
    coalesce(col("mobile_number"), col("home_phone"), lit("NOT_PROVIDED"))
).withColumn(
    "risk_tier_category",
    when(col("risk_score") >= 80, "HIGH")
    .when((col("risk_score") >= 40) & (col("risk_score") < 80), "MEDIUM")
    .otherwise("LOW")
)

# Fill null numeric amounts with zero and empty text with default
cleansed_df = sanitized_customers_df.na.fill({"credit_limit": 0.0, "country_code": "EU"})
```

**Enterprise Use Case:**  
Resolving missing contact and rating details in credit applicant records prior to scoring modeling.

**Architectural Invariants & Gotchas:**
- coalesce evaluates columns from left to right and returns the first non-null value.
- when().otherwise() executes conditional switch logic, behaving like SQL CASE WHEN.
- df.na.fill() fills null values selectively using a Python dictionary mapping column names to defaults.
- Using lit() is required to supply literal values (e.g. 'NOT_PROVIDED') inside Spark expressions.

---

### Q2.4: User Defined Functions (UDFs & Pandas UDFs) (Custom Transformations)

**Question & Concept Overview:**  
Extending Spark functionality with custom Python functions. Distinguishes between standard row-by-row Python UDFs and vectorized Pandas UDFs (PyArrow) that perform calculations in batch.

**Production Implementation:**
```python
import pandas as pd
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import DoubleType

# Vectorized Pandas UDF: Calculate European VAT (Value Added Tax)
@pandas_udf(DoubleType())
def calculate_vat_pandas_udf(amounts: pd.Series, rate_pct: pd.Series) -> pd.Series:
    # Operations are executed using fast vectorized pandas/numpy
    return amounts * (rate_pct / 100.0)

# Apply the vectorized UDF to DataFrame columns
vat_calculated_df = invoices_df.withColumn(
    "vat_amount",
    calculate_vat_pandas_udf(col("net_amount"), col("tax_rate"))
)
```

**Enterprise Use Case:**  
Executing complex EU tax calculations on millions of invoice records inside Azure Databricks without JVM-to-Python bottlenecks.

**Architectural Invariants & Gotchas:**
- Standard Python UDFs suffer from serialization overhead because data is moved between the JVM and Python interpreter row by row.
- Pandas UDFs use Apache Arrow to transfer data in batches, allowing high-performance vectorized operations.
- Ensure PyArrow is installed and enabled (spark.sql.execution.arrow.pyspark.enabled = true) to run Pandas UDFs.
- Type hinting in @pandas_udf is mandatory to define the return structure of the function.

---

### Q2.5: Delta Time Travel & History (Delta Lake Features)

**Question & Concept Overview:**  
Querying historical versions of a Delta table using transaction log snapshots. This is essential for reconstructing past data states for auditing, debugging, and rollback.

**Production Implementation:**
```python
# 1. Retrieve the audit trail history of a Delta table
history_df = spark.read.format("delta") \
    .load("abfss://silver@euadlsprod.dfs.core.windows.net/ledger") \
    .history()

# 2. Query historical data as of version 5
df_version_5 = spark.read.format("delta") \
    .option("versionAsOf", "5") \
    .load("abfss://silver@euadlsprod.dfs.core.windows.net/ledger")

# 3. Query historical data as of a specific date and time
df_timestamp = spark.read.format("delta") \
    .option("timestampAsOf", "2026-01-01 12:00:00") \
    .load("abfss://silver@euadlsprod.dfs.core.windows.net/ledger")
```

**Enterprise Use Case:**  
Reconstructing monthly account balances as of a precise audit cut-off date to satisfy EBA regulatory inspectors.

**Architectural Invariants & Gotchas:**
- Delta Lake retains historical files based on table properties like logRetentionDuration and deletedFileRetentionDuration.
- versionAsOf reads the specific commit file from the _delta_log directory.
- timestampAsOf searches for the closest log commit timestamp that matches the criteria.
- Time travel is read-only; to rollback, overwrite the active table with the historical version's contents.

---

### Q2.6: Auto Loader (cloudFiles) for Streaming Ingestion (Data Ingestion)

**Question & Concept Overview:**  
Incrementally and efficiently processing new files as they land in cloud storage (ADLS Gen2 / OneLake) without managing file lists. Auto Loader maintains state using RocksDB checkpoints.

**Production Implementation:**
```python
# Configure Auto Loader to stream incoming JSON transaction files
streaming_df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "abfss://meta@euadlsprod.dfs.core.windows.net/schemas/txns") \
    .option("cloudFiles.rescuedDataColumn", "_rescued_data") \
    .load("abfss://landing@euadlsprod.dfs.core.windows.net/incoming_files/*.json")

# Write the stream to a bronze Delta table using structured checkpointing
query = streaming_df.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://meta@euadlsprod.dfs.core.windows.net/checkpoints/txns") \
    .start("abfss://bronze@euadlsprod.dfs.core.windows.net/bronze_transactions")
```

**Enterprise Use Case:**  
Ingesting volatile and unstructured payment stream feeds continuously from landing zones to Bronze Delta layers.

**Architectural Invariants & Gotchas:**
- cloudFiles.schemaLocation is required to store the schema state and support schema evolution automatically.
- rescuedDataColumn captures columns that do not match the schema, preserving corrupt data for auditing.
- Auto Loader can scale to process billions of files using cloud notifications (SQS/Event Grid) or directory listing.
- A checkpoint location is mandatory for streaming to guarantee exactly-once processing semantics.

---

### Q2.7: Date and Time Manipulation (Time Series Operations)

**Question & Concept Overview:**  
Standard functions for parsing, formatting, and arithmetic operations on dates and timestamps. Critical for defining reporting boundaries and calculating periods in compliance schedules.

**Production Implementation:**
```python
from pyspark.sql.functions import to_date, to_timestamp, date_add, date_trunc, date_format, col

# Parse string representations of dates, truncate to month, and format
datetime_df = raw_dates_df.select(
    col("txn_id"),
    to_timestamp(col("raw_timestamp"), "yyyy-MM-dd HH:mm:ss.SSS").alias("txn_ts"),
    to_date(col("raw_date"), "dd/MM/yyyy").alias("txn_date")
).withColumn(
    "interest_calc_start", date_add(col("txn_date"), 30)
).withColumn(
    "fiscal_month", date_trunc("MM", col("txn_date"))
).withColumn(
    "report_year_week", date_format(col("txn_date"), "yyyy-w")
)

# Output schema verification
datetime_df.printSchema()
```

**Enterprise Use Case:**  
Normalizing heterogeneous date string formats from international payment gateways into a standardized ISO format.

**Architectural Invariants & Gotchas:**
- Use to_date and to_timestamp with explicit format patterns (Java SimpleDateFormat) to avoid parsing failures.
- date_trunc truncates dates/timestamps to a specified level like year ('YYYY'), month ('MM'), or hour ('HH').
- date_format converts date columns to formatted strings, useful for generating report paths or partition names.
- Operations automatically account for leap years and month lengths.

---

### Q2.8: Partitioning and Bucketing (Storage Optimization)

**Question & Concept Overview:**  
Optimizing physical storage layout in cloud storage. Partitioning isolates data into folders based on low-cardinality keys, while bucketing divides data into fixed files based on high-cardinality keys to eliminate shuffles.

**Production Implementation:**
```python
# 1. Write DataFrame partitioned by transaction date (Low Cardinality)
transactions_df.write.format("delta") \
    .mode("append") \
    .partitionBy("txn_date") \
    .save("abfss://silver@euadlsprod.dfs.core.windows.net/partitioned_txns")

# 2. Write DataFrame bucketed by account ID (High Cardinality)
# Bucketing is saved as a managed table structure
transactions_df.write.format("parquet") \
    .mode("overwrite") \
    .bucketBy(100, "account_id") \
    .sortBy("txn_timestamp") \
    .saveAsTable("eu_bfsi_prod.silver.bucketed_txns")
```

**Enterprise Use Case:**  
Designing storage layout for credit transaction tables, using date-partitioning for analytical queries and account-bucketing for reconciliation joins.

**Architectural Invariants & Gotchas:**
- Partitioning is highly effective for filtering (partition pruning) on queries, but should not exceed 10,000 partitions to avoid directory scanning issues.
- Liquid Clustering (Delta Lake DBR 13.3+) replaces static partitioning, resolving partition-drift performance issues.
- bucketBy requires saveAsTable because metadata must be saved in the Spark metastore to let readers skip shuffles.
- Use bucketing when you routinely perform joins or aggregations on high-cardinality columns (e.g. account_id).

---

### Q2.9: Deduplication Retaining Latest Timestamp via Window Functions (Window Functions & Deduplication)

**Question & Concept Overview:**  
Deterministically removes duplicate records for an entity by ranking rows based on timestamp descending and filtering for rank 1.

**Production Implementation:**
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Sample dataset with customer updates
data = [
    ("cust_101", "2026-10-01 10:00:00", "Logged In"),
    ("cust_101", "2026-10-01 14:30:00", "Purchased Item"),  # Latest for 101
    ("cust_102", "2026-10-01 09:15:00", "Added to Cart"),
    ("cust_102", "2026-10-01 09:00:00", "Viewed Page")
]
df = spark.createDataFrame(data, ["customer_id", "event_time", "activity"])

# Define partition window ordered by event_time descending
window_spec = Window.partitionBy("customer_id").orderBy(F.col("event_time").desc())

# Assign row number and retain only the latest record
df_latest = df.withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

df_latest.show(truncate=False)
```

**Enterprise Use Case:**  
Ensuring dimensional tables reflect the most recent state of a customer profile during real-time or batch event ingestion.

**Architectural Invariants & Gotchas:**
- Avoid using dropDuplicates(['customer_id']) when you need the most recent row, as dropDuplicates drops non-deterministically.
- Add a secondary column in orderBy() to break exact timestamp ties consistently.

---

### Q2.10: Auto Loader (cloudFiles) Streaming Ingestion with Rescued Data (Streaming & Ingestion)

**Question & Concept Overview:**  
Production Auto Loader pattern for high-throughput, low-latency file ingestion from cloud object storage (ADLS Gen2 / S3). Implements automatic schema inference, directory notification mode, schema evolution, and bad-data isolation via _rescued_data.

**Production Implementation:**
```python
from pyspark.sql.functions import col, current_timestamp

# Configure Auto Loader streaming source
raw_stream_df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "abfss://checkpoints@datalake.dfs.core.windows.net/orders/schema") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.useNotifications", "true") \
    .load("abfss://landing@datalake.dfs.core.windows.net/inbound_orders/*/*.json")

# Add ingestion audit metadata
enriched_stream = raw_stream_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("file_source", col("_metadata.file_name"))

# Stream write to Bronze Delta Table with checkpointing
query = enriched_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://checkpoints@datalake.dfs.core.windows.net/orders/bronze_chk") \
    .trigger(availableNow=True) \
    .toTable("bronze.orders_raw")

query.awaitTermination()
```

**Enterprise Use Case:**  
Ingesting millions of daily order events arriving as nested JSON blobs into a Bronze Delta table with zero schema drift failures.

**Architectural Invariants & Gotchas:**
- cloudFiles.useNotifications uses cloud queue events (Event Grid / SNS) to discover millions of files without expensive directory listings.
- addNewColumns schema evolution automatically adapts to new incoming JSON attributes without failing the streaming job.
- _rescued_data automatically captures malformed rows or type mismatches for audit without dropping records.
- trigger(availableNow=True) gives micro-batch serverless cost efficiency with streaming semantics.

---

### Q2.11: DBUtils File System & Unity Catalog Volumes API (DBFS & File Systems)

**Question & Concept Overview:**  
Programmatic file operations using dbutils.fs across DBFS and Unity Catalog Volumes (/Volumes/catalog/schema/volume/). Covers listing, recursive copying, moving, and safe directory creation without legacy mount dependencies.

**Production Implementation:**
```python
# 1. List directory files with metadata in Unity Catalog Volume
files = dbutils.fs.ls("/Volumes/prod_catalog/raw_data/landing_zone/")
for f in files:
    print(f"File: {f.name} | Size: {f.size / (1024*1024):.2f} MB | Path: {f.path}")

# 2. Copy files recursively between landing and staging
dbutils.fs.cp(
    "/Volumes/prod_catalog/raw_data/landing_zone/2026/",
    "/Volumes/prod_catalog/raw_data/staging/",
    recurse=True
)

# 3. Atomically move/rename processed file
dbutils.fs.mv(
    "/Volumes/prod_catalog/raw_data/staging/inbound.csv",
    "/Volumes/prod_catalog/raw_data/archive/inbound_20261022.csv"
)

# 4. Create directory structure
dbutils.fs.mkdirs("/Volumes/prod_catalog/raw_data/checkpoints/orders_stream/")

# 5. Read preview bytes directly into Python
content_preview = dbutils.fs.head("/Volumes/prod_catalog/raw_data/landing_zone/sample.json", maxBytes=1024)
print(content_preview)
```

**Enterprise Use Case:**  
Standardizing ingestion pipelines to interact with Unity Catalog Volumes rather than deprecated DBFS mount points, ensuring strict audit compliance and fine-grained access control.

**Architectural Invariants & Gotchas:**
- Unity Catalog Volumes are the modern replacement for legacy DBFS mounts (/mnt/).
- Volumes support standard POSIX file APIs via /Volumes/ paths in Python, Bash, and C libraries.
- dbutils.fs.rm(path, recurse=True) permanently deletes directories; use with extreme caution in production.
- Use dbutils.fs.head() for fast schema and header inspection without spinning up large Spark reader jobs.

---

### Q2.12: Parameterized Notebooks with Widgets & JSON Exit (Orchestration & Workflow)

**Question & Concept Overview:**  
Parameterizing Databricks notebooks using dbutils.widgets and returning structured JSON payloads to parent orchestrators using dbutils.notebook.exit. Essential for building modular Databricks workflow DAGs.

**Production Implementation:**
```python
# ------------------------------------------------------------------------------
# IN CHILD NOTEBOOK: Parameter definition & payload return
# ------------------------------------------------------------------------------
# 1. Define interactive widgets with defaults
dbutils.widgets.text("batch_date", "2026-10-22", "1. Ingestion Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("environment", "prod", ["dev", "staging", "prod"], "2. Environment")

# 2. Retrieve parameter values safely
batch_date = dbutils.widgets.get("batch_date")
env = dbutils.widgets.get("environment")

# 3. Execute business logic
records_processed = 250000

# 4. Exit notebook and pass structured metadata to caller
import json
result_payload = {
    "status": "SUCCESS",
    "batch_date": batch_date,
    "records_processed": records_processed,
    "execution_duration_sec": 42.5
}
dbutils.notebook.exit(json.dumps(result_payload))

# ------------------------------------------------------------------------------
# IN PARENT NOTEBOOK: Chaining execution & parsing results
# ------------------------------------------------------------------------------
# raw_res = dbutils.notebook.run("./child_pipeline", timeout_seconds=1800, arguments={"batch_date": "2026-10-22"})
# summary = json.loads(raw_res)
# print(f"Processed {summary['records_processed']} records in {summary['execution_duration_sec']}s")
```

**Enterprise Use Case:**  
Orchestrating multi-hop Medallion architecture pipelines where bronze, silver, and gold stages run as modular notebooks passing status payloads and record counts.

**Architectural Invariants & Gotchas:**
- dbutils.notebook.exit() only accepts string payloads; always serialize Python dictionaries via json.dumps().
- dbutils.notebook.run() spawns an ephemeral execution context that shares cluster compute but has isolated local namespace.
- Widgets can be bound directly to Databricks Workflow job parameters in the UI or via `06_platform_adapters/databricks_asset_bundles/databricks.yml` asset bundles.
- dbutils.widgets.removeAll() cleans up widget definitions when authoring interactive templates.

---

### Q2.13: Zero-Plaintext Credential Management via dbutils.secrets (Security & Governance)

**Question & Concept Overview:**  
Securely retrieving API keys, database connection strings, and certificates from Azure Key Vault or AWS Secrets Manager scopes without exposing plaintext credentials in notebooks, logs, or version control.

**Production Implementation:**
```python
# 1. Inspect available scopes for the cluster identity
scopes = dbutils.secrets.listScopes()
print("Available Scopes:", [s.name for s in scopes])

# 2. Fetch secret securely (Databricks automatically masks value as [REDACTED] in all stdout)
db_password = dbutils.secrets.get(scope="azure-keyvault-prod", key="finance-sql-password")
client_secret = dbutils.secrets.get(scope="azure-keyvault-prod", key="aad-sp-secret")

# 3. Inject secret into JDBC or Cloud Connector options
jdbc_url = "jdbc:sqlserver://eu-finance-db.database.windows.net:1433;databaseName=LedgerDW"

fact_ledger_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.FactGeneralLedger")
    .option("user", "svc_databricks_reader")
    .option("password", db_password)  # Zero plaintext exposure in code or logs
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("fetchsize", "10000")
    .load()
)
```

**Enterprise Use Case:**  
Ingesting confidential enterprise financial balances from on-premises or cloud SQL Server databases into bronze Delta lakehouse tables without storing database passwords in Git.

**Architectural Invariants & Gotchas:**
- Databricks runtime actively redacts secrets fetched via dbutils.secrets.get() from output cells and driver logs.
- Never print secrets or write them to temporary files on disk to prevent accidental leaks.
- Link Azure Key Vault to Databricks Secret Scopes via the #secrets/createScope URL in the Databricks workspace.
- Combine with Unity Catalog Service Principals for end-to-end passwordless OAuth connectivity where possible.

---

### Q2.14: Higher-Order Functions on Complex Nested Arrays (Complex Data Types)

**Question & Concept Overview:**  
Transforming, filtering, and aggregating complex nested arrays in-place using PySpark higher-order functions (transform, filter, aggregate). Eliminates expensive explode and re-group shuffles.

**Production Implementation:**
```python
from pyspark.sql import functions as F

# Sample orders with nested array of line item structs
orders_df = spark.table("prod_catalog.silver.orders")

# 1. Transform: Apply 15% discount to all line item prices in-place
discounted_df = orders_df.withColumn(
    "discounted_items",
    F.transform(
        "line_items",
        lambda item: F.struct(
            item.item_id.alias("item_id"),
            item.quantity.alias("quantity"),
            F.round(item.unit_price * 0.85, 2).alias("unit_price")
        )
    )
)

# 2. Filter: Keep only line items where item quantity > 3
bulk_orders_df = discounted_df.withColumn(
    "bulk_items",
    F.filter("line_items", lambda item: item.quantity > 3)
)

# 3. Aggregate: Calculate total order weight by summing array items
weighted_orders_df = bulk_orders_df.withColumn(
    "total_order_weight_kg",
    F.aggregate(
        "line_items",
        F.lit(0.0), # Initial accumulator value
        lambda acc, item: acc + (item.quantity * item.unit_weight_kg)
    )
)

# 4. Check if any line item contains specific hazardous SKU
hazardous_df = weighted_orders_df.withColumn(
    "has_hazardous_materials",
    F.exists("line_items", lambda item: item.is_hazardous == True)
)
```

**Enterprise Use Case:**  
Processing complex eCommerce shopping carts containing hundreds of nested line items, computing shipping weights and category discounts with zero shuffle serialization.

**Architectural Invariants & Gotchas:**
- Higher-order functions execute within Catalyst expression trees without unpacking arrays into separate rows.
- Replacing explode() + groupBy() with transform() and aggregate() avoids multi-gigabyte shuffle spills.
- Works seamlessly with deeply nested JSON structures, telemetry packets, and eCommerce shopping carts.
- F.exists() short-circuits evaluation as soon as the first matching element is encountered in the array.

---

### Q2.15: #009 Deduplication Strategies: dropDuplicates vs Window Row-Number (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Implement deduplication: (1) `distinct()` for exact row matches, (2) `dropDuplicates([subset])` for primary key deduplication with arbitrary winner selection, and (3) Deterministic deduplication using `row_number()` over Window partitions to retain the latest timestamp record.

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
        .appName("UseCase_009_Deduplication_Strategies_dropDuplicates_vs_Window_Row_Number")
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
webhook_events = [
    ("ORD-101", "CREATED", "2023-11-20 09:00:00", 120.00),
    ("ORD-101", "PAID",    "2023-11-20 09:05:00", 120.00),
    ("ORD-101", "PAID",    "2023-11-20 09:05:00", 120.00), # Exact duplicate
    ("ORD-102", "CREATED", "2023-11-20 09:10:00", 85.50),
    ("ORD-102", "CANCELLED","2023-11-20 09:12:00", 85.50),
    ("ORD-103", "CREATED", "2023-11-20 09:15:00", 430.00)
]

schema = "order_id STRING, order_status STRING, updated_at STRING, total_amount DOUBLE"
df_webhooks = spark.createDataFrame(webhook_events, schema)
df_webhooks.show()

# --- Execution Step ---
# Distinct drops identical rows
df_distinct = df_webhooks.distinct()
print(f"Original: {df_webhooks.count()} | After distinct: {df_distinct.count()}")

# dropDuplicates on order_id keeps an arbitrary update
df_arbitrary = df_webhooks.dropDuplicates(["order_id"])
print("Arbitrary dropDuplicates (non-deterministic latest status):")
df_arbitrary.show()

# --- Execution Step ---
window_spec = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())

df_deterministic = (
    df_webhooks
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print("Deterministic Deduplication (Always Keeps Latest Status):")
df_deterministic.orderBy("order_id").show()
```

**Enterprise Use Case:**  
An e-commerce order ingestion pipeline receives duplicate HTTP webhook payloads. Some duplicates are exact identical records, while others represent updated state payloads for the same order with later timestamps.

**Architectural Invariants & Gotchas:**
- `dropDuplicates()` uses a HashAggregate under the hood. While efficient, it is non-deterministic regarding which non-key column values survive if not pre-sorted. Window functions (`row_number`) provide 100% deterministic deduplication by explicitly controlling sorting criteria.

---

### Q2.16: #010 Transformations vs Actions, Lazy Evaluation & Lineage Graph (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Demonstrate Spark's lazy evaluation mechanism. Differentiate between Narrow dependencies (map, filter) and Wide dependencies (groupBy, join, distinct). Inspect the RDD execution lineage graph using `toDebugString()` and profile action execution times.

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
        .appName("UseCase_010_Transformations_vs_Actions_Lazy_Evaluation_and_Lineage_Graph")
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
# Generate sequence of 100,000 numbers
df_nums = spark.range(1, 100001, 1, 4)

# Narrow transformation: filter (stays within partition)
df_narrow = df_nums.filter(F.col("id") % 2 == 0).withColumn("squared", F.col("id") * F.col("id"))

# Wide transformation: groupBy and agg (triggers shuffle across partitions)
df_wide = df_narrow.groupBy(F.col("id") % 10).agg(F.sum("squared").alias("sum_squares"))

print("Transformations registered in Catalyst. No execution has occurred yet!")

# --- Execution Step ---
print("=== CATALYST PHYSICAL PLAN (Spark Connect-safe; .rdd.toDebugString() is classic JVM only) ===")
df_wide.explain("formatted")

# --- Execution Step ---
import time

start = time.time()
result_count = df_wide.count()
elapsed = time.time() - start

print(f"Action triggered! Result rows: {result_count} (Execution Time: {elapsed:.3f}s)")
df_wide.show()
```

**Enterprise Use Case:**  
A performance engineering lead must diagnose why a data pipeline fails after running for 40 minutes, only to crash on a lightweight `.count()` action. The team must learn to trace lineage graphs and identify narrow vs wide transformations.

**Architectural Invariants & Gotchas:**
- Spark constructs a Directed Acyclic Graph (DAG) of stages separated by shuffle boundaries. Narrow transformations execute entirely within the same partition in memory (pipelining). Wide transformations require data shuffling across the network to new executor partitions, creating new stages.

---

### Q2.17: #011 Custom PySpark UDFs vs Vectorized Pandas UDFs (PyArrow) (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Implement the calculation using: (1) Standard Python UDF (`@udf`), (2) Native Spark SQL built-in expressions, and (3) Vectorized Pandas UDF (`@pandas_udf`). Compare execution time, serialization behavior, and memory footprint.

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
        .appName("UseCase_011_Custom_PySpark_UDFs_vs_Vectorized_Pandas_UDFs_PyArrow")
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
df_accounts = spark.range(1, 50001, 1, 4).select(
    F.col("id").alias("account_id"),
    (F.rand(seed=42) * 800 + 300).alias("credit_score"),
    (F.rand(seed=101) * 50000 + 1000).alias("debt_amount"),
    (F.rand(seed=202) * 100000 + 20000).alias("annual_income")
)

df_accounts.show(5)

# --- Execution Step ---
@F.udf(returnType=T.DoubleType())
def calculate_risk_standard(credit_score, debt, income):
    if income == 0 or credit_score is None:
        return 0.0
    dti = debt / income
    score_factor = (850 - credit_score) / 550.0
    return round((dti * 0.6) + (score_factor * 0.4), 4)

import time
t0 = time.time()
df_res1 = df_accounts.withColumn("risk_std", calculate_risk_standard("credit_score", "debt_amount", "annual_income"))
c1 = df_res1.filter(F.col("risk_std") > 0.5).count()
t1 = time.time() - t0
print(f"Standard UDF Count: {c1} in {t1:.3f} seconds")

# --- Execution Step ---
import pandas as pd

@F.pandas_udf(T.DoubleType())
def calculate_risk_vectorized(credit_score: pd.Series, debt: pd.Series, income: pd.Series) -> pd.Series:
    dti = debt / income.replace(0, 1.0)
    score_factor = (850.0 - credit_score) / 550.0
    return ((dti * 0.6) + (score_factor * 0.4)).round(4)

t0 = time.time()
df_res2 = df_accounts.withColumn("risk_vec", calculate_risk_vectorized("credit_score", "debt_amount", "annual_income"))
c2 = df_res2.filter(F.col("risk_vec") > 0.5).count()
t2 = time.time() - t0
print(f"Vectorized Pandas UDF Count: {c2} in {t2:.3f} seconds (Speedup factor: {t1/max(t2, 0.001):.1f}x)")
```

**Enterprise Use Case:**  
A credit scoring model applies complex non-linear mathematical risk adjustments to customer accounts. The current implementation uses standard Python UDFs and causes severe CPU throttling and worker memory spikes.

**Architectural Invariants & Gotchas:**
- Standard Python UDFs serialize JVM row data to a Python worker process row-by-row via Py4J, incurring massive serialization and deserialization overhead and disabling Catalyst optimizations. Vectorized Pandas UDFs use Apache Arrow to transfer columnar batches into Pandas Series, achieving near-native C-speed vectorization.

---

### Q2.18: #012 Emulating Databricks DBUtils, Widgets & Secret Scopes Locally (Core PySpark Foundations & DataFrame API)

**Question & Concept Overview:**  
Design a clean, modular Python compatibility wrapper class `MockDBUtils` that provides identical interfaces for `dbutils.fs` (`ls`, `cp`, `mv`, `rm`, `head`, `put`), `dbutils.widgets` (`text`, `dropdown`, `get`), and `dbutils.secrets` (`get`) using environment variables and local disk storage.

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
        .appName("UseCase_012_Emulating_Databricks_DBUtils_Widgets_and_Secret_Scopes_Locally")
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

# --- Execution Step ---
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

# --- Execution Step ---
# Configure notebook widgets
dbutils.widgets.dropdown("env", "staging", ["dev", "staging", "prod"], "Target Environment")
dbutils.widgets.text("processing_date", "2023-11-20", "Batch Date")

current_env = dbutils.widgets.get("env")
proc_date = dbutils.widgets.get("processing_date")
api_key = dbutils.secrets.get(scope="finance_scope", key="stripe_token")

print(f"Running pipeline for ENV={current_env}, DATE={proc_date}")
print(f"Retrieved Secret: {api_key[:4]}**** (Masked)")
```

**Enterprise Use Case:**  
An enterprise migration team develops Databricks pipelines locally inside VS Code or CI/CD test runners before deploying to production workspaces. Code relying on `dbutils.fs`, `dbutils.secrets`, and `dbutils.widgets` crashes outside of Databricks.

**Architectural Invariants & Gotchas:**
- In Databricks, `dbutils` is a Java/Scala bridge injected into the Python REPL. Writing pipeline code that imports or references `dbutils` directly creates tight vendor lock-in. Wrapping it in a helper or using Databricks Connect decouples code for multi-cloud and local portability.

---

### Q2.19: #013 Array Creation, Element Extraction and Array Search Functions (Complex Data Types & Nested Structures Handling)

**Question & Concept Overview:**  
Master array operations: create arrays from multiple columns (`array()`), test element membership (`array_contains()`), retrieve element by index with 1-based and negative indexing (`element_at`), sort array elements (`array_sort`), and compute array sizes.

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
        .appName("UseCase_013_Array_Creation_Element_Extraction_and_Array_Search_Functions")
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
user_media = [
    ("U-101", ["Sci-Fi", "Action", "Thriller"], ["HD", "Dolby Atmos", "4K"]),
    ("U-102", ["Romance", "Comedy"], ["SD"]),
    ("U-103", ["Documentary"], ["HD", "Subtitles"]),
    ("U-104", ["Action", "Anime", "Fantasy", "Adventure"], ["4K", "HDR10"])
]

schema = "user_id STRING, genres ARRAY<STRING>, stream_features ARRAY<STRING>"
df_users = spark.createDataFrame(user_media, schema)
df_users.show(truncate=False)

# --- Execution Step ---
df_array_ops = df_users.withColumn(
    "genre_count", F.size(F.col("genres"))
).withColumn(
    "likes_action", F.array_contains(F.col("genres"), "Action")
).withColumn(
    "primary_genre", F.element_at(F.col("genres"), 1)  # 1-based index: first
).withColumn(
    "last_genre", F.element_at(F.col("genres"), -1)     # negative index: last
).withColumn(
    "sorted_genres", F.array_sort(F.col("genres"))
)

df_array_ops.select("user_id", "genre_count", "likes_action", "primary_genre", "last_genre", "sorted_genres").show(truncate=False)

# --- Execution Step ---
# Simulate secondary recommendations array
df_with_recs = df_users.withColumn("recommended_genres", F.array(F.lit("Action"), F.lit("Mystery"), F.lit("Sci-Fi")))

df_sets = df_with_recs.withColumn(
    "all_genres_union", F.array_distinct(F.array_union(F.col("genres"), F.col("recommended_genres")))
).withColumn(
    "common_genres", F.array_intersect(F.col("genres"), F.col("recommended_genres"))
)

df_sets.select("user_id", "all_genres_union", "common_genres").show(truncate=False)
```

**Enterprise Use Case:**  
A digital streaming platform logs user engagement tags, genre preferences, and content playlists as arrays. Marketing analysts need to inspect array length, test for specific genre membership, and extract primary tags.

**Architectural Invariants & Gotchas:**
- Spark represents ArrayType as native columnar array buffers in Tungsten memory. `element_at` uses 1-based indexing for positive indices (1 = first element) and -1 for the last element, differing from standard Python 0-based indexing.

---

### Q2.20: #014 Spark Higher-Order Functions: Transform, Filter and Aggregate (Complex Data Types & Nested Structures Handling)

**Question & Concept Overview:**  
Implement Spark 3+ Higher-Order Functions directly on arrays: `transform` (lambda mapping), `filter` (predicate filtering), `aggregate` (reduce/fold), `exists`, and `forall` without incurring expensive explode-and-groupBy shuffles.

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
        .appName("UseCase_014_Spark_Higher_Order_Functions_Transform_Filter_and_Aggregate")
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
loyalty_data = [
    ("ACC-1", [150, 45, 800, 20, 310]),
    ("ACC-2", [50, 25, 10]),
    ("ACC-3", [1200, 450, 90, 850]),
    ("ACC-4", [5, 12, 18])
]

df_loyalty = spark.createDataFrame(loyalty_data, ["account_id", "point_events"])
df_loyalty.show(truncate=False)

# --- Execution Step ---
# 1. Transform: add 10% bonus to all points: x -> cast(x * 1.10 as int)
df_transformed = df_loyalty.withColumn(
    "bonus_points",
    F.expr("transform(point_events, x -> cast(x * 1.10 AS INT))")
)

# 2. Filter: keep only significant transactions >= 50
df_filtered = df_transformed.withColumn(
    "significant_events",
    F.expr("filter(point_events, x -> x >= 50)")
)

df_filtered.show(truncate=False)

# --- Execution Step ---
# Aggregate: sum all points starting from initial accumulator 0: (acc, x) -> acc + x
df_agg = df_filtered.withColumn(
    "total_points",
    F.expr("aggregate(point_events, 0, (acc, x) -> acc + x)")
).withColumn(
    "has_super_bonus",
    F.expr("exists(point_events, x -> x >= 500)")
).withColumn(
    "all_positive",
    F.expr("forall(point_events, x -> x > 0)")
)

df_agg.select("account_id", "point_events", "total_points", "has_super_bonus", "all_positive").show(truncate=False)
```

**Enterprise Use Case:**  
A fintech loyalty platform tracks customer reward points earned across multiple merchant transactions in an array. The team needs to adjust point values, filter out expired promo points, and sum net balances without exploding rows.

**Architectural Invariants & Gotchas:**
- Higher-order functions run inline lambda expressions inside the JVM on array elements without flattening the DataFrame. Exploding an array multiplies the row count and requires a subsequent `groupBy()` shuffle to reconstitute the row, causing 10x-100x slower execution.

---

### Q2.21: #015 Exploding and Flattening Arrays: explode vs explode_outer and posexplode (Complex Data Types & Nested Structures Handling)

**Question & Concept Overview:**  
Compare `explode()` vs `explode_outer()` to prevent data loss on empty/null arrays. Use `posexplode()` to capture element sequence positions (e.g. flight leg order 0, 1, 2). Flatten multi-dimensional nested arrays (`flatten()`).

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
        .appName("UseCase_015_Exploding_and_Flattening_Arrays_explode_vs_explode_outer_and_posexplode")
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
bookings = [
    ("BKG-001", "Delta", ["ATL", "JFK", "LHR"], ["Alice", "Bob"]),
    ("BKG-002", "United", ["ORD", "FRA"], ["Charlie"]),
    ("BKG-003", "Lufthansa", ["MUC"], []),          # Empty passenger array
    ("BKG-004", "British Airways", ["LHR", "SIN"], None) # Null passenger array
]

schema = "booking_id STRING, airline STRING, flight_legs ARRAY<STRING>, passengers ARRAY<STRING>"
df_bookings = spark.createDataFrame(bookings, schema)
df_bookings.show(truncate=False)

# --- Execution Step ---
# Standard explode: drops rows with empty or null arrays
df_standard = df_bookings.select("booking_id", "airline", F.explode("passengers").alias("passenger"))
print(f"Standard explode row count: {df_standard.count()} (BKG-003 and BKG-004 dropped!)")
df_standard.show()

# explode_outer: retains parent row with null
df_outer = df_bookings.select("booking_id", "airline", F.explode_outer("passengers").alias("passenger"))
print(f"explode_outer row count: {df_outer.count()} (Preserves all bookings!)")
df_outer.show()

# --- Execution Step ---
# posexplode generates leg_sequence and airport
df_legs = df_bookings.select(
    "booking_id",
    "airline",
    F.posexplode("flight_legs").alias("leg_seq", "airport")
)
df_legs.show()

# Flatten 2D nested arrays: [[A, B], [C, D]] -> [A, B, C, D]
df_nested_arr = spark.createDataFrame([("TEST", [["A", "B"], ["C", "D", "E"]])], ["id", "matrix"])
df_flat = df_nested_arr.withColumn("flat_matrix", F.flatten("matrix"))
df_flat.show(truncate=False)
```

**Enterprise Use Case:**  
A flight booking aggregator normalizes JSON payloads of airline bookings. Bookings contain lists of passengers and stops. Some bookings have empty passenger lists (infant reservation hold) that must not be accidentally dropped during normalization.

**Architectural Invariants & Gotchas:**
- Standard `explode()` drops rows where the array is empty or NULL, leading to silent data loss. `explode_outer()` emits a row with NULL for the exploded element, preserving the parent record. `posexplode()` generates both position (zero-based index) and element.

---

### Q2.22: #016 Struct Creation, Nested Field Access and Updating Sub-Fields (Complex Data Types & Nested Structures Handling)

**Question & Concept Overview:**  
Create nested `StructType` columns using `struct()`. Access inner fields via dot notation and `col.getField()`. Demonstrate updating a single nested field inside a struct in PySpark 3.1+ without manually re-specifying all sibling fields.

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
        .appName("UseCase_016_Struct_Creation_Nested_Field_Access_and_Updating_Sub_Fields")
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
customers_flat = [
    ("C-01", "Alice", "Smith", "123 Elm St", "Austin", "TX", "78701", 85000),
    ("C-02", "Bob", "Jones", "456 Oak Ave", "Seattle", "WA", "98101", 92000)
]

cols = ["id", "first_name", "last_name", "street", "city", "state", "zip", "salary"]
df_flat = spark.createDataFrame(customers_flat, cols)

# Bundle into nested structs
df_nested = df_flat.select(
    F.col("id"),
    F.struct(F.col("first_name"), F.col("last_name")).alias("name"),
    F.struct(F.col("street"), F.col("city"), F.col("state"), F.col("zip")).alias("address"),
    F.col("salary")
)

df_nested.printSchema()
df_nested.show(truncate=False)

# --- Execution Step ---
# Accessing nested fields
df_selected = df_nested.select(
    "id",
    "name.last_name",
    F.col("address.city").alias("home_city"),
    F.col("address").getField("state").alias("home_state")
)

df_selected.show()

# --- Execution Step ---
# Update city to uppercase and add a new nested field country
df_updated_struct = df_nested.withColumn(
    "address",
    F.col("address")
     .withField("city", F.upper(F.col("address.city")))
     .withField("country", F.lit("USA"))
)

df_updated_struct.printSchema()
df_updated_struct.select("id", "address").show(truncate=False)
```

**Enterprise Use Case:**  
A customer identity platform groups personal attributes, contact points, and address details into nested structs for clean schema organization. Downstream systems need to query specific sub-fields and update an address without reconstructing the entire object.

**Architectural Invariants & Gotchas:**
- StructType is a compound type that stores multiple typed named fields together in memory. In earlier Spark versions, updating one nested field required re-creating the entire struct. Spark 3.1+ introduced `withField()` and `dropFields()` on Struct columns to manipulate nested fields cleanly.

---

### Q2.23: #018 MapType Creation, Key Extraction and Dynamic Properties (Complex Data Types & Nested Structures Handling)

**Question & Concept Overview:**  
Master MapType operations: construct maps from column pairs (`create_map()`), extract keys (`map_keys()`), extract values (`map_values()`), build maps from paired arrays (`map_from_arrays()`), and query dynamic attributes safely without null pointer exceptions.

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
        .appName("UseCase_018_MapType_Creation_Key_Extraction_and_Dynamic_Properties")
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
# Build map using paired key-value lists
campaign_data = [
    ("EV-1", ["utm_source", "utm_campaign", "device"], ["google", "black_friday", "mobile"]),
    ("EV-2", ["utm_source", "experiment_group"], ["facebook", "variant_b"]),
    ("EV-3", ["utm_source", "utm_medium", "utm_campaign"], ["linkedin", "cpc", "q4_enterprise"])
]

df_raw_campaigns = spark.createDataFrame(campaign_data, ["event_id", "param_keys", "param_vals"])

# Convert parallel arrays into MapType
df_mapped = df_raw_campaigns.withColumn(
    "attributes",
    F.map_from_arrays(F.col("param_keys"), F.col("param_vals"))
)

df_mapped.printSchema()
df_mapped.select("event_id", "attributes").show(truncate=False)

# --- Execution Step ---
# Querying specific keys
df_extracted = df_mapped.withColumn(
    "source", F.col("attributes")["utm_source"]
).withColumn(
    "campaign", F.coalesce(F.col("attributes")["utm_campaign"], F.lit("ORGANIC"))
).withColumn(
    "all_keys", F.map_keys(F.col("attributes"))
).withColumn(
    "all_values", F.map_values(F.col("attributes"))
)

df_extracted.select("event_id", "source", "campaign", "all_keys").show(truncate=False)

# --- Execution Step ---
df_kv_pairs = df_mapped.select(
    "event_id",
    F.explode("attributes").alias("attr_key", "attr_value")
)

df_kv_pairs.show()
```

**Enterprise Use Case:**  
An advertising technology platform captures custom user ad-tracking attributes (cookies, campaign IDs, UTM tags, experiment variants) as dynamic key-value pairs where keys vary across partners and cannot be fixed into a static schema.

**Architectural Invariants & Gotchas:**
- MapType represents key-value collections with arbitrary keys of identical data type. Unlike StructType (where field names are fixed in schema metadata), MapType allows dynamic runtime keys, making it ideal for sparse key-value attributes.

---

### Q2.24: #019 Parsing Raw JSON Payloads: from_json, to_json and schema_of_json (Complex Data Types & Nested Structures Handling)

**Question & Concept Overview:**  
Master JSON string handling: dynamically infer schema using `schema_of_json()`, parse stringified JSON into structured structs using `from_json()`, serialize DataFrames back to JSON using `to_json()`, and handle schema evolution options.

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
        .appName("UseCase_019_Parsing_Raw_JSON_Payloads_from_json_to_json_and_schema_of_json")
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
kafka_stream = [
    ("K-001", '{"order_id": "ORD-1", "customer": {"id": "C-10", "email": "c10@mail.com"}, "amount": 149.99, "tags": ["sale", "prime"]}'),
    ("K-002", '{"order_id": "ORD-2", "customer": {"id": "C-20", "email": "c20@mail.com"}, "amount": 89.50, "tags": ["clearance"]}'),
    ("K-003", '{"order_id": "ORD-3", "customer": {"id": "C-30", "email": "c30@mail.com"}, "amount": 420.00, "tags": []}')
]

df_kafka = spark.createDataFrame(kafka_stream, ["kafka_offset", "payload"])
df_kafka.show(truncate=False)

# --- Execution Step ---
# 1. Infer sample DDL schema using schema_of_json
sample_json = df_kafka.select("payload").first()[0]
inferred_ddl = spark.range(1).select(F.schema_of_json(F.lit(sample_json))).first()[0]
print(f"Inferred Schema DDL: {inferred_ddl}")

# 2. Parse using explicit StructType schema for production safety
order_schema = T.StructType([
    T.StructField("order_id", T.StringType(), True),
    T.StructField("customer", T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("email", T.StringType(), True)
    ]), True),
    T.StructField("amount", T.DoubleType(), True),
    T.StructField("tags", T.ArrayType(T.StringType()), True)
])

df_parsed = df_kafka.withColumn("data", F.from_json(F.col("payload"), order_schema))
df_parsed.printSchema()
df_parsed.select("kafka_offset", "data.order_id", "data.customer.email", "data.amount", "data.tags").show(truncate=False)

# --- Execution Step ---
df_re_serialized = df_parsed.select(
    "kafka_offset",
    F.to_json("data").alias("exported_json")
)

df_re_serialized.show(truncate=False)
```

**Enterprise Use Case:**  
A payment gateway streams raw JSON transaction events via Apache Kafka or cloud storage. Each record arrives as a single raw string column containing nested transaction data that must be parsed into typed columns.

**Architectural Invariants & Gotchas:**
- `from_json()` parses JSON strings inside JVM tasks using Jackson parser, avoiding Python serialization. Specifying an explicit schema avoids costly full-table scans to infer JSON structure.

---

### Q2.25: #020 Handling Corrupted & Malformed JSON: Permissive vs DropMalformed vs FailFast (Complex Data Types & Nested Structures Handling)

**Question & Concept Overview:**  
Compare Spark's JSON read modes: `PERMISSIVE`, `DROPMALFORMED`, and `FAILFAST`. Configure the `columnNameOfCorruptRecord` option to capture raw malformed strings into a `_corrupt_record` quarantine column for compliance review.

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
        .appName("UseCase_020_Handling_Corrupted_and_Malformed_JSON_Permissive_vs_DropMalformed_vs_FailFast")
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
bad_json_lines = [
    '{"id": "EVT-1", "user": "alice", "score": 95}',
    '{"id": "EVT-2", "user": "bob", "score": INVALID_NUMBER}', # Malformed token
    '{"id": "EVT-3", "user": "charlie", "score": 82}',
    'NOT_EVEN_JSON_PAYLOAD',                                      # Completely unparseable
    '{"id": "EVT-4", "user": "diana", "score": 100}'
]

# Spark Connect-safe: avoid spark.sparkContext.parallelize(); parse JSON payloads via from_json
df_raw = spark.createDataFrame([(line,) for line in bad_json_lines], ["raw_json"])

# --- Execution Step ---
payload_schema = T.StructType([
    T.StructField("id", T.StringType(), True),
    T.StructField("user", T.StringType(), True),
    T.StructField("score", T.IntegerType(), True),
])

df_permissive = (
    df_raw
    .withColumn("parsed", F.from_json(F.col("raw_json"), payload_schema, {"mode": "PERMISSIVE"}))
    .withColumn("_corrupt_record", F.when(F.col("parsed").isNull(), F.col("raw_json")))
    .select("parsed.*", "_corrupt_record")
)

print("Ingested Records with Quarantine Column:")
df_permissive.show(truncate=False)

# --- Execution Step ---
df_clean = df_permissive.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
df_dead_letter = df_permissive.filter(F.col("_corrupt_record").isNotNull()).select("_corrupt_record")

print(f"Clean Records: {df_clean.count()}")
df_clean.show()

print(f"Corrupted Dead-Letter Records: {df_dead_letter.count()}")
df_dead_letter.show(truncate=False)
```

**Enterprise Use Case:**  
A public data ingestion pipeline scrapes third-party web endpoints. Up to 5% of incoming payloads contain syntax errors (truncated braces, mismatched quotes, non-JSON tokens). The pipeline must never crash and must capture all corrupted lines into an audit column.

**Architectural Invariants & Gotchas:**
- `PERMISSIVE` is Spark's default mode: corrupted fields evaluate to NULL and the unparseable string is diverted to `_corrupt_record`. `FAILFAST` halts execution immediately upon encountering syntax errors. `DROPMALFORMED` silently drops damaged records.

---
