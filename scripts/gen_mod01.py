"""
Module 01: Core PySpark Foundations & DataFrame API (Notebooks 001 - 012)
"""

import re
from pathlib import Path
from notebook_builder import build_notebook, write_notebook

MODULE_FOLDER = "01_core_dataframe_api"
MODULE_NAME = "Module 01: Core PySpark Foundations & DataFrame API"

def sanitize_filename(nb_id: str, title: str) -> str:
    cleaned = title.lower().replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return f"{nb_id}_{cleaned}.ipynb"

def generate(base_dir: Path):
    out_dir = base_dir / "notebooks" / MODULE_FOLDER
    out_dir.mkdir(parents=True, exist_ok=True)

    notebooks = [
        {
            "nb_id": "001",
            "title": "SparkSession Initialization & DataFrame Creation Strategies",
            "difficulty": "Beginner",
            "yt_ref": "Video 04: A Tour Around Databricks Workspace & Video 06: Master Databricks Notebooks",
            "business_scenario": "A digital commerce platform needs to ingest customer profile extracts from multiple legacy sources (in-memory lists, Python dictionaries, Pandas exports, and RDDs) into a unified PySpark DataFrame with strict schema validation.",
            "problem_statement": "Demonstrate the creation of a resilient SparkSession, configuring custom execution properties, and creating DataFrames using: (1) Programmatic RDD with StructType schema, (2) Python dictionaries/tuples with DDL string schema, (3) Pandas DataFrame conversion with PyArrow optimization, and (4) Empty DataFrames with explicit schemas for pipeline initialization.",
            "architectural_notes": "SparkSession serves as the unified entry point introduced in Spark 2.0, encapsulating SparkContext, SQLContext, and HiveContext. In Databricks, the SparkSession variable `spark` is pre-instantiated. When creating DataFrames from Python collections, PySpark serializes Python objects to Java Virtual Machine (JVM) objects via Py4J. Enabling Arrow (`spark.sql.execution.arrow.pyspark.enabled`) accelerates Pandas-to-Spark conversions by eliminating per-row serialization overhead.",
            "sections": [
                {
                    "title": "Strategy 1: Programmatic Schema with StructType and Tuples",
                    "explanation": "Explicit StructType schemas prevent costly schema inference passes over distributed data and ensure strict type validation.",
                    "code": """# 1. Define explicit StructType schema
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
df_customers.show(truncate=False)"""
                },
                {
                    "title": "Strategy 2: DDL-Formatted Schema String",
                    "explanation": "DDL strings provide a concise, readable alternative to nested StructType declarations while retaining full type safety.",
                    "code": """# Define schema using SQL DDL syntax
ddl_schema = "order_id STRING, customer_id STRING, order_amount DOUBLE, order_status STRING, item_count INT"

raw_orders = [
    ("ORD-901", "CUST-1001", 145.20, "COMPLETED", 3),
    ("ORD-902", "CUST-1002", 49.99, "PENDING", 1),
    ("ORD-903", "CUST-1001", 310.00, "COMPLETED", 5),
    ("ORD-904", "CUST-1004", 1250.40, "SHIPPED", 8)
]

df_orders = spark.createDataFrame(raw_orders, schema=ddl_schema)
df_orders.show()"""
                },
                {
                    "title": "Strategy 3: High-Performance Pandas Conversion via PyArrow",
                    "explanation": "When working with data scientist outputs or REST API responses, converting Pandas DataFrames using Apache Arrow offers dramatic speedups.",
                    "code": """import pandas as pd
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
df_products.show()"""
                },
                {
                    "title": "Verification & Pipeline Readiness Check",
                    "explanation": "Assert non-empty DataFrames, column integrity, and schema compliance before feeding downstream medallion pipelines.",
                    "code": """# Verification assertions
assert df_customers.count() == 5, "Customer count mismatch"
assert "loyalty_tier" in df_customers.columns, "Missing loyalty_tier column"
assert df_orders.filter(F.col("order_status") == "COMPLETED").count() == 2, "Completed orders mismatch"

print("All DataFrame creation strategies and assertions verified successfully!")"""
                }
            ],
            "key_takeaways": [
                "Always supply explicit StructType or DDL schemas in production to avoid schema inference performance penalties.",
                "Enable Arrow (`spark.sql.execution.arrow.pyspark.enabled`) for zero-copy memory transfers between Pandas and PySpark.",
                "Spark DataFrames are lazily evaluated representations of Catalyst logical plans—`createDataFrame` only builds the plan until an action is triggered."
            ],
            "interview_questions": [
                "What happens under the hood when you omit a schema in `spark.createDataFrame(data)`?",
                "How does Apache Arrow optimize PySpark-to-Pandas serialization compared to default Py4J pickling?",
                "What is the difference between `master('local[*]')` and cluster mode execution when configuring a SparkSession?"
            ]
        },
        {
            "nb_id": "002",
            "title": "Schema Enforcement, Safe Type Casting & Try-Cast Handling",
            "difficulty": "Beginner",
            "yt_ref": "Video 12: Debugging Code in Databricks & Video 17: Lakeflow Declarative Pipelines",
            "business_scenario": "A payments processor receives raw transaction dumps where numeric amounts contain currency symbols, timestamps are in non-standard formats, and corrupted text entries cause pipeline aborts on type casting.",
            "problem_statement": "Implement resilient schema transformations: safely cast string columns to numeric/date types, handle malformed rows using `try_cast` (or regex sanitization), and separate valid records from corrupted data without halting execution.",
            "architectural_notes": "In ANSI SQL mode (`spark.sql.ansi.enabled = true`), standard `.cast()` throws runtime exceptions on invalid values (e.g. casting 'INVALID' to INT). Using `try_cast` or `when` expressions ensures invalid values convert to NULL rather than failing long-running batch jobs.",
            "sections": [
                {
                    "title": "Mocking Raw Unclean Ingestion Feed",
                    "explanation": "We simulate an external vendor feed with varied formatting, currency signs, and corrupt values.",
                    "code": """raw_feed = [
    ("TXN-001", "$1,450.50", "2023-10-01 10:30:00", "APPROVED"),
    ("TXN-002", "€320.00", "01/10/2023 11:15", "APPROVED"),
    ("TXN-003", "CORRUPTED_AMT", "2023-10-02", "FAILED"),
    ("TXN-004", "$8,900.75", "2023-10-03 14:00:00", "APPROVED"),
    ("TXN-005", "N/A", "INVALID_DATE", "PENDING")
]

df_raw = spark.createDataFrame(raw_feed, ["txn_id", "raw_amount", "raw_timestamp", "status"])
df_raw.show(truncate=False)"""
                },
                {
                    "title": "Sanitization & Resilient Type Casting",
                    "explanation": "Strip currency symbols, commas, and employ `regexp_replace` paired with safe casting.",
                    "code": """# 1. Cleanse raw_amount by stripping non-numeric characters (except period)
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

df_typed.show(truncate=False)"""
                },
                {
                    "title": "Quarantine Corrupted Records vs Clean Silver Stream",
                    "explanation": "Split data into valid records and a quarantine/dead-letter dataset for investigation.",
                    "code": """# Valid records have both parsed amount and parsed timestamp
df_valid = df_typed.filter(F.col("clean_amount").isNotNull() & F.col("txn_timestamp").isNotNull())
df_quarantine = df_typed.filter(F.col("clean_amount").isNull() | F.col("txn_timestamp").isNull())

print(f"Valid Silver Records: {df_valid.count()}")
df_valid.show(truncate=False)

print(f"Quarantined Dead-Letter Records: {df_quarantine.count()}")
df_quarantine.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "In Spark 3.2+, `F.expr('try_cast(col AS TYPE)')` safely converts invalid strings to NULL without raising runtime exceptions.",
                "Always quarantine records where mission-critical keys or metrics fail type validation.",
                "Combine `F.coalesce` with multiple date format strings to parse heterogeneous date inputs in a single distributed pass."
            ],
            "interview_questions": [
                "What is the impact of `spark.sql.ansi.enabled` on type casting in Spark 3.x?",
                "How does `try_cast` differ from standard `cast` in terms of pipeline failure recovery?",
                "How do you design a Silver layer to capture unparseable rows without failing the entire stream?"
            ]
        },
        {
            "nb_id": "003",
            "title": "Column Manipulation, Batch Addition & Projection Pruning",
            "difficulty": "Beginner",
            "yt_ref": "Video 06: Master Databricks Notebooks & Video 29: Productivity Hacks",
            "business_scenario": "A SaaS telemetry service needs to enrich raw session events with calculated duration, derived flags, normalized client identifiers, and drop redundant legacy headers without incurring multiple Catalyst plan rebuilds.",
            "problem_statement": "Compare single `withColumn` chaining against Spark 3.3+ `withColumns` batch operations. Rename multiple columns dynamically using dictionary mapping, drop unwanted columns, and explain projection pruning optimizations.",
            "architectural_notes": "Chaining dozens of `.withColumn()` calls produces a deep Catalyst logical plan tree that can drastically increase driver planning and compilation time. Using `withColumns` (or a single `select` projection with all expressions) flattens the project node into a single Catalyst Project operation.",
            "sections": [
                {
                    "title": "Mocking Telemetry Events",
                    "explanation": "Create a dataset of client telemetry logs with raw attributes.",
                    "code": """telemetry_data = [
    ("EVT-1", "user_101", "mobile_app", "iOS", 120, 2, "2023-11-01"),
    ("EVT-2", "user_102", "web_browser", "Chrome", 450, 8, "2023-11-01"),
    ("EVT-3", "user_103", "mobile_app", "Android", 30, 1, "2023-11-01"),
    ("EVT-4", "user_104", "api_client", "Postman", 600, 15, "2023-11-01")
]

columns = ["event_id", "user_id", "client_type", "client_os", "duration_seconds", "actions_count", "event_date"]
df_telemetry = spark.createDataFrame(telemetry_data, columns)
df_telemetry.show()"""
                },
                {
                    "title": "High-Performance Batch Column Addition (withColumns)",
                    "explanation": "In Spark 3.3+, `withColumns` accepts a dictionary of column names to expressions, constructing a single Project plan.",
                    "code": """# Batch enrichments using withColumns
enrichments = {
    "duration_minutes": F.round(F.col("duration_seconds") / 60.0, 2),
    "is_high_engagement": F.when((F.col("duration_seconds") > 300) & (F.col("actions_count") > 5), True).otherwise(False),
    "platform_tag": F.concat_ws("::", F.upper(F.col("client_type")), F.col("client_os")),
    "actions_per_minute": F.round(F.col("actions_count") / (F.col("duration_seconds") / 60.0), 2)
}

df_enriched = df_telemetry.withColumns(enrichments)
df_enriched.show(truncate=False)"""
                },
                {
                    "title": "Dynamic Column Renaming via Dictionary Mapping",
                    "explanation": "Rename multiple fields programmatically using `toDF` or comprehension projections.",
                    "code": """rename_map = {
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
df_final.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Avoid chaining 20+ `.withColumn()` calls in loops; use `withColumns()` (Spark 3.3+) or `.select()` projections.",
                "Projection pruning ensures Catalyst only reads and transmits requested columns from Parquet/Delta storage.",
                "`withColumnRenamed()` is a metadata operation that doesn't trigger shuffles."
            ],
            "interview_questions": [
                "Why is chaining many `.withColumn()` calls considered an anti-pattern in PySpark?",
                "How does Catalyst Optimizer optimize a single `.select()` containing multiple expressions compared to multiple `.withColumn()` steps?",
                "What is projection pruning and how does it leverage columnar storage like Parquet/Delta?"
            ]
        },
        {
            "nb_id": "004",
            "title": "Filtering, Multi-Condition Predicates & Pushdown Analysis",
            "difficulty": "Beginner",
            "yt_ref": "Video 12: Debugging Code in Databricks & Video 23: Interview Questions",
            "business_scenario": "A logistics fleet management company needs to identify critical alerts: vehicles with engine temperatures exceeding safe thresholds, low tire pressure, or battery degradation across specific geographic territories.",
            "problem_statement": "Build complex multi-condition filters using bitwise operators (`&`, `|`, `~`), evaluate `isin()`, string regex matching (`rlike`), and inspect the Catalyst physical plan to verify predicate pushdown into the file reader.",
            "architectural_notes": "Predicate pushdown allows the storage layer (Parquet/Delta) to skip entire row groups and data files using file-level min/max statistics without deserializing data into the JVM heap. Complex functions wrapped around columns can prevent pushdown, forcing full table scans.",
            "sections": [
                {
                    "title": "Creating Fleet Telemetry Records",
                    "explanation": "Prepare sensor readings from IoT vehicle diagnostics.",
                    "code": """telemetry = [
    ("V-01", "North America", 102.5, 32.0, 94.0, "WARN_TEMP"),
    ("V-02", "Europe", 88.0, 33.5, 98.0, "NORMAL"),
    ("V-03", "North America", 115.0, 26.0, 65.0, "CRITICAL_PRESSURE"),
    ("V-04", "Asia Pacific", 92.5, 31.0, 45.0, "WARN_BATTERY"),
    ("V-05", "Europe", 110.2, 27.5, 50.0, "CRITICAL_TEMP_PRESSURE"),
    ("V-06", "Latin America", 85.0, 34.0, 99.0, "NORMAL")
]

schema = "vehicle_id STRING, region STRING, engine_temp DOUBLE, tire_pressure DOUBLE, battery_pct DOUBLE, alert_code STRING"
df_fleet = spark.createDataFrame(telemetry, schema)
df_fleet.show()"""
                },
                {
                    "title": "Executing Multi-Condition Logic",
                    "explanation": "Construct complex boolean predicates using bitwise syntax (`&`, `|`, `~`). Parentheses around each condition are mandatory in PySpark.",
                    "code": """# 1. Vehicles with severe alerts in North America or Europe
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
df_critical.show()"""
                },
                {
                    "title": "Inspecting Catalyst Physical Plan for Predicate Pushdown",
                    "explanation": "Analyze the physical plan to verify Filter operator placement.",
                    "code": """print("=== PHYSICAL PLAN FOR FILTER QUERY ===")
df_severe_alerts.explain(mode="formatted")"""
                }
            ],
            "key_takeaways": [
                "Always wrap individual filter conditions in parentheses `(cond1) & (cond2)` due to Python bitwise operator precedence.",
                "Predicate pushdown relies on keeping column expressions clean—avoid `func(col) == val` which breaks pushdown; use `col == inverse_func(val)`.",
                "`where()` and `filter()` are exact synonyms in the PySpark DataFrame API."
            ],
            "interview_questions": [
                "Why do PySpark filter operations require bitwise operators (`&`, `|`) instead of Python keywords (`and`, `or`)?",
                "How does Parquet row-group statistics enable predicate pushdown?",
                "What is partition pruning versus data skipping via file-level stats?"
            ]
        },
        {
            "nb_id": "005",
            "title": "String Operations, Regex Extraction & Data Cleansing",
            "difficulty": "Beginner",
            "yt_ref": "Video 22: PII Data Masterclass & Video 23: Interview Questions",
            "business_scenario": "A marketing automation system ingests unstructured customer lead notes containing messy phone numbers, email addresses with inconsistent cases, and unformatted address strings.",
            "problem_statement": "Apply string manipulation functions: trimming whitespace, case normalization, regex value extraction (`regexp_extract`), masking sensitive values with `regexp_replace`, and parsing delimited tokens into structured columns.",
            "architectural_notes": "Spark SQL string functions are backed by native UTF-8 binary representations in Apache Spark's Tungsten project, avoiding Java String object allocation overhead.",
            "sections": [
                {
                    "title": "Generating Unclean Lead Data",
                    "explanation": "Simulate raw, unstandardized user inputs.",
                    "code": """leads = [
    ("LEAD-1", "  JOHN.DOE@GMAIL.COM  ", "Phone: (555) 123-4567 | City: New York, NY"),
    ("LEAD-2", "SARAH_CONNOR@skyNET.io", "Cell: 555.987.6543 | City: Los Angeles, CA"),
    ("LEAD-3", "unknown_user", "No contact provided | City: Chicago, IL"),
    ("LEAD-4", "bruce.wayne@WayneEnt.com ", "Direct: +1-555-444-3333 | City: Gotham, NJ")
]

df_leads = spark.createDataFrame(leads, ["lead_id", "raw_email", "raw_contact_str"])
df_leads.show(truncate=False)"""
                },
                {
                    "title": "Sanitizing Emails & Extracting Structured Fields via Regex",
                    "explanation": "Normalize text and extract patterns using regular expressions.",
                    "code": """# 1. Clean email: trim whitespace and lower case
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

df_extracted.select("lead_id", "clean_email", "phone_digits", "city", "state").show(truncate=False)"""
                },
                {
                    "title": "PII Masking on Email & Phone",
                    "explanation": "Mask phone numbers and email usernames for compliance before landing in analytics tables.",
                    "code": """# Mask email username: j***e@domain.com
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

df_masked.select("lead_id", "masked_email", "masked_phone", "city", "state").show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "`regexp_extract(col, pattern, group_index)` extracts capturing groups where group 0 is the full match.",
                "Tungsten's native UTF8String implementation avoids garbage collection overhead during large-scale string operations.",
                "Combine regex cleaning with conditional `when/otherwise` masks to ensure privacy compliance."
            ],
            "interview_questions": [
                "How does Spark's `regexp_extract` handle missing or non-matching patterns?",
                "What is the performance difference between built-in SQL string functions and a custom Python regex UDF?",
                "How does Tungsten memory management optimize string representations in Spark?"
            ]
        },
        {
            "nb_id": "006",
            "title": "Date & Timestamp Mastery: Parsing, Arithmetic & Timezones",
            "difficulty": "Beginner",
            "yt_ref": "Video 17: Lakeflow Declarative Pipelines & Video 20: Finance Domain E2E Project",
            "business_scenario": "A global ride-sharing platform records ride dispatch, pickup, and dropoff events across multiple international timezones. Operations teams require accurate trip duration in minutes, local business hour classifications, and SLA reporting.",
            "problem_statement": "Master datetime transformations: parse timestamps with explicit timezone offsets, convert between UTC and local timezones (`from_utc_timestamp`), perform date arithmetic (`date_add`, `datediff`), and compute precise duration intervals.",
            "architectural_notes": "Spark 3.0 introduced Proleptic Gregorian calendar support by default (`spark.sql.session.timeZone`). Spark stores timestamps internally as UTC epoch microseconds. Using timezone-aware functions ensures consistency across distributed worker nodes regardless of server OS timezone settings.",
            "sections": [
                {
                    "title": "Mocking Global Ride Events",
                    "explanation": "Generate ride event timestamps with varied timezones.",
                    "code": """rides_data = [
    ("RIDE-01", "2023-11-15T08:15:00Z", "2023-11-15T08:42:30Z", "America/New_York"),
    ("RIDE-02", "2023-11-15T14:30:00Z", "2023-11-15T15:05:00Z", "Europe/London"),
    ("RIDE-03", "2023-11-15T22:45:00Z", "2023-11-15T23:25:15Z", "Asia/Tokyo"),
    ("RIDE-04", "2023-11-16T01:10:00Z", "2023-11-16T01:35:00Z", "America/Los_Angeles")
]

df_rides = spark.createDataFrame(rides_data, ["ride_id", "pickup_utc", "dropoff_utc", "tz_city"])
df_rides.show(truncate=False)"""
                },
                {
                    "title": "Parsing Timestamps & Computing Duration",
                    "explanation": "Convert ISO strings to native timestamps and compute duration in seconds and fractional minutes.",
                    "code": """df_parsed = df_rides.withColumn(
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

df_duration.select("ride_id", "pickup_ts", "dropoff_ts", "duration_minutes").show()"""
                },
                {
                    "title": "Converting to Local Time & Extracting Temporal Dimensions",
                    "explanation": "Translate UTC timestamps to local city timezones and derive reporting dimensions.",
                    "code": """df_local = df_duration.withColumn(
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

df_local.select("ride_id", "tz_city", "pickup_local", "pickup_hour", "day_of_week", "is_peak_hour").show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Always store raw ingestion timestamps in UTC to prevent daylight saving ambiguities.",
                "Use `from_utc_timestamp` and `to_utc_timestamp` for timezone shifts rather than manual hour additions.",
                "Calculate sub-minute precision by casting timestamps to `LongType` (epoch seconds) or using `unix_timestamp`."
            ],
            "interview_questions": [
                "How does Apache Spark handle daylight saving time transitions?",
                "What changed between the Legacy Julian/Gregorian calendar and Spark 3.0 Proleptic Gregorian calendar?",
                "How do you calculate the business days between two dates in PySpark without a UDF?"
            ]
        },
        {
            "nb_id": "007",
            "title": "Handling Nulls, NaNs & Imputation Strategies",
            "difficulty": "Beginner",
            "yt_ref": "Video 11: Auto-loader & Video 17: Lakeflow Declarative Pipelines",
            "business_scenario": "A medical diagnostics platform aggregates clinical lab test results. Missing biomarker readings occur due to machine sensor dropouts or patient cancellations. The pipeline must differentiate between true NULLs, NaN values, and impute baseline values.",
            "problem_statement": "Detect and quantify missing values across all columns. Compare `coalesce`, `fillna`/`na.fill`, and `dropna`/`na.drop` with thresholds. Implement statistical imputation (replacing missing numerical values with category medians).",
            "architectural_notes": "In PySpark, `NULL` (unknown value) and `NaN` (IEEE 754 Not-a-Number float) have distinct semantics. `isnan()` only applies to Double/Float columns, while `isNull()` applies to all types. Functions like `count()` ignore NULLs, but `count(*)` counts all rows.",
            "sections": [
                {
                    "title": "Creating Clinical Dataset with Nulls and NaNs",
                    "explanation": "Create sample medical records with missing patient readings.",
                    "code": """clinical_data = [
    ("P-01", "Metabolic", 98.6, 120.0, "NORMAL"),
    ("P-02", "Cardiac", float("nan"), 145.0, "ELEVATED"),
    ("P-03", "Metabolic", None, None, None),
    ("P-04", "Cardiac", 101.2, float("nan"), "CRITICAL"),
    ("P-05", "Metabolic", 97.9, 115.0, "NORMAL"),
    ("P-06", "Neurology", None, 130.0, None)
]

schema = "patient_id STRING, dept STRING, body_temp DOUBLE, systolic_bp DOUBLE, assessment STRING"
df_clinical = spark.createDataFrame(clinical_data, schema)
df_clinical.show()"""
                },
                {
                    "title": "Auditing Missing Values Across All Columns",
                    "explanation": "Compute a comprehensive null and NaN count audit in a single aggregation pass.",
                    "code": """# Build expressions dynamically for all columns
null_audit_exprs = [
    F.count(
        F.when(F.col(c).isNull() | (F.isnan(F.col(c)) if t in ["double", "float"] else F.lit(False)), c)
    ).alias(f"{c}_missing_count")
    for c, t in df_clinical.dtypes
]

df_audit = df_clinical.agg(*null_audit_exprs)
df_audit.show(truncate=False)"""
                },
                {
                    "title": "Threshold Dropping and Imputation",
                    "explanation": "Drop records with too many missing values, then impute departmental averages for remaining gaps.",
                    "code": """# 1. Drop rows with at least 3 null fields
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

df_imputed.select("patient_id", "dept", "systolic_bp", "systolic_bp_imputed", "assessment_clean").show()"""
                }
            ],
            "key_takeaways": [
                "`F.isnan()` applies strictly to Float/Double types, while `F.isNull()` applies universally.",
                "Use `dropna(thresh=k)` to enforce a minimum number of valid non-null columns per row.",
                "`F.coalesce(*cols)` returns the first non-null expression in evaluation order."
            ],
            "interview_questions": [
                "What is the difference between `NULL` and `NaN` in Spark SQL, and how does `count(col)` treat them?",
                "How does `F.coalesce()` differ from `DataFrame.coalesce()` in PySpark?",
                "How do you implement categorical mode imputation in a single Spark transformation pass?"
            ]
        },
        {
            "nb_id": "008",
            "title": "Sorting, Ordering Semantics & Stratified Sampling",
            "difficulty": "Beginner",
            "yt_ref": "Video 13: Databricks Clusters & Video 23: Interview Questions",
            "business_scenario": "A financial auditing team must sample transaction logs for regulatory compliance. The sample must be stratified across risk tiers (Low, Medium, High) to reflect true operational risk distribution without skewing toward high-volume low-risk transactions.",
            "problem_statement": "Implement ordering with explicit null placement (`asc_nulls_first`, `desc_nulls_last`). Compare `sample()` vs `sampleBy()` for stratified sampling across categorical groups, and evaluate execution performance.",
            "architectural_notes": "`orderBy()` and `sort()` trigger a global RangePartitioner shuffle across all executor partitions, which can be expensive on multi-terabyte datasets. `sortWithinPartitions()` sorts locally inside each partition without shuffling.",
            "sections": [
                {
                    "title": "Creating Auditing Dataset",
                    "explanation": "Generate audit logs with risk tiers and financial values.",
                    "code": """import random

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
df_audit_logs.show(5)"""
                },
                {
                    "title": "Multi-Column Sorting with Explicit Null Placement",
                    "explanation": "Sort by risk tier and amount, ensuring null amounts are placed at the end.",
                    "code": """df_sorted = df_audit_logs.orderBy(
    F.col("risk_tier").asc(),
    F.col("amount").desc_nulls_last()
)

print("Top 10 Sorted Records:")
df_sorted.show(10)"""
                },
                {
                    "title": "Stratified Sampling via sampleBy",
                    "explanation": "Extract representative samples per risk tier using exact fractions.",
                    "code": """# Define stratification fractions: sample 10% of LOW, 30% of MEDIUM, 80% of HIGH, 100% of CRITICAL
fractions = {
    "LOW": 0.10,
    "MEDIUM": 0.30,
    "HIGH": 0.80,
    "CRITICAL": 1.00
}

df_stratified = df_audit_logs.sampleBy("risk_tier", fractions=fractions, seed=42)

print("Stratified Sample Distribution:")
df_stratified.groupBy("risk_tier").count().show()"""
                }
            ],
            "key_takeaways": [
                "`orderBy` performs a global sort via RangePartitioner shuffle, while `sortWithinPartitions` performs local memory sorts without shuffle.",
                "`sampleBy()` performs stratified sampling without requiring manual filtering and union steps.",
                "Always specify `seed` in sampling calls when building deterministic test suites or reproducible ML splits."
            ],
            "interview_questions": [
                "What is the difference between `sort()` and `sortWithinPartitions()` in PySpark?",
                "How does Spark's RangePartitioner sample keys to determine partition boundaries during a global sort?",
                "Why can `df.sample(withReplacement=False, fraction=0.1)` produce slightly more or less than 10% of total rows?"
            ]
        },
        {
            "nb_id": "009",
            "title": "Deduplication Strategies: dropDuplicates vs Window Row-Number",
            "difficulty": "Intermediate",
            "yt_ref": "Video 11: Auto-loader & Video 23: Interview Questions",
            "business_scenario": "An e-commerce order ingestion pipeline receives duplicate HTTP webhook payloads. Some duplicates are exact identical records, while others represent updated state payloads for the same order with later timestamps.",
            "problem_statement": "Implement deduplication: (1) `distinct()` for exact row matches, (2) `dropDuplicates([subset])` for primary key deduplication with arbitrary winner selection, and (3) Deterministic deduplication using `row_number()` over Window partitions to retain the latest timestamp record.",
            "architectural_notes": "`dropDuplicates()` uses a HashAggregate under the hood. While efficient, it is non-deterministic regarding which non-key column values survive if not pre-sorted. Window functions (`row_number`) provide 100% deterministic deduplication by explicitly controlling sorting criteria.",
            "sections": [
                {
                    "title": "Generating Webhook Ingestion Duplicates",
                    "explanation": "Create dataset containing both exact duplicates and out-of-order state updates.",
                    "code": """webhook_events = [
    ("ORD-101", "CREATED", "2023-11-20 09:00:00", 120.00),
    ("ORD-101", "PAID",    "2023-11-20 09:05:00", 120.00),
    ("ORD-101", "PAID",    "2023-11-20 09:05:00", 120.00), # Exact duplicate
    ("ORD-102", "CREATED", "2023-11-20 09:10:00", 85.50),
    ("ORD-102", "CANCELLED","2023-11-20 09:12:00", 85.50),
    ("ORD-103", "CREATED", "2023-11-20 09:15:00", 430.00)
]

schema = "order_id STRING, order_status STRING, updated_at STRING, total_amount DOUBLE"
df_webhooks = spark.createDataFrame(webhook_events, schema)
df_webhooks.show()"""
                },
                {
                    "title": "Strategy 1 & 2: distinct() and dropDuplicates()",
                    "explanation": "Demonstrate the limitations of simple deduplication.",
                    "code": """# Distinct drops identical rows
df_distinct = df_webhooks.distinct()
print(f"Original: {df_webhooks.count()} | After distinct: {df_distinct.count()}")

# dropDuplicates on order_id keeps an arbitrary update
df_arbitrary = df_webhooks.dropDuplicates(["order_id"])
print("Arbitrary dropDuplicates (non-deterministic latest status):")
df_arbitrary.show()"""
                },
                {
                    "title": "Strategy 3: Deterministic Window Deduplication",
                    "explanation": "Partition by primary key, order by timestamp descending, and filter where row_number == 1.",
                    "code": """window_spec = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())

df_deterministic = (
    df_webhooks
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print("Deterministic Deduplication (Always Keeps Latest Status):")
df_deterministic.orderBy("order_id").show()"""
                }
            ],
            "key_takeaways": [
                "`distinct()` hashes all columns across the dataset, triggering a shuffle.",
                "`dropDuplicates(subset)` is faster than window functions but does not allow ordering criteria for which duplicate record survives.",
                "For event-driven CDC feeds, always use `row_number().over(Window.partitionBy(pk).orderBy(F.col('ts').desc())) == 1`."
            ],
            "interview_questions": [
                "Why can `dropDuplicates(['id'])` return non-deterministic values for remaining columns?",
                "What is the memory and shuffle difference between `dropDuplicates` and `row_number()` window filtering?",
                "How does Delta Lake `MERGE INTO` replace the need for batch deduplication in medallion Silver tables?"
            ]
        },
        {
            "nb_id": "010",
            "title": "Transformations vs Actions, Lazy Evaluation & Lineage Graph",
            "difficulty": "Intermediate",
            "yt_ref": "Video 02: Databricks Intro & Video 12: Debugging Code in Databricks",
            "business_scenario": "A performance engineering lead must diagnose why a data pipeline fails after running for 40 minutes, only to crash on a lightweight `.count()` action. The team must learn to trace lineage graphs and identify narrow vs wide transformations.",
            "problem_statement": "Demonstrate Spark's lazy evaluation mechanism. Differentiate between Narrow dependencies (map, filter) and Wide dependencies (groupBy, join, distinct). Inspect the RDD execution lineage graph using `toDebugString()` and profile action execution times.",
            "architectural_notes": "Spark constructs a Directed Acyclic Graph (DAG) of stages separated by shuffle boundaries. Narrow transformations execute entirely within the same partition in memory (pipelining). Wide transformations require data shuffling across the network to new executor partitions, creating new stages.",
            "sections": [
                {
                    "title": "Building a Pipeline Lineage Graph",
                    "explanation": "Chain narrow and wide transformations without triggering computation.",
                    "code": """# Generate sequence of 100,000 numbers
df_nums = spark.range(1, 100001, 1, 4)

# Narrow transformation: filter (stays within partition)
df_narrow = df_nums.filter(F.col("id") % 2 == 0).withColumn("squared", F.col("id") * F.col("id"))

# Wide transformation: groupBy and agg (triggers shuffle across partitions)
df_wide = df_narrow.groupBy(F.col("id") % 10).agg(F.sum("squared").alias("sum_squares"))

print("Transformations registered in Catalyst. No execution has occurred yet!")"""
                },
                {
                    "title": "Inspecting RDD Lineage Graph via toDebugString",
                    "explanation": "Inspect how Spark groups operations into stages separated by shuffles.",
                    "code": """print("=== RDD LINEAGE GRAPH (toDebugString) ===")
print(df_wide.rdd.toDebugString().decode("utf-8"))"""
                },
                {
                    "title": "Triggering Actions & Profiling Execution",
                    "explanation": "Measure execution when actions (`count`, `collect`, `take`) trigger Catalyst optimization, stage generation, and task execution.",
                    "code": """import time

start = time.time()
result_count = df_wide.count()
elapsed = time.time() - start

print(f"Action triggered! Result rows: {result_count} (Execution Time: {elapsed:.3f}s)")
df_wide.show()"""
                }
            ],
            "key_takeaways": [
                "Transformations build the logical plan; actions trigger physical job execution and DAG scheduling.",
                "Narrow transformations pipeline within the same JVM task; wide transformations create stage boundaries and write shuffle files.",
                "Understanding the DAG via `rdd.toDebugString()` helps locate unnecessary shuffles before deployment."
            ],
            "interview_questions": [
                "What is the difference between narrow and wide dependencies in Apache Spark?",
                "Why does calling `.show()` take time while defining 10 `.filter()` statements executes instantaneously?",
                "What causes stage boundary creation in a Spark DAG?"
            ]
        },
        {
            "nb_id": "011",
            "title": "Custom PySpark UDFs vs Vectorized Pandas UDFs (PyArrow)",
            "difficulty": "Intermediate",
            "yt_ref": "Video 12: Debugging Code in Databricks & Video 25: Genie Code & Optimization",
            "business_scenario": "A credit scoring model applies complex non-linear mathematical risk adjustments to customer accounts. The current implementation uses standard Python UDFs and causes severe CPU throttling and worker memory spikes.",
            "problem_statement": "Implement the calculation using: (1) Standard Python UDF (`@udf`), (2) Native Spark SQL built-in expressions, and (3) Vectorized Pandas UDF (`@pandas_udf`). Compare execution time, serialization behavior, and memory footprint.",
            "architectural_notes": "Standard Python UDFs serialize JVM row data to a Python worker process row-by-row via Py4J, incurring massive serialization and deserialization overhead and disabling Catalyst optimizations. Vectorized Pandas UDFs use Apache Arrow to transfer columnar batches into Pandas Series, achieving near-native C-speed vectorization.",
            "sections": [
                {
                    "title": "Creating Benchmark Financial Dataset",
                    "explanation": "Prepare 50,000 accounts for algorithmic risk scoring.",
                    "code": """df_accounts = spark.range(1, 50001, 1, 4).select(
    F.col("id").alias("account_id"),
    (F.rand(seed=42) * 800 + 300).alias("credit_score"),
    (F.rand(seed=101) * 50000 + 1000).alias("debt_amount"),
    (F.rand(seed=202) * 100000 + 20000).alias("annual_income")
)

df_accounts.show(5)"""
                },
                {
                    "title": "Method 1: Standard Python UDF (Row-by-Row Serialization)",
                    "explanation": "Define a standard Python function and wrap it with `@F.udf`.",
                    "code": """@F.udf(returnType=T.DoubleType())
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
print(f"Standard UDF Count: {c1} in {t1:.3f} seconds")"""
                },
                {
                    "title": "Method 2: Vectorized Pandas UDF via PyArrow",
                    "explanation": "Define a vectorized Arrow function operating over Pandas Series batches.",
                    "code": """import pandas as pd

@F.pandas_udf(T.DoubleType())
def calculate_risk_vectorized(credit_score: pd.Series, debt: pd.Series, income: pd.Series) -> pd.Series:
    dti = debt / income.replace(0, 1.0)
    score_factor = (850.0 - credit_score) / 550.0
    return ((dti * 0.6) + (score_factor * 0.4)).round(4)

t0 = time.time()
df_res2 = df_accounts.withColumn("risk_vec", calculate_risk_vectorized("credit_score", "debt_amount", "annual_income"))
c2 = df_res2.filter(F.col("risk_vec") > 0.5).count()
t2 = time.time() - t0
print(f"Vectorized Pandas UDF Count: {c2} in {t2:.3f} seconds (Speedup factor: {t1/max(t2, 0.001):.1f}x)")"""
                }
            ],
            "key_takeaways": [
                "Always favor native Spark SQL functions first before writing custom functions.",
                "If custom Python logic is unavoidable, always use `@pandas_udf` instead of standard `@udf`.",
                "Standard UDFs serialize data row-by-row through a Python subprocess, while Pandas UDFs batch data via shared Arrow memory."
            ],
            "interview_questions": [
                "Why are standard Python UDFs considered a major performance bottleneck in PySpark?",
                "How does Apache Arrow enable zero-copy data transfer in Pandas UDFs?",
                "Can Catalyst Optimizer optimize or push down predicates past a Python UDF?"
            ]
        },
        {
            "nb_id": "012",
            "title": "Emulating Databricks DBUtils, Widgets & Secret Scopes Locally",
            "difficulty": "Intermediate",
            "yt_ref": "Video 06: Master Databricks Notebooks, Video 08: Master Secret Scopes & Video 09: DBUtils",
            "business_scenario": "An enterprise migration team develops Databricks pipelines locally inside VS Code or CI/CD test runners before deploying to production workspaces. Code relying on `dbutils.fs`, `dbutils.secrets`, and `dbutils.widgets` crashes outside of Databricks.",
            "problem_statement": "Design a clean, modular Python compatibility wrapper class `MockDBUtils` that provides identical interfaces for `dbutils.fs` (`ls`, `cp`, `mv`, `rm`, `head`, `put`), `dbutils.widgets` (`text`, `dropdown`, `get`), and `dbutils.secrets` (`get`) using environment variables and local disk storage.",
            "architectural_notes": "In Databricks, `dbutils` is a Java/Scala bridge injected into the Python REPL. Writing pipeline code that imports or references `dbutils` directly creates tight vendor lock-in. Wrapping it in a helper or using Databricks Connect decouples code for multi-cloud and local portability.",
            "sections": [
                {
                    "title": "Building the Universal DBUtils Emulation Bridge",
                    "explanation": "Create a drop-in replacement that activates when native `dbutils` is unavailable.",
                    "code": """import os
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
print("Universal DBUtils Emulator initialized successfully!")"""
                },
                {
                    "title": "Testing DBUtils Filesystem Operations",
                    "explanation": "Create, list, read, and delete files using the dbutils interface.",
                    "code": """# 1. Put file in mock DBFS
dbutils.fs.put("/tmp/dbfs_test/config.json", '{"environment": "staging", "batch_size": 5000}')

# 2. Read file back with head
content = dbutils.fs.head("/tmp/dbfs_test/config.json")
print("Read Content:", content)

# 3. List directory
files = dbutils.fs.ls("/tmp/dbfs_test")
for f in files:
    print(f"File found: {f.name} ({f.size} bytes)")

# 4. Clean up
dbutils.fs.rm("/tmp/dbfs_test", recurse=True)"""
                },
                {
                    "title": "Testing Widgets & Secure Secret Retrieval",
                    "explanation": "Pass dynamic parameters and fetch credentials without hardcoding.",
                    "code": """# Configure notebook widgets
dbutils.widgets.dropdown("env", "staging", ["dev", "staging", "prod"], "Target Environment")
dbutils.widgets.text("processing_date", "2023-11-20", "Batch Date")

current_env = dbutils.widgets.get("env")
proc_date = dbutils.widgets.get("processing_date")
api_key = dbutils.secrets.get(scope="finance_scope", key="stripe_token")

print(f"Running pipeline for ENV={current_env}, DATE={proc_date}")
print(f"Retrieved Secret: {api_key[:4]}**** (Masked)")"""
                }
            ],
            "key_takeaways": [
                "Never hardcode credentials in notebooks—use secret scopes via `dbutils.secrets.get()` in Databricks or environment variables locally.",
                "Widgets allow parameterized workflow execution from Databricks Jobs and Airflow DAGs.",
                "Emulating `dbutils` enables seamless local development and automated CI testing outside Databricks."
            ],
            "interview_questions": [
                "What is the difference between `%run` and `dbutils.notebook.run()` when chaining notebooks?",
                "How does Databricks redact secrets retrieved via `dbutils.secrets.get()` in driver output logs?",
                "What are Unity Catalog Volumes and how do they replace legacy DBFS mounts?"
            ]
        }
    ]

    for nb in notebooks:
        nb_json = build_notebook(
            nb_id=nb["nb_id"],
            title=nb["title"],
            module_folder=MODULE_FOLDER,
            module_name=MODULE_NAME,
            difficulty=nb["difficulty"],
            yt_ref=nb["yt_ref"],
            business_scenario=nb["business_scenario"],
            problem_statement=nb["problem_statement"],
            architectural_notes=nb["architectural_notes"],
            sections=nb["sections"],
            key_takeaways=nb["key_takeaways"],
            interview_questions=nb["interview_questions"]
        )
        filename = sanitize_filename(nb["nb_id"], nb["title"])
        write_notebook(out_dir / filename, nb_json)
    print(f"Generated {len(notebooks)} notebooks for {MODULE_NAME}")

if __name__ == "__main__":
    generate(Path(__file__).parent.parent)
