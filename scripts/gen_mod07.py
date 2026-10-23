"""
Module 07: Lakehouse Data Modeling, CDC & SCD Implementation (Notebooks 064 - 073)
"""

import re
from pathlib import Path
from notebook_builder import build_notebook, write_notebook

MODULE_FOLDER = "07_lakehouse_modeling_scd"
MODULE_NAME = "Module 07: Lakehouse Data Modeling, CDC & SCD Implementation"

def sanitize_filename(nb_id: str, title: str) -> str:
    cleaned = title.lower().replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return f"{nb_id}_{cleaned}.py"

def generate(base_dir: Path):
    out_dir = base_dir / "notebooks" / MODULE_FOLDER
    out_dir.mkdir(parents=True, exist_ok=True)

    notebooks = [
        {
            "nb_id": "064",
            "title": "Medallion Architecture: Bronze Ingestion Layer Standards and Audit Trails",
            "difficulty": "Intermediate",
            "business_scenario": "An enterprise data lakehouse ingests raw transactional data from ERP, CRM, and partner APIs. Data must land in an append-only Bronze layer preserving raw formats, capturing metadata lineage, and ensuring raw historical payloads are never lost.",
            "problem_statement": "Implement the Bronze layer of the Medallion Architecture: configure append-only raw landing, add mandatory audit columns (`_ingest_timestamp`, `_source_file_path`, `_batch_id`), enforce schema preservation, and handle schema evolution without rewriting data.",
            "architectural_notes": "The Bronze layer is an append-only, raw historical archive of source data. No cleansing, deduplication, or business logic is applied here. If a downstream pipeline bug corrupts Silver or Gold tables, the entire lakehouse can be recomputed from the Bronze layer.",
            "sections": [
                {
                    "title": "Simulating Inbound Vendor Files",
                    "explanation": "Create raw JSON vendor payloads.",
                    "code": """bronze_path = "/tmp/lakehouse_bronze_064"
if Path(bronze_path).exists():
    shutil.rmtree(bronze_path)

raw_records = [
    '{"vendor_id": "V-01", "invoice_no": "INV-1001", "gross_amount": 1500.0, "status": "SUBMITTED"}',
    '{"vendor_id": "V-02", "invoice_no": "INV-1002", "gross_amount": 420.50, "status": "APPROVED"}'
]

rdd_raw = spark.sparkContext.parallelize(raw_records)
df_raw = spark.read.json(rdd_raw)
df_raw.show()"""
                },
                {
                    "title": "Enriching with Standard Bronze Audit Columns",
                    "explanation": "Add standard enterprise lineage and audit attributes.",
                    "code": """from datetime import datetime

df_bronze = (
    df_raw
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file_path", F.lit("s3://vendor-inbox/invoices/batch_20231120.json"))
    .withColumn("_batch_id", F.lit("BATCH-20231120-001"))
    .withColumn("_raw_payload_checksum", F.sha2(F.to_json(F.struct(df_raw.columns)), 256))
)

df_bronze.printSchema()
df_bronze.show(truncate=False)"""
                },
                {
                    "title": "Landing into Append-Only Delta Bronze Table",
                    "explanation": "Write to Delta table with append mode and verify immutability.",
                    "code": """df_bronze.write.format("delta").mode("append").save(bronze_path)

print("Bronze Table Successfully Written:")
spark.read.format("delta").load(bronze_path).show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Bronze layer is an immutable, append-only copy of raw incoming source data.",
                "Always inject audit metadata: ingestion timestamp, source file path, batch run ID, and payload hash.",
                "Never apply business filters or destructive transformations in Bronze."
            ],
            "interview_questions": [
                "What is the exact purpose of the Bronze layer in Databricks Medallion Architecture?",
                "Why should you never deduplicate or cleanse data directly in the Bronze layer?",
                "How does the Bronze layer protect organizations against catastrophic downstream pipeline bugs?"
            ]
        },
        {
            "nb_id": "065",
            "title": "Medallion Architecture: Silver Transformation, Cleansing and Conformance",
            "difficulty": "Intermediate",
            "business_scenario": "Raw invoices from the Bronze layer contain duplicate entries, unstandardized status codes ('appr', 'APPROVED', 'approved'), missing tax codes, and stringified numeric values. The Silver layer must conform and cleanse these into enterprise-ready records.",
            "problem_statement": "Build the Silver layer transformation: filter and clean raw data, standardize categorical codes, enforce data quality rules (non-negative amounts), cast data to strict primitive types, and execute deterministic deduplication into a conformed Delta Silver table.",
            "architectural_notes": "The Silver layer represents the enterprise-conformed, cleansed version of truth. It standardizes naming conventions, parses strings into structured types, deduplicates records using business keys, and joins disparate Bronze feeds into consolidated entity tables.",
            "sections": [
                {
                    "title": "Reading from Upstream Bronze Layer",
                    "explanation": "Read uncleaned records containing duplicates and messy categories.",
                    "code": """raw_bronze = [
    ("INV-1001", "V-01", "1,500.00", "appr", "2023-11-20 10:00:00"),
    ("INV-1001", "V-01", "1,500.00", "APPROVED", "2023-11-20 10:05:00"), # Duplicate update
    ("INV-1002", "V-02", "420.50", "PENDING", "2023-11-20 10:01:00"),
    ("INV-1003", "V-03", "-50.00", "REJECTED", "2023-11-20 10:02:00")   # Invalid negative invoice!
]

schema = "invoice_no STRING, vendor_id STRING, raw_amount STRING, raw_status STRING, updated_at STRING"
df_bronze_in = spark.createDataFrame(raw_bronze, schema)
df_bronze_in.show()"""
                },
                {
                    "title": "Applying Cleansing, Type Casting and Status Standardization",
                    "explanation": "Standardize uppercase status, clean currency strings, and enforce positive amounts.",
                    "code": """# Clean amount and standardize status
df_cleansed = (
    df_bronze_in
    .withColumn("clean_amount", F.regexp_replace("raw_amount", r"[^\d.]", "").cast(T.DoubleType()))
    .withColumn(
        "standard_status",
        F.when(F.upper(F.col("raw_status")).isin(["APPR", "APPROVED"]), "APPROVED")
         .when(F.upper(F.col("raw_status")).isin(["PEND", "PENDING"]), "PENDING")
         .otherwise(F.upper(F.col("raw_status")))
    )
    # Quality filter: reject invoices with non-positive amounts
    .filter(F.col("clean_amount") > 0)
)

df_cleansed.show()"""
                },
                {
                    "title": "Deterministic Deduplication and Silver Table Landing",
                    "explanation": "Retain only the latest update per invoice_no using window deduplication.",
                    "code": """silver_path = "/tmp/lakehouse_silver_065"
if Path(silver_path).exists():
    shutil.rmtree(silver_path)

win_dedup = Window.partitionBy("invoice_no").orderBy(F.col("updated_at").desc())

df_silver = (
    df_cleansed
    .withColumn("rn", F.row_number().over(win_dedup))
    .filter(F.col("rn") == 1)
    .select(
        "invoice_no",
        "vendor_id",
        F.col("clean_amount").alias("invoice_amount"),
        F.col("standard_status").alias("invoice_status"),
        F.to_timestamp("updated_at").alias("last_updated_at"),
        F.current_timestamp().alias("_silver_processed_at")
    )
)

df_silver.write.format("delta").mode("overwrite").save(silver_path)

print("Conformed Silver Delta Table:")
spark.read.format("delta").load(silver_path).show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Silver layer provides conformed, cleansed, deduplicated data for cross-functional consumption.",
                "Standardize categorical codes and parse strings into strictly typed columns.",
                "Always filter out records violating fundamental business domain rules before landing in Silver."
            ],
            "interview_questions": [
                "What transformations belong in the Silver layer versus the Gold layer?",
                "How do you handle records that fail data quality rules during Silver transformation?",
                "How do you ensure Silver tables maintain low query latency for analytical users?"
            ]
        },
        {
            "nb_id": "066",
            "title": "Medallion Architecture: Gold Business Aggregations and Dimensional Serving",
            "difficulty": "Intermediate",
            "business_scenario": "Executive leadership and Power BI dashboards require pre-aggregated quarterly vendor performance scorecards: total invoiced volume, approval rate percentages, average turnaround time, and vendor tier classifications.",
            "problem_statement": "Build the Gold layer of the Medallion Architecture: read from conformed Silver Delta tables, compute executive business metrics, apply business KPIs and risk score calculations, and write high-performance Gold aggregate tables optimized for BI consumption.",
            "architectural_notes": "The Gold layer consists of project-specific, business-level aggregated tables, star schema dimension and fact tables, or feature store tables. Gold tables are typically optimized with Z-Ordering or Liquid Clustering to ensure sub-second dashboard query response times.",
            "sections": [
                {
                    "title": "Reading from Silver Layer Invoices",
                    "explanation": "Create realistic Silver invoice dataset.",
                    "code": """silver_data = [
    ("INV-1", "V-01", 1500.0, "APPROVED", "2023-10-01"),
    ("INV-2", "V-01", 3200.0, "APPROVED", "2023-10-15"),
    ("INV-3", "V-01", 800.0,  "REJECTED", "2023-10-20"),
    ("INV-4", "V-02", 450.0,  "APPROVED", "2023-10-05"),
    ("INV-5", "V-02", 900.0,  "PENDING",  "2023-10-25"),
    ("INV-6", "V-03", 12500.0,"APPROVED", "2023-10-12")
]

schema = "invoice_no STRING, vendor_id STRING, amount DOUBLE, status STRING, invoice_date STRING"
df_silver_in = spark.createDataFrame(silver_data, schema)
df_silver_in.show()"""
                },
                {
                    "title": "Computing Gold KPI Aggregations and Business Tiers",
                    "explanation": "Calculate approval rate, total volume, and classify vendor risk tiers.",
                    "code": """df_gold_kpis = (
    df_silver_in
    .groupBy("vendor_id")
    .agg(
        F.count("invoice_no").alias("total_invoices_submitted"),
        F.round(F.sum("amount"), 2).alias("total_invoiced_spend"),
        F.round(F.avg("amount"), 2).alias("avg_invoice_value"),
        F.sum(F.when(F.col("status") == "APPROVED", 1).otherwise(0)).alias("approved_invoices_count"),
        F.sum(F.when(F.col("status") == "REJECTED", 1).otherwise(0)).alias("rejected_invoices_count")
    )
    .withColumn(
        "approval_rate_pct",
        F.round((F.col("approved_invoices_count") / F.col("total_invoices_submitted")) * 100, 1)
    )
    .withColumn(
        "vendor_volume_tier",
        F.when(F.col("total_invoiced_spend") >= 10000, "STRATEGIC_TIER_1")
         .when(F.col("total_invoiced_spend") >= 2000, "PREFERRED_TIER_2")
         .otherwise("STANDARD_TIER_3")
    )
)

print("Gold Vendor Performance Scorecard:")
df_gold_kpis.show(truncate=False)"""
                },
                {
                    "title": "Writing to Gold Delta Lake Table with Liquid Clustering",
                    "explanation": "Save to Gold destination ready for BI and semantic metrics.",
                    "code": """gold_path = "/tmp/lakehouse_gold_066"
if Path(gold_path).exists():
    shutil.rmtree(gold_path)

df_gold_kpis.write.format("delta").mode("overwrite").save(gold_path)

print("Gold Table successfully landed. Ready for Power BI / Tableau consumption!")"""
                }
            ],
            "key_takeaways": [
                "Gold tables represent domain-specific business aggregates and dimensional models designed for end-user analytics.",
                "Pre-aggregating KPIs in Gold reduces compute consumption on BI tools and guarantees single-version-of-truth metrics.",
                "Apply clustering (`CLUSTER BY`) on Gold tables based on typical dashboard filter columns."
            ],
            "interview_questions": [
                "Why should BI tools query Gold tables rather than directly querying Silver tables?",
                "How does the Medallion Architecture ensure compliance with GDPR 'Right to be Forgotten' across Bronze, Silver, and Gold?",
                "What is the difference between a Gold table and a materialized view in Databricks?"
            ]
        },
        {
            "nb_id": "067",
            "title": "Distributed Surrogate Key Generation: monotonically_increasing_id vs xxhash64 vs UUIDs",
            "difficulty": "Intermediate",
            "business_scenario": "A lakehouse dimensional model builds customer and account dimension tables. Business analysts require synthetic integer or hash surrogate keys to decouple dimensional records from operational natural keys and enable SCD Type 2 version tracking.",
            "problem_statement": "Evaluate distributed surrogate key generation strategies in PySpark: (1) `monotonically_increasing_id()`, (2) Deterministic cryptographic hashing (`xxhash64` / `sha2`), and (3) Universally unique identifiers (`uuid`). Contrast deterministic replayability, performance, and storage footprint.",
            "architectural_notes": "`monotonically_increasing_id()` generates 64-bit integers where the upper 33 bits represent the partition ID and the lower 31 bits represent the row ID within the partition. While fast, IDs are NOT contiguous and change if the partition count changes. `xxhash64(concat_ws())` produces deterministic 64-bit hash keys that remain identical across pipeline replays.",
            "sections": [
                {
                    "title": "Generating Customer Natural Keys Across Partitions",
                    "explanation": "Create customers distributed across multiple partitions.",
                    "code": """natural_customers = [
    ("NAT-1001", "Alice Smith", "2023-01-10"),
    ("NAT-1002", "Bob Jones", "2023-02-15"),
    ("NAT-1003", "Charlie Day", "2023-03-20"),
    ("NAT-1004", "Diana Prince", "2023-04-25")
]

schema = "natural_key STRING, name STRING, created_at STRING"
df_cust = spark.createDataFrame(natural_customers, schema).repartition(2)
print(f"Partitions: {df_cust.rdd.getNumPartitions()}")"""
                },
                {
                    "title": "Comparing Key Generation Strategies",
                    "explanation": "Implement monotonically_increasing_id, xxhash64, and UUID generation.",
                    "code": """df_keys = (
    df_cust
    # 1. Monotonically increasing ID (Fast, non-contiguous, partition-dependent)
    .withColumn("surrogate_monotonic_id", F.monotonically_increasing_id())
    # 2. Deterministic 64-bit integer hash (Replayable, 100% deterministic, 8 bytes)
    .withColumn("surrogate_xxhash64", F.xxhash64(F.col("natural_key"), F.col("created_at")))
    # 3. MD5/SHA256 string hash (Deterministic, 32/64 character string, higher storage cost)
    .withColumn("surrogate_sha256", F.sha2(F.col("natural_key"), 256))
    # 4. Random UUID (Unique, non-deterministic across pipeline reruns)
    .withColumn("surrogate_uuid", F.expr("uuid()"))
)

df_keys.select("natural_key", "surrogate_monotonic_id", "surrogate_xxhash64", "surrogate_uuid").show(truncate=False)"""
                },
                {
                    "title": "Deterministic Replayability Verification",
                    "explanation": "Verify that xxhash64 produces the exact same key upon re-running the pipeline.",
                    "code": """# Re-evaluating xxhash64 on the same inputs yields identical hash values
df_recheck = df_cust.withColumn("recheck_hash", F.xxhash64(F.col("natural_key"), F.col("created_at")))
diff_count = df_keys.join(df_recheck, on="natural_key").filter(F.col("surrogate_xxhash64") != F.col("recheck_hash")).count()

assert diff_count == 0, "Deterministic hash mismatch detected!"
print("xxhash64 verified as 100% deterministic across distributed runs!")"""
                }
            ],
            "key_takeaways": [
                "`xxhash64(concat_ws('||', *keys))` is the recommended surrogate key strategy for distributed Lakehouses: 64-bit integer, fast, and 100% deterministic.",
                "`monotonically_increasing_id()` generates gaps between partitions and changes if data is repartitioned.",
                "UUIDs are non-deterministic and prevent idempotent pipeline reruns."
            ],
            "interview_questions": [
                "How does `monotonically_increasing_id()` construct its 64-bit integer across partitions?",
                "Why is `xxhash64` preferred over UUIDs for dimensional surrogate keys in Lakehouse architectures?",
                "What is the risk of hash collisions in 64-bit hashes on tables with billions of rows?"
            ]
        },
        {
            "nb_id": "068",
            "title": "Slowly Changing Dimension Type 1 (SCD Type 1) In-Place Overwrites via Delta Merge",
            "difficulty": "Intermediate",
            "business_scenario": "A master customer dimension stores current contact email, phone number, and address. Under business policy, when a customer moves or updates contact details, past values are overwritten immediately—no historical records are retained.",
            "problem_statement": "Implement Slowly Changing Dimension Type 1 (SCD Type 1) using Delta Lake `MERGE INTO`: update existing records in-place with latest attributes, insert newly discovered customers, and maintain deterministic idempotency.",
            "architectural_notes": "SCD Type 1 overwrites old data with new data in-place without maintaining historical versions. In Delta Lake, this is executed efficiently via `DeltaTable.merge()` with `whenMatchedUpdate` and `whenNotMatchedInsert`.",
            "sections": [
                {
                    "title": "Initializing Current Customer Dimension Table (SCD1 Target)",
                    "explanation": "Create target customer dimension table.",
                    "code": """from delta.tables import DeltaTable

scd1_path = "/tmp/delta_dim_customer_scd1_068"
if Path(scd1_path).exists():
    shutil.rmtree(scd1_path)

initial_customers = [
    ("C-01", "Alice Smith", "alice@oldmail.com", "Austin", "2023-01-01"),
    ("C-02", "Bob Jones", "bob@corp.com", "Seattle", "2023-01-01")
]

schema = "cust_id STRING, full_name STRING, email STRING, city STRING, last_updated STRING"
spark.createDataFrame(initial_customers, schema).write.format("delta").mode("overwrite").save(scd1_path)

dt_scd1 = DeltaTable.forPath(spark, scd1_path)
print("Initial Customer Dimension (Target):")
dt_scd1.toDF().show()"""
                },
                {
                    "title": "Preparing Inbound CDC Updates (Source Feed)",
                    "explanation": "Alice updates her email and moves to Denver; Charlie is a new customer.",
                    "code": """incoming_cdc = [
    ("C-01", "Alice Smith", "alice@newmail.com", "Denver", "2023-11-20"), # Overwrite existing
    ("C-03", "Charlie Day", "charlie@bar.com", "Philadelphia", "2023-11-20") # Insert new
]

df_cdc = spark.createDataFrame(incoming_cdc, schema)
print("Incoming Inbound CDC Updates:")
df_cdc.show()"""
                },
                {
                    "title": "Executing SCD Type 1 Delta MERGE INTO",
                    "explanation": "Merge source batch into target table in-place.",
                    "code": """(
    dt_scd1.alias("target")
    .merge(
        source=df_cdc.alias("source"),
        condition="target.cust_id = source.cust_id"
    )
    .whenMatchedUpdate(
        set={
            "full_name": F.col("source.full_name"),
            "email": F.col("source.email"),
            "city": F.col("source.city"),
            "last_updated": F.col("source.last_updated")
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Target Customer Dimension after SCD Type 1 Merge:")
dt_scd1.toDF().orderBy("cust_id").show()"""
                }
            ],
            "key_takeaways": [
                "SCD Type 1 overwrites old attributes in-place, keeping only the latest current state.",
                "Executed atomically in Delta Lake via `whenMatchedUpdate` and `whenNotMatchedInsertAll()`.",
                "Ideal for master data attributes where historical lineage is irrelevant or legally forbidden to store."
            ],
            "interview_questions": [
                "What is the business impact of choosing SCD Type 1 versus SCD Type 2?",
                "How does Delta Lake optimize MERGE operations on tables with millions of rows?",
                "Can an SCD Type 1 table track previous historical states without Time Travel?"
            ]
        },
        {
            "nb_id": "069",
            "title": "Slowly Changing Dimension Type 2 (SCD Type 2) Historical Tracking with Effective Dates",
            "difficulty": "Advanced",
            "business_scenario": "An insurance underwriting platform tracks policyholder residential address history. Premium risk calculations depend strictly on the policyholder's address at the time a claim occurred, requiring complete historical tracking of address changes.",
            "problem_statement": "Implement Slowly Changing Dimension Type 2 (SCD Type 2) in PySpark: maintain `effective_start_date`, `effective_end_date`, and `is_current` active flag columns. When an attribute changes, expire the current record by setting `effective_end_date` and insert a new active record.",
            "architectural_notes": "SCD Type 2 preserves the full history of attribute changes over time. Delta Lake implements SCD2 using a two-pass MERGE pattern or Union technique: (1) Update existing current records to close the effective date window (`is_current = false`), and (2) Insert the new state with `is_current = true` and `effective_end_date = NULL` (or 9999-12-31).",
            "sections": [
                {
                    "title": "Initializing SCD Type 2 Customer Table in Delta",
                    "explanation": "Create target dimension table with surrogate keys and validity dates.",
                    "code": """from delta.tables import DeltaTable

scd2_path = "/tmp/delta_dim_customer_scd2_069"
if Path(scd2_path).exists():
    shutil.rmtree(scd2_path)

initial_scd2 = [
    (1, "C-101", "Alice", "Austin", "2023-01-01", "9999-12-31", True),
    (2, "C-102", "Bob", "Seattle", "2023-01-01", "9999-12-31", True)
]

schema = "sk_id LONG, cust_id STRING, name STRING, city STRING, start_date STRING, end_date STRING, is_current BOOLEAN"
spark.createDataFrame(initial_scd2, schema).write.format("delta").mode("overwrite").save(scd2_path)

dt_scd2 = DeltaTable.forPath(spark, scd2_path)
print("Initial Active SCD Type 2 Dimension:")
dt_scd2.toDF().show()"""
                },
                {
                    "title": "Preparing Inbound Update (Alice moves from Austin to Denver)",
                    "explanation": "Alice moves to Denver on 2023-11-20; Bob remains unchanged.",
                    "code": """new_cdc_feed = [
    ("C-101", "Alice", "Denver", "2023-11-20")
]

df_incoming = spark.createDataFrame(new_cdc_feed, ["cust_id", "name", "city", "change_date"])
df_incoming.show()"""
                },
                {
                    "title": "Executing 2-Stage SCD Type 2 Delta Merge",
                    "explanation": "Expire the previous version and insert the new active record.",
                    "code": """# Step 1: Identify existing records that need expiration
df_current = dt_scd2.toDF().filter(F.col("is_current") == True)

df_to_expire = df_incoming.join(
    df_current,
    (df_incoming["cust_id"] == df_current["cust_id"]) & (df_incoming["city"] != df_current["city"]),
    how="inner"
).select(df_current["sk_id"], df_incoming["change_date"])

# Expire old records: set end_date and is_current = False
for row in df_to_expire.collect():
    dt_scd2.update(
        condition=F.col("sk_id") == row.sk_id,
        set={"end_date": F.lit(row.change_date), "is_current": F.lit(False)}
    )

# Step 2: Insert new active record
max_sk = dt_scd2.toDF().agg(F.max("sk_id")).first()[0] or 0

df_new_inserts = df_incoming.select(
    (F.lit(max_sk) + F.monotonically_increasing_id() + 1).alias("sk_id"),
    "cust_id",
    "name",
    "city",
    F.col("change_date").alias("start_date"),
    F.lit("9999-12-31").alias("end_date"),
    F.lit(True).alias("is_current")
)

df_new_inserts.write.format("delta").mode("append").save(scd2_path)

print("Final SCD Type 2 Dimension Table (Notice Alice's historical record preserved!):")
dt_scd2.toDF().orderBy("cust_id", "start_date").show()"""
                }
            ],
            "key_takeaways": [
                "SCD Type 2 preserves complete historical lineage using effective start date, end date, and `is_current` flags.",
                "Surrogate keys (not natural keys) are mandatory because the same natural key appears across multiple rows.",
                "Queries join fact tables to SCD2 dimensions using point-in-time condition: `fact.date BETWEEN dim.start_date AND dim.end_date`."
            ],
            "interview_questions": [
                "What is the difference between SCD Type 1, SCD Type 2, and SCD Type 3?",
                "How do point-in-time queries join transaction facts with SCD Type 2 dimension tables?",
                "Why can natural keys NOT serve as primary keys in an SCD Type 2 dimension table?"
            ]
        },
        {
            "nb_id": "070",
            "title": "Slowly Changing Dimension Type 3 and Type 4 Multi-Tier Implementation",
            "difficulty": "Intermediate",
            "business_scenario": "A sales commission system tracks territory reassignments. Sales reps occasionally switch regions. Compensation analysts only need to compare sales under the 'current territory' vs 'immediately previous territory', while enterprise audit requires an external historical archive.",
            "problem_statement": "Implement SCD Type 3 (adding `current_territory` and `previous_territory` columns in the active table) and SCD Type 4 (maintaining a fast current-state table paired with a separate append-only historical audit archive table).",
            "architectural_notes": "SCD Type 3 stores previous and current attribute values side-by-side in the same row, simplifying year-over-year territory comparisons. SCD Type 4 splits storage: the main dimension holds only current rows for fast lookups, while an external history table records all historical mutation events.",
            "sections": [
                {
                    "title": "Implementing SCD Type 3 (Previous vs Current Columns)",
                    "explanation": "Create sales rep table with current and prior territory columns.",
                    "code": """scd3_data = [
    ("REP-01", "Alice", "West", None, "2023-01-01"),
    ("REP-02", "Bob", "East", None, "2023-01-01")
]

schema3 = "rep_id STRING, rep_name STRING, current_territory STRING, previous_territory STRING, last_reassigned STRING"
df_scd3 = spark.createDataFrame(scd3_data, schema3)
print("Initial SCD Type 3 Dimension Table:")
df_scd3.show()

# Alice moves from West to North territory
df_scd3_updated = df_scd3.withColumn(
    "previous_territory",
    F.when(F.col("rep_id") == "REP-01", F.col("current_territory")).otherwise(F.col("previous_territory"))
).withColumn(
    "current_territory",
    F.when(F.col("rep_id") == "REP-01", F.lit("North")).otherwise(F.col("current_territory"))
).withColumn(
    "last_reassigned",
    F.when(F.col("rep_id") == "REP-01", F.lit("2023-11-20")).otherwise(F.col("last_reassigned"))
)

print("Updated SCD Type 3 Dimension (Comparing Current vs Previous):")
df_scd3_updated.show()"""
                },
                {
                    "title": "Implementing SCD Type 4 (Current Table + Separate Historical Archive)",
                    "explanation": "Maintain a fast active table and append modifications to history table.",
                    "code": """# Active Table (Current state only -> ultra-fast lookups)
active_table = [("REP-01", "Alice", "North"), ("REP-02", "Bob", "East")]
df_active = spark.createDataFrame(active_table, ["rep_id", "name", "current_territory"])

# Historical Archive Table (Append-only audit trail)
archive_table = [
    ("REP-01", "West", "2023-01-01", "2023-11-20"),
    ("REP-01", "North", "2023-11-20", "9999-12-31"),
    ("REP-02", "East", "2023-01-01", "9999-12-31")
]
df_archive = spark.createDataFrame(archive_table, ["rep_id", "territory", "effective_from", "effective_to"])

print("SCD Type 4 Active Dimension:")
df_active.show()
print("SCD Type 4 Historical Archive Table:")
df_archive.show()"""
                }
            ],
            "key_takeaways": [
                "SCD Type 3 tracks immediate prior state by adding a `previous_value` column to the same row.",
                "SCD Type 4 maintains two physical tables: a fast current-state table and a separate historical audit log.",
                "SCD Type 4 maximizes query performance on current records while retaining full compliance history."
            ],
            "interview_questions": [
                "When is SCD Type 3 preferred over SCD Type 2 in dimensional modeling?",
                "What are the operational advantages of SCD Type 4 for high-frequency update tables?",
                "How do fact tables join against an SCD Type 4 architecture?"
            ]
        },
        {
            "nb_id": "071",
            "title": "Change Data Capture (CDC) Ingestion from Debezium/Kafka into Lakehouse",
            "difficulty": "Advanced",
            "business_scenario": "A transactional MySQL inventory database streams row-level changes into Kafka via Debezium CDC. Debezium publishes events containing `before`, `after`, and `op` flags (`c` for create, `u` for update, `d` for delete). The lakehouse must reconstruct current database state.",
            "problem_statement": "Ingest and process Debezium CDC streams in PySpark: parse the nested JSON envelope, handle out-of-order CDC events using sequence numbers/timestamps, execute conditional MERGE into target Delta tables, and handle hard deletes gracefully.",
            "architectural_notes": "Debezium CDC events contain metadata fields: `op` (operation: c=create, u=update, d=delete, r=snapshot read) and `ts_ms` (source commit timestamp). Applying CDC records requires deduplicating within each micro-batch by taking the latest `ts_ms` before merging.",
            "sections": [
                {
                    "title": "Generating Debezium CDC JSON Payloads",
                    "explanation": "Create representative Debezium change events including create, update, and delete.",
                    "code": """cdc_raw_events = [
    # Insert SKU-101
    ('{"before": null, "after": {"sku": "SKU-101", "stock": 100, "price": 20.0}, "op": "c", "ts_ms": 1700560000}'),
    # Update SKU-101 stock to 80
    ('{"before": {"sku": "SKU-101", "stock": 100, "price": 20.0}, "after": {"sku": "SKU-101", "stock": 80, "price": 20.0}, "op": "u", "ts_ms": 1700560500}'),
    # Insert SKU-102
    ('{"before": null, "after": {"sku": "SKU-102", "stock": 50, "price": 45.0}, "op": "c", "ts_ms": 1700560100}'),
    # Delete SKU-102
    ('{"before": {"sku": "SKU-102", "stock": 50, "price": 45.0}, "after": null, "op": "d", "ts_ms": 1700560800}')
]

rdd_cdc = spark.sparkContext.parallelize(cdc_raw_events)
df_raw_cdc = spark.read.json(rdd_cdc)
df_raw_cdc.printSchema()
df_raw_cdc.show(truncate=False)"""
                },
                {
                    "title": "Flattening CDC Payload & Filtering Latest Event per Primary Key",
                    "explanation": "Extract primary keys, payload attributes, and deduplicate by timestamp.",
                    "code": """# Extract target attributes from after struct (or before struct for deletes)
df_flattened_cdc = df_raw_cdc.select(
    F.coalesce(F.col("after.sku"), F.col("before.sku")).alias("sku"),
    F.col("after.stock").alias("stock"),
    F.col("after.price").alias("price"),
    F.col("op"),
    F.col("ts_ms")
)

# Deduplicate to find the latest operation per SKU
win_latest = Window.partitionBy("sku").orderBy(F.col("ts_ms").desc())
df_latest_cdc = (
    df_flattened_cdc
    .withColumn("rn", F.row_number().over(win_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print("Deduplicated Latest CDC State:")
df_latest_cdc.show()"""
                },
                {
                    "title": "Merging CDC State into Target Delta Lakehouse Table",
                    "explanation": "Apply conditional update, insert, and delete using Delta merge.",
                    "code": """from delta.tables import DeltaTable

delta_cdc_target = "/tmp/delta_inventory_cdc_071"
if Path(delta_cdc_target).exists():
    shutil.rmtree(delta_cdc_target)

# Initialize target table
init_schema = "sku STRING, stock INT, price DOUBLE, last_ts LONG"
spark.createDataFrame([], init_schema).write.format("delta").mode("overwrite").save(delta_cdc_target)

target_tbl = DeltaTable.forPath(spark, delta_cdc_target)

(
    target_tbl.alias("target")
    .merge(
        source=df_latest_cdc.alias("source"),
        condition="target.sku = source.sku"
    )
    .whenMatchedDelete(
        condition="source.op = 'd'"
    )
    .whenMatchedUpdate(
        set={"stock": F.col("source.stock"), "price": F.col("source.price"), "last_ts": F.col("source.ts_ms")}
    )
    .whenNotMatchedInsert(
        condition="source.op != 'd'",
        values={"sku": F.col("source.sku"), "stock": F.col("source.stock"), "price": F.col("source.price"), "last_ts": F.col("source.ts_ms")}
    )
    .execute()
)

print("Target Delta Inventory Table after CDC Replay (SKU-101 updated, SKU-102 deleted):")
target_tbl.toDF().show()"""
                }
            ],
            "key_takeaways": [
                "Debezium CDC envelopes provide `before`, `after`, `op`, and commit sequence timestamps.",
                "Always deduplicate the incoming CDC micro-batch by primary key ordering on `ts_ms` before calling Delta MERGE.",
                "Handle delete operations explicitly with `whenMatchedDelete(condition='source.op == \"d\"')`."
            ],
            "interview_questions": [
                "How do you handle out-of-order CDC events arriving from Kafka in a Spark Structured Streaming job?",
                "What is the difference between soft deletes and hard deletes in Lakehouse CDC architectures?",
                "How does Delta Lake's Change Data Feed (CDF) compare with external CDC sources like Debezium?"
            ]
        },
        {
            "nb_id": "072",
            "title": "Star Schema Implementation: Fact and Conformed Dimension Modeling in Spark",
            "difficulty": "Intermediate",
            "business_scenario": "A retail enterprise transitions from a monolithic relational data warehouse to a Databricks Lakehouse. BI analysts need classic Star Schema structures (Sales Fact table surrounded by Date, Customer, Store, and Product conformed dimensions).",
            "problem_statement": "Design and build a complete Star Schema in PySpark: construct conformed dimensions with surrogate keys, create the centralized Sales Fact table with foreign keys and degenerate dimensions, and execute star-join queries optimized for BI tools.",
            "architectural_notes": "Star Schemas remain the gold standard for BI reporting in Lakehouses due to user familiarity and tool compatibility (Power BI / Tableau). Spark optimizes star joins using Dynamic Partition Pruning (DPP) and Broadcast Hash Joins.",
            "sections": [
                {
                    "title": "Building Conformed Dimensions (Customer & Product)",
                    "explanation": "Construct dimension tables with surrogate keys.",
                    "code": """dim_cust = spark.createDataFrame([
    (101, "CUST-A", "Alice Smith", "Gold", "US"),
    (102, "CUST-B", "Bob Jones", "Silver", "UK")
], ["dim_customer_sk", "customer_id", "full_name", "loyalty_tier", "country"])

dim_prod = spark.createDataFrame([
    (501, "PROD-X", "Mechanical Keyboard", "Hardware", 89.99),
    (502, "PROD-Y", "USB-C Hub", "Hardware", 29.99)
], ["dim_product_sk", "product_id", "product_name", "category", "unit_price"])

print("Customer Dimension:")
dim_cust.show()
print("Product Dimension:")
dim_prod.show()"""
                },
                {
                    "title": "Building the Centralized Sales Fact Table",
                    "explanation": "Construct fact table referencing dimension surrogate keys.",
                    "code": """fact_sales = spark.createDataFrame([
    ("TXN-901", 101, 501, 2, 179.98, "2023-11-20"),
    ("TXN-902", 101, 502, 1, 29.99,  "2023-11-20"),
    ("TXN-903", 102, 501, 1, 89.99,  "2023-11-20")
], ["txn_id", "dim_customer_sk", "dim_product_sk", "quantity", "total_sales_amount", "order_date"])

print("Sales Fact Table:")
fact_sales.show()"""
                },
                {
                    "title": "Executing Star-Join Query with Dynamic Pruning",
                    "explanation": "Join fact table with dimensions for executive slice-and-dice reporting.",
                    "code": """df_star_report = (
    fact_sales
    .join(dim_cust, on="dim_customer_sk")
    .join(dim_prod, on="dim_product_sk")
    .groupBy("country", "category")
    .agg(
        F.sum("total_sales_amount").alias("gross_revenue"),
        F.sum("quantity").alias("units_sold")
    )
    .orderBy(F.desc("gross_revenue"))
)

print("Star-Schema Analytics Report:")
df_star_report.show()"""
                }
            ],
            "key_takeaways": [
                "Star Schemas feature a central numeric Fact table surrounded by denormalized Dimension tables.",
                "Surrogate keys (integers) improve join performance and decouple reporting from source transactional IDs.",
                "Dynamic Partition Pruning (DPP) allows Spark to skip fact table partitions dynamically based on dimension filters."
            ],
            "interview_questions": [
                "How does Dynamic Partition Pruning (DPP) optimize Star Schema queries in Apache Spark?",
                "What is the difference between a Star Schema and a Snowflake Schema in terms of normalization and query performance?",
                "What is a Degenerate Dimension and where is it stored in a Star Schema?"
            ]
        },
        {
            "nb_id": "073",
            "title": "Data Vault 2.0 in the Lakehouse: Hubs, Links and Satellites Modeling",
            "difficulty": "Advanced",
            "business_scenario": "A large banking institution integrates 30 disparate core banking, loan, and credit systems. Source systems change schemas frequently. The enterprise architecture mandates Data Vault 2.0 to provide 100% auditability, parallel loading, and agile schema evolution.",
            "problem_statement": "Implement the core entities of Data Vault 2.0 in PySpark: (1) Hubs (unique business keys and hash keys), (2) Links (relationships between business keys), and (3) Satellites (contextual descriptive attributes with load timestamps).",
            "architectural_notes": "Data Vault 2.0 separates business keys (Hubs), relationships (Links), and descriptive attributes (Satellites). Hash keys (`sha2(natural_key)`) allow distributed parallel ingestion into Hubs, Links, and Satellites simultaneously without foreign key lookup bottlenecks.",
            "sections": [
                {
                    "title": "1. Building Hub Customer (Hubs: Unique Business Keys)",
                    "explanation": "Create Hub table containing hash keys and business keys.",
                    "code": """raw_cust = [("CUST-101", "CRM"), ("CUST-102", "CORE_BANKING")]
df_raw_cust = spark.createDataFrame(raw_cust, ["customer_id", "record_source"])

hub_customer = df_raw_cust.select(
    F.sha2(F.col("customer_id"), 256).alias("hk_customer"),
    F.col("customer_id").alias("bk_customer_id"),
    F.current_timestamp().alias("load_date"),
    F.col("record_source")
)

print("Hub Customer (Hub):")
hub_customer.show(truncate=False)"""
                },
                {
                    "title": "2. Building Link Customer-Account (Links: Relationships)",
                    "explanation": "Create Link table capturing the relationship between Customer and Account hash keys.",
                    "code": """raw_links = [("CUST-101", "ACC-9001", "CORE_BANKING")]
df_raw_links = spark.createDataFrame(raw_links, ["customer_id", "account_id", "record_source"])

link_cust_acc = df_raw_links.select(
    F.sha2(F.concat(F.col("customer_id"), F.lit("||"), F.col("account_id")), 256).alias("hk_link_cust_acc"),
    F.sha2(F.col("customer_id"), 256).alias("hk_customer"),
    F.sha2(F.col("account_id"), 256).alias("hk_account"),
    F.current_timestamp().alias("load_date"),
    F.col("record_source")
)

print("Link Customer Account (Link):")
link_cust_acc.show(truncate=False)"""
                },
                {
                    "title": "3. Building Satellite Customer Details (Satellites: Descriptive Data)",
                    "explanation": "Create Satellite capturing point-in-time descriptive attributes with hash diff.",
                    "code": """raw_sat = [("CUST-101", "Alice Smith", "Gold", "CORE_BANKING")]
df_raw_sat = spark.createDataFrame(raw_sat, ["customer_id", "name", "tier", "record_source"])

sat_customer = df_raw_sat.select(
    F.sha2(F.col("customer_id"), 256).alias("hk_customer"),
    F.current_timestamp().alias("load_date"),
    # Hashdiff detects if descriptive attributes have changed since last load
    F.sha2(F.concat_ws("||", F.col("name"), F.col("tier")), 256).alias("hashdiff"),
    F.col("name"),
    F.col("tier"),
    F.col("record_source")
)

print("Satellite Customer (Satellite):")
sat_customer.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Data Vault 2.0 decomposes data into Hubs (business keys), Links (associations), and Satellites (context).",
                "Hash keys (`sha2`) eliminate dependency bottlenecks, allowing Hubs, Links, and Satellites to load concurrently.",
                "`hashdiff` columns enable instant change detection without checking every individual descriptive column."
            ],
            "interview_questions": [
                "What problem does Data Vault 2.0 solve compared to traditional Inmon 3NF and Kimball Star Schemas?",
                "Why are cryptographic hash keys used instead of database sequences in Data Vault 2.0?",
                "What is the role of the `hashdiff` column in a Data Vault Satellite table?"
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
