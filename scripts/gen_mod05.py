"""
Module 05: Data Ingestion Patterns, Auto Loader & File Formats (Notebooks 044 - 053)
"""

import re
from pathlib import Path
from notebook_builder import build_notebook, write_notebook

MODULE_FOLDER = "05_data_ingestion_autoloader"
MODULE_NAME = "Module 05: Data Ingestion Patterns, Auto Loader & File Formats"

def sanitize_filename(nb_id: str, title: str) -> str:
    cleaned = title.lower().replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return f"{nb_id}_{cleaned}.py"

def generate(base_dir: Path):
    out_dir = base_dir / "notebooks" / MODULE_FOLDER
    out_dir.mkdir(parents=True, exist_ok=True)

    notebooks = [
        {
            "nb_id": "044",
            "title": "CSV Ingestion Edge Cases: Multiline, Escaped Quotes and Custom Delimiters",
            "difficulty": "Intermediate",
            "business_scenario": "A financial auditing pipeline imports legacy bank statement extracts. The files contain multiline transaction descriptions with embedded newlines, pipe delimiters (`|`), quotes with backslash escapes, and corrupted header rows.",
            "problem_statement": "Configure robust CSV reader options in PySpark: handle multiline records (`multiLine=True`), custom delimiters (`sep='|'`), escape characters (`escape='\\\\'`), quote handling, explicit datetime inference, and graceful handling of malformed lines.",
            "architectural_notes": "By default, Spark splits CSV files across byte boundaries for parallel reading, which breaks if records contain embedded newlines. Setting `multiLine=true` disables byte-offset splitting for that file, forcing a single thread per file to parse line boundaries safely.",
            "sections": [
                {
                    "title": "Creating Edge-Case CSV File on Disk",
                    "explanation": "Write raw CSV data with embedded newlines, pipe delimiter, and escaped quotes.",
                    "code": """import shutil
from pathlib import Path

csv_dir = Path("/tmp/csv_edge_cases_044")
if csv_dir.exists():
    shutil.rmtree(csv_dir)
csv_dir.mkdir(parents=True, exist_ok=True)

csv_content = \"\"\"id|customer|amount|notes|txn_date
101|Alice Corp|1500.50|Invoice #1234\\nPayment split across accounts|2023-11-01
102|Bob & Co|450.00|Standard payment for \\"consulting services\\"|2023-11-02
103|Charlie Ltd|8900.25|Rush order\\nDirect wire transfer|2023-11-03\"\"\"

file_path = csv_dir / "transactions.csv"
with open(file_path, "w") as f:
    f.write(csv_content)

print(f"Sample edge-case CSV created at {file_path}")"""
                },
                {
                    "title": "Configuring Resilient PySpark CSV Reader",
                    "explanation": "Apply multiLine, sep, escape, and quote options.",
                    "code": """# Configure options to handle all edge cases
df_csv = (
    spark.read
    .option("header", "true")
    .option("sep", "|")
    .option("multiLine", "true")
    .option("quote", "\"")
    .option("escape", "\\\\")
    .option("inferSchema", "true")
    .csv(str(csv_dir))
)

print("Parsed Schema:")
df_csv.printSchema()

print("Parsed Data (Notice multi-line notes preserved within rows):")
df_csv.show(truncate=False)"""
                },
                {
                    "title": "Verifying Row Integrity Assertions",
                    "explanation": "Verify exactly 3 records were ingested without split-line corruption.",
                    "code": """assert df_csv.count() == 3, f"Expected 3 rows, found {df_csv.count()} (Multiline issue!)"
assert "\\n" in df_csv.filter(F.col("id") == 101).select("notes").first()[0], "Embedded newline lost"
print("All CSV edge-case ingestion assertions passed successfully!")"""
                }
            ],
            "key_takeaways": [
                "`multiLine=true` is required when fields contain embedded newlines; note that it disables file splitting.",
                "Always specify explicit `escape` and `quote` characters when ingesting non-standard CSV extracts.",
                "For production pipelines, prefer Parquet/Delta over CSV to eliminate textual parsing ambiguities."
            ],
            "interview_questions": [
                "How does `multiLine=true` affect the parallelism and performance of reading large CSV files in Spark?",
                "What is the difference between `inferSchema=true` and providing an explicit StructType on CSV ingestion?",
                "How do you handle CSV files where the header appears periodically throughout the file (concatenated extracts)?"
            ]
        },
        {
            "nb_id": "045",
            "title": "Parquet and ORC Columnar Ingestion: Predicate Pushdown and Dictionary Encoding",
            "difficulty": "Intermediate",
            "business_scenario": "A petabyte-scale telemetry lakehouse queries 500 million vehicle diagnostic events daily. Analyst queries typically filter on single vehicle IDs and read only 3 of the 80 columns. Inefficient readers trigger multi-terabyte disk reads and network bottlenecks.",
            "problem_statement": "Demonstrate the power of columnar formats: compare Parquet and ORC file formats, observe Column Projection (reading only requested columns), and inspect Catalyst physical plans to verify Predicate Pushdown (file and row-group skipping via min/max statistics).",
            "architectural_notes": "Columnar storage arranges data by column rather than row. Row groups contain metadata statistics (min, max, null count). Spark's Parquet reader evaluates query predicates against row-group metadata before reading page data from disk, skipping entire blocks that cannot contain matching rows.",
            "sections": [
                {
                    "title": "Generating Wide Diagnostic Telemetry Dataset",
                    "explanation": "Create dataset with multiple columns and save as Parquet.",
                    "code": """parquet_path = "/tmp/parquet_telemetry_045"
if Path(parquet_path).exists():
    shutil.rmtree(parquet_path)

import random
random.seed(42)

# Generate 50,000 telemetry readings with 6 columns
rows = [
    (i, f"V-{random.randint(1, 100):03d}", round(random.uniform(50.0, 120.0), 2), round(random.uniform(20.0, 45.0), 2), f"FW-v{random.randint(1, 4)}.0", "OK")
    for i in range(1, 50001)
]

schema = "reading_id INT, vehicle_id STRING, speed DOUBLE, temp DOUBLE, firmware STRING, status STRING"
df_source = spark.createDataFrame(rows, schema)

# Write as Parquet with Snappy compression
df_source.write.format("parquet").mode("overwrite").save(parquet_path)
print(f"Parquet dataset written with 50,000 rows.")"""
                },
                {
                    "title": "Demonstrating Column Projection (Zero-Cost Column Pruning)",
                    "explanation": "Select only 2 columns and observe Catalyst projected schema.",
                    "code": """# Read only reading_id and speed
df_pruned = spark.read.parquet(parquet_path).select("reading_id", "speed")

print("=== PHYSICAL PLAN FOR COLUMN PROJECTION ===")
# Notice: 'ReadSchema: struct<reading_id:int,speed:double>' -> Spark skips reading all other 4 columns from disk!
df_pruned.explain(mode="formatted")"""
                },
                {
                    "title": "Demonstrating Predicate Pushdown (Row-Group Skipping)",
                    "explanation": "Filter on vehicle_id and verify PushedFilters in physical plan.",
                    "code": """df_filtered = spark.read.parquet(parquet_path).filter(F.col("vehicle_id") == "V-042").select("vehicle_id", "speed")

print("=== PHYSICAL PLAN FOR PREDICATE PUSHDOWN ===")
# Notice: 'PushedFilters: [IsNotNull(vehicle_id), EqualTo(vehicle_id,V-042)]'
df_filtered.explain(mode="formatted")
df_filtered.show(5)"""
                }
            ],
            "key_takeaways": [
                "Column Projection reads only required columns from disk, dramatically reducing I/O and network transfer.",
                "Predicate Pushdown pushes WHERE filters directly to the Parquet/ORC file reader to skip row groups using min/max stats.",
                "Dictionary encoding replaces repetitive string values with small integer keys, compressing high-repetition columns efficiently."
            ],
            "interview_questions": [
                "What is the internal structure of a Parquet file (Header, Row Groups, Column Chunks, Pages, Footer)?",
                "Why does wrapping a column in a function (e.g. `WHERE upper(col) == 'VAL'`) disable Parquet predicate pushdown?",
                "How does ORC's Lightweight Index compare to Parquet's Row Group Statistics?"
            ]
        },
        {
            "nb_id": "046",
            "title": "Avro Binary Ingestion and Schema Registry Compatibility",
            "difficulty": "Intermediate",
            "business_scenario": "An enterprise Apache Kafka event bus publishes high-throughput customer account updates serialized in Apache Avro format. Data engineering must consume Avro payloads using explicit Avro schemas compatible with the Confluent Schema Registry.",
            "problem_statement": "Ingest and serialize Apache Avro binary records in PySpark: use `from_avro()` and `to_avro()`, define JSON Avro schema specifications, handle schema evolution between writer and reader schemas, and package binary payloads.",
            "architectural_notes": "Apache Avro is a row-oriented binary serialization format widely used for streaming pipelines due to its compact size and schema evolution rules. The `from_avro` function decodes binary Kafka bytes into Spark StructType columns using an Avro schema string.",
            "sections": [
                {
                    "title": "Defining Avro Schema JSON Specification",
                    "explanation": "Define standard Avro JSON schema.",
                    "code": """avro_schema_json = \"\"\"{
  "type": "record",
  "name": "CustomerEvent",
  "namespace": "com.enterprise.events",
  "fields": [
    {"name": "customer_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "account_balance", "type": "double"},
    {"name": "event_timestamp", "type": "long"}
  ]
}\"\"\"

print("Avro Schema JSON defined successfully.")"""
                },
                {
                    "title": "Simulating Avro Ingestion and Struct Conversion",
                    "explanation": "Create events and demonstrate Avro integration patterns.",
                    "code": """# Create representative structured records
events = [
    ("CUST-101", "LOGIN", 1450.50, 1700560800),
    ("CUST-102", "PURCHASE", 320.00, 1700560950),
    ("CUST-103", "LOGOUT", 8900.00, 1700561100)
]

cols = ["customer_id", "event_type", "account_balance", "event_timestamp"]
df_events = spark.createDataFrame(events, cols)
df_events.printSchema()
df_events.show()"""
                },
                {
                    "title": "Packaging Structs for Streaming Message Buses",
                    "explanation": "Bundle fields into structs for downstream message publishing.",
                    "code": """# Convert to unified message payload struct matching Avro schema
df_payload = df_events.select(
    F.col("customer_id").alias("key"),
    F.struct("customer_id", "event_type", "account_balance", "event_timestamp").alias("value")
)

df_payload.printSchema()
df_payload.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Avro is the industry standard format for streaming message buses (Kafka, Event Hubs) due to compact binary encoding.",
                "`from_avro(col, schema)` parses binary payloads using the exact Avro schema definition.",
                "Schema evolution in Avro allows adding optional fields with default values without breaking downstream consumers."
            ],
            "interview_questions": [
                "Why is Avro preferred for message bus streaming while Parquet is preferred for analytical storage?",
                "How does Confluent Schema Registry prevent incompatible schema changes in Kafka-Spark pipelines?",
                "What is the difference between Writer Schema and Reader Schema in Apache Avro?"
            ]
        },
        {
            "nb_id": "047",
            "title": "JDBC Parallel Ingestion: Partitioning by Column, fetchsize and Connection Sizing",
            "difficulty": "Intermediate",
            "business_scenario": "A night batch pipeline extracts 50 million sales transaction records from an operational PostgreSQL production database into the Lakehouse. A single-threaded JDBC query takes 6 hours and causes database connection timeouts.",
            "problem_statement": "Optimize Spark JDBC ingestion: configure parallel partition extraction using `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions`. Tune `fetchsize` to balance executor memory vs network round-trips, and avoid database connection exhaustion.",
            "architectural_notes": "By default, `spark.read.jdbc()` executes a single query on a single executor task (`SELECT * FROM table`), creating a severe driver bottleneck. By specifying partition parameters, Spark splits the query into $N$ parallel slice queries (`WHERE col >= lower AND col < upper`) distributed across executor workers.",
            "sections": [
                {
                    "title": "Reviewing Default vs Partitioned JDBC Architecture",
                    "explanation": "Illustrate how Spark generates parallel SQL queries across partitions.",
                    "code": """# Architectural Blueprint:
# Spark creates N independent tasks, each executing a sliced query:
# Task 1: SELECT * FROM sales WHERE id >= 1 AND id < 2500000
# Task 2: SELECT * FROM sales WHERE id >= 2500000 AND id < 5000000
# Task 3: SELECT * FROM sales WHERE id >= 5000000 AND id < 7500000
# Task 4: SELECT * FROM sales WHERE id >= 7500000 AND id <= 10000000

print("Parallel JDBC partitioning architecture mapped.")"""
                },
                {
                    "title": "Constructing Resilient JDBC Ingestion Configuration",
                    "explanation": "Build production-grade JDBC parameter dictionary.",
                    "code": """jdbc_options = {
    "url": "jdbc:postgresql://db-host.internal:5432/retail_db",
    "dbtable": "public.orders",
    "user": "etl_reader",
    "password": "redacted_secret",
    "driver": "org.postgresql.Driver",
    # Crucial parallel parameters:
    "partitionColumn": "order_id",
    "lowerBound": "1",
    "upperBound": "10000000",
    "numPartitions": "16",       # 16 parallel tasks reading simultaneously
    "fetchsize": "10000"         # Read 10,000 rows per network roundtrip (prevents OOM)
}

print("Production JDBC configuration dictionary:")
for k, v in jdbc_options.items():
    if k != "password":
        print(f"  {k}: {v}")"""
                },
                {
                    "title": "Simulating Parallel Ingestion via Sliced Partitions Locally",
                    "explanation": "Demonstrate the mathematical partition slice generator in PySpark.",
                    "code": """# Simulate 4 parallel partitions over order_id range 1 to 100
df_simulated_db = spark.range(1, 101, 1, 4).withColumnRenamed("id", "order_id").withColumn(
    "order_amount", F.round(F.rand(seed=42) * 500 + 10, 2)
)

print(f"Number of partitions: {df_simulated_db.rdd.getNumPartitions()}")
print("Row distribution per partition:")
df_simulated_db.withColumn("partition_id", F.spark_partition_id()).groupBy("partition_id").agg(
    F.min("order_id").alias("min_id"),
    F.max("order_id").alias("max_id"),
    F.count("order_id").alias("rows")
).orderBy("partition_id").show()"""
                }
            ],
            "key_takeaways": [
                "Always supply `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` when reading large JDBC tables.",
                "The `partitionColumn` MUST be numeric, indexed, and evenly distributed (e.g. auto-incrementing primary key).",
                "Tune `fetchsize` (e.g. 5,000 to 50,000) to avoid default single-row roundtrips or massive memory spikes."
            ],
            "interview_questions": [
                "What happens if you run `spark.read.jdbc()` without specifying `partitionColumn`?",
                "What happens if the `partitionColumn` in a JDBC read has severe data skew (e.g. 90% of rows have id=1)?",
                "How does `fetchsize` impact both the source database memory and the Spark executor memory?"
            ]
        },
        {
            "nb_id": "048",
            "title": "Databricks Auto Loader Directory Mode: cloudFiles, Schema Inference and Checkpointing",
            "difficulty": "Intermediate",
            "business_scenario": "A cloud storage bucket receives thousands of landing files (JSON and CSV) every hour from IoT devices. Traditional batch file listings (`os.listdir` or `dbutils.fs.ls`) become prohibitively slow as the bucket scales past 100,000 files.",
            "problem_statement": "Implement Databricks Auto Loader using the `cloudFiles` format: configure directory listing mode, enable automatic schema inference (`cloudFiles.inferColumnTypes`), maintain state checkpointing, and stream landing files into a Bronze Delta table with exactly-once guarantees.",
            "architectural_notes": "Auto Loader uses `cloudFiles` source to incrementally discover landing files without re-scanning previously processed files. Directory listing mode scans file metadata incrementally, tracking discovered files in RocksDB state storage within the checkpoint directory.",
            "sections": [
                {
                    "title": "Creating Mock Landing Storage Directory",
                    "explanation": "Create staging directories for landing files and checkpoint state.",
                    "code": """landing_dir = Path("/tmp/autoloader_landing_048")
checkpoint_dir = Path("/tmp/autoloader_chk_048")
bronze_table_dir = Path("/tmp/autoloader_bronze_048")

for p in [landing_dir, checkpoint_dir, bronze_table_dir]:
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)

# Write initial landing JSON file
batch1_file = landing_dir / "sensor_batch_1.json"
with open(batch1_file, "w") as f:
    f.write('{"device_id": "IOT-1", "temp": 24.5, "reading_ts": "2023-11-20T10:00:00Z"}\\n')
    f.write('{"device_id": "IOT-2", "temp": 31.2, "reading_ts": "2023-11-20T10:00:00Z"}\\n')

print(f"Landing batch 1 written to {batch1_file}")"""
                },
                {
                    "title": "Auto Loader Configuration Blueprint (cloudFiles)",
                    "explanation": "Display the production Databricks Auto Loader syntax.",
                    "code": """# Production Databricks Auto Loader Streaming Pipeline:
autoloader_stream_code = f\"\"\"
df_autoloader = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "{checkpoint_dir}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("{landing_dir}")
)

query = (
    df_autoloader.writeStream
    .format("delta")
    .option("checkpointLocation", "{checkpoint_dir}/cp")
    .trigger(availableNow=True)
    .start("{bronze_table_dir}")
)
query.awaitTermination()
\"\"\"

print("Auto Loader Blueprint Code:")
print(autoloader_stream_code)"""
                },
                {
                    "title": "Simulating Incremental Auto Loader Discovery Locally",
                    "explanation": "Process batch files incrementally into Delta Bronze table.",
                    "code": """# In standard PySpark local mode, we ingest the landing JSON and write to Delta Bronze
df_landing = spark.read.json(str(landing_dir))
df_landing.write.format("delta").mode("append").save(str(bronze_table_dir))

print("Bronze Table after First Ingestion:")
spark.read.format("delta").load(str(bronze_table_dir)).show()"""
                }
            ],
            "key_takeaways": [
                "Databricks Auto Loader (`format('cloudFiles')`) incrementally ingests millions of files with low latency and zero duplicate processing.",
                "`cloudFiles.schemaLocation` persists inferred schemas across stream restarts, preventing schema drift failures.",
                "Auto Loader supports Directory Listing mode (for smaller loads) and File Notification mode (using AWS SQS / Azure Event Grid) for millions of files."
            ],
            "interview_questions": [
                "What is the difference between Directory Listing mode and File Notification mode in Databricks Auto Loader?",
                "Why is Auto Loader superior to standard `spark.read.json('s3://bucket/*.json')` on large cloud buckets?",
                "How does Auto Loader ensure exactly-once ingestion semantics?"
            ]
        },
        {
            "nb_id": "049",
            "title": "Auto Loader Schema Evolution and Rescued Data Column (_rescued_data)",
            "difficulty": "Intermediate",
            "business_scenario": "Third-party vendors send telemetry files where new columns appear without prior warning, or column types change unexpectedly. If a pipeline drops these unexpected fields, valuable business telemetry is lost permanently.",
            "problem_statement": "Configure Auto Loader schema evolution: set `cloudFiles.schemaEvolutionMode = 'addNewColumns'`, and configure the `_rescued_data` column to automatically capture unexpected fields, mismatched data types, or corrupted text lines without halting the pipeline.",
            "architectural_notes": "When `schemaEvolutionMode` is enabled, Auto Loader automatically evolves the target schema when new columns arrive. Any values that cannot be parsed into the existing schema (e.g. string arriving in integer column) are automatically diverted into the JSON `_rescued_data` column, ensuring zero data loss.",
            "sections": [
                {
                    "title": "Demonstrating Rescued Data Column Mechanics",
                    "explanation": "Simulate schema evolution and rescued data capture.",
                    "code": """# Architectural Concept:
# Incoming Record: {"device_id": "IOT-99", "temp": "INVALID_FLOAT", "new_feature": "GPS_ACTIVE"}
# Target Schema: device_id STRING, temp DOUBLE
#
# Auto Loader Output:
# device_id: "IOT-99"
# temp: NULL
# _rescued_data: '{"temp": "INVALID_FLOAT", "new_feature": "GPS_ACTIVE"}'

print("Auto Loader _rescued_data architecture mapped.")"""
                },
                {
                    "title": "Simulating Rescued Data Capture in PySpark",
                    "explanation": "Create an explicit schema with a fallback JSON rescue column.",
                    "code": """# Mock incoming data with mismatched types and extra columns
raw_stream = [
    ("D-1", "25.5", "NORMAL"),
    ("D-2", "CORRUPTED_TEMP", "CRITICAL"),
    ("D-3", "30.0", "NORMAL")
]

df_raw = spark.createDataFrame(raw_stream, ["device_id", "raw_temp", "status"])

# Resilient parsing capturing unparseable values in rescued_data
clean_expr = F.expr("try_cast(raw_temp AS DOUBLE)")

df_rescued = df_raw.withColumn("temp", clean_expr).withColumn(
    "_rescued_data",
    F.when(
        F.col("temp").isNull() & F.col("raw_temp").isNotNull(),
        F.to_json(F.struct(F.col("raw_temp"), F.lit("Type mismatch: expected DOUBLE").alias("error_reason")))
    ).otherwise(None)
)

print("Parsed Records with Rescued Data Capture:")
df_rescued.select("device_id", "temp", "status", "_rescued_data").show(truncate=False)"""
                },
                {
                    "title": "Production Auto Loader Schema Evolution Configuration",
                    "explanation": "Show production options for cloudFiles.",
                    "code": """autoloader_options = {
    "cloudFiles.format": "json",
    "cloudFiles.schemaEvolutionMode": "addNewColumns",  # Options: addNewColumns, rescue, failOnNewColumns
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.schemaRescuePath": "/tmp/schema_rescue"
}

print("Production Auto Loader Evolution Settings:")
for k, v in autoloader_options.items():
    print(f"  {k} = '{v}'")"""
                }
            ],
            "key_takeaways": [
                "`cloudFiles.schemaEvolutionMode = 'addNewColumns'` automatically alters target Delta tables to include new columns.",
                "`_rescued_data` captures type mismatches, null violations, and unmapped keys as a JSON string.",
                "Data teams can parse `_rescued_data` later to backfill historical columns without reprocessing raw files."
            ],
            "interview_questions": [
                "What are the four schema evolution modes supported by Databricks Auto Loader?",
                "What does the `_rescued_data` column contain and when is it populated?",
                "How does Auto Loader notify users when a schema evolution event occurs?"
            ]
        },
        {
            "nb_id": "050",
            "title": "Building a Metadata-Driven Multi-Format Ingestion Engine",
            "difficulty": "Advanced",
            "business_scenario": "A centralized lakehouse ingestion platform must onboard 200+ distinct source datasets daily spanning CSV, Parquet, JSON, and ORC. Writing bespoke ingestion notebooks for each dataset is unmaintainable and error-prone.",
            "problem_statement": "Design an extensible, configuration-driven multi-format ingestion class `UnifiedIngestionEngine` in PySpark that takes a JSON/dictionary configuration spec, validates format options, applies standard audit metadata columns (`_ingested_at`, `_source_file`), and writes standardized Delta tables.",
            "architectural_notes": "A metadata-driven framework separates orchestration logic from data transformation code. Pipeline configuration (source paths, file formats, options, partition keys, schema definitions) can be stored in Delta configuration tables or YAML files and executed dynamically.",
            "sections": [
                {
                    "title": "Designing Metadata Ingestion Specification Dictionary",
                    "explanation": "Define configuration objects for heterogeneous data sources.",
                    "code": """pipeline_specs = [
    {
        "table_name": "src_orders",
        "format": "json",
        "raw_path": "/tmp/src_orders_050.json",
        "target_path": "/tmp/delta_orders_050",
        "partition_cols": ["order_status"],
        "options": {"multiline": "false"}
    },
    {
        "table_name": "src_products",
        "format": "csv",
        "raw_path": "/tmp/src_products_050.csv",
        "target_path": "/tmp/delta_products_050",
        "partition_cols": [],
        "options": {"header": "true", "inferSchema": "true"}
    }
]

print(f"Loaded {len(pipeline_specs)} pipeline specifications.")"""
                },
                {
                    "title": "Implementing the UnifiedIngestionEngine Framework",
                    "explanation": "Create framework class that reads sources, enriches with audit columns, and writes Delta tables.",
                    "code": """from datetime import datetime

class UnifiedIngestionEngine:
    def __init__(self, spark_session):
        self.spark = spark_session

    def ingest(self, spec: dict):
        table_name = spec["table_name"]
        fmt = spec["format"]
        raw_path = spec["raw_path"]
        target_path = spec["target_path"]
        partition_cols = spec.get("partition_cols", [])
        options = spec.get("options", {})

        print(f"--> Ingesting {table_name} [Format: {fmt}] from {raw_path}")

        # 1. Read dynamically using format and options
        reader = self.spark.read.format(fmt)
        for k, v in options.items():
            reader = reader.option(k, v)
        df_raw = reader.load(raw_path)

        # 2. Add standard audit columns
        df_enriched = (
            df_raw
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(datetime.utcnow().strftime("%Y%m%d%H%M%S")))
        )

        # 3. Write standardized Delta table
        writer = df_enriched.write.format("delta").mode("append")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)

        count = df_enriched.count()
        print(f"--> Successfully landed {count} records into {target_path}")
        return df_enriched

print("UnifiedIngestionEngine class ready.")"""
                },
                {
                    "title": "Testing Framework with Synthetic Source Files",
                    "explanation": "Create synthetic files and execute the dynamic pipeline.",
                    "code": """# Prepare sample files
with open("/tmp/src_orders_050.json", "w") as f:
    f.write('{"order_id": "O1", "amount": 120.0, "order_status": "COMPLETED"}\\n')
    f.write('{"order_id": "O2", "amount": 85.0, "order_status": "PENDING"}\\n')

with open("/tmp/src_products_050.csv", "w") as f:
    f.write("prod_id,prod_name,cost\\nP1,Widget,10.50\\nP2,Gizmo,25.00\\n")

engine = UnifiedIngestionEngine(spark)
for spec in pipeline_specs:
    engine.ingest(spec)

# Verify written Delta table
print("Verified Delta Table Output (src_orders):")
spark.read.format("delta").load("/tmp/delta_orders_050").show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Metadata-driven engines decouple data engineering transformation rules from execution infrastructure.",
                "Always enrich ingested Bronze data with audit columns: `_ingested_at`, `_source_file`, and `_batch_id`.",
                "Using a unified engine ensures consistent security, error handling, and telemetry across hundreds of pipelines."
            ],
            "interview_questions": [
                "How do you design a metadata-driven ingestion pipeline that dynamically scales to 500+ tables?",
                "What is `_input_file_name()` in PySpark and why is it invaluable for audit tracing in Bronze tables?",
                "How do you store and version ingestion pipeline metadata (JSON, YAML, or Delta tables)?"
            ]
        },
        {
            "nb_id": "051",
            "title": "Incremental Batch File Discovery: Watermarking and Modification Timestamps",
            "difficulty": "Intermediate",
            "business_scenario": "A legacy data pipeline runs hourly batch jobs that read from an S3/ADLS bucket where external partners drop new files. The pipeline must process only files added since the last successful run without re-processing older files or utilizing streaming.",
            "problem_statement": "Implement incremental batch file ingestion in PySpark: use file modification timestamps (`modifiedAfter` and `modifiedBefore`), track high-watermark state in a persistent Delta metadata table, and prevent race conditions with actively written files.",
            "architectural_notes": "Spark's file readers support path options `modifiedAfter` and `modifiedBefore` (ISO 8601 strings). By querying the maximum timestamp of the previous batch and setting `modifiedAfter`, Spark skips listing and reading older files, achieving efficient incremental batch execution.",
            "sections": [
                {
                    "title": "Simulating Landing Bucket with Progressive Timestamps",
                    "explanation": "Create staging directory with multiple files.",
                    "code": """inc_dir = Path("/tmp/incremental_batch_051")
if inc_dir.exists():
    shutil.rmtree(inc_dir)
inc_dir.mkdir(parents=True, exist_ok=True)

# Write batch 1 files
with open(inc_dir / "file_batch1.csv", "w") as f:
    f.write("id,val\\n1,A\\n2,B\\n")

print(f"Initial files written to {inc_dir}")"""
                },
                {
                    "title": "Incremental Discovery Using modifiedAfter Option",
                    "explanation": "Read files filtered by modification timestamp threshold.",
                    "code": """from datetime import datetime, timedelta

# Ingest all files modified after 10 minutes ago
watermark_time = (datetime.utcnow() - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")

df_incremental = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("modifiedAfter", watermark_time)
    .load(str(inc_dir))
)

print(f"Reading files modified after {watermark_time}:")
df_incremental.show()"""
                },
                {
                    "title": "Maintaining Watermark State in Metadata Delta Table",
                    "explanation": "Persist the new high-watermark timestamp for the next run.",
                    "code": """# High-watermark metadata record
new_watermark = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
watermark_record = [("landing_partner_bucket", new_watermark, df_incremental.count())]

df_wm = spark.createDataFrame(watermark_record, ["pipeline_name", "last_watermark_ts", "records_processed"])
df_wm.write.format("delta").mode("append").save("/tmp/pipeline_watermarks_051")

print("Updated High-Watermark State Table:")
spark.read.format("delta").load("/tmp/pipeline_watermarks_051").show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "`modifiedAfter` filters cloud storage files at the source based on object modification timestamps.",
                "Always persist high-watermark state in an ACID Delta table to ensure crash-recovery idempotency.",
                "Apply a small safety overlap buffer (e.g. 1-2 minutes) and deduplicate downstream to catch clock drift."
            ],
            "interview_questions": [
                "What is the difference between `modifiedAfter` and using Databricks Auto Loader?",
                "How do you handle files that are still being written by an external producer when the batch job begins?",
                "Why can clock skew between cloud storage servers and Spark nodes cause missing files in timestamp watermarking?"
            ]
        },
        {
            "nb_id": "052",
            "title": "Handling Compressed Archives (.zip, .tar.gz, .bz2) in Distributed PySpark",
            "difficulty": "Advanced",
            "business_scenario": "A regulatory agency deposits quarterly compliance archives as massive 5GB ZIP and TAR.GZ files. Standard Spark readers cannot split ZIP archives across executors, causing single-node driver OutOfMemory (OOM) crashes.",
            "problem_statement": "Ingest and process compressed archives in distributed Spark: understand file splittability differences across codecs (Gzip, Bzip2, Snappy, Zip), implement parallel extraction using `binaryFiles` and distributed RDD partitions, and convert decompressed streams into DataFrames.",
            "architectural_notes": "Compression formats without sync markers (GZIP, ZIP) are not splittable: a single 5GB GZIP file must be processed by exactly one Spark task. Bzip2 and Snappy (within container formats like Parquet/Avro) are splittable across tasks.",
            "sections": [
                {
                    "title": "Creating a Mock In-Memory Compressed ZIP Archive",
                    "explanation": "Generate a sample ZIP file containing multiple CSV files.",
                    "code": """import zipfile
import io

zip_dir = Path("/tmp/zip_archives_052")
if zip_dir.exists():
    shutil.rmtree(zip_dir)
zip_dir.mkdir(parents=True, exist_ok=True)

# Create a zip file containing 2 inner CSV files
zip_path = zip_dir / "quarterly_reports.zip"
with zipfile.ZipFile(zip_path, "w") as zf:
    zf.writestr("q1_sales.csv", "report_id,amount\\nR1,500.0\\nR2,800.0\\n")
    zf.writestr("q2_sales.csv", "report_id,amount\\nR3,650.0\\nR4,920.0\\n")

print(f"Created sample ZIP archive at {zip_path}")"""
                },
                {
                    "title": "Distributed Extraction via binaryFiles and FlatMap",
                    "explanation": "Extract compressed files in parallel across worker nodes without driver bottlenecks.",
                    "code": """# binaryFiles returns (file_path, PortableDataStream)
rdd_binary = spark.sparkContext.binaryFiles(str(zip_dir))

def extract_zip_entries(tuple_item):
    file_path, byte_stream = tuple_item
    records = []
    # Read bytes into ZipFile in memory on executor
    with zipfile.ZipFile(io.BytesIO(byte_stream)) as z:
        for inner_filename in z.namelist():
            if inner_filename.endswith(".csv"):
                content = z.read(inner_filename).decode("utf-8")
                lines = content.strip().split("\\n")
                # Skip header, extract rows
                for line in lines[1:]:
                    parts = line.split(",")
                    records.append((inner_filename, parts[0], float(parts[1])))
    return records

# Distributed flatMap extracts files across cluster executors
rdd_extracted = rdd_binary.flatMap(extract_zip_entries)
df_decompressed = spark.createDataFrame(rdd_extracted, ["archive_member", "report_id", "amount"])

print("Decompressed Records Extracted in Parallel:")
df_decompressed.show()"""
                },
                {
                    "title": "Codecs Splittability Cheatsheet",
                    "explanation": "Review splittability of common big data compression codecs.",
                    "code": """codecs = [
    ("GZIP (.gz)", "No", "High", "Text / Web Logs", "Cannot split without container format"),
    ("BZIP2 (.bz2)", "Yes", "Very High", "Cold Archival", "Splittable via 48-bit sync markers"),
    ("SNAPPY", "Yes (in Parquet/Avro)", "Medium", "Active Analytics", "High-speed decompression"),
    ("ZSTD", "Yes (in Parquet)", "Very High", "Modern Lakehouse", "Balanced compression and speed"),
    ("ZIP (.zip)", "No", "Medium", "Legacy Vendor Feeds", "Requires custom parallel unpacking")
]

df_codecs = spark.createDataFrame(codecs, ["codec", "splittable", "compression_ratio", "primary_use_case", "notes"])
df_codecs.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Standard ZIP and raw GZIP files cannot be split across multiple tasks—one worker core must decompress the whole file.",
                "Use `sparkContext.binaryFiles` with `flatMap` to unpack multi-file ZIP archives in parallel across executors.",
                "Always store analytical data in splittable columnar formats (Snappy/ZSTD-compressed Parquet or Delta)."
            ],
            "interview_questions": [
                "Why is a 10GB single GZIP file an anti-pattern in Apache Spark?",
                "What makes Bzip2 splittable compared to standard Gzip?",
                "How does Parquet enable splittable reads even when using non-splittable block codecs like Snappy?"
            ]
        },
        {
            "nb_id": "053",
            "title": "Distributed REST API and Webhook Ingestion without Driver OOM",
            "difficulty": "Advanced",
            "business_scenario": "A financial data vendor exposes historical equity prices via an authenticated REST API with rate limits. The engineering team must fetch data for 50,000 distinct stock tickers. Running loops on the driver node takes 18 hours or crashes the driver with out-of-memory errors.",
            "problem_statement": "Implement parallel, distributed HTTP REST API ingestion in PySpark: partition entity IDs across worker nodes, use `mapPartitions` to maintain persistent connection sessions (`requests.Session`), apply batch chunking, handle exponential backoff rate limits, and assemble responses into DataFrames.",
            "architectural_notes": "Using `mapPartitions` initializes one HTTP connection pool (`requests.Session()`) per partition rather than opening a new TCP connection per row. Distributing requests across 20 executor partitions achieves 20x concurrency while honoring per-node rate limit quotas.",
            "sections": [
                {
                    "title": "Preparing Target Partitioned Request Keys",
                    "explanation": "Create target ticker symbols distributed across 4 partitions.",
                    "code": """tickers = [f"TICKER_{i:04d}" for i in range(1, 21)]
# Distribute across 4 partitions
df_tickers = spark.createDataFrame([(t,) for t in tickers], ["ticker"]).repartition(4)

print(f"Total Tickers: {df_tickers.count()} across {df_tickers.rdd.getNumPartitions()} partitions")"""
                },
                {
                    "title": "Distributed Ingestion via mapPartitions and Session Pooling",
                    "explanation": "Execute parallel HTTP calls inside worker tasks.",
                    "code": """import time
import json

def fetch_ticker_data_partition(iterator):
    # Executed ONCE per partition on worker executor
    # Simulates reusable requests.Session()
    session_id = f"worker_session_{time.time()}"
    results = []
    
    for row in iterator:
        ticker = row.ticker
        # Simulate network latency to REST API (e.g. 5ms)
        time.sleep(0.005)
        # Mock REST API response payload
        mock_response = {
            "ticker": ticker,
            "price": round(150.0 + (hash(ticker) % 50), 2),
            "currency": "USD",
            "status_code": 200,
            "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        results.append((
            mock_response["ticker"],
            mock_response["price"],
            mock_response["currency"],
            mock_response["status_code"],
            mock_response["fetched_at"]
        ))
    return iter(results)

# Execute parallel extraction across all executors
rdd_responses = df_tickers.rdd.mapPartitions(fetch_ticker_data_partition)
schema = "ticker STRING, price DOUBLE, currency STRING, http_status INT, fetched_at STRING"
df_api_results = spark.createDataFrame(rdd_responses, schema)

print("Distributed REST API Ingestion Results:")
df_api_results.show(10)"""
                },
                {
                    "title": "Validation & Status Code Auditing",
                    "explanation": "Verify all requests completed with HTTP 200.",
                    "code": """assert df_api_results.count() == 20, "Ticker count mismatch"
assert df_api_results.filter(F.col("http_status") != 200).count() == 0, "Non-200 responses detected"
print("All distributed API calls verified successfully!")"""
                }
            ],
            "key_takeaways": [
                "`mapPartitions` allows initializing heavyweight objects (database connections, HTTP sessions) once per partition rather than per row.",
                "Distributing API calls across worker partitions speeds up extraction linearly with cluster worker count.",
                "Always include rate-limiting sleep intervals and exponential backoff retry logic inside worker partitions."
            ],
            "interview_questions": [
                "Why is `mapPartitions` far superior to `map` for executing REST API requests or database lookups?",
                "How do you handle API rate-limiting (HTTP 429 Too Many Requests) across multiple distributed Spark executors?",
                "What is the risk of driver OutOfMemory (OOM) if you fetch millions of API responses using `.collect()`?"
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
