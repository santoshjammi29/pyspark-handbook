"""
Module 06: Streaming & Near Real-Time Processing (Notebooks 054 - 063)
"""

import re
from pathlib import Path
from notebook_builder import build_notebook, write_notebook

MODULE_FOLDER = "06_streaming_realtime"
MODULE_NAME = "Module 06: Streaming & Near Real-Time Processing"

def sanitize_filename(nb_id: str, title: str) -> str:
    cleaned = title.lower().replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return f"{nb_id}_{cleaned}.py"

def generate(base_dir: Path):
    out_dir = base_dir / "notebooks" / MODULE_FOLDER
    out_dir.mkdir(parents=True, exist_ok=True)

    notebooks = [
        {
            "nb_id": "054",
            "title": "Structured Streaming Foundations: Sources, Execution Model and Query Progress",
            "difficulty": "Intermediate",
            "business_scenario": "A payments platform monitors live transaction throughput. As card swipes occur across the globe, an unbounded stream must be parsed, enriched, and aggregated in real time without dropping micro-batches.",
            "problem_statement": "Master Spark Structured Streaming fundamentals: create a streaming DataFrame from the memory/rate stream source, inspect stream schema, configure streaming queries (`readStream` / `writeStream`), monitor execution metrics via `StreamingQueryProgress`, and manage query lifecycle.",
            "architectural_notes": "Spark Structured Streaming treats a live stream as an unbounded, continuously appended table. The engine processes micro-batches incrementally, maintaining offsets in a write-ahead log (WAL) and state in RocksDB/memory, providing end-to-end exactly-once fault tolerance.",
            "sections": [
                {
                    "title": "Creating an Unbounded Synthetic Streaming Source",
                    "explanation": "Use Spark's built-in 'rate' stream source to generate synthetic streaming timestamps.",
                    "code": """# rate source generates timestamp and monotonic value at configured rows per second
df_stream_raw = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .option("numPartitions", 2)
    .load()
)

print(f"Is DataFrame streaming: {df_stream_raw.isStreaming}")
df_stream_raw.printSchema()"""
                },
                {
                    "title": "Streaming Transformations: Deriving Live Transaction Metrics",
                    "explanation": "Apply standard DataFrame transformations over the unbounded streaming dataset.",
                    "code": """df_transactions_stream = df_stream_raw.select(
    F.col("timestamp").alias("event_time"),
    F.concat(F.lit("TXN-"), F.col("value")).alias("transaction_id"),
    F.round((F.col("value") % 500) + 10.50, 2).alias("amount"),
    F.when(F.col("value") % 10 == 0, "HIGH_RISK").otherwise("STANDARD").alias("risk_category")
)

df_transactions_stream.printSchema()"""
                },
                {
                    "title": "Starting Memory Sink & Inspecting StreamingQueryProgress",
                    "explanation": "Start stream writing to in-memory table for interactive querying and inspect progress metrics.",
                    "code": """import time

# Start streaming query writing to in-memory table
query = (
    df_transactions_stream.writeStream
    .format("memory")
    .queryName("live_transactions")
    .outputMode("append")
    .start()
)

print(f"Streaming query started! ID: {query.id} | Status: {query.status['message']}")

# Let the stream run for 3 seconds to accumulate micro-batches
time.sleep(3)

# Query recent progress
progress = query.lastProgress
if progress:
    print(f"Input Rate: {progress.get('inputRowsPerSecond', 0):.1f} rows/sec")
    print(f"Processing Rate: {progress.get('processedRowsPerSecond', 0):.1f} rows/sec")
    print(f"Batch ID: {progress.get('batchId', 0)}")

# Query the in-memory streaming table using Spark SQL
print("Live In-Memory Streaming Snapshot:")
spark.sql("SELECT * FROM live_transactions ORDER BY event_time DESC LIMIT 5").show(truncate=False)

# Safely stop streaming query
query.stop()
print("Streaming query safely stopped.")"""
                }
            ],
            "key_takeaways": [
                "Structured Streaming abstracts streaming into an unbounded table queried using standard DataFrame API operations.",
                "Output modes: `append` (emits new rows), `update` (emits changed rows), `complete` (emits entire aggregated table).",
                "`query.lastProgress` provides vital telemetry: input rows per second, processing latency, and state memory."
            ],
            "interview_questions": [
                "What is the difference between micro-batch execution and Continuous Processing mode in Spark Streaming?",
                "How does the checkpoint directory guarantee exactly-once processing during executor node crashes?",
                "What are the three output modes in Structured Streaming and when must each be used?"
            ]
        },
        {
            "nb_id": "055",
            "title": "Streaming Triggers: ProcessingTime, Once and AvailableNow for Cost Optimization",
            "difficulty": "Intermediate",
            "business_scenario": "A cloud financial platform runs streaming pipelines. Running clusters 24/7 costs $50,000/month. Because source data arrives in hourly file batches, operations wants to run streaming pipelines incrementally on-demand, process all backlogged data, and terminate clusters immediately.",
            "problem_statement": "Compare and configure Structured Streaming triggers: (1) Fixed interval micro-batching (`Trigger.ProcessingTime('10 seconds')`), (2) Legacy single-batch (`Trigger.Once()`), and (3) Modern Databricks/Spark 3.3+ cost-optimized `Trigger.AvailableNow()` which processes all outstanding data across multiple micro-batches before shutting down.",
            "architectural_notes": "`Trigger.AvailableNow` solves the major limitation of `Trigger.Once`. While `Trigger.Once` loads all pending data into a single giant micro-batch (causing OutOfMemory crashes on large backlogs), `Trigger.AvailableNow` respects rate-limiting configurations, executing multiple properly-sized micro-batches until all backlogged data is processed, then cleanly terminates.",
            "sections": [
                {
                    "title": "Trigger Architecture Comparison Blueprint",
                    "explanation": "Evaluate the execution lifecycle of different streaming triggers.",
                    "code": """# Architectural Comparison of Triggers:
# 1. ProcessingTime('30 seconds'): Continuously running 24/7 cluster. Evaluates every 30s.
# 2. Once(): Single micro-batch. Dangerous if 100GB of backlogged data has accumulated.
# 3. AvailableNow(): Best practice for cost-effective incremental batching!
#    Processes backlog in multiple rate-limited micro-batches, commits checkpoints, and shuts down.

print("Streaming trigger architectural profiles defined.")"""
                },
                {
                    "title": "Simulating Incremental Processing with Trigger.AvailableNow",
                    "explanation": "Run an incremental streaming pass using AvailableNow.",
                    "code": """import shutil
from pathlib import Path

stream_source_dir = Path("/tmp/stream_trigger_source_055")
stream_sink_dir = Path("/tmp/stream_trigger_sink_055")
stream_chk_dir = Path("/tmp/stream_trigger_chk_055")

for p in [stream_source_dir, stream_sink_dir, stream_chk_dir]:
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)

# Write initial batch of files to source
with open(stream_source_dir / "events_1.json", "w") as f:
    f.write('{"id": 1, "val": "A"}\\n{"id": 2, "val": "B"}\\n')

schema = "id INT, val STRING"
df_file_stream = spark.readStream.schema(schema).json(str(stream_source_dir))

# Execute using Trigger.AvailableNow (terminates automatically when done)
query = (
    df_file_stream.writeStream
    .format("delta")
    .option("checkpointLocation", str(stream_chk_dir))
    .trigger(availableNow=True)
    .start(str(stream_sink_dir))
)

query.awaitTermination()
print("Trigger.AvailableNow completed successfully and terminated cleanly!")

print("Target Delta Table Output:")
spark.read.format("delta").load(str(stream_sink_dir)).show()"""
                },
                {
                    "title": "Verifying Incremental Offset Advancement",
                    "explanation": "Add more files and run another AvailableNow pass to verify exactly-once continuation.",
                    "code": """# Add second batch of files
with open(stream_source_dir / "events_2.json", "w") as f:
    f.write('{"id": 3, "val": "C"}\\n{"id": 4, "val": "D"}\\n')

# Run second AvailableNow pass using the SAME checkpoint directory
query2 = (
    df_file_stream.writeStream
    .format("delta")
    .option("checkpointLocation", str(stream_chk_dir))
    .trigger(availableNow=True)
    .start(str(stream_sink_dir))
)
query2.awaitTermination()

print("Target Delta Table after Second AvailableNow Run (Total 4 records):")
spark.read.format("delta").load(str(stream_sink_dir)).show()"""
                }
            ],
            "key_takeaways": [
                "`Trigger.AvailableNow` processes all outstanding backlog across multiple micro-batches and terminates automatically.",
                "Ideal for orchestrating streaming pipelines from Databricks Workflows or Airflow on transient single-job clusters.",
                "Provides the exact streaming checkpointing and exactly-once semantics without running expensive 24/7 idle clusters."
            ],
            "interview_questions": [
                "Why was `Trigger.AvailableNow` introduced in Spark 3.3 to replace `Trigger.Once`?",
                "How does `Trigger.AvailableNow` enable significant cloud FinOps cost savings?",
                "What happens to streaming state when a query with `Trigger.AvailableNow` completes?"
            ]
        },
        {
            "nb_id": "056",
            "title": "Streaming Window Aggregations and Watermarking for Late-Arriving Events",
            "difficulty": "Advanced",
            "business_scenario": "A mobile gaming platform aggregates player score events over 10-minute tumbling windows. Due to intermittent mobile connectivity and airplane mode, score events arrive up to 15 minutes late. The pipeline must incorporate late events without letting state size grow infinitely.",
            "problem_statement": "Implement streaming window aggregations with watermarking: configure `withWatermark('event_time', '10 minutes')`, compute tumbling and sliding window metrics, and explain how the watermark dictates state eviction from RocksDB memory.",
            "architectural_notes": "A watermark specifies the maximum threshold by which event-time can lag behind the maximum event-time observed by the engine ($Watermark = Max(event\_time) - Delay$). Once a window's end time falls behind the watermark, Spark emits the final aggregate and purges the window's state from memory.",
            "sections": [
                {
                    "title": "Understanding Watermark State Cleanup Mathematics",
                    "explanation": "Illustrate how watermarking bounds memory growth in stateful streaming.",
                    "code": """# Architectural Blueprint:
# Event Time: 12:00 -> Watermark = 12:00 - 10 min = 11:50
# Window [11:30 - 11:40]: Completely older than 11:50 -> State Evicted! No longer accepts updates.
# Window [11:45 - 11:55]: End time (11:55) > 11:50 -> Window still active in state memory.

print("Watermark state eviction semantics mapped.")"""
                },
                {
                    "title": "Simulating Event Stream with Deliberate Late-Arriving Records",
                    "explanation": "Create memory stream with varied event timestamps.",
                    "code": """from datetime import datetime, timedelta

base_time = datetime(2023, 11, 20, 12, 0, 0)

# Simulate events arriving out of chronological order
batch_events = [
    # On-time events
    (base_time, "Alice", 100),
    (base_time + timedelta(minutes=2), "Bob", 150),
    # Advance watermark to 12:15
    (base_time + timedelta(minutes=15), "Charlie", 300),
    # Late event from 12:03 (Arrives when max time is 12:15, delta = 12 min. Allowed if watermark >= 15 min!)
    (base_time + timedelta(minutes=3), "Alice", 50)
]

schema = "event_time TIMESTAMP, player STRING, score INT"
df_events = spark.createDataFrame(batch_events, schema)
df_events.show(truncate=False)"""
                },
                {
                    "title": "Configuring Window Aggregations with Watermark",
                    "explanation": "Apply 10-minute tumbling window with 15-minute watermark delay.",
                    "code": """# Tumbling window aggregation with watermark
df_windowed = (
    df_events
    .withWatermark("event_time", "15 minutes")
    .groupBy(
        F.window("event_time", "10 minutes"),
        "player"
    )
    .agg(
        F.sum("score").alias("total_score"),
        F.count("score").alias("events_count")
    )
    .select(
        "window.start",
        "window.end",
        "player",
        "total_score",
        "events_count"
    )
    .orderBy("start", "player")
)

print("Aggregated Window Output (Late Event Incorporated):")
df_windowed.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "`withWatermark(time_col, delay)` informs Spark how long to retain intermediate aggregation state for late data.",
                "Events arriving older than the current watermark are dropped without affecting state.",
                "Watermarking is mandatory for stateful streaming aggregations in `append` mode to prevent infinite memory leaks."
            ],
            "interview_questions": [
                "What is a Watermark in Spark Structured Streaming and how is it calculated?",
                "What happens to an event if its `event_time` is strictly older than the current watermark?",
                "Why can stateful streaming aggregations NOT run in `append` mode without a watermark?"
            ]
        },
        {
            "nb_id": "057",
            "title": "Stream-Static Joins with Slowly Changing Dimension Lookups",
            "difficulty": "Intermediate",
            "business_scenario": "A financial fraud detection system processes streaming credit card swipe authorizations. Each incoming transaction contains only a `customer_id`. The stream must be joined with a static Customer Profile table to enrich swipes with user credit limit, billing address, and risk score.",
            "problem_statement": "Implement Stream-Static relational joins in PySpark: join an unbounded streaming DataFrame with a static Delta dimension table, observe how Spark automatically refreshes static table reads across micro-batches, and evaluate performance considerations.",
            "architectural_notes": "In a Stream-Static join, the static DataFrame is re-evaluated by Spark at the beginning of each streaming micro-batch. If the static table is stored in Delta Lake, updates made to the static table are picked up automatically by subsequent micro-batches without restarting the streaming query.",
            "sections": [
                {
                    "title": "Creating Static Customer Profile Dimension Table in Delta",
                    "explanation": "Create static customer lookup table.",
                    "code": """static_dim_path = "/tmp/delta_dim_customers_057"
if Path(static_dim_path).exists():
    shutil.rmtree(static_dim_path)

cust_profiles = [
    ("C-101", "Alice Smith", "PLATINUM", 25000.0, "US"),
    ("C-102", "Bob Jones", "GOLD", 10000.0, "UK"),
    ("C-103", "Charlie Day", "STANDARD", 2000.0, "CA")
]

schema = "customer_id STRING, customer_name STRING, tier STRING, credit_limit DOUBLE, country STRING"
spark.createDataFrame(cust_profiles, schema).write.format("delta").mode("overwrite").save(static_dim_path)

# Load as static DataFrame
df_static_customers = spark.read.format("delta").load(static_dim_path)
print("Static Customer Dimension Table:")
df_static_customers.show()"""
                },
                {
                    "title": "Simulating Streaming Transaction Batch",
                    "explanation": "Create streaming transactions to join against static customer dimension.",
                    "code": """transactions = [
    ("TXN-01", "C-101", 450.00, "2023-11-20 14:00:00"),
    ("TXN-02", "C-102", 12000.00, "2023-11-20 14:02:00"), # Exceeds credit limit!
    ("TXN-03", "C-103", 85.50, "2023-11-20 14:05:00"),
    ("TXN-04", "C-999", 50.00, "2023-11-20 14:10:00")    # Unknown customer
]

schema_txn = "txn_id STRING, customer_id STRING, amount DOUBLE, txn_time STRING"
df_stream_mock = spark.createDataFrame(transactions, schema_txn)
df_stream_mock.show()"""
                },
                {
                    "title": "Executing Stream-Static Join & Fraud Flag Derivation",
                    "explanation": "Join streaming authorizations with static profile to detect credit limit breaches.",
                    "code": """# Left join streaming transactions with static customer profiles
df_enriched_stream = df_stream_mock.join(
    df_static_customers,
    on="customer_id",
    how="left"
).withColumn(
    "is_limit_breached",
    F.when(F.col("amount") > F.col("credit_limit"), True).otherwise(False)
).withColumn(
    "fraud_alert",
    F.when(F.col("customer_name").isNull(), "UNKNOWN_CUSTOMER")
     .when(F.col("is_limit_breached"), "CREDIT_LIMIT_EXCEEDED")
     .otherwise("APPROVED")
)

print("Enriched Stream Results with Fraud Flagging:")
df_enriched_stream.select(
    "txn_id", "customer_id", "customer_name", "amount", "credit_limit", "fraud_alert"
).show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Stream-Static joins do NOT require state tracking or watermarks because the static table is fully bounded.",
                "When joining with a static Delta table, Spark reads the latest snapshot of the Delta table for each micro-batch.",
                "Outer stream-static joins are supported: `stream.join(static, how='left')` retains all stream records."
            ],
            "interview_questions": [
                "Does a Stream-Static join require watermarking or state management?",
                "What happens if new rows are added to the static Delta table while the stream is running?",
                "Can you perform a right-outer join where the stream is on the right and static table is on the left?"
            ]
        },
        {
            "nb_id": "058",
            "title": "Stream-Stream Dual Watermark Joins with Interval Conditions",
            "difficulty": "Advanced",
            "business_scenario": "An online retail logistics platform matches checkout order events with payment confirmation events arriving from Stripe/PayPal across separate event streams. Orders and payments can arrive seconds or minutes apart.",
            "problem_statement": "Implement Stream-Stream joins in PySpark: configure dual watermarks on both streaming sides, specify mandatory time-range interval join constraints (`payment_time BETWEEN order_time AND order_time + INTERVAL 1 HOUR`), and explain how state retention boundaries are enforced.",
            "architectural_notes": "In a stream-stream join, both datasets are unbounded. Spark must buffer incoming rows from stream A in state store until a matching row arrives from stream B. To prevent state store memory from growing infinitely, Spark strictly requires both a watermark on each stream and a time interval constraint.",
            "sections": [
                {
                    "title": "Stream-Stream Join Architectural Requirements Blueprint",
                    "explanation": "Detail the three mandatory requirements for stream-stream joins in PySpark.",
                    "code": """# Mandatory Requirements for Stream-Stream Joins:
# 1. Watermark defined on Stream 1: df_orders.withWatermark("order_time", "2 hours")
# 2. Watermark defined on Stream 2: df_payments.withWatermark("payment_time", "3 hours")
# 3. Time Interval Condition:
#    (orders.order_id == payments.order_id) &
#    (payments.payment_time >= orders.order_time) &
#    (payments.payment_time <= orders.order_time + expr("INTERVAL 1 HOUR"))

print("Stream-Stream state retention constraints blueprint mapped.")"""
                },
                {
                    "title": "Simulating Dual Order and Payment Streams",
                    "explanation": "Create representative streams with varied timestamp offsets.",
                    "code": """from datetime import datetime, timedelta

base_ts = datetime(2023, 11, 20, 10, 0, 0)

orders_batch = [
    ("ORD-1", 120.00, base_ts),
    ("ORD-2", 450.00, base_ts + timedelta(minutes=5)),
    ("ORD-3", 85.00,  base_ts + timedelta(minutes=10))
]

payments_batch = [
    ("ORD-1", "PAY-901", base_ts + timedelta(minutes=2)),   # Matched (2 min after order)
    ("ORD-2", "PAY-902", base_ts + timedelta(minutes=45)),  # Matched (45 min after order)
    ("ORD-99", "PAY-999", base_ts + timedelta(minutes=15))  # Unmatched payment
]

schema_ord = "order_id STRING, amount DOUBLE, order_time TIMESTAMP"
schema_pay = "order_id STRING, payment_id STRING, payment_time TIMESTAMP"

df_ord = spark.createDataFrame(orders_batch, schema_ord)
df_pay = spark.createDataFrame(payments_batch, schema_pay)

print("Orders Stream Batch:")
df_ord.show(truncate=False)
print("Payments Stream Batch:")
df_pay.show(truncate=False)"""
                },
                {
                    "title": "Executing Stream-Stream Join with Interval Condition",
                    "explanation": "Join with time range boundary constraints.",
                    "code": """# Join condition pairing primary key match with time range window
join_cond = (
    (df_ord["order_id"] == df_pay["order_id"]) &
    (df_pay["payment_time"] >= df_ord["order_time"]) &
    (df_pay["payment_time"] <= df_ord["order_time"] + F.expr("INTERVAL 1 HOUR"))
)

df_matched_orders = df_ord.join(df_pay, on=join_cond, how="inner").select(
    df_ord["order_id"],
    "amount",
    "order_time",
    "payment_id",
    "payment_time",
    F.round((F.col("payment_time").cast("long") - F.col("order_time").cast("long")) / 60.0, 1).alias("payment_delay_minutes")
)

print("Stream-Stream Matched Order-Payment Events:")
df_matched_orders.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Stream-Stream joins require both watermarks AND time interval boundaries to limit state store retention.",
                "Without interval constraints, Spark would need to buffer every un-joined order forever in memory.",
                "Outer stream-stream joins (left/right outer) emit null records only after the watermark passes the interval window."
            ],
            "interview_questions": [
                "Why are time interval constraints mandatory for stream-stream joins in Apache Spark?",
                "When does an outer stream-stream join emit a row with NULLs for the missing side?",
                "How does RocksDB state store manage memory across dual-stream joins?"
            ]
        },
        {
            "nb_id": "059",
            "title": "Streaming Deduplication Across Time Windows with Watermarks",
            "difficulty": "Intermediate",
            "business_scenario": "An IoT gateway ingests sensor readings from connected vehicles over cellular networks. When vehicles enter tunnels or experience network reconnects, duplicate messages are re-sent. The pipeline must eliminate duplicate sensor IDs within a rolling 20-minute window.",
            "problem_statement": "Implement streaming deduplication: apply `withWatermark()` paired with `dropDuplicates()` on primary keys and event timestamps. Explain how the watermark bounds the historical key state maintained by the streaming engine.",
            "architectural_notes": "In Structured Streaming, calling `dropDuplicates(['key'])` without a watermark causes Spark to store every primary key ever seen in memory forever, eventually triggering an Executor OOM. Supplying a watermark allows Spark to evict keys older than the watermark threshold from the state store.",
            "sections": [
                {
                    "title": "Understanding Streaming Deduplication State Store Eviction",
                    "explanation": "Illustrate how watermarking enables bounded key tracking in state stores.",
                    "code": """# Architectural Blueprint:
# stream.withWatermark("event_time", "20 minutes").dropDuplicates(["device_id", "event_time"])
# Keys older than (max_event_time - 20 minutes) are evicted from memory.
# New duplicates within the 20-minute window are safely identified and dropped!

print("Streaming deduplication memory bounds blueprint mapped.")"""
                },
                {
                    "title": "Simulating Sensor Stream with Duplicate Ingestion",
                    "explanation": "Create dataset containing both unique and duplicate readings across time.",
                    "code": """from datetime import datetime, timedelta

base_ts = datetime(2023, 11, 20, 15, 0, 0)

telemetry_stream = [
    ("DEV-01", base_ts, 22.5),
    ("DEV-02", base_ts, 25.0),
    ("DEV-01", base_ts, 22.5), # Exact duplicate within window -> MUST DROP
    ("DEV-01", base_ts + timedelta(minutes=5), 23.0), # Valid new reading -> KEEP
    ("DEV-03", base_ts + timedelta(minutes=10), 19.5)
]

schema = "device_id STRING, event_time TIMESTAMP, temp DOUBLE"
df_stream_telemetry = spark.createDataFrame(telemetry_stream, schema)
df_stream_telemetry.show(truncate=False)"""
                },
                {
                    "title": "Executing Watermarked Streaming Deduplication",
                    "explanation": "Apply watermark and dropDuplicates over device_id and event_time.",
                    "code": """# Deduplicate streaming data within a 20-minute watermark window
df_deduped = (
    df_stream_telemetry
    .withWatermark("event_time", "20 minutes")
    .dropDuplicates(["device_id", "event_time"])
)

print("Deduplicated Stream Output:")
df_deduped.orderBy("device_id", "event_time").show(truncate=False)

# Verification assertion
assert df_deduped.count() == 4, f"Expected 4 unique readings, found {df_deduped.count()}"
print("Streaming deduplication assertion verified successfully!")"""
                }
            ],
            "key_takeaways": [
                "Always combine `dropDuplicates([keys, timestamp_col])` with `withWatermark()` in streaming jobs.",
                "Omitting the watermark causes state store memory to grow monotonically until the cluster crashes.",
                "Duplicates arriving after the watermark has passed may be re-processed as new records; size the watermark to cover maximum expected network delay."
            ],
            "interview_questions": [
                "What happens to the state store if you call `dropDuplicates(['id'])` without a watermark in Structured Streaming?",
                "Why must the event-time column be included in `dropDuplicates` when using watermarks?",
                "How does RocksDB state store handle state recovery during executor restarts?"
            ]
        },
        {
            "nb_id": "060",
            "title": "Idempotent ACID Delta Upserts using writeStream.foreachBatch",
            "difficulty": "Advanced",
            "business_scenario": "A payments streaming pipeline consumes live financial transactions that include both new purchases and refund updates to existing transactions. Standard streaming `append` creates duplicate rows; the stream must perform idempotent ACID `MERGE INTO` operations into target Delta tables.",
            "problem_statement": "Master `writeStream.foreachBatch()`: design a micro-batch function that executes an atomic `DeltaTable.merge()` operation, ensures idempotency across micro-batch retries, and avoids race conditions in distributed streaming sinks.",
            "architectural_notes": "`foreachBatch` gives access to the output DataFrame of each micro-batch as a standard static DataFrame. This allows executing non-streaming operations—such as Delta Lake `MERGE INTO`, writing to multiple destinations, or executing custom API calls—inside a streaming query.",
            "sections": [
                {
                    "title": "Initializing Target Delta Silver Ledger Table",
                    "explanation": "Create target Delta table for transactions.",
                    "code": """from delta.tables import DeltaTable

silver_ledger_path = "/tmp/delta_silver_ledger_060"
if Path(silver_ledger_path).exists():
    shutil.rmtree(silver_ledger_path)

initial_txns = [
    ("TXN-101", "CUST-1", 100.0, "PENDING", "2023-11-20 10:00:00"),
    ("TXN-102", "CUST-2", 250.0, "SETTLED", "2023-11-20 10:05:00")
]

schema = "txn_id STRING, customer_id STRING, amount DOUBLE, status STRING, updated_at STRING"
spark.createDataFrame(initial_txns, schema).write.format("delta").mode("overwrite").save(silver_ledger_path)

target_delta = DeltaTable.forPath(spark, silver_ledger_path)
print("Initial Silver Ledger Delta Table:")
target_delta.toDF().show()"""
                },
                {
                    "title": "Defining the Idempotent foreachBatch Upsert Function",
                    "explanation": "Implement micro-batch merge function with deduplication and ACID upsert.",
                    "code": """def upsert_to_delta(micro_batch_df, batch_id):
    \"\"\"
    Invoked for each micro-batch. Performs idempotent MERGE into target Delta table.
    \"\"\"
    print(f"--> Processing Micro-Batch ID: {batch_id} (Rows: {micro_batch_df.count()})")
    
    # 1. Deduplicate micro-batch by taking the latest update per txn_id
    window_spec = Window.partitionBy("txn_id").orderBy(F.col("updated_at").desc())
    deduped_batch = micro_batch_df.withColumn("rn", F.row_number().over(window_spec))\
                                 .filter(F.col("rn") == 1)\
                                 .drop("rn")
    
    # 2. Atomic MERGE into target Delta table
    target_table = DeltaTable.forPath(spark, silver_ledger_path)
    (
        target_table.alias("target")
        .merge(
            source=deduped_batch.alias("source"),
            condition="target.txn_id = source.txn_id"
        )
        .whenMatchedUpdate(
            set={
                "amount": F.col("source.amount"),
                "status": F.col("source.status"),
                "updated_at": F.col("source.updated_at")
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"--> Micro-Batch {batch_id} merged successfully into Delta table.")

print("foreachBatch upsert function defined.")"""
                },
                {
                    "title": "Testing foreachBatch Execution with Incoming Stream Batch",
                    "explanation": "Simulate streaming micro-batch executing the upsert logic.",
                    "code": """# Incoming micro-batch: TXN-101 is updated from PENDING to SETTLED; TXN-103 is a new transaction
micro_batch_1 = [
    ("TXN-101", "CUST-1", 100.0, "SETTLED", "2023-11-20 10:15:00"), # Update
    ("TXN-103", "CUST-3", 75.0,  "SETTLED", "2023-11-20 10:20:00")  # Insert
]

df_mb1 = spark.createDataFrame(micro_batch_1, schema)

# Execute the upsert function directly (simulating writeStream.foreachBatch)
upsert_to_delta(df_mb1, batch_id=1)

print("Final Target Delta Table (Notice TXN-101 updated, TXN-103 inserted):")
target_delta.toDF().orderBy("txn_id").show()"""
                }
            ],
            "key_takeaways": [
                "`foreachBatch()` bridges streaming DataFrames with batch APIs like Delta Lake `MERGE INTO`.",
                "Always deduplicate the micro-batch DataFrame before calling `merge()` to avoid `DeltaAnalysisException` on duplicate source keys.",
                "Ensure operations inside `foreachBatch` are idempotent so pipeline retries upon failure do not duplicate data."
            ],
            "interview_questions": [
                "Why can't you use standard `.writeStream.format('delta')` with `.mode('update')` directly in PySpark?",
                "How does `foreachBatch` handle failure recovery and ensure exactly-once semantics?",
                "What performance issues can arise if the micro-batch in `foreachBatch` is not pre-deduplicated?"
            ]
        },
        {
            "nb_id": "061",
            "title": "Arbitrary Stateful Streaming Operations with FlatMapGroupsWithState Concepts",
            "difficulty": "Advanced",
            "business_scenario": "A telecommunications security monitor detects SIM-swap and fraud patterns across customer cell towers. A fraud flag must trigger only if a user connects to two distinct cell towers separated by 500+ miles within a 15-minute window.",
            "problem_statement": "Master arbitrary stateful streaming concepts: compare simple window aggregations with custom state machines, understand the mechanics of `mapGroupsWithState` and `flatMapGroupsWithState`, manage state expiration timeouts (ProcessingTime vs EventTime), and handle state state eviction.",
            "architectural_notes": "`mapGroupsWithState` and `flatMapGroupsWithState` allow developers to maintain arbitrary user-defined state objects across streaming batches. While full API implementation requires Scala/Java or PySpark with custom encoders, understanding the stateful machine model is critical for senior data engineers.",
            "sections": [
                {
                    "title": "Arbitrary Stateful Processing Architectural Blueprint",
                    "explanation": "Illustrate how state machines track custom sessions and timeout states.",
                    "code": """# State Machine Model:
# Inputs: (Key: user_id, Value: current_cell_event, State: UserTravelHistory)
# Operations:
# 1. If State is Empty: Initialize state with current tower and timestamp.
# 2. If State Exists: Calculate speed/distance from last tower.
# 3. If impossible velocity detected (> 500 mph): Emit FRAUD_ALERT event!
# 4. Update state with current tower.
# 5. Set Timeout: Evict state after 30 minutes of inactivity to bound RocksDB memory.

print("Arbitrary state machine architecture mapped.")"""
                },
                {
                    "title": "Simulating Stateful Machine Transitions in PySpark",
                    "explanation": "Model state transitions using window state tracking.",
                    "code": """tower_events = [
    ("USER-1", "TOWER-NYC", 40.7128, -74.0060, "2023-11-20 10:00:00"),
    ("USER-1", "TOWER-BOS", 42.3601, -71.0589, "2023-11-20 10:08:00"), # 200 miles in 8 min -> Impossible speed!
    ("USER-2", "TOWER-SFO", 37.7749, -122.4194, "2023-11-20 10:00:00"),
    ("USER-2", "TOWER-OAK", 37.8044, -122.2712, "2023-11-20 10:30:00") # 10 miles in 30 min -> Normal
]

schema = "user_id STRING, tower_id STRING, lat DOUBLE, lon DOUBLE, event_time STRING"
df_towers = spark.createDataFrame(tower_events, schema).withColumn("ts", F.to_timestamp("event_time"))

# Stateful calculation: compare with previous tower event in state
user_state_win = Window.partitionBy("user_id").orderBy("ts")

df_state_transitions = (
    df_towers
    .withColumn("prev_tower", F.lag("tower_id").over(user_state_win))
    .withColumn("prev_lat", F.lag("lat").over(user_state_win))
    .withColumn("prev_lon", F.lag("lon").over(user_state_win))
    .withColumn("prev_ts", F.lag("ts").over(user_state_win))
    .withColumn(
        "elapsed_minutes",
        (F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 60.0
    )
    .withColumn(
        # Approximate distance in miles
        "distance_miles",
        F.round(F.sqrt(F.pow((F.col("lat") - F.col("prev_lat")) * 69.0, 2) + F.pow((F.col("lon") - F.col("prev_lon")) * 55.0, 2)), 1)
    )
    .withColumn(
        "calculated_mph",
        F.when(F.col("elapsed_minutes") > 0, F.round(F.col("distance_miles") / (F.col("elapsed_minutes") / 60.0), 1)).otherwise(0.0)
    )
    .withColumn(
        "is_fraud_velocity",
        F.when(F.col("calculated_mph") > 500.0, True).otherwise(False)
    )
)

print("Stateful Machine Velocity Analysis:")
df_state_transitions.select("user_id", "tower_id", "prev_tower", "elapsed_minutes", "distance_miles", "calculated_mph", "is_fraud_velocity").show(truncate=False)"""
                },
                {
                    "title": "State Timeout & Eviction Strategy",
                    "explanation": "Highlight how timeout policies prevent state store memory leaks.",
                    "code": """# Production Timeout Guidelines:
# 1. EventTimeTimeout: State expires based on the watermark timestamp (reproducible across replays).
# 2. ProcessingTimeTimeout: State expires based on wall-clock time (simpler, but non-deterministic in replays).
# Always call state.remove() upon session termination to reclaim RocksDB memory!

print("State timeout and memory cleanup rules verified.")"""
                }
            ],
            "key_takeaways": [
                "`mapGroupsWithState` enables custom complex state machines beyond simple tumbling/sliding aggregations.",
                "Always configure timeouts (`GroupStateTimeout.EventTimeTimeout`) to evict stale keys from state stores.",
                "RocksDB is the recommended state store provider for large stateful pipelines in production."
            ],
            "interview_questions": [
                "What is the difference between `mapGroupsWithState` and `flatMapGroupsWithState`?",
                "What are the two timeout types supported in stateful streaming and how do they differ?",
                "How does RocksDB state store overcome JVM heap limitations for multi-gigabyte streaming states?"
            ]
        },
        {
            "nb_id": "062",
            "title": "Dead-Letter Queue (DLQ) Quarantine Streaming Pattern in PySpark",
            "difficulty": "Intermediate",
            "business_scenario": "A real-time clickstream ingestion pipeline processes 10,000 events/second. A software release on the web client introduces malformed JSON payloads. Crashing the streaming query halts all company analytics; silently ignoring errors causes undetected data loss.",
            "problem_statement": "Implement the enterprise Dead-Letter Queue (DLQ) streaming pattern: split incoming streams into a clean Silver table and a Quarantine DLQ Delta table within `foreachBatch`, capturing failure stack traces and raw payloads without interrupting the live pipeline.",
            "architectural_notes": "A Dead-Letter Queue pattern ensures continuous pipeline uptime (zero downtime). Clean records flow unimpeded to production analytical tables, while corrupted, unparseable, or schema-violating rows are diverted to a quarantine table for engineering alerting and automated replay.",
            "sections": [
                {
                    "title": "Simulating Mixed Clean and Corrupted Streaming Ingestion",
                    "explanation": "Create a batch with valid and damaged message payloads.",
                    "code": """quarantine_dlq_path = "/tmp/delta_dlq_062"
clean_silver_path = "/tmp/delta_clean_062"

for p in [quarantine_dlq_path, clean_silver_path]:
    if Path(p).exists():
        shutil.rmtree(p)

incoming_events = [
    ("E-01", '{"user_id": "U1", "amount": 150.0}'),
    ("E-02", 'CORRUPTED_NON_JSON_STRING'),
    ("E-03", '{"user_id": "U2", "amount": -9999.0}'), # Semantic business violation
    ("E-04", '{"user_id": "U3", "amount": 85.0}')
]

schema = "event_id STRING, raw_payload STRING"
df_incoming = spark.createDataFrame(incoming_events, schema)
df_incoming.show(truncate=False)"""
                },
                {
                    "title": "DLQ Routing Logic with Schema Parsing and Validation",
                    "explanation": "Parse JSON with schema and apply validation rules to segregate clean vs dirty rows.",
                    "code": """payload_schema = T.StructType([
    T.StructField("user_id", T.StringType(), True),
    T.StructField("amount", T.DoubleType(), True)
])

# Attempt parsing
df_attempt = df_incoming.withColumn("parsed", F.from_json("raw_payload", payload_schema))

# Classify validation errors
df_classified = df_attempt.withColumn(
    "error_reason",
    F.when(F.col("parsed").isNull() | F.col("parsed.user_id").isNull(), "MALFORMED_JSON_SYNTAX")
     .when(F.col("parsed.amount") <= 0, "INVALID_NEGATIVE_AMOUNT")
     .otherwise(None)
)

df_classified.show(truncate=False)"""
                },
                {
                    "title": "Routing to Clean Silver Table vs Dead-Letter Queue Sink",
                    "explanation": "Write clean records to Silver and route failed records to DLQ with audit metadata.",
                    "code": """# 1. Clean stream
df_clean = df_classified.filter(F.col("error_reason").isNull()).select(
    "event_id",
    "parsed.user_id",
    "parsed.amount",
    F.current_timestamp().alias("processed_at")
)

df_clean.write.format("delta").mode("append").save(clean_silver_path)

# 2. Dead-letter quarantine stream
df_dlq = df_classified.filter(F.col("error_reason").isNotNull()).select(
    "event_id",
    "raw_payload",
    "error_reason",
    F.current_timestamp().alias("quarantined_at")
)

df_dlq.write.format("delta").mode("append").save(quarantine_dlq_path)

print("Clean Silver Table:")
spark.read.format("delta").load(clean_silver_path).show(truncate=False)

print("Dead-Letter Queue Quarantine Table:")
spark.read.format("delta").load(quarantine_dlq_path).show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "The Dead-Letter Queue (DLQ) pattern isolates malformed data without stopping production streaming pipelines.",
                "Always capture the raw unparsed payload and an explicit `error_reason` column in the quarantine sink.",
                "Build replay mechanisms so that once upstream schema fixes are deployed, quarantined data can be re-ingested."
            ],
            "interview_questions": [
                "What is a Dead Letter Queue (DLQ) pattern and why is it essential for 24/7 streaming SLAs?",
                "How do you alert on-call data engineers when the DLQ error rate exceeds a safety threshold?",
                "How do you re-process or replay records from a Delta DLQ table back into the main pipeline?"
            ]
        },
        {
            "nb_id": "063",
            "title": "Streaming Observability: StreamingQueryListener and SLA Latency Alerting",
            "difficulty": "Advanced",
            "business_scenario": "An enterprise fintech streaming pipeline must maintain an SLA of processing events within 60 seconds of origination. If a backlog builds up or micro-batch processing time spikes, on-call engineers must be alerted before SLA violations occur.",
            "problem_statement": "Implement streaming observability in PySpark: register a custom `StreamingQueryListener`, capture query startup, termination, and micro-batch progress events (`onQueryProgress`), calculate input rate vs processing rate, and generate automated SLA breach alerts.",
            "architectural_notes": "Spark's `StreamingQueryListener` provides an asynchronous callback API. When registered with `spark.streams.addListener()`, it captures event telemetry for all queries running in the SparkSession without introducing runtime overhead into the data processing tasks.",
            "sections": [
                {
                    "title": "Defining Custom StreamingQueryListener Class",
                    "explanation": "Implement listener to intercept query progress and measure SLA metrics.",
                    "code": """from pyspark.sql.streaming import StreamingQueryListener

class PipelineSLAListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"[OBSERVABILITY] Query Started! ID: {event.id} | Name: {event.name}")

    def onQueryProgress(self, event):
        progress = event.progress
        batch_id = progress.batchId
        input_rate = progress.inputRowsPerSecond
        proc_rate = progress.processedRowsPerSecond
        num_input = progress.numInputRows
        
        # Check if processing rate lags behind input rate (Backlog accumulation!)
        status = "HEALTHY"
        if input_rate > 0 and proc_rate < (input_rate * 0.8):
            status = "WARNING: PROCESSING_LAG_DETECTED"
            
        print(f"[METRIC] Batch: {batch_id} | In: {num_input} rows | InRate: {input_rate:.1f}/s | ProcRate: {proc_rate:.1f}/s | {status}")

    def onQueryTerminated(self, event):
        if event.exception:
            print(f"[ALERT] Query CRASHED with error: {event.exception}")
        else:
            print("[OBSERVABILITY] Query Terminated cleanly.")

print("PipelineSLAListener class defined.")"""
                },
                {
                    "title": "Registering Listener with SparkSession Streams Manager",
                    "explanation": "Register the listener to begin capturing streaming telemetry.",
                    "code": """# Register the listener
sla_listener = PipelineSLAListener()
spark.streams.addListener(sla_listener)
print("StreamingQueryListener registered with SparkSession.")"""
                },
                {
                    "title": "Running Diagnostic Stream to Trigger Listener Callbacks",
                    "explanation": "Execute a short rate stream to observe listener telemetry in the logs.",
                    "code": """import time

df_rate = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

q_monitored = (
    df_rate.writeStream
    .format("memory")
    .queryName("sla_telemetry_demo")
    .start()
)

# Let run for 2.5 seconds to trigger progress callbacks
time.sleep(2.5)

q_monitored.stop()
print("Monitored query completed.")"""
                }
            ],
            "key_takeaways": [
                "`StreamingQueryListener` provides non-invasive, asynchronous observability into all streaming queries.",
                "Monitor `inputRowsPerSecond` vs `processedRowsPerSecond`: when processing rate drops below input rate, backpressure develops.",
                "Send listener alerts to Slack, PagerDuty, or Datadog to detect silent streaming stalls."
            ],
            "interview_questions": [
                "What are the three core callback methods in Spark's `StreamingQueryListener`?",
                "How do you detect backpressure in a Spark Structured Streaming query programmatically?",
                "What is the difference between `triggerExecution` duration and `batchDuration` in streaming metrics?"
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
