"""
Module 10: End-to-End Enterprise Projects & Production Workflows (Notebooks 096 - 105)
"""

import re
from pathlib import Path
from notebook_builder import build_notebook, write_notebook

MODULE_FOLDER = "10_end_to_end_projects"
MODULE_NAME = "Module 10: End-to-End Enterprise Projects & Production Workflows"

def sanitize_filename(nb_id: str, title: str) -> str:
    cleaned = title.lower().replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return f"{nb_id}_{cleaned}.py"

def generate(base_dir: Path):
    out_dir = base_dir / "notebooks" / MODULE_FOLDER
    out_dir.mkdir(parents=True, exist_ok=True)

    notebooks = [
        {
            "nb_id": "096",
            "title": "Project Finance: Real-Time Credit Card Fraud Detection and Customer 360",
            "difficulty": "Advanced",
            "business_scenario": "A tier-1 retail bank processes 50 million credit card swipe transactions daily. Risk engineering must build an end-to-end Medallion pipeline that ingests raw swipes, calculates real-time risk scores (geographical velocity, transaction spikes > 3x average, rapid successive swipes within 2 minutes), and flags fraudulent transactions for real-time card blocking.",
            "problem_statement": "Build an end-to-end financial data engineering project: (1) Ingest raw swipes into Bronze Delta table with audit columns, (2) Cleanse, parse timestamps, and join against customer profiles in Silver, (3) Apply window-based fraud detection rules (swipes within 2 minutes in different cities), and (4) Publish a Gold Customer 360 Risk Summary table.",
            "architectural_notes": "Financial fraud detection requires low latency and high consistency. Combining Delta Lake ACID guarantees with window-based temporal heuristics allows detecting impossible geographic travel velocities and abnormal spend spikes without complex external ML infrastructure.",
            "sections": [
                {
                    "title": "Step 1: Ingesting Raw Swipes into Bronze Delta Table",
                    "explanation": "Create raw card swipe feed and land into an append-only Bronze Delta table.",
                    "code": """bronze_fraud_path = "/tmp/project_finance_bronze_096"
if Path(bronze_fraud_path).exists():
    shutil.rmtree(bronze_fraud_path)

raw_swipes = [
    ("SW-01", "CARD-101", 85.50,  "New York", "2023-11-20 12:00:00"),
    ("SW-02", "CARD-101", 1200.00,"London",   "2023-11-20 12:05:00"), # 5 min later in London -> FRAUD!
    ("SW-03", "CARD-102", 45.00,  "Chicago",  "2023-11-20 12:00:00"),
    ("SW-04", "CARD-102", 52.00,  "Chicago",  "2023-11-20 12:45:00"),
    ("SW-05", "CARD-103", 9500.00,"Miami",    "2023-11-20 12:10:00")  # Huge transaction
]

schema = "swipe_id STRING, card_id STRING, amount DOUBLE, city STRING, swipe_time STRING"
df_bronze_swipes = spark.createDataFrame(raw_swipes, schema).withColumn("_ingested_at", F.current_timestamp())

df_bronze_swipes.write.format("delta").mode("append").save(bronze_fraud_path)
print("Step 1 Complete: Raw card swipes landed in Bronze Delta table.")"""
                },
                {
                    "title": "Step 2: Silver Cleansing & Velocity-Based Fraud Scoring",
                    "explanation": "Enrich with previous swipe city/time using window functions and identify geographical fraud velocity.",
                    "code": """df_bronze = spark.read.format("delta").load(bronze_fraud_path).withColumn("ts", F.to_timestamp("swipe_time"))

card_window = Window.partitionBy("card_id").orderBy("ts")

df_silver_fraud = (
    df_bronze
    .withColumn("prev_city", F.lag("city").over(card_window))
    .withColumn("prev_ts", F.lag("ts").over(card_window))
    .withColumn("elapsed_minutes", (F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 60.0)
    .withColumn(
        "is_geo_velocity_fraud",
        F.when(
            (F.col("prev_city").isNotNull()) & 
            (F.col("city") != F.col("prev_city")) & 
            (F.col("elapsed_minutes") < 30.0),
            True
        ).otherwise(False)
    )
    .withColumn(
        "is_high_value_alert",
        F.when(F.col("amount") >= 5000.0, True).otherwise(False)
    )
    .withColumn(
        "fraud_risk_score",
        F.when(F.col("is_geo_velocity_fraud"), 95)
         .when(F.col("is_high_value_alert"), 75)
         .otherwise(10)
    )
)

print("Step 2 Complete: Silver Cleansed Swipes with Fraud Risk Scores:")
df_silver_fraud.select("swipe_id", "card_id", "amount", "city", "prev_city", "elapsed_minutes", "fraud_risk_score").show()"""
                },
                {
                    "title": "Step 3: Gold Customer 360 Risk Summary",
                    "explanation": "Aggregate fraud metrics to produce a card-level risk scorecard.",
                    "code": """df_gold_card360 = (
    df_silver_fraud
    .groupBy("card_id")
    .agg(
        F.count("swipe_id").alias("total_swipes"),
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.max("fraud_risk_score").alias("peak_risk_score"),
        F.sum(F.when(F.col("fraud_risk_score") >= 75, 1).otherwise(0)).alias("suspicious_events_count")
    )
    .withColumn(
        "account_status",
        F.when(F.col("peak_risk_score") >= 90, "LOCKED_FRAUD_HOLD")
         .when(F.col("peak_risk_score") >= 70, "FLAGGED_FOR_REVIEW")
         .otherwise("GOOD_STANDING")
    )
)

print("Step 3 Complete: Gold Customer 360 Risk Scorecard:")
df_gold_card360.show()"""
                }
            ],
            "key_takeaways": [
                "Medallion architecture provides clear separation between raw append (Bronze), enriched logic (Silver), and aggregates (Gold).",
                "Temporal window functions (`lag`) detect velocity-based fraud without external ML inference.",
                "Gold summary tables drive automated real-time action systems like card locking."
            ],
            "interview_questions": [
                "How do you design a real-time fraud pipeline that processes 50,000 transactions/second with sub-second latency?",
                "How does the Medallion Architecture ensure auditability during financial regulatory reviews?",
                "What is the difference between rule-based fraud detection in Spark and ML-based fraud detection?"
            ]
        },
        {
            "nb_id": "097",
            "title": "Project Retail: Omnichannel Multi-Source Lakehouse ETL",
            "difficulty": "Advanced",
            "business_scenario": "A major omnichannel retailer operates physical retail stores, an e-commerce website, and customer support via Salesforce. Data resides in three separate silos: PostgreSQL (store inventory), Clickstream logs (web), and Salesforce (customer CRM). Marketing requires a consolidated 360 customer purchase profile.",
            "problem_statement": "Execute an end-to-end retail multi-source integration pipeline: (1) Ingest multi-format sources (Postgres JDBC simulation, Web clickstream JSON, and Salesforce CSV), (2) Reconcile identity across customer identifiers, (3) Perform Delta Lake `MERGE INTO` upserts, and (4) Build a multi-channel revenue analytics model.",
            "architectural_notes": "Omnichannel retail requires resolving disparate identity keys (e.g. loyalty card ID from store POS vs email from web cookies). The Silver layer performs identity resolution, linking disparate keys to a conformed master `customer_sk`.",
            "sections": [
                {
                    "title": "Step 1: Simulating Multi-Source Ingestion Feeds",
                    "explanation": "Create store POS transactions, web clickstream sessions, and CRM profiles.",
                    "code": """# Source 1: In-Store POS (from PostgreSQL)
pos_sales = [("POS-1", "CUST-A", 120.0, "STORE_01"), ("POS-2", "CUST-B", 45.0, "STORE_02")]
df_pos = spark.createDataFrame(pos_sales, ["txn_id", "cust_id", "amount", "channel_id"]).withColumn("channel", F.lit("IN_STORE"))

# Source 2: Web E-Commerce Orders (from Web JSON logs)
web_sales = [("WEB-91", "CUST-A", 89.99, "WEB_US"), ("WEB-92", "CUST-C", 310.0, "WEB_US")]
df_web = spark.createDataFrame(web_sales, ["txn_id", "cust_id", "amount", "channel_id"]).withColumn("channel", F.lit("ONLINE"))

# Source 3: CRM Customer Profiles (from Salesforce)
crm_customers = [("CUST-A", "Alice Smith", "VIP"), ("CUST-B", "Bob Jones", "Standard"), ("CUST-C", "Charlie Day", "VIP")]
df_crm = spark.createDataFrame(crm_customers, ["cust_id", "name", "segment"])

print("Multi-Source Ingestion Feeds Prepared.")"""
                },
                {
                    "title": "Step 2: Unified Silver Transaction Conformance",
                    "explanation": "Union all sales channels and enrich with CRM customer segmentation.",
                    "code": """# Union POS and Web sales into unified Silver transactions stream
df_all_sales = df_pos.unionByName(df_web)

# Enrich with Salesforce CRM attributes
df_silver_omnichannel = (
    df_all_sales
    .join(df_crm, on="cust_id", how="left")
    .withColumn("_conformed_at", F.current_timestamp())
)

print("Conformed Silver Omnichannel Transactions:")
df_silver_omnichannel.show()"""
                },
                {
                    "title": "Step 3: Gold Customer Omnichannel Lifetime Value Summary",
                    "explanation": "Calculate spend per channel and omnichannel behavior flags.",
                    "code": """df_gold_clv = (
    df_silver_omnichannel
    .groupBy("cust_id", "name", "segment")
    .agg(
        F.round(F.sum("amount"), 2).alias("total_lifetime_spend"),
        F.count("txn_id").alias("total_purchases"),
        F.sum(F.when(F.col("channel") == "IN_STORE", F.col("amount")).otherwise(0)).alias("store_spend"),
        F.sum(F.when(F.col("channel") == "ONLINE", F.col("amount")).otherwise(0)).alias("online_spend"),
        F.countDistinct("channel").alias("active_channels_count")
    )
    .withColumn(
        "shopper_type",
        F.when(F.col("active_channels_count") > 1, "OMNICHANNEL_HYBRID")
         .when(F.col("store_spend") > 0, "IN_STORE_ONLY")
         .otherwise("ONLINE_ONLY")
    )
)

print("Gold Customer Omnichannel Value Scorecard:")
df_gold_clv.show()"""
                }
            ],
            "key_takeaways": [
                "Omnichannel data pipelines reconcile disparate source systems into a unified Lakehouse schema.",
                "`unionByName()` combines disparate streams while preserving column order and alignment.",
                "Omnichannel customer classification identifies high-value hybrid shoppers for personalized marketing."
            ],
            "interview_questions": [
                "How do you perform identity resolution across disparate retail channels without a universal user ID?",
                "What is the difference between `union()` and `unionByName()` in PySpark?",
                "How do you design a Delta Lake medallion architecture to handle multi-source CDC merges?"
            ]
        },
        {
            "nb_id": "098",
            "title": "Project Healthcare: HIPAA-Compliant Clinical Records De-Identification",
            "difficulty": "Advanced",
            "business_scenario": "A hospital research network prepares electronic health records (EHR) for clinical cancer research. HIPAA Safe Harbor compliance mandates the complete removal or cryptographic hashing of 18 direct identifiers (names, dates of birth, MRNs, zip codes) before sharing with researchers.",
            "problem_statement": "Build a HIPAA-compliant clinical pipeline: (1) Ingest raw clinical encounter records, (2) Apply Safe Harbor de-identification (mask names, truncate zip codes to 3 digits, shift admission dates using patient-specific cryptographically salted offsets), and (3) Calculate patient length-of-stay (LOS) metrics.",
            "architectural_notes": "HIPAA Safe Harbor requires removing names, phone numbers, and truncating zip codes where populations exceed 20,000. Dates must not be precise; date shifting preserves the chronological duration between events (e.g. admission and discharge) without revealing true calendar dates.",
            "sections": [
                {
                    "title": "Simulating Sensitive Clinical Records",
                    "explanation": "Create EHR patient records with protected health information (PHI).",
                    "code": """raw_patients = [
    ("PAT-1001", "John Doe", "1975-06-12", "78701", "2023-11-01 08:00:00", "2023-11-05 14:00:00", "Cardiology"),
    ("PAT-1002", "Jane Roe", "1988-09-24", "98101", "2023-11-02 10:00:00", "2023-11-03 18:00:00", "Neurology")
]

schema = "mrn STRING, name STRING, dob STRING, zip_code STRING, admit_ts STRING, discharge_ts STRING, diagnosis STRING"
df_raw_ehr = spark.createDataFrame(raw_patients, schema)
print("Raw PHI Patient Dataset (Non-Compliant for Researchers):")
df_raw_ehr.show()"""
                },
                {
                    "title": "Applying HIPAA Safe Harbor De-Identification Rules",
                    "explanation": "Mask names, truncate zip codes, and pseudonymize MRNs with salted hash.",
                    "code": """SECRET_SALT = "CLINICAL_SALT_KEY_99"

df_deidentified = (
    df_raw_ehr
    # 1. Pseudonymize MRN using salted SHA-256 hash
    .withColumn("research_patient_id", F.substring(F.sha2(F.concat(F.col("mrn"), F.lit(SECRET_SALT)), 256), 1, 16))
    # 2. Drop direct name entirely
    .drop("name", "mrn")
    # 3. Truncate ZIP code to first 3 digits
    .withColumn("zip3", F.substring(F.col("zip_code"), 1, 3))
    .drop("zip_code")
    # 4. Calculate patient age instead of revealing full DOB
    .withColumn("patient_age", F.floor(F.datediff(F.current_date(), F.to_date("dob")) / 365.25))
    .drop("dob")
)

df_deidentified.show()"""
                },
                {
                    "title": "Computing Length of Stay (LOS) & Research Table Publication",
                    "explanation": "Calculate admission stay duration in fractional days without exposing calendar dates.",
                    "code": """df_research_gold = (
    df_deidentified
    .withColumn("admit", F.to_timestamp("admit_ts"))
    .withColumn("discharge", F.to_timestamp("discharge_ts"))
    .withColumn(
        "length_of_stay_days",
        F.round((F.col("discharge").cast("long") - F.col("admit").cast("long")) / 86400.0, 2)
    )
    .drop("admit_ts", "discharge_ts", "admit", "discharge")
)

print("HIPAA-Compliant De-Identified Research Table (Safe for ML & Clinical Trials):")
df_research_gold.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "HIPAA Safe Harbor requires de-identifying 18 categories of protected health information (PHI).",
                "Pseudonymize primary identifiers using irreversible salted hashes.",
                "Calculate derived intervals (Length of Stay, Age) to prevent exposing calendar dates."
            ],
            "interview_questions": [
                "What are the 18 HIPAA Safe Harbor identifiers and how do you handle them in Spark?",
                "What is date-shifting in healthcare data engineering and why is it preferred over dropping dates?",
                "How do you ensure pseudonymized patient IDs cannot be re-identified through linkage attacks?"
            ]
        },
        {
            "nb_id": "099",
            "title": "Project Telecom: Customer Churn Prediction Feature Store and Window Features",
            "difficulty": "Advanced",
            "business_scenario": "A major mobile carrier experiences 2% monthly customer churn. Data scientists need a centralized Lakehouse Feature Store containing 30-day rolling aggregate features (drop in data usage, spike in customer service calls, late bill payments) to train machine learning churn models.",
            "problem_statement": "Build an enterprise Telecom Feature Engineering pipeline in PySpark: (1) Compute rolling 30-day temporal window aggregations per subscriber, (2) Calculate trend ratios (current 7-day usage vs 30-day average), (3) Assemble features into a Databricks Feature Store format, and (4) Validate feature drift.",
            "architectural_notes": "Feature stores prevent online/offline training-serving skew. Point-in-time correctness ensures features computed for model training reflect only the data available at the historical prediction timestamp, preventing data leakage.",
            "sections": [
                {
                    "title": "Generating Daily Subscriber Usage Telemetry",
                    "explanation": "Create daily data and voice usage logs per subscriber.",
                    "code": """daily_usage = [
    ("SUB-1", "2023-11-01", 1.5, 45, 0),
    ("SUB-1", "2023-11-05", 2.0, 30, 0),
    ("SUB-1", "2023-11-15", 0.5, 10, 1), # Usage dropping, customer service call!
    ("SUB-1", "2023-11-20", 0.1, 5,  2), # Severe drop, multiple calls
    ("SUB-2", "2023-11-01", 3.0, 60, 0),
    ("SUB-2", "2023-11-20", 3.2, 55, 0)
]

schema = "subscriber_id STRING, usage_date STRING, data_gb DOUBLE, call_mins INT, support_calls INT"
df_usage = spark.createDataFrame(daily_usage, schema).withColumn("ts", F.to_date("usage_date"))
df_usage.show()"""
                },
                {
                    "title": "Computing 30-Day Windowed Feature Aggregations",
                    "explanation": "Calculate rolling 30-day average usage and support call counts using window specifications.",
                    "code": """# Convert date to epoch days for rangeBetween window
df_with_days = df_usage.withColumn("day_num", F.col("ts").cast("date").cast("long"))

# Rolling 30-day window (29 preceding days + current day)
win_30d = Window.partitionBy("subscriber_id").orderBy("day_num").rangeBetween(-29, 0)

df_features = (
    df_with_days
    .withColumn("avg_data_gb_30d", F.round(F.avg("data_gb").over(win_30d), 2))
    .withColumn("total_support_calls_30d", F.sum("support_calls").over(win_30d))
    .withColumn("max_daily_call_mins_30d", F.max("call_mins").over(win_30d))
)

df_features.select("subscriber_id", "usage_date", "data_gb", "avg_data_gb_30d", "total_support_calls_30d").show()"""
                },
                {
                    "title": "Deriving Churn Risk Indicators & Feature Store Packaging",
                    "explanation": "Calculate feature ratios and assemble final ML training dataset.",
                    "code": """df_final_features = (
    df_features
    # Usage ratio: current data vs 30-day average (ratio < 0.5 indicates drop)
    .withColumn(
        "data_usage_drop_ratio",
        F.when(F.col("avg_data_gb_30d") > 0, F.round(F.col("data_gb") / F.col("avg_data_gb_30d"), 2)).otherwise(1.0)
    )
    .withColumn(
        "is_high_churn_risk",
        F.when((F.col("data_usage_drop_ratio") < 0.4) & (F.col("total_support_calls_30d") >= 2), 1).otherwise(0)
    )
    .select(
        "subscriber_id",
        "usage_date",
        "avg_data_gb_30d",
        "total_support_calls_30d",
        "data_usage_drop_ratio",
        "is_high_churn_risk"
    )
)

print("Final ML Feature Store Table:")
df_final_features.show()"""
                }
            ],
            "key_takeaways": [
                "Feature Stores decouple ML feature engineering from real-time model serving.",
                "Use `rangeBetween` on timestamp or epoch day columns to compute rolling time-based features.",
                "Ratios comparing short-term activity to long-term baseline (e.g. 7-day vs 30-day) are powerful predictors."
            ],
            "interview_questions": [
                "What is point-in-time correctness in Feature Store design and how does it prevent data leakage?",
                "What is the difference between online and offline feature stores?",
                "How do you implement rolling 30-day window features in PySpark on irregular daily observations?"
            ]
        },
        {
            "nb_id": "100",
            "title": "Project Cybersecurity: Web Server Access Log Analytics and Threat Detection",
            "difficulty": "Advanced",
            "business_scenario": "A cyber defense operations center monitors 100 million web server access logs daily. Security analysts must parse unformatted Nginx/Apache logs, detect DDoS attacks (IPs generating > 100 requests in 1 minute), identify SQL injection attempts, and geolocate threat origins.",
            "problem_statement": "Build an end-to-end cybersecurity log analytics pipeline: (1) Parse Common Log Format (CLF) / Nginx access logs using regex, (2) Detect HTTP 4xx/5xx error bursts and potential SQL injection signatures (`UNION SELECT`, `' OR 1=1`), (3) Execute sliding window velocity checks, and (4) Output an active threat IP blacklist.",
            "architectural_notes": "Log parsing in big data pipelines is CPU-intensive. Using native `regexp_extract()` with pre-compiled regex groups in Catalyst operates inside Tungsten UTF-8 binary format, avoiding Java String object garbage collection pressure.",
            "sections": [
                {
                    "title": "Simulating Raw Nginx Access Log Entries",
                    "explanation": "Create representative access logs with normal traffic and malicious attacks.",
                    "code": """raw_logs = [
    ('192.168.1.10 - - [20/Nov/2023:12:00:01 +0000] "GET /index.html HTTP/1.1" 200 4500'),
    ('192.168.1.10 - - [20/Nov/2023:12:00:05 +0000] "GET /about.html HTTP/1.1" 200 2300'),
    # Attack 1: SQL Injection attempt
    ('10.0.0.99 - - [20/Nov/2023:12:01:00 +0000] "GET /login?user=admin\'%20OR%201=1-- HTTP/1.1" 401 520'),
    # Attack 2: Directory traversal attempt
    ('10.0.0.99 - - [20/Nov/2023:12:01:02 +0000] "GET /../../etc/passwd HTTP/1.1" 403 310'),
    # Attack 3: High-frequency brute force
    ('10.0.0.99 - - [20/Nov/2023:12:01:04 +0000] "POST /api/v1/auth HTTP/1.1" 401 120')
]

df_raw_logs = spark.createDataFrame([(l,) for l in raw_logs], ["raw_log"])
df_raw_logs.show(truncate=False)"""
                },
                {
                    "title": "Parsing Common Log Format (CLF) via Regex",
                    "explanation": "Extract client IP, timestamp, HTTP method, URL, status code, and response bytes.",
                    "code": """clf_regex = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) \S+" (\d{3}) (\d+)'

df_parsed_logs = (
    df_raw_logs
    .withColumn("client_ip", F.regexp_extract("raw_log", clf_regex, 1))
    .withColumn("request_ts_str", F.regexp_extract("raw_log", clf_regex, 2))
    .withColumn("http_method", F.regexp_extract("raw_log", clf_regex, 3))
    .withColumn("request_uri", F.regexp_extract("raw_log", clf_regex, 4))
    .withColumn("status_code", F.regexp_extract("raw_log", clf_regex, 5).cast(T.IntegerType()))
    .withColumn("bytes_sent", F.regexp_extract("raw_log", clf_regex, 6).cast(T.LongType()))
)

df_parsed_logs.select("client_ip", "http_method", "request_uri", "status_code").show(truncate=False)"""
                },
                {
                    "title": "Threat Signature Detection & Blacklist Generation",
                    "explanation": "Identify SQLi, path traversal, and aggregate high-risk attack IPs.",
                    "code": """df_threats = (
    df_parsed_logs
    .withColumn(
        "threat_type",
        F.when(F.lower(F.col("request_uri")).rlike(r"(or%201=1|union%20select|drop%20table)"), "SQL_INJECTION")
         .when(F.col("request_uri").contains("../"), "PATH_TRAVERSAL")
         .when((F.col("status_code") == 401) & (F.col("http_method") == "POST"), "BRUTE_FORCE_AUTH")
         .otherwise(None)
    )
)

print("Identified Cybersecurity Incidents:")
df_threats.filter(F.col("threat_type").isNotNull()).select("client_ip", "request_uri", "status_code", "threat_type").show(truncate=False)

# Build active IP blacklist table
df_blacklist = (
    df_threats
    .filter(F.col("threat_type").isNotNull())
    .groupBy("client_ip")
    .agg(
        F.count("threat_type").alias("incident_count"),
        F.collect_set("threat_type").alias("detected_attacks")
    )
    .withColumn("action_taken", F.lit("BLOCK_AT_FIREWALL"))
)

print("Automated Firewall IP Blacklist Feed:")
df_blacklist.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "High-throughput log analytics leverages regex pattern extraction inside Tungsten binary representations.",
                "Detect threat signatures (SQLi, path traversal, credential stuffing) using vectorized regex expressions.",
                "Automated Gold threat tables feed downstream firewall APIs (Cloudflare, AWS WAF) for immediate IP blocking."
            ],
            "interview_questions": [
                "How do you process 500GB of daily unstructured Nginx logs in PySpark without driver OOM?",
                "What is the performance difference between `regexp_extract()` and Python's `re.search()` inside a UDF?",
                "How do you implement real-time sliding window DDoS detection using Structured Streaming?"
            ]
        },
        {
            "nb_id": "101",
            "title": "Project IoT: Industrial Telemetry Anomaly Detection via Z-Score Statistics",
            "difficulty": "Advanced",
            "business_scenario": "A smart manufacturing plant operates 5,000 robotic welding arms. Bearings experience mechanical fatigue before catastrophic failure. Plant reliability engineers need a real-time statistical anomaly detector that computes moving Z-scores on vibration sensor streams.",
            "problem_statement": "Implement distributed statistical anomaly detection in PySpark: calculate rolling mean and standard deviation over temporal windows, compute the Z-score for vibration signals ($Z = \\frac{x - \\mu}{\\sigma}$), flag readings where $|Z| > 3.0$ (three-sigma rule), and trigger predictive maintenance tickets.",
            "architectural_notes": "The three-sigma ($3\\sigma$) rule assumes sensor readings under normal operation follow an approximate Gaussian distribution. By computing dynamic moving baselines per machine rather than static thresholds, the pipeline adapts to normal operating temperature and load shifts.",
            "sections": [
                {
                    "title": "Generating Sensor Vibration Telemetry",
                    "explanation": "Create high-frequency vibration sensor logs with normal baseline and sudden spike.",
                    "code": """sensor_readings = [
    ("ARM-01", "2023-11-20 10:00:00", 12.1),
    ("ARM-01", "2023-11-20 10:01:00", 12.3),
    ("ARM-01", "2023-11-20 10:02:00", 12.0),
    ("ARM-01", "2023-11-20 10:03:00", 12.2),
    ("ARM-01", "2023-11-20 10:04:00", 28.5), # Severe vibration spike -> Bearing failure!
    ("ARM-02", "2023-11-20 10:00:00", 8.0),
    ("ARM-02", "2023-11-20 10:01:00", 8.1)
]

schema = "machine_id STRING, reading_time STRING, vibration_mm DOUBLE"
df_sensors = spark.createDataFrame(sensor_readings, schema).withColumn("ts", F.to_timestamp("reading_time"))
df_sensors.show()"""
                },
                {
                    "title": "Calculating Moving Baseline (Mean & Standard Deviation)",
                    "explanation": "Apply window functions over preceding readings per machine.",
                    "code": """win_machine = Window.partitionBy("machine_id").orderBy("ts").rowsBetween(-4, Window.currentRow)

df_stats = (
    df_sensors
    .withColumn("moving_avg", F.round(F.avg("vibration_mm").over(win_machine), 2))
    .withColumn("moving_stddev", F.round(F.stddev("vibration_mm").over(win_machine), 2))
)

df_stats.show()"""
                },
                {
                    "title": "Computing Z-Scores & Flagging Critical Mechanical Anomalies",
                    "explanation": "Evaluate Z = (x - mean) / stddev and flag anomalies exceeding threshold.",
                    "code": """df_anomalies = (
    df_stats
    .withColumn(
        "z_score",
        F.when(
            (F.col("moving_stddev").isNotNull()) & (F.col("moving_stddev") > 0),
            F.round(F.abs(F.col("vibration_mm") - F.col("moving_avg")) / F.col("moving_stddev"), 2)
        ).otherwise(0.0)
    )
    .withColumn(
        "maintenance_alert",
        F.when(F.col("z_score") >= 2.0, "CRITICAL: BEARING_FATIGUE_PREDICTED").otherwise("NORMAL")
    )
)

print("Predictive Maintenance Telemetry Report:")
df_anomalies.select("machine_id", "reading_time", "vibration_mm", "moving_avg", "z_score", "maintenance_alert").show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Statistical Z-score detection identifies anomalous values relative to a dynamic, per-machine baseline.",
                "Using rolling window aggregates (`rowsBetween`) calculates moving statistics in linear distributed time.",
                "Predictive maintenance models prevent multi-million dollar equipment breakdowns before catastrophic failure."
            ],
            "interview_questions": [
                "How do you handle division-by-zero when computing Z-scores on sensor data with zero variance?",
                "What is the difference between fixed threshold alerting and statistical Z-score anomaly detection?",
                "How do you deploy this model to a Structured Streaming pipeline with watermarking?"
            ]
        },
        {
            "nb_id": "102",
            "title": "Project Supply Chain: Inventory Replenishment Forecasting and Reorder Triggers",
            "difficulty": "Advanced",
            "business_scenario": "A national grocery chain manages 10,000 SKUs across 20 regional distribution centers. Supply chain disruptions cause frequent stockouts of essential items. The forecasting team needs an automated Lakehouse pipeline that calculates Daily Run-Rate (DRR), Safety Stock, and triggers automated purchase orders.",
            "problem_statement": "Build an automated supply chain replenishment model in PySpark: (1) Compute 14-day moving daily sales velocity, (2) Estimate lead-time demand and safety stock using standard formulas ($SS = Z \\times \\sigma_{LTD}$), (3) Calculate dynamic Reorder Points (ROP), and (4) Generate automated Purchase Order requisitions.",
            "architectural_notes": "Supply chain optimization requires reconciling historical sales velocity with supplier delivery lead times. Calculating dynamic reorder points in PySpark enables automated daily replenishment recommendations across millions of warehouse-SKU combinations.",
            "sections": [
                {
                    "title": "Preparing Inventory Levels and Daily Demand Feed",
                    "explanation": "Create SKU inventory stock levels and supplier lead-time parameters.",
                    "code": """inventory = [
    ("SKU-101", "Milk Whole Gallon", 150, 4, 35.0, 5.0), # Current stock: 150, Lead time: 4 days, Daily sales: 35
    ("SKU-102", "Organic Eggs 12pk", 40,  3, 20.0, 4.0), # Current stock: 40, Lead time: 3 days, Daily sales: 20 -> Stockout imminent!
    ("SKU-103", "Artisan Bread",     80,  2, 15.0, 2.0)  # Current stock: 80, Lead time: 2 days, Daily sales: 15
]

schema = "sku STRING, product_name STRING, current_stock INT, lead_time_days INT, avg_daily_sales DOUBLE, stddev_daily_sales DOUBLE"
df_inventory = spark.createDataFrame(inventory, schema)
df_inventory.show()"""
                },
                {
                    "title": "Calculating Safety Stock & Dynamic Reorder Point (ROP)",
                    "explanation": "Apply standard industrial supply chain formulas in PySpark.",
                    "code": """# Service factor Z = 1.65 (95% service level / stockout protection)
Z_FACTOR = 1.65

df_rop = (
    df_inventory
    # Lead time demand = lead_time_days * avg_daily_sales
    .withColumn("lead_time_demand", F.round(F.col("lead_time_days") * F.col("avg_daily_sales"), 1))
    # Safety stock = Z * sqrt(lead_time_days) * stddev_daily_sales
    .withColumn(
        "safety_stock",
        F.round(F.lit(Z_FACTOR) * F.sqrt(F.col("lead_time_days")) * F.col("stddev_daily_sales"), 0).cast(T.IntegerType())
    )
    # Reorder Point (ROP) = Lead Time Demand + Safety Stock
    .withColumn("reorder_point", F.round(F.col("lead_time_demand") + F.col("safety_stock"), 0).cast(T.IntegerType()))
)

df_rop.select("sku", "product_name", "current_stock", "lead_time_demand", "safety_stock", "reorder_point").show()"""
                },
                {
                    "title": "Generating Automated Purchase Orders (PO Requisitions)",
                    "explanation": "Trigger purchase orders when current stock drops below reorder point.",
                    "code": """df_purchase_orders = (
    df_rop
    .withColumn("needs_reorder", F.col("current_stock") <= F.col("reorder_point"))
    .withColumn(
        "suggested_order_qty",
        F.when(F.col("needs_reorder"), (F.col("reorder_point") * 2) - F.col("current_stock")).otherwise(0)
    )
    .filter(F.col("needs_reorder") == True)
)

print("Automated Purchase Order Requisitions Triggered:")
df_purchase_orders.select("sku", "product_name", "current_stock", "reorder_point", "suggested_order_qty").show()"""
                }
            ],
            "key_takeaways": [
                "Reorder Point (ROP) equals Lead Time Demand plus statistical Safety Stock.",
                "Automating ROP calculations prevents stockouts while minimizing expensive warehouse carrying costs.",
                "Gold replenishment tables trigger automated ERP purchase order webhooks via Databricks Workflows."
            ],
            "interview_questions": [
                "How do you calculate Safety Stock statistically and what is the role of the Z-score service factor?",
                "How do you handle supplier lead time variability in supply chain data models?",
                "What is the difference between periodic review and continuous review inventory policies?"
            ]
        },
        {
            "nb_id": "103",
            "title": "Project E-Commerce: Collaborative Filtering Recommendation Engine with Spark MLlib",
            "difficulty": "Advanced",
            "business_scenario": "An online retail store has 1 million products and 5 million shoppers. Product managers want personalized 'Recommended For You' item carousels on the homepage to increase average order value and user engagement.",
            "problem_statement": "Build an enterprise Recommendation Engine using Spark MLlib: prepare implicit user-item interaction matrices (user_id, item_id, rating), train an Alternating Least Squares (ALS) matrix factorization model, generate top-5 personalized recommendations per user, and evaluate model performance.",
            "architectural_notes": "Spark MLlib ALS (Alternating Least Squares) factorizes the sparse user-item interaction matrix $R$ into low-rank user matrix $U$ and item matrix $V$ ($R \approx U \times V^T$). It runs in distributed memory, scaling to hundreds of millions of user-item ratings across cluster nodes.",
            "sections": [
                {
                    "title": "Preparing User-Product Rating Interaction Dataset",
                    "explanation": "Create user rating interactions.",
                    "code": """from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

ratings_data = [
    (0, 101, 5.0), # User 0 loves Product 101
    (0, 102, 4.0),
    (0, 103, 1.0),
    (1, 101, 5.0),
    (1, 103, 1.0),
    (2, 102, 4.0),
    (2, 103, 2.0),
    (3, 101, 4.0),
    (3, 102, 5.0)
]

schema = "user_id INT, product_id INT, rating FLOAT"
df_ratings = spark.createDataFrame(ratings_data, schema)
df_ratings.show()"""
                },
                {
                    "title": "Training Alternating Least Squares (ALS) Model",
                    "explanation": "Fit ALS collaborative filtering model with rank and regularization parameters.",
                    "code": """# Configure ALS model
als = ALS(
    maxIter=5,
    regParam=0.01,
    userCol="user_id",
    itemCol="product_id",
    ratingCol="rating",
    coldStartStrategy="drop" # Prevents NaN on unseen users/items during evaluation
)

model = als.fit(df_ratings)
print("ALS Collaborative Filtering Model successfully trained on distributed Spark cluster.")"""
                },
                {
                    "title": "Generating Top-2 Personalized Recommendations per User",
                    "explanation": "Generate recommendations for all users and unpack recommendations array.",
                    "code": """# Generate top 2 product recommendations for all users
user_recs = model.recommendForAllUsers(2)

# Flatten recommendation struct array
df_unpacked_recs = user_recs.select(
    "user_id",
    F.explode("recommendations").alias("rec")
).select(
    "user_id",
    F.col("rec.product_id").alias("recommended_product_id"),
    F.round(F.col("rec.rating"), 2).alias("predicted_affinity_score")
)

print("Personalized Product Recommendations per User:")
df_unpacked_recs.show()"""
                }
            ],
            "key_takeaways": [
                "Spark MLlib ALS performs distributed matrix factorization for collaborative filtering recommendations.",
                "Always set `coldStartStrategy='drop'` to prevent evaluation failures when new users/items appear.",
                "Persist generated recommendations to Gold Delta tables or low-latency key-value stores (Redis, DynamoDB) for web serving."
            ],
            "interview_questions": [
                "How does the Alternating Least Squares (ALS) algorithm work mathematically in distributed Spark?",
                "What is the difference between explicit feedback ratings and implicit feedback signals (clicks, views)?",
                "How do you handle the 'Cold Start' problem for new users or brand new products in recommendation systems?"
            ]
        },
        {
            "nb_id": "104",
            "title": "Project Agentic AI & RAG: Lakehouse Document Chunking and Vector Search",
            "difficulty": "Advanced",
            "business_scenario": "An enterprise customer service platform deploys an Agentic AI chatbot powered by Databricks Genie and LLMs. The chatbot needs to answer questions about company product manuals, warranty terms, and legal policies stored across unstructured PDF and text documents in the Lakehouse.",
            "problem_statement": "Build an enterprise RAG (Retrieval-Augmented Generation) data preparation pipeline: (1) Ingest raw documentation from Unity Catalog Volumes, (2) Split long text into overlapping chunks using sliding windows, (3) Generate dense vector embeddings simulation, and (4) Prepare Gold vector index tables for Vector Search.",
            "architectural_notes": "RAG architectures require chunking large documents into semantically coherent segments (e.g. 500 characters with 100 character overlap) and embedding them into high-dimensional vectors. Databricks Vector Search indexes Delta tables natively, enabling hybrid semantic and keyword search.",
            "sections": [
                {
                    "title": "Ingesting Unstructured Product Documentation",
                    "explanation": "Create sample product warranty and documentation text.",
                    "code": """docs = [
    ("DOC-101", "Smart TV Manual", "The Smart TV comes with a 2-year manufacturer warranty covering panel defects and audio failure. To request service, submit your receipt online within 30 days of the incident. Accidental drops and water damage are not covered under the warranty terms."),
    ("DOC-102", "Return Policy", "Customers may return unopened items within 14 days of purchase for a full refund to the original payment method. Opened items are subject to a 15% restocking fee unless defective. Software downloads are strictly non-refundable.")
]

schema = "doc_id STRING, title STRING, body_text STRING"
df_docs = spark.createDataFrame(docs, schema)
df_docs.show(truncate=False)"""
                },
                {
                    "title": "Text Chunking via Sliding Windows in PySpark",
                    "explanation": "Split text into overlapping chunks using higher-order functions or UDFs.",
                    "code": """def chunk_text(text, chunk_size=120, overlap=30):
    words = text.split(" ")
    chunks = []
    i = 0
    step = max(1, (chunk_size - overlap) // 6)
    window_words = chunk_size // 6
    while i < len(words):
        chunk = " ".join(words[i : i + window_words])
        if chunk.strip():
            chunks.append(chunk)
        i += step
    return chunks

chunk_udf = F.udf(chunk_text, T.ArrayType(T.StringType()))

df_chunked = (
    df_docs
    .withColumn("chunks", chunk_udf("body_text"))
    .select("doc_id", "title", F.explode("chunks").alias("chunk_text"))
    .withColumn("chunk_id", F.concat(F.col("doc_id"), F.lit("_CHK_"), F.monotonically_increasing_id()))
)

print("Document Chunks Prepared for Embedding Generation:")
df_chunked.select("chunk_id", "title", "chunk_text").show(truncate=False)"""
                },
                {
                    "title": "Vector Embedding Representation & Gold Vector Index Schema",
                    "explanation": "Represent dense embedding vectors and prepare schema for Databricks Vector Search.",
                    "code": """# Simulate 8-dimensional dense vector embeddings for demonstration
df_vector_ready = (
    df_chunked
    .withColumn(
        "embedding_vector",
        F.array([F.round(F.rand(seed=42 + i) * 2 - 1, 4) for i in range(8)])
    )
    .withColumn("_embedded_at", F.current_timestamp())
)

print("Vector Search Index Table Schema:")
df_vector_ready.printSchema()
df_vector_ready.select("chunk_id", "title", "embedding_vector").show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "RAG pipelines bridge unstructured enterprise documents with LLM reasoning via Vector Search.",
                "Chunking with overlap preserves context across sentence boundaries.",
                "Delta Lake serves as the unified storage layer for both raw text documents and high-dimensional vector embeddings."
            ],
            "interview_questions": [
                "What is Chunk Overlap in RAG pipelines and why is it important for semantic retrieval?",
                "How does Databricks Vector Search synchronize automatically with underlying Delta tables?",
                "What are the trade-offs between dense semantic vector search and sparse keyword search (BM25)?"
            ]
        },
        {
            "nb_id": "105",
            "title": "Master Enterprise Production Orchestration, Workflow Parameters and Alerting",
            "difficulty": "Advanced",
            "business_scenario": "A mission-critical enterprise financial pipeline executes 15 dependent tasks every night. The workflow must accept dynamic run parameters, retry failed tasks with exponential backoff, send Slack/Teams webhook alerts on failure, and write audit run logs.",
            "problem_statement": "Design an enterprise orchestration harness in PySpark: implement parameterized workflow execution, simulate task dependency graphs (DAGs), capture task execution durations, catch runtime exceptions, and dispatch webhook failure alerts.",
            "architectural_notes": "Databricks Workflows orchestrates complex multi-task DAGs. Using parameters (`dbutils.widgets`), tasks can be parameterized across environments. Implementing centralized audit logging inside tasks ensures compliance and accelerates root cause analysis during pipeline failures.",
            "sections": [
                {
                    "title": "Designing Parameterized Task Execution Harness",
                    "explanation": "Create an orchestration class that executes pipeline tasks with timing and error handling.",
                    "code": """import time
import traceback
from datetime import datetime

class EnterpriseTaskRunner:
    def __init__(self, run_env: str, batch_date: str):
        self.run_env = run_env
        self.batch_date = batch_date
        self.audit_log = []

    def execute_task(self, task_name: str, task_func):
        print(f"=== [START] Task: {task_name} | Env: {self.run_env} | Date: {self.batch_date} ===")
        start_time = time.time()
        status = "SUCCESS"
        error_msg = None
        
        try:
            task_func()
        except Exception as e:
            status = "FAILED"
            error_msg = str(e)
            print(f"!!! [ERROR] Task {task_name} failed: {error_msg}")
            self.dispatch_alert(task_name, error_msg)
        finally:
            duration = round(time.time() - start_time, 3)
            self.audit_log.append({
                "task_name": task_name,
                "status": status,
                "duration_seconds": duration,
                "error_message": error_msg,
                "executed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            })
            print(f"=== [END] Task: {task_name} | Status: {status} | Duration: {duration}s ===\\n")

    def dispatch_alert(self, task_name: str, error_msg: str):
        # Simulate Slack / PagerDuty webhook dispatch
        print(f"--> [WEBHOOK DISPATCH] Paging On-Call: Task '{task_name}' failed in {self.run_env}! Error: {error_msg}")

print("EnterpriseTaskRunner class initialized.")"""
                },
                {
                    "title": "Executing Multi-Task Orchestration Sequence",
                    "explanation": "Run tasks representing Ingestion, Transformation, and Quality Checks.",
                    "code": """# Define individual pipeline task functions
def task_ingest():
    time.sleep(0.05)
    print("Ingestion complete: 15,000 records processed.")

def task_transform():
    time.sleep(0.08)
    print("Transformation complete: Cleaned and conformed Silver records.")

def task_quality_check():
    time.sleep(0.02)
    # Simulate data quality failure under bad conditions
    print("Quality check passed: 100% compliance.")

runner = EnterpriseTaskRunner(run_env="production", batch_date="2023-11-20")

runner.execute_task("Task_1_Bronze_Ingestion", task_ingest)
runner.execute_task("Task_2_Silver_Transformation", task_transform)
runner.execute_task("Task_3_Data_Quality_Validation", task_quality_check)"""
                },
                {
                    "title": "Publishing Master Workflow Audit Log to Delta Table",
                    "explanation": "Persist run execution telemetry for FinOps and SLA monitoring.",
                    "code": """df_audit = spark.createDataFrame(runner.audit_log)
print("Master Workflow Execution Audit Log:")
df_audit.show(truncate=False)

# In production, save to system audit table:
# df_audit.write.format("delta").mode("append").save("/Volumes/enterprise/audit/workflow_runs")"""
                }
            ],
            "key_takeaways": [
                "Production pipelines require parameterized orchestration across environments (dev, test, prod).",
                "Always capture start time, duration, status, and error stack traces in a centralized audit Delta table.",
                "Automate webhook alerts to Slack or PagerDuty on task failure to meet enterprise SLAs."
            ],
            "interview_questions": [
                "How do you pass dynamic parameters and return values between tasks in Databricks Workflows?",
                "What is the difference between task-level retries and job-level retries in orchestration DAGs?",
                "How do you design an audit logging architecture that captures pipeline execution metrics across 500+ jobs?"
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
