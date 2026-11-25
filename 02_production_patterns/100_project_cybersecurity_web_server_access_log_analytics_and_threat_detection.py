"""Project Cybersecurity: Web Server Access Log Analytics & Threat Detection.

Scenario & Scale:
    A SOC analytics pipeline parses 100M+ daily Nginx/Apache Common Log Format
    (CLF) entries. Malformed lines, SQL injection probes, and brute-force
    bursts must be detected under a 10-minute SLA without driver OOM or ANSI crashes.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


CLF_REGEX = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) \S+" (\d{3}) (\d+|-)'


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Parsing logs via Python `re.search` UDFs or casting empty
# `regexp_extract` results directly with `.cast("int")` under Spark 4.0 ANSI mode.
# Otherwise: Python regex UDFs defeat Tungsten UTF-8 execution (10-30x slower),
# and a single unmatched log line returns `""`, crashing `.cast("int")` with
# `SparkNumberFormatException`.
def transform_parse_logs_unsafe_bad(df_raw: DataFrame) -> DataFrame:
    return df_raw.withColumn("status_code", F.regexp_extract("raw_log", CLF_REGEX, 5).cast("int"))


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Use Catalyst `regexp_extract` in Tungsten binary format with
# `F.try_cast` on extracted numeric fields, then classify threats and aggregate
# an automated firewall blacklist.
def transform_parse_clf_logs(df_raw: DataFrame) -> DataFrame:
    """Extract CLF fields natively and cast status/bytes safely under ANSI mode."""
    return (
        df_raw.withColumn("client_ip", F.regexp_extract("raw_log", CLF_REGEX, 1))
        .withColumn("request_ts_str", F.regexp_extract("raw_log", CLF_REGEX, 2))
        .withColumn("http_method", F.regexp_extract("raw_log", CLF_REGEX, 3))
        .withColumn("request_uri", F.regexp_extract("raw_log", CLF_REGEX, 4))
        .withColumn("status_code", F.try_cast(F.regexp_extract("raw_log", CLF_REGEX, 5), T.IntegerType()))
        .withColumn("bytes_sent", F.try_cast(F.regexp_extract("raw_log", CLF_REGEX, 6), T.LongType()))
        .filter(F.col("client_ip") != F.lit(""))
    )


def transform_detect_threats_and_blacklist(df_parsed: DataFrame) -> DataFrame:
    """Flag SQLi, path traversal, and auth brute force into a firewall blocklist."""
    df_threats = df_parsed.withColumn(
        "threat_type",
        F.when(F.lower(F.col("request_uri")).rlike(r"(or%201=1|union%20select|drop%20table)"), F.lit("SQL_INJECTION"))
        .when(F.col("request_uri").contains("../"), F.lit("PATH_TRAVERSAL"))
        .when((F.col("status_code") == 401) & (F.col("http_method") == "POST"), F.lit("BRUTE_FORCE_AUTH"))
        .otherwise(F.lit(None).cast("string")),
    )
    return (
        df_threats.filter(F.col("threat_type").isNotNull())
        .groupBy("client_ip")
        .agg(
            F.count("threat_type").alias("incident_count"),
            F.collect_set("threat_type").alias("detected_attacks"),
        )
        .withColumn("action_taken", F.lit("BLOCK_AT_FIREWALL"))
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` confirms `RegExpExtract` + `try_cast` execute inside
#   JVM `WholeStageCodegen` directly over UTF-8 byte buffers without Python workers.
# - `collect_set("threat_type")` inside `groupBy("client_ip")` stays bounded
#   since `threat_type` has < 10 distinct enum values.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `regexp_extract` returns `""` (empty string), NOT `NULL`, on non-matching
#    lines; in Spark 4.0 ANSI mode, `""` -> `INT` throws unless wrapped in `F.try_cast`.
# 2. HTTP logs often emit `"-"` for 0-byte responses (`(\d+|-)`), which `F.try_cast`
#    gracefully maps to `NULL` instead of aborting the batch.
# 3. For real-time DDoS velocity detection (> 100 req/min/IP), combine this
#    parser with Structured Streaming sliding windows (`F.window("event_ts", "1 minute", "15 seconds")`).


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("100_cybersecurity_logs")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    logs = [
        ('192.168.1.10 - - [20/Jan/2025:12:00:01 +0000] "GET /index.html HTTP/1.1" 200 4500',),
        ('10.0.0.99 - - [20/Jan/2025:12:01:00 +0000] "GET /login?user=admin%20OR%201=1 HTTP/1.1" 401 -',),
        ('10.0.0.99 - - [20/Jan/2025:12:01:02 +0000] "GET /../../etc/passwd HTTP/1.1" 403 310',),
        ('10.0.0.99 - - [20/Jan/2025:12:01:04 +0000] "POST /api/v1/auth HTTP/1.1" 401 120',),
    ]
    df_parsed = transform_parse_clf_logs(spark.createDataFrame(logs, "raw_log STRING"))
    transform_detect_threats_and_blacklist(df_parsed).show(truncate=False)
