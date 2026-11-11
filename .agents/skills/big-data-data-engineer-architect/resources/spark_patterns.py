"""
PySpark Reusable Patterns for Data Engineering
Reference: big-data-data-engineer-architect skill
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

# ============================================================
# PATTERN 1: Incremental Load with Watermark
# ============================================================

def incremental_load_watermark(
    spark: SparkSession,
    source_query: str,
    target_table: str,
    watermark_column: str,
    unique_key: str,
    jdbc_url: str,
    jdbc_props: dict
) -> int:
    """
    Incremental load using a high-watermark column (e.g., updated_at).
    
    Returns:
        int: Number of records written
    """
    # 1. Get current high-watermark from target
    try:
        watermark_df = spark.sql(f"SELECT MAX({watermark_column}) AS wm FROM {target_table}")
        current_wm = watermark_df.collect()[0]["wm"]
    except Exception:
        current_wm = "1900-01-01"

    logger.info(f"Loading from {target_table} where {watermark_column} > {current_wm}")

    # 2. Extract only new/changed records from source
    incremental_query = f"""
        ({source_query}) src
        WHERE {watermark_column} > '{current_wm}'
    """
    source_df = spark.read.jdbc(
        url=jdbc_url,
        table=incremental_query,
        properties=jdbc_props
    )

    count = source_df.count()
    if count == 0:
        logger.info("No new records found. Skipping write.")
        return 0

    # 3. MERGE into target (upsert)
    delta_table = DeltaTable.forName(spark, target_table)
    delta_table.alias("target").merge(
        source_df.alias("source"),
        f"target.{unique_key} = source.{unique_key}"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    logger.info(f"Merged {count} records into {target_table}")
    return count


# ============================================================
# PATTERN 2: SCD Type 2 Implementation
# ============================================================

def apply_scd_type2(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    business_key: str,
    tracked_columns: List[str],
    effective_date_col: str = "effective_date",
    expiry_date_col: str = "expiry_date",
    is_current_col: str = "is_current",
    surrogate_key_col: str = "sk"
) -> None:
    """
    Apply SCD Type 2 logic to a Delta Lake dimension table.
    Expires old records and inserts new versions for changed rows.
    """
    today = F.current_date()

    # Get current active records from target
    target_df = spark.table(target_table).filter(F.col(is_current_col) == True)

    # Detect changes
    joined = source_df.alias("src").join(
        target_df.alias("tgt"),
        on=F.col(f"src.{business_key}") == F.col(f"tgt.{business_key}"),
        how="left"
    )

    # Build change detection condition
    change_condition = F.lit(False)
    for col in tracked_columns:
        change_condition = change_condition | (
            F.col(f"src.{col}") != F.col(f"tgt.{col}")
        )

    new_records = joined.filter(
        F.col(f"tgt.{business_key}").isNull() |  # New entity
        change_condition                           # Changed attributes
    ).select([F.col(f"src.{c}") for c in source_df.columns])

    if new_records.count() == 0:
        logger.info("No SCD2 changes detected.")
        return

    # Expire changed records in target
    delta_table = DeltaTable.forName(spark, target_table)
    delta_table.alias("target").merge(
        new_records.alias("source"),
        f"target.{business_key} = source.{business_key} AND target.{is_current_col} = true"
    ).whenMatchedUpdate(set={
        is_current_col: F.lit(False),
        expiry_date_col: F.date_sub(today, 1)
    }).execute()

    # Insert new versions
    new_records_with_sk = new_records \
        .withColumn(surrogate_key_col, F.monotonically_increasing_id()) \
        .withColumn(effective_date_col, today) \
        .withColumn(expiry_date_col, F.lit("9999-12-31").cast("date")) \
        .withColumn(is_current_col, F.lit(True))

    new_records_with_sk.write.format("delta").mode("append").saveAsTable(target_table)
    logger.info(f"SCD2: inserted {new_records.count()} new versions into {target_table}")


# ============================================================
# PATTERN 3: Deduplication with Window Functions
# ============================================================

def deduplicate_by_key(
    df: DataFrame,
    partition_keys: List[str],
    order_by_col: str,
    order_desc: bool = True
) -> DataFrame:
    """
    Keep only the latest record per partition_keys combination.
    
    Example: Latest record per customer_id ordered by updated_at DESC
    """
    window = Window.partitionBy(*partition_keys).orderBy(
        F.col(order_by_col).desc() if order_desc else F.col(order_by_col).asc()
    )
    return df \
        .withColumn("_row_num", F.row_number().over(window)) \
        .filter(F.col("_row_num") == 1) \
        .drop("_row_num")


# ============================================================
# PATTERN 4: Bronze Ingestion (Auto-Loader / CloudFiles)
# ============================================================

def ingest_bronze_autoloader(
    spark: SparkSession,
    source_path: str,
    target_table: str,
    file_format: str = "json",  # json, csv, parquet, avro
    source_system: str = "unknown",
    checkpoint_path: str = None
) -> None:
    """
    Streaming ingestion from cloud storage using Auto-Loader (Databricks).
    Adds standard Bronze metadata columns.
    """
    if checkpoint_path is None:
        checkpoint_path = f"/mnt/checkpoints/{target_table.replace('.', '_')}"

    stream = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", file_format) \
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .load(source_path) \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_system", F.lit(source_system)) \
        .withColumn("_source_file", F.input_file_name()) \
        .withColumn("_ingestion_date", F.current_date())

    stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .toTable(target_table)


# ============================================================
# PATTERN 5: Structured Streaming with Kafka
# ============================================================

def stream_from_kafka(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    topic: str,
    target_table: str,
    checkpoint_path: str,
    starting_offsets: str = "latest",
    watermark_delay: str = "10 minutes"
) -> None:
    """
    Read events from Kafka/Event Hubs, apply watermark, write to Delta.
    """
    from pyspark.sql.types import StructType, StringType, TimestampType

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offsets) \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON payload
    parsed = raw_stream \
        .select(F.from_json(
            F.col("value").cast("string"),
            schema=_get_event_schema()  # define per event type
        ).alias("data"), "timestamp") \
        .select("data.*", F.col("timestamp").alias("event_timestamp"))

    # Apply watermark for late data handling
    with_watermark = parsed \
        .withWatermark("event_timestamp", watermark_delay)

    # Write to Delta Lake
    with_watermark.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .toTable(target_table)


def _get_event_schema():
    """Override this with your specific event schema."""
    from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType
    return StructType() \
        .add("event_id", StringType()) \
        .add("event_type", StringType()) \
        .add("user_id", StringType()) \
        .add("amount", DoubleType()) \
        .add("created_at", TimestampType())


# ============================================================
# PATTERN 6: Data Quality Validation
# ============================================================

def validate_dataframe(
    df: DataFrame,
    table_name: str,
    rules: dict
) -> tuple[DataFrame, DataFrame]:
    """
    Apply data quality rules and split into good/bad records.
    
    rules: {
        "order_id_not_null": "order_id IS NOT NULL",
        "positive_revenue": "revenue > 0",
        "valid_status": "status IN ('PENDING', 'COMPLETED', 'CANCELLED')"
    }
    
    Returns:
        (good_df, bad_df) — pass and fail records
    """
    all_rules_combined = " AND ".join(f"({condition})" for condition in rules.values())

    good_df = df.filter(all_rules_combined)
    bad_df = df.filter(f"NOT ({all_rules_combined})") \
               .withColumn("_dq_failed_rules", F.lit(str(rules.keys()))) \
               .withColumn("_dq_table", F.lit(table_name)) \
               .withColumn("_dq_timestamp", F.current_timestamp())

    fail_count = bad_df.count()
    total_count = df.count()

    if total_count > 0:
        fail_rate = fail_count / total_count
        logger.info(f"DQ [{table_name}]: {fail_count}/{total_count} failed ({fail_rate:.1%})")
        if fail_rate > 0.05:  # Alert if > 5% failure rate
            logger.warning(f"HIGH DQ FAILURE RATE: {fail_rate:.1%} for {table_name}")

    return good_df, bad_df


# ============================================================
# PATTERN 7: Spark Performance — Skew Handling
# ============================================================

def handle_skew_with_salting(
    df: DataFrame,
    skewed_key: str,
    skewed_values: List[str],
    salt_buckets: int = 20
) -> DataFrame:
    """
    Add a salt column to skewed keys to distribute data evenly.
    Use when certain key values (e.g., NULL, 'UNKNOWN') have disproportionate counts.
    """
    return df.withColumn(
        "_salted_key",
        F.when(
            F.col(skewed_key).isin(skewed_values),
            F.concat(F.col(skewed_key), F.lit("_"), (F.rand() * salt_buckets).cast("int"))
        ).otherwise(F.col(skewed_key))
    )


# ============================================================
# PATTERN 8: Delta Lake Table Maintenance
# ============================================================

def run_delta_maintenance(
    spark: SparkSession,
    table_name: str,
    z_order_columns: Optional[List[str]] = None,
    vacuum_hours: int = 168  # 7 days
) -> None:
    """
    Run OPTIMIZE (with optional Z-ORDER) and VACUUM on a Delta table.
    Schedule daily for Gold tables, weekly for Silver, monthly for Bronze.
    """
    logger.info(f"Running OPTIMIZE on {table_name}")
    if z_order_columns:
        cols = ", ".join(z_order_columns)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols})")
    else:
        spark.sql(f"OPTIMIZE {table_name}")

    logger.info(f"Running VACUUM on {table_name} (retain {vacuum_hours}h)")
    spark.sql(f"VACUUM {table_name} RETAIN {vacuum_hours} HOURS")
    logger.info(f"Maintenance complete for {table_name}")
