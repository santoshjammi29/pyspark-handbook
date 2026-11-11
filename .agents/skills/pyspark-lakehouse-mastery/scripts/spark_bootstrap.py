#!/usr/bin/env python3
"""
SparkSession Bootstrap Helper
Provides seamless SparkSession initialization across local CLI, CI/CD runners, and Databricks clusters.
Supports Delta Lake 3.2+ and Adaptive Query Execution.
"""

import sys
import os

def get_or_create_spark(app_name: str = "PySpark_Lakehouse_Session", local_cores: str = "*"):
    """
    Returns an active SparkSession.
    If running inside Databricks, returns the existing 'spark' session.
    If running locally or in CI, initializes a local SparkSession with Delta Lake extensions.
    """
    try:
        # Databricks environment check
        import IPython
        ip = IPython.get_ipython()
        if ip and "spark" in ip.user_ns:
            return ip.user_ns["spark"]
    except Exception:
        pass

    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("ERROR: pyspark is not installed in the active environment.", file=sys.stderr)
        print("Run: pip install pyspark delta-spark", file=sys.stderr)
        sys.exit(1)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{local_cores}]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "4g")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

if __name__ == "__main__":
    spark = get_or_create_spark("PySpark_Bootstrap_Test")
    print(f"Spark Version: {spark.version}")
    print(f"Master: {spark.sparkContext.master}")
    df = spark.createDataFrame([(1, "Bronze"), (2, "Silver"), (3, "Gold")], ["stage_id", "tier"])
    df.show()
