"""Distributed Compressed Archives (.zip, .gz, .bz2) in PySpark 4.0+.

Scenario & Scale:
    A regulatory partner deposits quarterly 5 GB `.zip` and `.tar.gz` compliance
    archives. Naive ingestion or driver-side decompression crashes executors/driver
    with OOM; archive members must be unpacked across workers via DataFrame APIs.
"""

import io
import tempfile
import zipfile
from pathlib import Path
from typing import Iterator

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F


EXTRACTED_SCHEMA = "archive_path STRING, archive_member STRING, report_id STRING, amount DOUBLE"


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Calling `.collect()` on `binaryFile` rows to unpack ZIP files
# on the driver, or treating huge single `.gz`/`.zip` files as splittable inputs.
# Otherwise: Multi-gigabyte compressed payloads blow driver JVM/Python heap, and
# non-splittable `.gz`/`.zip` files pin an entire multi-GB file to a single core.
def unpack_archives_driver_bad(spark: SparkSession, archive_uri: str) -> list:
    return spark.read.format("binaryFile").load(archive_uri).collect()  # Driver OOM


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Read archives with `binaryFile` DataFrame source and unpack
# across executors via Arrow-backed `mapInPandas` (Spark Connect & 4.0 safe).
def _unpack_zip_batches(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Executor-side ZIP extractor operating on Arrow/Pandas DataFrame batches."""
    for pdf in batches:
        rows = []
        for path, raw_bytes in zip(pdf["path"], pdf["content"]):
            with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
                for member in zf.namelist():
                    if not member.endswith(".csv"):
                        continue
                    lines = zf.read(member).decode("utf-8").splitlines()
                    for line in lines[1:]:
                        parts = line.split(",")
                        if len(parts) == 2:
                            rows.append((path, member, parts[0], float(parts[1])))
        if rows:
            yield pd.DataFrame(
                rows,
                columns=["archive_path", "archive_member", "report_id", "amount"],
            )


def transform_unpack_zip_archives(df_binary: DataFrame) -> DataFrame:
    """Decompress binary ZIP archive rows into structured DataFrame records."""
    return (
        df_binary.select("path", "content")
        .mapInPandas(_unpack_zip_batches, schema=EXTRACTED_SCHEMA)
        .withColumn("amount", F.try_cast(F.col("amount"), "double"))
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows `FileScan binaryFile` -> `MapInPandas` inside
#   an Arrow-backed stage using pure DataFrame APIs (Spark Connect compatible).
# - Set `spark.sql.files.maxPartitionBytes` (e.g., 64m) when reading many small
#   archives so `binaryFile` splits archive paths evenly across executor tasks.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Splittability cheat sheet: GZIP & ZIP are NOT splittable (1 task per file);
#    BZIP2 is splittable via 48-bit sync markers; Snappy/ZSTD are splittable
#    only inside container formats (Parquet/ORC/Avro) at block/row-group level.
# 2. A single 5 GB `.zip` still loads 5 GB into one executor task's memory via
#    `binaryFile`; split oversized vendor archives upstream or stream chunks.
# 3. Always convert unpacked staging data immediately to ZSTD Parquet/Delta so
#    downstream analytical reads never pay single-file decompression penalties.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("052_distributed_zip_extraction")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_file = Path(tmp_dir) / "quarterly.zip"
        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr("q1.csv", "report_id,amount\nR1,500.0\nR2,800.0\n")
            zf.writestr("q2.csv", "report_id,amount\nR3,650.0\n")
        df_bin = spark.read.format("binaryFile").load(tmp_dir)
        transform_unpack_zip_archives(df_bin).show(truncate=False)
