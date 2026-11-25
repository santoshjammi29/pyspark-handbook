"""
087 — Unity Catalog Volumes for Governed Unstructured & Semi-Structured Data.

Scenario & Scale:
    ML and document pipelines ingest millions of PDFs, images, audio clips, and
    model weights. Legacy DBFS mounts (`/mnt/...`) grant coarse workspace-wide
    access with weak audit trails and fail under Spark Connect isolation.
"""

from __future__ import annotations

import re


# ==============================================================================
# 2. Anti-Pattern: Legacy DBFS Mounts & Unbounded Local Driver Staging
# ==============================================================================
# ❌ Bad Practice:
#   dbutils.fs.mount(source="wasbs://raw@acct.blob.core.windows.net", mount_point="/mnt/pdfs")
#   df = spark.read.format("binaryFile").load("/mnt/pdfs/*.pdf")
#   # Or copying files to local ephemeral driver disk before parsing
#
# Otherwise:
#   DBFS mounts expose storage keys workspace-wide, bypass Unity Catalog lineage and
#   audit logs, and local driver staging causes disk exhaustion on large batches.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Unity Catalog Volumes & POSIX/Spark Access
# ==============================================================================
# ✅ Good Practice: Address non-tabular assets via governed `/Volumes/<cat>/<schema>/<vol>/`
#                   URIs and read scalable binary metadata with `binaryFile` format.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def build_volume_posix_path(catalog: str, schema: str, volume: str, relative_path: str = "") -> str:
    """Build a canonical Unity Catalog Volume POSIX path."""
    for part_name, val in (("catalog", catalog), ("schema", schema), ("volume", volume)):
        if not _IDENT_RE.match(val):
            raise ValueError(f"Invalid {part_name} identifier: {val!r}")
    clean_rel = relative_path.lstrip("/")
    base = f"/Volumes/{catalog}/{schema}/{volume}"
    return f"{base}/{clean_rel}" if clean_rel else base


def build_volume_ddl(
    catalog: str,
    schema: str,
    volume: str,
    external_location: str | None = None,
) -> str:
    """Return Unity Catalog DDL for a Managed or External Volume."""
    fqn = f"{catalog}.{schema}.{volume}"
    if external_location:
        return f"CREATE EXTERNAL VOLUME IF NOT EXISTS {fqn} LOCATION '{external_location}'"
    return f"CREATE VOLUME IF NOT EXISTS {fqn}"


def summarize_unstructured_inventory(df_binary):
    """Aggregate binaryFile metadata safely using ANSI-safe Spark 4.0+ SQL functions."""
    from pyspark.sql import functions as F

    return df_binary.agg(
        F.count("*").alias("file_count"),
        F.sum("length").alias("total_bytes"),
        F.expr("try_divide(sum(length), 1048576.0)").alias("total_mib"),
    )


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `df_binary.explain("formatted")`, `FileScan binaryFile` displays `ReadSchema`
#   (`path`, `modificationTime`, `length`, `content`). Selecting only `path` and `length`
#   enables column pruning so raw `content` bytes are NOT read into executor memory.
# - In Spark UI -> SQL tab, verify `Scan binaryFile` data size stays near metadata
#   overhead when `content` is excluded from downstream projections.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: Unity Catalog Volumes replace DBFS mounts and `dbfs:/FileStore` with
#   fine-grained `READ VOLUME` / `WRITE VOLUME` grants and full access auditing.
# - Gotcha 2: Single-node Python libraries (`pypdf`, `Pillow`, `torch`) can read
#   `/Volumes/<catalog>/<schema>/<volume>/...` directly via POSIX `open()` without `dbutils`.
# - Gotcha 3: Always prefer `spark.read.format("binaryFile")` with projection pruning
#   before UDF-based PDF/image parsing so you never `collect()` raw file bytes to the driver.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_087_UC_Volumes").getOrCreate()
    vol_path = build_volume_posix_path("enterprise_prod", "finance", "raw_receipts", "2026/01")
    print("Volume POSIX path:", vol_path)
    print("DDL:", build_volume_ddl("enterprise_prod", "finance", "raw_receipts"))

    mock_files = [
        (f"{vol_path}/inv_01.pdf", 204800),
        (f"{vol_path}/inv_02.pdf", 512000),
    ]
    df_meta = spark.createDataFrame(mock_files, "path STRING, length BIGINT")
    summarize_unstructured_inventory(df_meta).show(truncate=False)
