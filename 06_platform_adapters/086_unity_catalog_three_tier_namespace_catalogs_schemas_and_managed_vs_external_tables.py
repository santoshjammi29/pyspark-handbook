"""
086 — Unity Catalog Three-Tier Namespace (catalog.schema.table) & Managed vs External Tables.

Scenario & Scale:
    An enterprise migrates 1,000+ tables from legacy 2-tier `hive_metastore.default`
    into Databricks Unity Catalog (`catalog.schema.table`) to isolate dev/staging/prod
    environments and enforce centralized governance over Managed vs External tables.
"""

from __future__ import annotations

import re


# ==============================================================================
# 2. Anti-Pattern: 2-Tier Hive Metastore & Hardcoded Raw Cloud Paths
# ==============================================================================
# ❌ Bad Practice:
#   spark.sql(f"CREATE TABLE default.{tbl} USING DELTA LOCATION 's3://raw-bucket/{tbl}'")
#   df = spark.read.parquet("s3://raw-bucket/finance/invoices/")
#
# Otherwise:
#   2-tier names collide across environments, raw S3/ADLS paths bypass Unity Catalog
#   RBAC/audit lineage, and string-formatted DDL invites SQL injection or syntax breaks.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Governed 3-Tier Namespace Builders
# ==============================================================================
# ✅ Good Practice: Enforce validated 3-tier identifiers (`catalog.schema.table`)
#                   and separate Managed vs External lifecycle semantics cleanly.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def format_three_tier_name(catalog: str, schema: str, table: str) -> str:
    """Validate and return a safe 3-tier Unity Catalog identifier."""
    for part_name, value in (("catalog", catalog), ("schema", schema), ("table", table)):
        if not _IDENT_RE.match(value):
            raise ValueError(f"Invalid Unity Catalog {part_name} identifier: {value!r}")
    return f"{catalog}.{schema}.{table}"


def build_unity_catalog_ddl(
    catalog: str,
    schema: str,
    table: str,
    external_location: str | None = None,
) -> list[str]:
    """Generate idempotent Unity Catalog DDL statements for Managed or External tables."""
    fqn = format_three_tier_name(catalog, schema, table)
    create_table = (
        f"CREATE TABLE IF NOT EXISTS {fqn} (invoice_id STRING NOT NULL, amount DECIMAL(18, 2)) "
        "USING DELTA"
    )
    if external_location:
        if not external_location.startswith(("s3://", "abfss://", "gs://")):
            raise ValueError(f"External location must be a cloud URI: {external_location}")
        create_table += f" LOCATION '{external_location}'"

    return [
        f"CREATE CATALOG IF NOT EXISTS {catalog}",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}",
        create_table,
    ]


def table_lifecycle_matrix(spark):
    """Compare Managed vs External table storage ownership and DROP behavior."""
    rows = [
        ("Managed Table", "Unity Catalog managed storage", "Deletes metadata AND purges files after retention"),
        ("External Table", "User-specified External Location", "Deletes metadata ONLY; physical files preserved"),
    ]
    return spark.createDataFrame(rows, "table_type STRING, storage_owner STRING, drop_behavior STRING")


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `spark.table("prod_catalog.finance.invoices").explain("formatted")`, verify
#   `Relation` displays the full 3-tier `catalog.schema.table` name and Catalyst
#   pushes `PartitionFilters` / `PushedFilters` through Unity Catalog credential vending.
# - In Spark UI -> SQL tab, check `Scan parquet/delta` to confirm partition pruning
#   happens at the catalog metadata layer before executor task dispatch.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: Dropping a Managed table marks underlying cloud objects for automated
#   deletion (typically 7–30 days depending on cloud/UC retention), whereas dropping
#   an External table leaves all Parquet/Delta files untouched in S3/ADLS/GCS.
# - Gotcha 2: Databricks Predictive Optimization (auto-OPTIMIZE / auto-VACUUM) applies
#   to Unity Catalog Managed tables; External tables still require scheduled maintenance.
# - Gotcha 3: Bind production catalogs strictly to production workspaces (`Workspace-Catalog`
#   bindings) so dev clusters cannot accidentally query or mutate prod data.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_086_UnityCatalog_3Tier").getOrCreate()
    for stmt in build_unity_catalog_ddl("enterprise_prod", "finance", "invoices"):
        print(stmt)
    table_lifecycle_matrix(spark).show(truncate=False)
