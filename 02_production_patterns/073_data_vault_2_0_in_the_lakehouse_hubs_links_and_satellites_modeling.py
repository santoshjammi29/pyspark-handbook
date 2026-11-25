"""Data Vault 2.0 in the Lakehouse: Hubs, Links & Satellites (PySpark 4.0+).

Scenario & Scale:
    A tier-1 bank integrates 30 core banking and lending feeds with frequent
    schema drift. Data Vault 2.0 enables parallel, auditable loading of Hubs,
    Links, and Satellites without cross-table foreign-key lookup bottlenecks.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Generating sequential integer surrogate keys in Hubs and
# performing blocking joins to populate Link/Satellite foreign keys, or using
# `concat(col1, col2)` without delimiters / null coalescing for hash keys.
# Otherwise: Forces serial table loads across 30 feeds and causes silent hash
# collisions between `("AB", "C")` and `("A", "BC")` or `NULL` propagation.
def transform_hub_unsafe_bad(df_raw: DataFrame) -> DataFrame:
    return df_raw.withColumn("hk_link", F.sha2(F.concat(F.col("customer_id"), F.col("account_id")), 256))


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Derive deterministic null-safe SHA-256 hash keys (`hk_*`)
# and attribute `hashdiff` columns independently so Hubs, Links, and Satellites
# load concurrently in map-only stages, skipping unchanged Satellite rows.
def _hash_key_expr(*cols: str) -> F.Column:
    normalized = [F.upper(F.trim(F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")))) for c in cols]
    return F.sha2(F.concat_ws("||", *normalized), 256)


def transform_build_hub_customer(df_raw: DataFrame) -> DataFrame:
    """Build Data Vault 2.0 Hub with normalized business key and SHA-256 hash key."""
    return (
        df_raw.filter(F.col("customer_id").isNotNull())
        .select(
            _hash_key_expr("customer_id").alias("hk_customer"),
            F.trim(F.col("customer_id")).alias("bk_customer_id"),
            F.current_timestamp().alias("load_dts"),
            F.col("record_source"),
        )
        .dropDuplicates(["hk_customer"])
    )


def transform_build_link_customer_account(df_raw: DataFrame) -> DataFrame:
    """Build Data Vault 2.0 Link capturing customer-account many-to-many relations."""
    return (
        df_raw.filter(F.col("customer_id").isNotNull() & F.col("account_id").isNotNull())
        .select(
            _hash_key_expr("customer_id", "account_id").alias("hk_link_cust_acc"),
            _hash_key_expr("customer_id").alias("hk_customer"),
            _hash_key_expr("account_id").alias("hk_account"),
            F.current_timestamp().alias("load_dts"),
            F.col("record_source"),
        )
        .dropDuplicates(["hk_link_cust_acc"])
    )


def transform_build_sat_customer(df_raw: DataFrame) -> DataFrame:
    """Build Data Vault 2.0 Satellite with `hashdiff` for change-only inserts."""
    return df_raw.filter(F.col("customer_id").isNotNull()).select(
        _hash_key_expr("customer_id").alias("hk_customer"),
        F.current_timestamp().alias("load_dts"),
        _hash_key_expr("name", "tier").alias("hashdiff"),
        F.col("name"),
        F.col("tier"),
        F.col("record_source"),
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `transform_build_sat_customer` executes purely inside a map-side `Project`
#   with zero shuffles; Hubs and Links only shuffle on `dropDuplicates(["hk_*"])`
#   (or better, insert via Delta `MERGE ... WHEN NOT MATCHED INSERT` by `hk_*`).


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Always normalize business keys (`upper(trim(coalesce(...)))`) before
#    hashing so trailing whitespace or casing drift across source systems does
#    not fracture one customer into multiple Hub keys.
# 2. Satellites are append-only (`INSERT` only when `hashdiff` differs from the
#    latest record for `hk_customer`); never overwrite Satellite rows in place.
# 3. Expose Kimball Star Schema Business Vault / Information Mart views (PIT &
#    Bridge tables) downstream so BI users never write 6-way Data Vault joins.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("073_data_vault_2_0")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    df_cust = spark.createDataFrame(
        [("CUST-101", "ACC-9001", "Alice Smith", "Gold", "CORE_BANKING")],
        "customer_id STRING, account_id STRING, name STRING, tier STRING, record_source STRING",
    )
    transform_build_hub_customer(df_cust).show(truncate=False)
    transform_build_link_customer_account(df_cust).show(truncate=False)
    transform_build_sat_customer(df_cust).show(truncate=False)
