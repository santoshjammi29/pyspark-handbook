"""Scenario & Scale: Ad-Tech dynamic UTM & experiment attribute pipeline (400M events/day).

Business Context:
Store variable partner tracking parameters in `MapType`, safely look up dynamic keys
under Spark 4.0 ANSI mode, inspect key/value arrays, and normalize sparse maps when needed.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "018_MapType_Dynamic_Properties") -> SparkSession:
    """Create or reuse a portable SparkSession with ANSI mode enabled."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 1. Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice:
# - Defining a 300-column sparse `StructType` for dynamic partner UTM/experiment keys,
#   or using `F.col("attributes")["utm_campaign"]` assuming strict ANSI key checks.
# Otherwise:
# - Static structs require constant schema migrations for every new partner tag.
# - Under `spark.sql.ansi.enabled=true` + `spark.sql.mapKeyDedupPolicy=EXCEPTION`,
#   duplicate keys in `map_from_arrays` abort the job unless handled/deduplicated.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: `MapType` via `map_from_arrays` + `F.try_element_at` for safe lookups.
CAMPAIGN_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=False),
        T.StructField("param_keys", T.ArrayType(T.StringType()), nullable=False),
        T.StructField("param_vals", T.ArrayType(T.StringType()), nullable=False),
    ]
)


def build_and_query_attributes(df: DataFrame) -> DataFrame:
    """Construct a MapType attribute bag and extract dynamic keys safely."""
    attributes_map = F.map_from_arrays(F.col("param_keys"), F.col("param_vals"))
    return df.withColumn("attributes", attributes_map).withColumns(
        {
            "source": F.try_element_at(F.col("attributes"), F.lit("utm_source")),
            "campaign": F.coalesce(
                F.try_element_at(F.col("attributes"), F.lit("utm_campaign")),
                F.lit("ORGANIC"),
            ),
            "all_keys": F.map_keys(F.col("attributes")),
            "all_values": F.map_values(F.col("attributes")),
        }
    )


def main() -> None:
    spark = build_spark()

    campaign_data = [
        ("EV-1", ["utm_source", "utm_campaign", "device"], ["google", "black_friday", "mobile"]),
        ("EV-2", ["utm_source", "experiment_group"], ["facebook", "variant_b"]),
        (
            "EV-3",
            ["utm_source", "utm_medium", "utm_campaign"],
            ["linkedin", "cpc", "q4_enterprise"],
        ),
    ]
    df_raw = spark.createDataFrame(campaign_data, schema=CAMPAIGN_SCHEMA)
    df_enriched = build_and_query_attributes(df_raw)

    df_kv = df_enriched.select("event_id", F.explode("attributes").alias("attr_key", "attr_value"))

    ev2 = df_enriched.filter(F.col("event_id") == "EV-2").first()
    assert ev2["source"] == "facebook" and ev2["campaign"] == "ORGANIC"
    assert df_kv.count() == 8


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_enriched.explain("formatted")` stays inside a narrow `Project` using native
#   `CreateMap` / `MapFromArrays` and `TryElementAt` expressions.
# - `F.explode("attributes")` introduces a `Generate explode(attributes)` expansion;
#   prefer `map_keys` / `map_filter` when full row normalization is not required.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `MapType` enforces homogeneous key and value types (e.g. `MAP<STRING, STRING>`);
#    for mixed-type semi-structured payloads in Spark 4.0+, prefer `VARIANT`.
# 2. `F.map_from_arrays` requires `keys` and `values` arrays of equal length on every row;
#    mismatched lengths raise an error in ANSI mode.
# 3. `F.map_filter` and `F.transform_values` allow in-place map pruning/masking
#    without exploding keys into rows.

if __name__ == "__main__":
    main()
