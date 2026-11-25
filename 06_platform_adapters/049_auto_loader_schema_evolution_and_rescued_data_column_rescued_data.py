"""
049 — Auto Loader Schema Evolution & Rescued Data Column (_rescued_data).

Scenario & Scale:
    Third-party telemetry vendors ship 50M+ JSON records/day and frequently add
    unannounced fields or emit malformed numeric strings (e.g., `"INVALID_FLOAT"`).
    Under Spark 4.0 ANSI mode (`spark.sql.ansi.enabled=true`), unsafe casts abort
    the entire batch, while dropping unknown keys causes permanent data loss.
"""

from __future__ import annotations


# ==============================================================================
# 2. Anti-Pattern: Strict Casts Under ANSI Mode & Ephemeral Local Rescue Paths
# ==============================================================================
# ❌ Bad Practice:
#   df.withColumn("temp", F.col("raw_temp").cast("double"))  # Fails in ANSI mode!
#   # Or cloudFiles.schemaEvolutionMode = "none" with schemaRescuePath = "file:/local/rescue"
#
# Otherwise:
#   In PySpark 4.0+ (ANSI enabled by default), `cast("double")` on `"CORRUPT"`
#   throws `CAST_INVALID_INPUT` and kills the streaming query. Writing rescued
#   metadata to ephemeral local disk loses auditability when workers terminate.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Evolution Modes + _rescued_data + try_cast
# ==============================================================================
# ✅ Good Practice: Enable `addNewColumns` + `_rescued_data` in Auto Loader and
#                   route ANSI type mismatches cleanly via `try_cast`.
def get_autoloader_evolution_options(
    schema_location: str,
    rescue_volume_path: str,
    mode: str = "addNewColumns",
) -> dict[str, str]:
    """Build Auto Loader schema evolution options targeting governed UC Volumes."""
    allowed_modes = {"addNewColumns", "rescue", "failOnNewColumns", "none"}
    if mode not in allowed_modes:
        raise ValueError(f"Unsupported schemaEvolutionMode: {mode}")

    return {
        "cloudFiles.format": "json",
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.schemaEvolutionMode": mode,
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "cloudFiles.schemaRescuePath": rescue_volume_path,
    }


def parse_with_rescued_data_fallback(df_raw):
    """ANSI-safe PySpark 4.0+ emulation of Auto Loader's `_rescued_data` capture."""
    from pyspark.sql import functions as F

    return (
        df_raw
        .withColumn("temp", F.expr("try_cast(raw_temp AS DOUBLE)"))
        .withColumn(
            "_rescued_data",
            F.when(
                F.col("temp").isNull() & F.col("raw_temp").isNotNull(),
                F.to_json(
                    F.struct(
                        F.col("raw_temp").alias("temp"),
                        F.lit("CAST_INVALID_INPUT: expected DOUBLE").alias("_error"),
                    )
                ),
            ).otherwise(F.lit(None).cast("string")),
        )
    )


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `df.explain("formatted")`, verify `Project` contains `try_cast(raw_temp#... as double)`
#   and `structstojson(...)` without Python UDF boundaries (`BatchEvalPython`).
# - In Spark UI -> SQL tab, inspect row counts split between clean Silver records
#   (`_rescued_data IS NULL`) and quarantined records (`_rescued_data IS NOT NULL`).


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: Auto Loader supports four evolution modes: `addNewColumns` (default when
#   inferring), `rescue` (keeps schema frozen, dumps new fields to `_rescued_data`),
#   `failOnNewColumns` (halts pipeline for manual review), and `none` (drops new fields).
# - Gotcha 2: `addNewColumns` intentionally throws an `UnknownFieldException` once after
#   updating `schemaLocation`, relying on Databricks Job auto-retry to resume cleanly.
# - Gotcha 3: `_rescued_data` is stored as a JSON string containing `{col: value, "_file_path": ...}`
#   so teams can backfill evolved columns later using `from_json` or Spark 4 `parse_json`.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_049_AutoLoader_RescuedData").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "true")

    opts = get_autoloader_evolution_options(
        schema_location="/Volumes/prod/iot/checkpoints/telemetry/schema",
        rescue_volume_path="/Volumes/prod/iot/checkpoints/telemetry/rescue",
    )
    print("Configured Auto Loader evolution options:", opts)

    sample = [
        ("D-1", "25.5", "NORMAL"),
        ("D-2", "CORRUPTED_TEMP", "CRITICAL"),
        ("D-3", "30.0", "NORMAL"),
    ]
    df_in = spark.createDataFrame(sample, ["device_id", "raw_temp", "status"])
    df_out = parse_with_rescued_data_fallback(df_in)
    df_out.select("device_id", "temp", "status", "_rescued_data").show(truncate=False)
