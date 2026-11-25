"""Project Healthcare: HIPAA Safe Harbor Clinical Records De-Identification.

Scenario & Scale:
    A hospital research network exports 20M+ clinical EHR encounters for cancer
    studies. HIPAA Safe Harbor mandates stripping/hashing 18 PHI identifiers
    (names, MRNs, full dates, 5-digit ZIPs) while preserving clinical duration.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Hashing MRNs without a secret salt, truncating ZIPs blindly
# without masking ages > 89, or writing unsanitized dates into research tables.
# Otherwise: Rainbow-table linkage attacks trivially re-identify MRNs, and
# HIPAA Safe Harbor violations (exact DOB, age > 89, < 20k-pop ZIP3) occur.
def transform_hipaa_unsalted_bad(df_ehr: DataFrame) -> DataFrame:
    return df_ehr.withColumn("research_id", F.sha2(F.col("mrn"), 256))  # Vulnerable to rainbow tables


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Pseudonymize MRNs via HMAC/salted SHA-256 (salt injected
# from secret store), drop direct PHI, cap ages >= 90 per Safe Harbor, and
# replace calendar timestamps with derived clinical durations (LOS in days).
def transform_deidentify_hipaa_safe_harbor(
    df_ehr: DataFrame, secret_salt: str
) -> DataFrame:
    """Apply HIPAA Safe Harbor de-identification and compute length of stay."""
    admit_ts = F.try_to_timestamp(F.col("admit_ts"), F.lit("yyyy-MM-dd HH:mm:ss"))
    discharge_ts = F.try_to_timestamp(F.col("discharge_ts"), F.lit("yyyy-MM-dd HH:mm:ss"))
    dob_date = F.try_to_date(F.col("dob"), "yyyy-MM-dd")

    raw_age = F.floor(F.try_divide(F.datediff(F.current_date(), dob_date), F.lit(365.25)))
    safe_harbor_age = F.when(raw_age >= F.lit(90), F.lit(90)).otherwise(raw_age)

    return (
        df_ehr.withColumn(
            "research_patient_id",
            F.substring(F.sha2(F.concat(F.col("mrn"), F.lit(secret_salt)), 256), 1, 16),
        )
        .withColumn("zip3", F.substring(F.trim(F.col("zip_code")), 1, 3))
        .withColumn("patient_age_capped", safe_harbor_age.cast(T.IntegerType()))
        .withColumn(
            "length_of_stay_days",
            F.round(
                F.try_divide(discharge_ts.cast("long") - admit_ts.cast("long"), F.lit(86400.0)),
                2,
            ),
        )
        .drop("mrn", "name", "dob", "zip_code", "admit_ts", "discharge_ts")
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` confirms all Safe Harbor masking (`sha2`, `substring`,
#   `datediff`, column drops) runs in a single map-side `Project` with zero shuffle.
# - Ensure `secret_salt` is passed as a literal/secret parameter at runtime and
#   never logged in Spark query plans or event logs (`spark.redaction.regex`).


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. HIPAA Safe Harbor requires aggregating all ages >= 90 into a single "90+"
#    bucket AND replacing restricted 3-digit ZIPs (population < 20,000) with "000".
# 2. Never hardcode `secret_salt` in source code; fetch it from a KMS / Key
#    Vault / Secrets Manager boundary at job startup.
# 3. Date-shifting (adding a deterministic per-patient random day offset `[-180, +180]`)
#    is preferred over dropping dates when seasonal/epidemiological analysis is needed.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("098_hipaa_deidentification")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    df_ehr = spark.createDataFrame(
        [
            ("PAT-1001", "John Doe", "1930-06-12", "78701", "2025-01-01 08:00:00", "2025-01-05 14:00:00", "Cardiology"),
            ("PAT-1002", "Jane Roe", "1988-09-24", "98101", "2025-01-02 10:00:00", "2025-01-03 18:00:00", "Neurology"),
        ],
        "mrn STRING, name STRING, dob STRING, zip_code STRING, admit_ts STRING, discharge_ts STRING, diagnosis STRING",
    )
    transform_deidentify_hipaa_safe_harbor(df_ehr, secret_salt="RUNTIME_KMS_SALT").show(truncate=False)
