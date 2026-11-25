"""
089 — Dynamic Column Masking for PII Protection & Compliance.

Scenario & Scale:
    A healthcare insurance table (`100M+` member rows) stores SSNs and emails.
    Compliance officers need cleartext access for audits, while analysts and BI
    dashboards must receive masked tokens (`***-**-6789`) from the same table.
"""

from __future__ import annotations


# ==============================================================================
# 2. Anti-Pattern: BI-Layer-Only Masking & Unmasked/Masked Table Duplication
# ==============================================================================
# ❌ Bad Practice:
#   # Writing a separate masked copy every night or masking only in Tableau/PowerBI
#   df.write.mode("overwrite").saveAsTable("prod.hr.members_masked_copy")
#
# Otherwise:
#   BI-only masking leaves raw PII exposed to anyone querying the warehouse via SQL
#   or notebooks, while duplicating masked tables doubles storage and creates lag.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Unity Catalog Column Masking Functions
# ==============================================================================
# ✅ Good Practice: Attach a dynamic SQL Column Mask to the base column so masking
#                   executes transparently at query compile time per caller identity.
COLUMN_MASK_DDL = """
CREATE OR REPLACE FUNCTION enterprise_prod.governance.ssn_mask(ssn STRING)
RETURN IF(
    is_account_group_member('compliance_officers'),
    ssn,
    CONCAT('***-**-', RIGHT(ssn, 4))
);

ALTER TABLE enterprise_prod.hr.t_members
ALTER COLUMN ssn SET MASK enterprise_prod.governance.ssn_mask;
""".strip()


def apply_portable_column_masks(df, is_compliance_officer: bool):
    """Portable PySpark 4.0+ PII masking transform for local validation."""
    from pyspark.sql import functions as F

    if is_compliance_officer:
        return df

    return (
        df.withColumn(
            "ssn",
            F.when(F.col("ssn").isNull(), F.lit(None).cast("string"))
            .otherwise(F.concat(F.lit("***-**-"), F.right(F.col("ssn"), F.lit(4)))),
        )
        .withColumn(
            "email",
            F.when(F.col("email").isNull(), F.lit(None).cast("string"))
            .otherwise(F.regexp_replace(F.col("email"), r"(^.).*(@.*$)", "$1***$2")),
        )
    )


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `df.explain("formatted")`, the masking expression is injected in the `Project`
#   node immediately above `FileScan` (`concat(***-**-, right(ssn, 4))`).
# - Notice that any `HashAggregate` or `Filter` on `ssn` sits ABOVE the `Project` mask,
#   meaning analysts query masked values rather than raw storage values.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: `COUNT(DISTINCT ssn)` or `GROUP BY ssn` for non-privileged users
#   aggregates over masked strings (collapsing distinct SSNs that share last-4 digits);
#   expose a salted surrogate key (`member_sk`) for safe analytical joins/counts.
# - Gotcha 2: `CREATE TABLE copy AS SELECT * FROM t_members` run by a non-compliance
#   user persists only masked values—raw PII cannot be exfiltrated via CTAS.
# - Gotcha 3: `F.right(col, 4)` handles varying string lengths safely under ANSI mode,
#   unlike hardcoded positional `substring(ssn, 8, 4)` on malformed inputs.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_089_Column_Masking").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "true")
    print(COLUMN_MASK_DDL)

    members = [
        ("M-101", "Alice Smith", "123-45-6789", "alice@corp.com"),
        ("M-102", "Bob Jones", "987-65-4321", "bob@corp.com"),
    ]
    df_members = spark.createDataFrame(members, "member_id STRING, name STRING, ssn STRING, email STRING")
    apply_portable_column_masks(df_members, is_compliance_officer=False).show(truncate=False)
