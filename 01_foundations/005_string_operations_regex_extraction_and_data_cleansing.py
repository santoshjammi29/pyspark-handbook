"""Scenario & Scale: Marketing CRM lead cleansing & PII masking (30M leads/day, 10m SLA).

Business Context:
Normalize unstructured contact strings, extract phone/city/state fields via Tungsten
UTF-8 regex functions, and mask PII before landing records into Silver analytics tables.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "005_String_Regex_PII_Masking") -> SparkSession:
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
#   @F.udf("string")
#   def extract_phone_py(s): return re.search(...).group(0)
# Otherwise:
# - Row-by-row Python UDFs serialize UTF-8 Tungsten buffers across Py4J/pickle to
#   Python workers, degrading throughput by 10x-50x and risking `AttributeError` on None.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Native JVM `regexp_extract` / `regexp_replace` + explicit PII masking.
LEAD_SCHEMA = T.StructType(
    [
        T.StructField("lead_id", T.StringType(), nullable=False),
        T.StructField("raw_email", T.StringType(), nullable=True),
        T.StructField("raw_contact_str", T.StringType(), nullable=True),
    ]
)

PHONE_REGEX = r"(\d{3})[^\d]?(\d{3})[^\d]?(\d{4})"


def cleanse_and_mask_leads(df: DataFrame) -> DataFrame:
    """Extract structured contact attributes and mask email/phone PII in a single pass."""
    clean_email = F.lower(F.trim(F.col("raw_email")))
    phone_digits = F.regexp_replace(
        F.regexp_extract(F.col("raw_contact_str"), PHONE_REGEX, 0),
        r"[^\d]",
        "",
    )

    return df.withColumns(
        {
            "clean_email": clean_email,
            "phone_digits": phone_digits,
            "city": F.regexp_extract(F.col("raw_contact_str"), r"City:\s*([A-Za-z\s]+),", 1),
            "state": F.regexp_extract(F.col("raw_contact_str"), r",\s*([A-Z]{2})", 1),
            "masked_email": F.regexp_replace(clean_email, r"(^.).*(@.*$)", "$1***$2"),
            "masked_phone": F.when(
                F.length(phone_digits) == 10,
                F.concat(F.lit("***-***-"), F.substring(phone_digits, 7, 4)),
            ).otherwise(F.lit("UNAVAILABLE")),
        }
    )


def main() -> None:
    spark = build_spark()

    leads = [
        ("LEAD-1", "  JOHN.DOE@GMAIL.COM  ", "Phone: (555) 123-4567 | City: New York, NY"),
        ("LEAD-2", "SARAH_CONNOR@skyNET.io", "Cell: 555.987.6543 | City: Los Angeles, CA"),
        ("LEAD-3", "unknown_user", "No contact provided | City: Chicago, IL"),
        ("LEAD-4", "bruce.wayne@WayneEnt.com ", "Direct: +1-555-444-3333 | City: Gotham, NJ"),
    ]
    df_leads = spark.createDataFrame(leads, schema=LEAD_SCHEMA)
    df_result = cleanse_and_mask_leads(df_leads)

    lead1 = df_result.filter(F.col("lead_id") == "LEAD-1").first()
    lead3 = df_result.filter(F.col("lead_id") == "LEAD-3").first()
    assert lead1["masked_email"] == "j***@gmail.com"
    assert lead1["masked_phone"] == "***-***-4567"
    assert lead1["city"] == "New York" and lead1["state"] == "NY"
    assert lead3["masked_phone"] == "UNAVAILABLE"


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_result.explain("formatted")` executes inside WholeStageCodegen `Project` on
#   Tungsten `UTF8String` off-heap/on-heap byte arrays without any `BatchEvalPython` node.
# - If `BatchEvalPython` appears in the plan, replace the Python UDF with built-in SQL regex.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `F.regexp_extract` returns `""` (empty string), NOT `NULL`, when no match is found;
#    wrap with `F.nullif(..., F.lit(""))` before downstream `isNotNull()` checks.
# 2. Avoid catastrophic backtracking in regexes (nested quantifiers like `(a+)+`) on
#    untrusted user text, which can stall executor threads for minutes per partition.
# 3. Prefer SHA-256 tokenization (`F.sha2`) over simple character masking when analysts
#    need deterministic join keys across anonymized Silver tables.

if __name__ == "__main__":
    main()
