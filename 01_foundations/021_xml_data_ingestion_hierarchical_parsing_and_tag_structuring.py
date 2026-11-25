"""Scenario & Scale: Healthcare XML insurance claim parsing (10M claims/day, 15m SLA).

Business Context:
Extract hierarchical claim IDs, patient attributes, and line-item billing amounts from
XML documents using Spark SQL XPath expressions and Spark 4.0 native XML capabilities.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "021_XML_Ingestion_XPath_Parsing") -> SparkSession:
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
# - Parsing XML payloads via `xml.etree.ElementTree` inside a Python `@F.udf`, or
#   reading multi-GB XML files with `inferSchema=True` on `format("xml")`.
# Otherwise:
# - Python ElementTree UDFs trigger full Py4J/pickle row serialization and risk XML
#   entity expansion vulnerabilities (XXE) if untrusted payloads are parsed naively.
# - `inferSchema=True` scans the entire XML corpus twice and misinfers sparse tags.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Built-in JVM XPath expressions (`xpath_string`, `xpath_double`) +
# `F.try_cast` (or Spark 4.0 native `spark.read.format("xml").schema(...)` for files).
RAW_XML_SCHEMA = T.StructType(
    [T.StructField("raw_xml", T.StringType(), nullable=False)]
)


def parse_xml_claims(df_raw: DataFrame) -> DataFrame:
    """Extract claim header and primary line-item fields via JVM XPath expressions."""
    raw_amount = F.nullif(
        F.xpath_string(F.col("raw_xml"), F.lit("claim/items/item[1]/@amount")),
        F.lit(""),
    )
    return df_raw.withColumns(
        {
            "claim_id": F.nullif(F.xpath_string(F.col("raw_xml"), F.lit("claim/@id")), F.lit("")),
            "patient_id": F.nullif(
                F.xpath_string(F.col("raw_xml"), F.lit("claim/patient/@id")),
                F.lit(""),
            ),
            "patient_name": F.xpath_string(F.col("raw_xml"), F.lit("claim/patient/name")),
            "patient_dob": F.try_to_date(
                F.xpath_string(F.col("raw_xml"), F.lit("claim/patient/dob")),
                F.lit("yyyy-MM-dd"),
            ),
            "primary_item_code": F.xpath_string(
                F.col("raw_xml"), F.lit("claim/items/item[1]/@code")
            ),
            "primary_item_amount": F.try_cast(raw_amount, T.DoubleType()),
        }
    )


def main() -> None:
    spark = build_spark()

    xml_payloads = [
        (
            '<claim id="CLM-001">'
            '<patient id="P-10"><name>John Doe</name><dob>1980-05-12</dob></patient>'
            '<items><item code="99213" amount="150.00"/><item code="80053" amount="75.50"/></items>'
            "</claim>",
        ),
        (
            '<claim id="CLM-002">'
            '<patient id="P-20"><name>Jane Smith</name><dob>1992-11-23</dob></patient>'
            '<items><item code="99214" amount="220.00"/></items>'
            "</claim>",
        ),
    ]
    df_raw_xml = spark.createDataFrame(xml_payloads, schema=RAW_XML_SCHEMA)
    df_parsed = parse_xml_claims(df_raw_xml)

    assert df_parsed.filter(F.col("claim_id").isNull()).count() == 0
    assert df_parsed.filter(F.col("primary_item_amount") <= 0).count() == 0
    clm1 = df_parsed.filter(F.col("claim_id") == "CLM-001").first()
    assert clm1["patient_name"] == "John Doe" and clm1["primary_item_amount"] == 150.0


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_parsed.explain("formatted")` executes JVM `XPathString` and `TryCast` inside a
#   single `Project` stage with zero Python worker process launch.
# - For multi-GB XML files on object storage, Spark 4.0 includes built-in `xml` source:
#   `spark.read.format("xml").option("rowTag", "claim").schema(claim_schema).load(uri)`.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `xpath_string` returns `""` (empty string) when a tag/attribute is missing; wrap
#    with `F.nullif(..., F.lit(""))` before `F.try_cast` or `isNotNull()` checks.
# 2. For multi-item arrays inside an XML claim, prefer Spark 4.0 `F.from_xml(col, schema)`
#    or `format("xml")` with `rowTag="claim"` rather than indexing `item[1]`, `item[2]`.
# 3. Multi-GB single XML documents cannot split cleanly unless `rowTag` boundaries are
#    indexed by the Hadoop `XmlInputFormat` splitter—avoid gzip-compressed monolith XMLs.

if __name__ == "__main__":
    main()
