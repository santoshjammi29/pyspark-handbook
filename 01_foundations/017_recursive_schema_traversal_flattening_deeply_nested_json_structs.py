"""Scenario & Scale: Enterprise CRM/ERP multi-level payload flattener (25M docs/day).

Business Context:
Flatten deeply nested `StructType` payloads (5+ levels) into BI-ready tabular columns
using metadata-driven schema traversal without running JVM actions or SparkContext APIs.
"""


from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "017_Recursive_Schema_Flattening") -> SparkSession:
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
# - Using `spark.sparkContext.parallelize([json.dumps(d) ...])` + `spark.read.json(rdd)`
#   to infer nested schemas, or hardcoding dozens of `col("a.b.c.d").alias(...)` paths.
# Otherwise:
# - `sparkContext.parallelize` breaks on Spark Connect and forces an expensive schema
#   inference pass across the entire payload corpus.
# - Calling `df = df.select(...)` recursively across 20+ nesting levels creates a deep
#   `Project` stack that can hit Catalyst planning limits or 64KB JVM bytecode limits.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Build an explicit nested `StructType` and flatten all struct levels
# into a SINGLE flat `df.select(...)` projection list via pure Python schema traversal.
DEEP_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), nullable=False),
        T.StructField(
            "company",
            T.StructType(
                [
                    T.StructField("name", T.StringType(), nullable=False),
                    T.StructField(
                        "headquarters",
                        T.StructType(
                            [
                                T.StructField("city", T.StringType(), nullable=False),
                                T.StructField(
                                    "geo",
                                    T.StructType(
                                        [
                                            T.StructField("lat", T.DoubleType(), nullable=False),
                                            T.StructField("lon", T.DoubleType(), nullable=False),
                                        ]
                                    ),
                                    nullable=False,
                                ),
                            ]
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=False,
        ),
        T.StructField(
            "metrics",
            T.StructType(
                [
                    T.StructField(
                        "financials",
                        T.StructType(
                            [
                                T.StructField("revenue", T.DoubleType(), nullable=False),
                                T.StructField("ebitda", T.DoubleType(), nullable=False),
                            ]
                        ),
                        nullable=False,
                    )
                ]
            ),
            nullable=False,
        ),
    ]
)


def _collect_flat_columns(
    schema: T.StructType,
    prefix: str = "",
    alias_prefix: str = "",
) -> list[Column]:
    """Traverse StructType metadata in Python and emit leaf Column aliases in one pass."""
    cols: list[Column] = []
    for field in schema.fields:
        col_path = f"{prefix}.{field.name}" if prefix else field.name
        alias_name = f"{alias_prefix}_{field.name}" if alias_prefix else field.name
        if isinstance(field.dataType, T.StructType):
            cols.extend(_collect_flat_columns(field.dataType, col_path, alias_name))
        else:
            cols.append(F.col(f"`{col_path.replace('.', '`.`')}`").alias(alias_name))
    return cols


def flatten_dataframe(df: DataFrame) -> DataFrame:
    """Flatten all nested StructType fields into top-level columns in a single Project."""
    return df.select(*_collect_flat_columns(df.schema))


def main() -> None:
    spark = build_spark()

    deep_data = [
        (
            "ACC-99",
            ("Acme Corp", ("San Francisco", (37.7749, -122.4194))),
            ((5000000.0, 1200000.0),),
        )
    ]
    df_deep = spark.createDataFrame(deep_data, schema=DEEP_SCHEMA)
    df_flattened = flatten_dataframe(df_deep)

    expected_cols = {
        "id",
        "company_name",
        "company_headquarters_city",
        "company_headquarters_geo_lat",
        "company_headquarters_geo_lon",
        "metrics_financials_revenue",
        "metrics_financials_ebitda",
    }
    assert set(df_flattened.columns) == expected_cols
    assert df_flattened.first()["company_headquarters_geo_lat"] == 37.7749


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - Because `_collect_flat_columns` resolves recursion purely on driver metadata,
#   `df_flattened.explain("formatted")` contains only ONE `Project` node rather than
#   one `Project` per nesting depth.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Always check for column name collisions (`a_b_c` vs `a.b_c`) when flattening
#    arbitrary third-party schemas—fail fast if duplicate aliases are detected.
# 2. If `ArrayType(StructType)` appears inside the schema, decide explicitly whether to
#    `explode_outer` (multiplies rows) or serialize as JSON/VARIANT rather than flattening blindly.
# 3. Schemas expanding to 1,000+ leaf columns can exceed JVM 64KB codegen method limits
#    (`CodeGenerator` fallback to interpreted mode) and bloat Parquet footers.

if __name__ == "__main__":
    main()
