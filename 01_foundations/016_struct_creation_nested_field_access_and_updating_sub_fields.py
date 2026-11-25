"""Scenario & Scale: Customer identity profile enrichment (60M profiles/day, 10m SLA).

Business Context:
Organize flat identity attributes into nested `StructType` hierarchies, project inner
sub-fields for columnar pruning, and mutate single nested fields via `withField` /
`dropFields` without rebuilding the entire struct.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "016_Struct_Creation_WithField") -> SparkSession:
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
#   df.withColumn("address", F.struct(F.col("address.street"), F.upper(F.col("address.city")), ...))
# Otherwise:
# - Reconstructing every sibling field manually whenever one nested attribute changes
#   is brittle: any newly evolved struct field added upstream is silently dropped.
# - Updating a `NULL` parent struct via `withField` produces `NULL` unless guarded.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Bundle with `F.struct()`, project with dot/`getField`, and mutate
# cleanly via `Column.withField()` / `Column.dropFields()`.
FLAT_CUSTOMER_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), nullable=False),
        T.StructField("first_name", T.StringType(), nullable=False),
        T.StructField("last_name", T.StringType(), nullable=False),
        T.StructField("street", T.StringType(), nullable=False),
        T.StructField("city", T.StringType(), nullable=False),
        T.StructField("state", T.StringType(), nullable=False),
        T.StructField("zip", T.StringType(), nullable=False),
        T.StructField("salary", T.IntegerType(), nullable=False),
    ]
)


def nest_and_update_profiles(df_flat: DataFrame) -> DataFrame:
    """Group customer columns into nested structs and update nested address fields."""
    df_nested = df_flat.select(
        F.col("id"),
        F.struct(F.col("first_name"), F.col("last_name")).alias("name"),
        F.struct(F.col("street"), F.col("city"), F.col("state"), F.col("zip")).alias("address"),
        F.col("salary"),
    )

    return df_nested.withColumn(
        "address",
        F.col("address")
        .withField("city", F.upper(F.col("address.city")))
        .withField("country", F.lit("USA"))
        .dropFields("zip"),
    )


def main() -> None:
    spark = build_spark()

    customers_flat = [
        ("C-01", "Alice", "Smith", "123 Elm St", "Austin", "TX", "78701", 85000),
        ("C-02", "Bob", "Jones", "456 Oak Ave", "Seattle", "WA", "98101", 92000),
    ]
    df_flat = spark.createDataFrame(customers_flat, schema=FLAT_CUSTOMER_SCHEMA)
    df_updated = nest_and_update_profiles(df_flat)

    df_projected = df_updated.select(
        "id",
        "name.last_name",
        F.col("address").getField("city").alias("home_city"),
        F.col("address.country").alias("country"),
    )

    row1 = df_projected.filter(F.col("id") == "C-01").first()
    assert row1["home_city"] == "AUSTIN" and row1["country"] == "USA"
    assert "zip" not in df_updated.select("address.*").columns


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_projected.explain("formatted")` triggers Catalyst `NestedColumnPruning`:
#   only `address.city` and `address.country` are decoded from Parquet row groups,
#   skipping unused sibling sub-fields (`street`, `state`).

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Calling `.withField()` on a `NULL` struct column yields `NULL` (SQL null-propagation);
#    coalesce with a typed empty struct if you need a non-null struct output.
# 2. Use `F.col("parent").getField("field.with.dots")` when nested field names contain
#    dots or spaces—dot notation `F.col("parent.field.with.dots")` fails resolution.
# 3. Parquet stores nested struct leaf columns in separate columnar chunks, so deeply
#    organized structs enjoy the same compression and column pruning as flat columns.

if __name__ == "__main__":
    main()
