"""Scenario & Scale: CRM-to-Billing customer reconciliation (80M accounts/day, 10m SLA).

Business Context:
Reconcile Salesforce CRM accounts against billing ledgers using `inner`, `full_outer`,
`left_semi` (`EXISTS`), and `left_anti` (`NOT EXISTS`) joins without row duplication or
ambiguous column collisions.
"""


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T


def build_spark(app_name: str = "030_Relational_Joins_Semi_Anti") -> SparkSession:
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
# - Finding unbilled CRM accounts via `df_crm.join(df_bill, df_crm["cust_id"] == ... , "left")`
#   followed by `.filter(df_bill["cust_id"].isNull())`.
# - Using an `inner` join to check existence when `df_bill` has multiple invoices per customer.
# Otherwise:
# - Joining with `df1["id"] == df2["id"]` leaves duplicate ambiguous `cust_id` columns.
# - `left` join + `isNull()` projects and shuffles right-side columns unnecessarily and
#   multiplies rows if multiple right matches exist before filtering.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: String `on="cust_id"` + `left_semi` (`EXISTS`) + `left_anti` (`NOT EXISTS`).
CRM_SCHEMA = T.StructType(
    [
        T.StructField("cust_id", T.StringType(), nullable=False),
        T.StructField("company_name", T.StringType(), nullable=False),
        T.StructField("tier", T.StringType(), nullable=False),
    ]
)

BILLING_SCHEMA = T.StructType(
    [
        T.StructField("cust_id", T.StringType(), nullable=False),
        T.StructField("mrr", T.DoubleType(), nullable=False),
        T.StructField("billing_status", T.StringType(), nullable=False),
    ]
)


def reconcile_crm_and_billing(
    df_crm: DataFrame,
    df_bill: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Produce matched, full footprint, semi-joined active, and anti-joined orphan views."""
    df_inner = df_crm.join(df_bill, on="cust_id", how="inner")
    df_outer = df_crm.join(df_bill, on="cust_id", how="full_outer")
    df_semi = df_crm.join(df_bill, on="cust_id", how="left_semi")
    df_anti = df_crm.join(df_bill, on="cust_id", how="left_anti")
    return df_inner, df_outer, df_semi, df_anti


def main() -> None:
    spark = build_spark()

    crm_data = [
        ("C-01", "Acme Corp", "Enterprise"),
        ("C-02", "Beta Ltd", "Mid-Market"),
        ("C-03", "Cyber Dyne", "SMB"),
        ("C-04", "Delta Air", "Enterprise"),
    ]
    billing_data = [
        ("C-01", 15000.0, "ACTIVE"),
        ("C-02", 4500.0, "ACTIVE"),
        ("C-05", 2200.0, "OVERDUE"),
    ]
    df_crm = spark.createDataFrame(crm_data, schema=CRM_SCHEMA)
    df_bill = spark.createDataFrame(billing_data, schema=BILLING_SCHEMA)

    df_inner, df_outer, df_semi, df_anti = reconcile_crm_and_billing(df_crm, df_bill)

    assert df_inner.count() == 2
    assert df_outer.count() == 5
    assert [r["cust_id"] for r in df_semi.orderBy("cust_id").collect()] == ["C-01", "C-02"]
    assert [r["cust_id"] for r in df_anti.orderBy("cust_id").collect()] == ["C-03", "C-04"]


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_semi.explain("formatted")` and `df_anti.explain("formatted")` use `LeftSemi` /
#   `LeftAnti` join operators that short-circuit on the first right-side match and emit
#   zero right-side columns into the output buffer.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `left_semi` never duplicates left-side rows even when the right table has 100
#    matching rows per key—making it safer than `inner` + `.distinct()`.
# 2. Beware NULLs in SQL `NOT IN (...)` vs `left_anti`: SQL `NOT IN` returns ZERO rows
#    if the subquery contains a single `NULL`; `left_anti` (or `NULL`-safe `<=>`) handles
#    missing keys safely without wiping out the result set.
# 3. When joining on differently named keys (`a.id == b.user_id`), drop the redundant
#    join column immediately or select explicitly to avoid downstream ambiguity errors.

if __name__ == "__main__":
    main()
