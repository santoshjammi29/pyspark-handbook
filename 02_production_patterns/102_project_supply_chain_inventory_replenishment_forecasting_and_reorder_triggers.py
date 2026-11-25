"""Project Supply Chain: Inventory Replenishment & Dynamic Reorder Triggers.

Scenario & Scale:
    A national grocery chain tracks 10,000 SKUs across 200+ stores/DCs (2M+
    SKU-DC pairs). Daily replenishment runs compute Safety Stock and Reorder
    Points (ROP) to trigger automated ERP purchase orders before stockouts occur.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Using fixed static reorder thresholds across seasonal SKUs or
# casting negative/corrupt lead-time inputs directly without ANSI range checks.
# Otherwise: High-velocity items stock out during supplier delays while slow
# perishables over-accumulate, and negative `lead_time_days` makes `sqrt()` fail/NaN.
def transform_reorder_static_bad(df_inv: DataFrame) -> DataFrame:
    return df_inv.filter(F.col("current_stock") < 50)  # Ignores velocity & lead time


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Compute statistical Safety Stock (`Z * sqrt(lead_time) * sigma`)
# and Reorder Point (`lead_time_demand + safety_stock`) using `try_cast` and
# non-negative guards under PySpark 4.0 ANSI mode.
def transform_compute_reorder_triggers(
    df_inventory: DataFrame, service_z_factor: float = 1.65
) -> DataFrame:
    """Compute Safety Stock, Reorder Point (ROP), and ERP PO quantities."""
    df_clean = (
        df_inventory.withColumn("stock", F.try_cast(F.col("current_stock"), T.IntegerType()))
        .withColumn("lead_days", F.greatest(F.try_cast(F.col("lead_time_days"), T.IntegerType()), F.lit(0)))
        .withColumn("daily_sales", F.try_cast(F.col("avg_daily_sales"), T.DoubleType()))
        .withColumn("daily_sigma", F.try_cast(F.col("stddev_daily_sales"), T.DoubleType()))
        .filter(
            F.col("stock").isNotNull()
            & F.col("lead_days").isNotNull()
            & F.col("daily_sales").isNotNull()
            & F.col("daily_sigma").isNotNull()
        )
    )

    return (
        df_clean.withColumn("lead_time_demand", F.round(F.col("lead_days") * F.col("daily_sales"), 1))
        .withColumn(
            "safety_stock",
            F.ceil(F.lit(service_z_factor) * F.sqrt(F.col("lead_days")) * F.col("daily_sigma")).cast(
                T.IntegerType()
            ),
        )
        .withColumn(
            "reorder_point",
            (F.ceil(F.col("lead_time_demand")) + F.col("safety_stock")).cast(T.IntegerType()),
        )
        .withColumn("needs_reorder", F.col("stock") <= F.col("reorder_point"))
        .withColumn(
            "suggested_order_qty",
            F.when(
                F.col("needs_reorder"),
                F.greatest((F.col("reorder_point") * F.lit(2)) - F.col("stock"), F.lit(0)),
            ).otherwise(F.lit(0)),
        )
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows pure map-side `Project` + `Filter` operations
#   within `WholeStageCodegen`; 2M SKU-DC combinations execute with zero shuffle
#   when upstream sales velocity is already pre-aggregated at `(dc_id, sku)`.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `sqrt(lead_time_days)` assumes independent daily demand variance AND
#    constant lead time; when supplier lead times vary (`sigma_LT > 0`), use the
#    joint formula: `Z * sqrt(LT * sigma_D^2 + D^2 * sigma_LT^2)`.
# 2. Guard `lead_time_days >= 0` before `F.sqrt()`; negative values produce `NaN`,
#    which breaks downstream integer casts or silently corrupts PO quantities.
# 3. Round Safety Stock up (`F.ceil`) rather than down for high-SLA items to
#    preserve the target 95% (`Z=1.65`) or 99% (`Z=2.33`) service level.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("102_supply_chain_rop")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    rows = [
        ("SKU-101", "Milk Whole Gallon", 150, 4, 35.0, 5.0),
        ("SKU-102", "Organic Eggs 12pk", 40, 3, 20.0, 4.0),
        ("SKU-103", "Artisan Bread", 80, 2, 15.0, 2.0),
    ]
    df_in = spark.createDataFrame(
        rows,
        "sku STRING, product_name STRING, current_stock INT, lead_time_days INT, avg_daily_sales DOUBLE, stddev_daily_sales DOUBLE",
    )
    transform_compute_reorder_triggers(df_in).filter(F.col("needs_reorder")).select(
        "sku", "product_name", "stock", "reorder_point", "suggested_order_qty"
    ).show(truncate=False)
