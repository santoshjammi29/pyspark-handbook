"""Project E-Commerce: Collaborative Filtering Recommendation Engine (MLlib ALS).

Scenario & Scale:
    An e-commerce marketplace serves 5M shoppers and 1M SKUs. Nightly Spark
    MLlib Alternating Least Squares (ALS) factorizes 200M+ ratings/clicks to
    pre-compute Top-N homepage recommendations written to a Gold serving table.
"""

from __future__ import annotations

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Training ALS with default `coldStartStrategy="nan"` or fitting
# on raw non-numeric string user/product IDs without deterministic encoding.
# Otherwise: Unseen users/items in the validation split produce `NaN` predictions
# that poison `RegressionEvaluator` RMSE (`NaN`), and string IDs fail ALS fitting.
def fit_als_unsafe_bad(df_ratings: DataFrame) -> ALSModel:
    return ALS(userCol="user_id", itemCol="product_id", ratingCol="rating").fit(df_ratings)


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Validate numeric IDs/ratings with `F.try_cast`, set
# `coldStartStrategy="drop"` and explicit `seed` for reproducibility, evaluate
# RMSE on a holdout split, and explode `recommendForAllUsers(top_k)` cleanly.
def transform_prepare_als_inputs(df_raw: DataFrame) -> DataFrame:
    """Cast user/item IDs and ratings safely under PySpark 4.0 ANSI mode."""
    return (
        df_raw.withColumn("user_id", F.try_cast(F.col("user_id"), T.IntegerType()))
        .withColumn("product_id", F.try_cast(F.col("product_id"), T.IntegerType()))
        .withColumn("rating", F.try_cast(F.col("rating"), T.FloatType()))
        .filter(
            F.col("user_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("rating").isNotNull()
        )
    )


def train_and_evaluate_als(
    df_ratings: DataFrame,
    rank: int = 10,
    max_iter: int = 5,
    reg_param: float = 0.05,
    implicit_prefs: bool = False,
) -> tuple[ALSModel, float]:
    """Train ALS model and return `(model, holdout_rmse)`."""
    train_df, test_df = df_ratings.randomSplit([0.8, 0.2], seed=42)
    als = ALS(
        rank=rank,
        maxIter=max_iter,
        regParam=reg_param,
        implicitPrefs=implicit_prefs,
        userCol="user_id",
        itemCol="product_id",
        ratingCol="rating",
        coldStartStrategy="drop",
        seed=42,
    )
    model = als.fit(train_df)
    predictions = model.transform(test_df)
    evaluator = RegressionEvaluator(
        metricName="rmse", labelCol="rating", predictionCol="prediction"
    )
    rmse = float(evaluator.evaluate(predictions)) if predictions.limit(1).count() > 0 else 0.0
    return model, rmse


def transform_unpack_recommendations(df_user_recs: DataFrame) -> DataFrame:
    """Flatten `recommendForAllUsers` struct array into a serving-ready Gold schema."""
    return df_user_recs.select(
        F.col("user_id"),
        F.explode("recommendations").alias("rec"),
    ).select(
        F.col("user_id"),
        F.col("rec.product_id").alias("recommended_product_id"),
        F.round(F.col("rec.rating"), 2).alias("predicted_affinity_score"),
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - ALS runs iterative alternating blocked matrix multiplications (`UserFactors`
#   <-> `ItemFactors`) with heavy shuffle per iteration; tune `numUserBlocks` /
#   `numItemBlocks` and checkpoint every few iterations (`setCheckpointInterval(5)`)
#   to truncate long RDD/DAG lineage graphs during training.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Explicit ratings (1-5 stars) use `implicitPrefs=False`; behavioral clicks,
#    views, and cart adds require `implicitPrefs=True` (confidence-weighted ALS).
# 2. For real-world string UUIDs (`user_uuid`, `sku`), hash deterministically to
#    32-bit `INT` or use `StringIndexer` and persist the mapping table alongside
#    the Gold recommendations.
# 3. Handle Cold-Start users/products in serving via fallback popular-by-category
#    or content-embedding similarity tables rather than serving empty carousels.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("103_mllib_als_recommender")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    ratings = [
        (0, 101, 5.0), (0, 102, 4.0), (0, 103, 1.0),
        (1, 101, 5.0), (1, 103, 1.0), (2, 102, 4.0),
        (2, 103, 2.0), (3, 101, 4.0), (3, 102, 5.0),
    ]
    df_clean = transform_prepare_als_inputs(
        spark.createDataFrame(ratings, "user_id INT, product_id INT, rating FLOAT")
    )
    als_model, val_rmse = train_and_evaluate_als(df_clean)
    print(f"Holdout RMSE: {val_rmse:.4f}")
    transform_unpack_recommendations(als_model.recommendForAllUsers(2)).show()
