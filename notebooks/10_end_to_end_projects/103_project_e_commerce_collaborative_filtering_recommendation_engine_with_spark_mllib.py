# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 103: Project E-Commerce: Collaborative Filtering Recommendation Engine with Spark MLlib
# MAGIC
# MAGIC **Module:** Module 10: End-to-End Enterprise Projects & Production Workflows  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC An online retail store has 1 million products and 5 million shoppers. Product managers want personalized 'Recommended For You' item carousels on the homepage to increase average order value and user engagement.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Build an enterprise Recommendation Engine using Spark MLlib: prepare implicit user-item interaction matrices (user_id, item_id, rating), train an Alternating Least Squares (ALS) matrix factorization model, generate top-5 personalized recommendations per user, and evaluate model performance.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Spark MLlib ALS (Alternating Least Squares) factorizes the sparse user-item interaction matrix $R$ into low-rank user matrix $U$ and item matrix $V$ ($R pprox U 	imes V^T$). It runs in distributed memory, scaling to hundreds of millions of user-item ratings across cluster nodes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Environment Setup & SparkSession Initialization
# MAGIC We configure a resilient `SparkSession` compatible with Databricks runtime, local PySpark development, or CI/CD test runners.

# COMMAND ----------

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Initialize SparkSession with Delta Lake support and optimized local configs
spark = (
    SparkSession.builder
    .appName("UseCase_103_Project_E-Commerce_Collaborative_Filtering_Recommendation_Engine_with_Spark_MLlib")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.sparkContext.appName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Preparing User-Product Rating Interaction Dataset
# MAGIC Create user rating interactions.

# COMMAND ----------

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

ratings_data = [
    (0, 101, 5.0), # User 0 loves Product 101
    (0, 102, 4.0),
    (0, 103, 1.0),
    (1, 101, 5.0),
    (1, 103, 1.0),
    (2, 102, 4.0),
    (2, 103, 2.0),
    (3, 101, 4.0),
    (3, 102, 5.0)
]

schema = "user_id INT, product_id INT, rating FLOAT"
df_ratings = spark.createDataFrame(ratings_data, schema)
df_ratings.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Training Alternating Least Squares (ALS) Model
# MAGIC Fit ALS collaborative filtering model with rank and regularization parameters.

# COMMAND ----------

# Configure ALS model
als = ALS(
    maxIter=5,
    regParam=0.01,
    userCol="user_id",
    itemCol="product_id",
    ratingCol="rating",
    coldStartStrategy="drop" # Prevents NaN on unseen users/items during evaluation
)

model = als.fit(df_ratings)
print("ALS Collaborative Filtering Model successfully trained on distributed Spark cluster.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Generating Top-2 Personalized Recommendations per User
# MAGIC Generate recommendations for all users and unpack recommendations array.

# COMMAND ----------

# Generate top 2 product recommendations for all users
user_recs = model.recommendForAllUsers(2)

# Flatten recommendation struct array
df_unpacked_recs = user_recs.select(
    "user_id",
    F.explode("recommendations").alias("rec")
).select(
    "user_id",
    F.col("rec.product_id").alias("recommended_product_id"),
    F.round(F.col("rec.rating"), 2).alias("predicted_affinity_score")
)

print("Personalized Product Recommendations per User:")
df_unpacked_recs.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Spark MLlib ALS performs distributed matrix factorization for collaborative filtering recommendations.**
# MAGIC - **Always set `coldStartStrategy='drop'` to prevent evaluation failures when new users/items appear.**
# MAGIC - **Persist generated recommendations to Gold Delta tables or low-latency key-value stores (Redis, DynamoDB) for web serving.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How does the Alternating Least Squares (ALS) algorithm work mathematically in distributed Spark?
# MAGIC 2. What is the difference between explicit feedback ratings and implicit feedback signals (clicks, views)?
# MAGIC 3. How do you handle the 'Cold Start' problem for new users or brand new products in recommendation systems?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
