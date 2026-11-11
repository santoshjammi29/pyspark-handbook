# Polyglot Multi-Engine Data Engineering Translation Matrix

This reference provides side-by-side syntax mappings across the major analytical engines for the most common data engineering patterns: **PySpark, Spark SQL, Snowflake, Google BigQuery, DuckDB, and Polars**.

---

## 1. Window Functions (Ranking & Cumulative Totals)

| Engine | Syntax |
| :--- | :--- |
| **PySpark** | `w = Window.partitionBy("region").orderBy(F.col("sales").desc())`<br>`df = df.withColumn("rk", F.rank().over(w))` |
| **Spark SQL** | `SELECT customer_id, amount, RANK() OVER (PARTITION BY region ORDER BY amount DESC) as rk FROM orders;` |
| **Snowflake** | `SELECT customer_id, amount, DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) as rk FROM orders;` |
| **BigQuery** | `SELECT customer_id, amount, SUM(amount) OVER (PARTITION BY region ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM orders;` |
| **DuckDB** | `SELECT customer_id, amount, SUM(amount) OVER (PARTITION BY region ORDER BY order_date) as run_tot FROM orders;` |
| **Polars** | `df.with_columns(pl.col("sales").sum().over("region").alias("region_total"))` |

---

## 2. MERGE INTO (Upsert Operations)

### PySpark (Delta Lake 3.2+)
```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "silver.customers")
target.alias("t").merge(
    source.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdate(
    condition="s.updated_at > t.updated_at",
    set={"name": "s.name", "email": "s.email", "updated_at": "s.updated_at"}
).whenNotMatchedInsert(
    values={"customer_id": "s.customer_id", "name": "s.name", "email": "s.email", "updated_at": "s.updated_at"}
).execute()
```

### Spark SQL
```sql
MERGE INTO silver.customers t 
USING updates s 
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### Snowflake
```sql
MERGE INTO silver.customers t 
USING updates s 
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (customer_id, name, updated_at) VALUES (s.customer_id, s.name, s.updated_at);
```

### Google BigQuery
```sql
MERGE `project.silver.customers` t 
USING updates s 
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT ROW;
```

### DuckDB
```sql
INSERT INTO customers 
SELECT * FROM updates 
ON CONFLICT (customer_id) DO UPDATE SET 
    name = EXCLUDED.name, 
    updated_at = EXCLUDED.updated_at;
```

---

## 3. CDC Deduplication & Latest Record Retrieval

### PySpark Structured Streaming
```python
stream = (
    spark.readStream.table("bronze.orders")
    .withWatermark("event_time", "15 minutes")
    .dropDuplicates(["order_id", "event_time"])
    .writeStream.format("delta")
    .option("checkpointLocation", "/Volumes/prod/meta/chk/orders")
    .table("silver.orders")
)
```

### Snowflake Dynamic Table
```sql
CREATE OR REPLACE DYNAMIC TABLE silver.orders
  TARGET_LAG = '1 minute'
  WAREHOUSE = transform_wh
AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY order_id ORDER BY event_time DESC
  ) as rn
  FROM bronze.orders_stream
) WHERE rn = 1;
```

### DuckDB
```sql
SELECT order_id, 
       arg_max(customer_id, event_time) as customer_id,
       arg_max(amount, event_time) as amount,
       max(event_time) as latest_event
FROM read_parquet('s3://lake/cdc/*.parquet')
GROUP BY order_id;
```

---

## 4. Slowly Changing Dimension Type 2 (SCD2) Pattern

### PySpark Delta Table Dual-Pass Merge
```python
# Stage updates: Union null merge_keys (inserts) with actual merge_keys (historical closures)
staged = staged_updates.selectExpr("NULL as merge_key", "*") \
    .union(staged_updates.selectExpr("customer_id as merge_key", "*"))

target.alias("t").merge(
    staged.alias("s"),
    "t.customer_id = s.merge_key AND t.is_current = true"
).whenMatchedUpdate(
    condition="t.address <> s.address",
    set={"is_current": "false", "end_date": "s.effective_date"}
).whenNotMatchedInsert(
    values={
        "customer_id": "s.customer_id",
        "address": "s.address",
        "is_current": "true",
        "effective_date": "s.effective_date",
        "end_date": "NULL"
    }
).execute()
```
