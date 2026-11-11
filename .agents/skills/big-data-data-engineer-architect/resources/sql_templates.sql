-- ============================================================
-- SQL Templates for Data Engineering
-- Reference: big-data-data-engineer-architect skill
-- Compatible with: T-SQL, Spark SQL, Synapse SQL, BigQuery
-- ============================================================


-- ============================================================
-- 1. WINDOW FUNCTIONS
-- ============================================================

-- Running total and moving average
SELECT
    order_date,
    product_id,
    revenue,
    SUM(revenue)   OVER (PARTITION BY product_id ORDER BY order_date
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    AVG(revenue)   OVER (PARTITION BY product_id ORDER BY order_date
                         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)         AS moving_avg_7d,
    LAG(revenue, 1, 0) OVER (PARTITION BY product_id ORDER BY order_date)  AS prev_day_revenue,
    LEAD(revenue, 1)   OVER (PARTITION BY product_id ORDER BY order_date)  AS next_day_revenue,
    RANK()             OVER (PARTITION BY order_date ORDER BY revenue DESC) AS daily_revenue_rank,
    DENSE_RANK()       OVER (ORDER BY revenue DESC)                         AS global_revenue_rank,
    NTILE(4)           OVER (ORDER BY revenue DESC)                         AS revenue_quartile,
    PERCENT_RANK()     OVER (ORDER BY revenue)                              AS revenue_percentile
FROM fact_orders;


-- Deduplication — keep latest record per key
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM staging_orders
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;


-- Session window analysis (identify sessions within 30-min gap)
WITH session_gaps AS (
    SELECT
        user_id,
        event_timestamp,
        LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS prev_event,
        CASE
            WHEN DATEDIFF(MINUTE,
                LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp),
                event_timestamp) > 30 OR
                 LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) IS NULL
            THEN 1 ELSE 0
        END AS is_new_session
    FROM user_events
),
sessions AS (
    SELECT
        user_id,
        event_timestamp,
        SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS session_id
    FROM session_gaps
)
SELECT
    user_id,
    session_id,
    MIN(event_timestamp) AS session_start,
    MAX(event_timestamp) AS session_end,
    COUNT(*) AS event_count,
    DATEDIFF(SECOND, MIN(event_timestamp), MAX(event_timestamp)) AS session_duration_sec
FROM sessions
GROUP BY user_id, session_id;


-- ============================================================
-- 2. ADVANCED AGGREGATIONS
-- ============================================================

-- GROUPING SETS — multiple aggregation levels in one query
SELECT
    COALESCE(CAST(year AS VARCHAR), 'ALL')    AS year,
    COALESCE(region, 'ALL')                   AS region,
    COALESCE(product_category, 'ALL')         AS product_category,
    SUM(revenue)                              AS total_revenue,
    COUNT(DISTINCT customer_id)               AS unique_customers,
    GROUPING(year)                            AS is_year_total,
    GROUPING(region)                          AS is_region_total
FROM fact_sales
GROUPING SETS (
    (year, region, product_category),  -- Detailed
    (year, region),                    -- Region subtotal
    (year),                            -- Year subtotal
    ()                                 -- Grand total
);

-- ROLLUP (hierarchical subtotals)
SELECT year, quarter, month, SUM(revenue) AS revenue
FROM fact_sales
GROUP BY ROLLUP(year, quarter, month);


-- ============================================================
-- 3. DELTA LAKE MERGE (Upsert Pattern)
-- ============================================================

-- Standard upsert (insert or update)
MERGE INTO silver.dim_customer AS target
USING (
    SELECT
        customer_id,
        customer_name,
        email,
        phone,
        country_code,
        updated_at
    FROM bronze.raw_customers
    WHERE _ingestion_date = CURRENT_DATE()
) AS source
ON target.customer_bk = source.customer_id
WHEN MATCHED AND (
    target.email     != source.email     OR
    target.phone     != source.phone     OR
    target.country_code != source.country_code
) THEN UPDATE SET
    target.customer_name  = source.customer_name,
    target.email          = source.email,
    target.phone          = source.phone,
    target.country_code   = source.country_code,
    target.updated_at     = source.updated_at,
    target._dw_updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    customer_bk, customer_name, email, phone, country_code,
    updated_at, _dw_created_at, _dw_updated_at
) VALUES (
    source.customer_id, source.customer_name, source.email,
    source.phone, source.country_code, source.updated_at,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);


-- ============================================================
-- 4. INCREMENTAL LOAD PATTERNS
-- ============================================================

-- High-watermark incremental extract (to be run in ADF/Spark JDBC)
DECLARE @LastWatermark DATETIME = (
    SELECT ISNULL(MAX(updated_at), '1900-01-01') FROM target.orders
);

SELECT *
FROM source.orders
WHERE updated_at > @LastWatermark
  AND updated_at <= GETDATE();


-- Date-partition incremental (Delta Lake / Spark SQL)
-- Load only today's partition
INSERT INTO silver.fact_orders
SELECT
    order_id,
    customer_id,
    product_id,
    CAST(order_timestamp AS DATE) AS order_date,
    order_amount                  AS revenue,
    quantity,
    CURRENT_TIMESTAMP()           AS _dw_loaded_at
FROM bronze.raw_orders
WHERE _ingestion_date = CURRENT_DATE()
  AND order_id NOT IN (SELECT order_id FROM silver.fact_orders WHERE order_date = CURRENT_DATE());


-- ============================================================
-- 5. DATA QUALITY CHECKS
-- ============================================================

-- Comprehensive DQ report for a table
WITH dq_checks AS (
    SELECT
        COUNT(*)                                           AS total_rows,
        COUNT(*) - COUNT(order_id)                         AS null_order_id,
        COUNT(*) - COUNT(customer_id)                      AS null_customer_id,
        COUNT(*) - COUNT(DISTINCT order_id)                AS duplicate_order_ids,
        SUM(CASE WHEN revenue < 0 THEN 1 ELSE 0 END)      AS negative_revenue,
        SUM(CASE WHEN revenue > 1000000 THEN 1 ELSE 0 END) AS outlier_revenue,
        SUM(CASE WHEN order_date > CURRENT_DATE() THEN 1 ELSE 0 END) AS future_dates,
        MIN(order_date)                                    AS min_date,
        MAX(order_date)                                    AS max_date,
        AVG(revenue)                                       AS avg_revenue,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue) AS median_revenue
    FROM silver.fact_orders
    WHERE order_date = CURRENT_DATE()
)
SELECT
    *,
    CASE
        WHEN null_order_id > 0 THEN 'FAIL: null order_id'
        WHEN duplicate_order_ids > 0 THEN 'FAIL: duplicate order_ids'
        WHEN negative_revenue > 0 THEN 'WARN: negative revenue'
        WHEN future_dates > 0 THEN 'FAIL: future dates'
        ELSE 'PASS'
    END AS dq_status
FROM dq_checks;


-- Referential integrity check
SELECT
    f.order_id,
    f.customer_id
FROM fact_orders f
LEFT JOIN dim_customer c ON f.customer_id = c.customer_bk
WHERE c.customer_bk IS NULL;  -- Orphaned fact records


-- ============================================================
-- 6. DIMENSION TABLE PATTERNS
-- ============================================================

-- Date dimension generation (Spark SQL / BigQuery)
WITH date_spine AS (
    SELECT EXPLODE(SEQUENCE(
        TO_DATE('2020-01-01'),
        TO_DATE('2030-12-31'),
        INTERVAL 1 DAY
    )) AS date_actual
)
SELECT
    CAST(DATE_FORMAT(date_actual, 'yyyyMMdd') AS INT)  AS date_sk,
    date_actual,
    YEAR(date_actual)                                  AS year,
    QUARTER(date_actual)                               AS quarter,
    MONTH(date_actual)                                 AS month,
    DATE_FORMAT(date_actual, 'MMMM')                   AS month_name,
    WEEKOFYEAR(date_actual)                            AS week_of_year,
    DAYOFWEEK(date_actual)                             AS day_of_week,
    DATE_FORMAT(date_actual, 'EEEE')                   AS day_name,
    CASE WHEN DAYOFWEEK(date_actual) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
    -- Fiscal year (example: fiscal year starts April 1)
    CASE WHEN MONTH(date_actual) >= 4
         THEN YEAR(date_actual)
         ELSE YEAR(date_actual) - 1
    END AS fiscal_year,
    CASE
        WHEN MONTH(date_actual) IN (4,5,6)   THEN 1
        WHEN MONTH(date_actual) IN (7,8,9)   THEN 2
        WHEN MONTH(date_actual) IN (10,11,12) THEN 3
        ELSE 4
    END AS fiscal_quarter
FROM date_spine;


-- ============================================================
-- 7. PERFORMANCE OPTIMISATION PATTERNS
-- ============================================================

-- Analyse query skew (identify skewed partitions)
SELECT
    spark_partition_id() AS partition_id,
    COUNT(*) AS partition_row_count
FROM large_table
GROUP BY spark_partition_id()
ORDER BY partition_row_count DESC;

-- Delta Lake table statistics
DESCRIBE DETAIL delta.`/mnt/gold/fact_sales`;
DESCRIBE HISTORY delta.`/mnt/gold/fact_sales`;

-- Time travel query (audit or recover data)
SELECT * FROM gold.fact_sales TIMESTAMP AS OF '2024-01-15 08:00:00'
WHERE order_date = '2024-01-14';

-- Restore table to previous version
RESTORE TABLE gold.fact_sales TO VERSION AS OF 5;

-- Delta Lake optimisation
OPTIMIZE gold.fact_orders ZORDER BY (order_date_sk, customer_sk);
ANALYZE TABLE gold.fact_orders COMPUTE STATISTICS FOR ALL COLUMNS;

-- Synapse dedicated pool: distribution optimisation
CREATE TABLE fact_sales
WITH (
    DISTRIBUTION = HASH(customer_id),  -- Distribute by most common join key
    CLUSTERED COLUMNSTORE INDEX,
    PARTITION (order_date_sk RANGE RIGHT FOR VALUES (
        20230101, 20230201, 20230301, 20230401,
        20230501, 20230601, 20230701, 20230801,
        20230901, 20231001, 20231101, 20231201
    ))
);
