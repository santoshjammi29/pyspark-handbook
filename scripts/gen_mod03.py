"""
Module 03: Advanced SQL, Aggregations & Analytical Window Functions (Notebooks 023 - 032)
"""

import re
from pathlib import Path
from notebook_builder import build_notebook, write_notebook

MODULE_FOLDER = "03_sql_and_window_functions"
MODULE_NAME = "Module 03: Advanced SQL, Aggregations & Analytical Window Functions"

def sanitize_filename(nb_id: str, title: str) -> str:
    cleaned = title.lower().replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return f"{nb_id}_{cleaned}.py"

def generate(base_dir: Path):
    out_dir = base_dir / "notebooks" / MODULE_FOLDER
    out_dir.mkdir(parents=True, exist_ok=True)

    notebooks = [
        {
            "nb_id": "023",
            "title": "Basic and Multi-Column Aggregations with Alias Management",
            "difficulty": "Beginner",
            "business_scenario": "A retail analytics engine calculates daily regional performance metrics across store departments: total gross revenue, average transaction value, maximum discount applied, and distinct active shoppers.",
            "problem_statement": "Perform multi-column aggregations using `groupBy()` paired with `agg()`. Demonstrate explicit column aliasing, combining multiple statistical functions (`sum`, `avg`, `min`, `max`, `countDistinct`, `approx_count_distinct`), and using dictionary-based aggregation syntax.",
            "architectural_notes": "Spark executes aggregations via HashAggregate (when data types are mutable) or SortAggregate. In two-phase aggregation, Spark first performs partial local aggregation on each partition before shuffling partial sums across the network, reducing network traffic.",
            "sections": [
                {
                    "title": "Generating Store Sales Transactions",
                    "explanation": "Create a dataset of store transactions with departments and regions.",
                    "code": """sales = [
    ("TXN-1", "West", "Electronics", 450.00, 0.10, "C-01"),
    ("TXN-2", "West", "Electronics", 1200.00, 0.15, "C-02"),
    ("TXN-3", "West", "Groceries", 65.50, 0.00, "C-01"),
    ("TXN-4", "East", "Electronics", 800.00, 0.05, "C-03"),
    ("TXN-5", "East", "Fashion", 150.00, 0.20, "C-04"),
    ("TXN-6", "East", "Fashion", 95.00, 0.10, "C-04"),
    ("TXN-7", "North", "Groceries", 45.00, 0.00, "C-05"),
    ("TXN-8", "North", "Groceries", 120.00, 0.05, "C-06")
]

schema = "txn_id STRING, region STRING, department STRING, amount DOUBLE, discount DOUBLE, customer_id STRING"
df_sales = spark.createDataFrame(sales, schema)
df_sales.show()"""
                },
                {
                    "title": "Multi-Column Aggregations with Explicit Aliasing",
                    "explanation": "Calculate comprehensive statistical metrics per region and department.",
                    "code": """df_regional_metrics = df_sales.groupBy("region", "department").agg(
    F.count("txn_id").alias("total_transactions"),
    F.round(F.sum("amount"), 2).alias("gross_revenue"),
    F.round(F.avg("amount"), 2).alias("avg_basket_size"),
    F.max("amount").alias("max_single_sale"),
    F.round(F.max("discount") * 100, 1).alias("max_discount_pct"),
    F.countDistinct("customer_id").alias("unique_shoppers")
).orderBy("region", F.desc("gross_revenue"))

df_regional_metrics.show()"""
                },
                {
                    "title": "Performance Optimization: approx_count_distinct for HyperLogLog Scaling",
                    "explanation": "Compare exact distinct count with approximate distinct count for massive datasets.",
                    "code": """# On multi-billion row tables, countDistinct forces massive shuffles.
# approx_count_distinct uses HyperLogLog++ to compute cardinality within 5% error using minimal memory.
df_approx = df_sales.groupBy("region").agg(
    F.countDistinct("customer_id").alias("exact_distinct_shoppers"),
    F.approx_count_distinct("customer_id", rsd=0.05).alias("approx_distinct_shoppers")
)

df_approx.show()"""
                }
            ],
            "key_takeaways": [
                "Always alias aggregated expressions immediately using `.alias()` to prevent messy default column names.",
                "Spark automatically leverages two-phase aggregation: partial aggregate on executors followed by final aggregate after shuffle.",
                "Use `approx_count_distinct` for high-cardinality distinct counts on big data to eliminate severe shuffle spills."
            ],
            "interview_questions": [
                "How does Spark's two-phase HashAggregate optimize network transfer during `groupBy()`?",
                "What algorithm powers `approx_count_distinct` and when is it acceptable in business reporting?",
                "What is the difference between HashAggregate and SortAggregate in Spark Catalyst execution plans?"
            ]
        },
        {
            "nb_id": "024",
            "title": "Advanced Multidimensional OLAP: Rollup, Cube and Grouping Sets",
            "difficulty": "Intermediate",
            "business_scenario": "A financial controller needs a single unified sales reporting view that includes: (1) Detailed breakdown by Year, Quarter, and Product, (2) Subtotals by Year and Quarter, (3) Subtotals by Year alone, and (4) The grand total across the entire enterprise.",
            "problem_statement": "Implement multidimensional OLAP aggregations without writing multiple queries and unions. Compare `rollup()` (hierarchical subtotals), `cube()` (all possible combinations of dimensions), and explicit `GROUPING SETS`. Use `grouping_id()` to identify aggregate levels.",
            "architectural_notes": "`rollup()` generates $N + 1$ grouping levels for $N$ dimensions in hierarchical order. `cube()` computes $2^N$ grouping combinations. Spark optimizes these into a single Pass using the Expand operator, avoiding multiple scans of the source table.",
            "sections": [
                {
                    "title": "Generating Financial Revenue Records",
                    "explanation": "Create multi-year revenue transactions.",
                    "code": """revenue_data = [
    (2022, "Q1", "Software", 50000.0),
    (2022, "Q1", "Hardware", 80000.0),
    (2022, "Q2", "Software", 60000.0),
    (2022, "Q2", "Hardware", 75000.0),
    (2023, "Q1", "Software", 70000.0),
    (2023, "Q1", "Hardware", 90000.0),
    (2023, "Q2", "Software", 85000.0),
    (2023, "Q2", "Hardware", 95000.0)
]

schema = "fiscal_year INT, fiscal_quarter STRING, product_line STRING, revenue DOUBLE"
df_rev = spark.createDataFrame(revenue_data, schema)
df_rev.show()"""
                },
                {
                    "title": "Hierarchical Rollup for Subtotals and Grand Total",
                    "explanation": "Compute hierarchical aggregates from finest grain to grand total.",
                    "code": """df_rollup = df_rev.rollup("fiscal_year", "fiscal_quarter", "product_line").agg(
    F.sum("revenue").alias("total_revenue"),
    F.grouping_id().alias("grouping_level")
).orderBy(F.col("fiscal_year").asc_nulls_last(), F.col("fiscal_quarter").asc_nulls_last(), F.col("product_line").asc_nulls_last())

print("Hierarchical Rollup (Notice Subtotals and Grand Total when dimensions are null):")
df_rollup.show(20, truncate=False)"""
                },
                {
                    "title": "Full Multidimensional Cube Aggregation",
                    "explanation": "Generate all 2^3 = 8 combinations of grouping dimensions.",
                    "code": """df_cube = df_rev.cube("fiscal_year", "product_line").agg(
    F.sum("revenue").alias("total_revenue")
).orderBy("fiscal_year", "product_line")

print("Cube: All Combinations of Fiscal Year and Product Line:")
df_cube.show()"""
                }
            ],
            "key_takeaways": [
                "`rollup()` computes hierarchical subtotals along a defined drill-down path (Year -> Quarter -> Product).",
                "`cube()` calculates all possible cross-dimensional combinations ($2^N$).",
                "The `grouping_id()` bitmask column identifies which dimensions are aggregated in each result row."
            ],
            "interview_questions": [
                "How does Catalyst Optimizer's `Expand` operator implement `cube()` and `rollup()` under the hood?",
                "What is the mathematical difference in row count between `rollup(A, B, C)` and `cube(A, B, C)`?",
                "How do you distinguish between a real NULL value in the data versus a NULL produced by a rollup subtotal?"
            ]
        },
        {
            "nb_id": "025",
            "title": "Pivoting and Unpivoting Data: pivot and stack Expression Mastery",
            "difficulty": "Intermediate",
            "business_scenario": "A business intelligence dashboard requires monthly revenue pivoted into wide columns (Jan, Feb, Mar...) for tabular display. Conversely, a data science model requires wide survey responses unpivoted (melted) back into long tidy format.",
            "problem_statement": "Master tabular reshaping: (1) Pivot narrow transactional rows into wide summary columns using `groupBy().pivot()`, optimizing performance by passing explicit pivot values. (2) Unpivot wide multi-column tables back into narrow key-value rows using Spark's `stack()` expression.",
            "architectural_notes": "Calling `.pivot('col')` without specifying distinct values forces an extra, expensive distributed pass over the data to discover all unique column values. Always pass the pre-defined list of distinct values: `.pivot('col', ['Val1', 'Val2'])` to eliminate this extra scan.",
            "sections": [
                {
                    "title": "Generating Monthly Department Sales (Narrow Format)",
                    "explanation": "Create transactional sales records across months.",
                    "code": """monthly_sales = [
    ("Electronics", "Jan", 12000.0),
    ("Electronics", "Feb", 14500.0),
    ("Electronics", "Mar", 16000.0),
    ("Groceries", "Jan", 8500.0),
    ("Groceries", "Feb", 9200.0),
    ("Groceries", "Mar", 8900.0),
    ("Apparel", "Jan", 4500.0),
    ("Apparel", "Feb", 5100.0),
    ("Apparel", "Mar", 6300.0)
]

df_narrow = spark.createDataFrame(monthly_sales, ["dept", "month", "sales"])
df_narrow.show()"""
                },
                {
                    "title": "Optimized Pivoting with Explicit Pivot Values",
                    "explanation": "Reshape months into columns, passing explicit month list to skip extra scan.",
                    "code": """months_list = ["Jan", "Feb", "Mar"]

# Highly optimized pivot with explicit values passed
df_pivoted = df_narrow.groupBy("dept").pivot("month", months_list).agg(
    F.coalesce(F.sum("sales"), F.lit(0.0))
)

print("Pivoted Wide Table:")
df_pivoted.show()"""
                },
                {
                    "title": "Unpivoting (Melting) Wide Table back to Narrow with stack()",
                    "explanation": "Use the SQL stack expression to transform wide columns back into rows.",
                    "code": """# stack(n, 'col1_name', col1, 'col2_name', col2, ...)
unpivot_expr = "stack(3, 'Jan', Jan, 'Feb', Feb, 'Mar', Mar) as (month, sales)"

df_unpivoted = df_pivoted.select("dept", F.expr(unpivot_expr))
print("Unpivoted Back to Tidy Narrow Format:")
df_unpivoted.show()"""
                }
            ],
            "key_takeaways": [
                "Always provide an explicit list of pivot values to `pivot(col, values)` to eliminate an expensive full-table discovery scan.",
                "Unpivoting in PySpark is achieved natively using `F.expr('stack(N, ...)')` without requiring external libraries.",
                "Pivoting creates wide schemas; excessive pivot columns can impact Catalyst compilation and memory."
            ],
            "interview_questions": [
                "What happens internally when you call `df.groupBy().pivot()` without providing the list of distinct values?",
                "How does the `stack()` expression work in Spark SQL to perform table melting?",
                "What are the alternatives to `pivot()` if you have 1,000+ distinct dynamic categories?"
            ]
        },
        {
            "nb_id": "026",
            "title": "Ranking Window Functions: row_number, rank, dense_rank and percent_rank",
            "difficulty": "Intermediate",
            "business_scenario": "A national sales management board ranks sales representatives within each geographic territory based on quarterly quota attainment. The ranking system must handle tie scores appropriately and calculate relative percentile percentiles for bonus distribution.",
            "problem_statement": "Compare and implement all four ranking window functions: `row_number()`, `rank()`, `dense_rank()`, and `percent_rank()`. Explain how tied values affect sequential numbering and subsequent rank positions. Filter for Top-N performers per partition.",
            "architectural_notes": "Window ranking functions require a WindowSpec with `partitionBy()` and `orderBy()`. Rows are partitioned across executors by partition key and sorted in-memory. If no `partitionBy()` is provided, all data is moved to a single executor partition, creating a catastrophic bottleneck.",
            "sections": [
                {
                    "title": "Generating Sales Rep Quota Scores with Ties",
                    "explanation": "Create sales rep performance dataset with identical quota scores.",
                    "code": """reps_data = [
    ("East", "Alice", 125.0),
    ("East", "Bob", 110.0),
    ("East", "Charlie", 110.0), # Tied with Bob
    ("East", "David", 95.0),
    ("West", "Eve", 140.0),
    ("West", "Frank", 130.0),
    ("West", "Grace", 130.0), # Tied with Frank
    ("West", "Heidi", 100.0)
]

schema = "region STRING, rep_name STRING, quota_pct DOUBLE"
df_reps = spark.createDataFrame(reps_data, schema)
df_reps.show()"""
                },
                {
                    "title": "Executing All Four Ranking Functions Simultaneously",
                    "explanation": "Observe behavior on tied quota scores.",
                    "code": """window_spec = Window.partitionBy("region").orderBy(F.col("quota_pct").desc())

df_ranked = df_reps.withColumn(
    "row_num", F.row_number().over(window_spec)
).withColumn(
    "rank", F.rank().over(window_spec)
).withColumn(
    "dense_rank", F.dense_rank().over(window_spec)
).withColumn(
    "percent_rank", F.round(F.percent_rank().over(window_spec), 2)
)

print("Comprehensive Ranking Comparison:")
df_ranked.show()"""
                },
                {
                    "title": "Top-2 Performers per Region Filtering",
                    "explanation": "Filter for top performers using dense_rank to include all tied runners-up.",
                    "code": """# Using dense_rank ensures ties at rank 2 are both included
df_top2 = df_ranked.filter(F.col("dense_rank") <= 2).select("region", "rep_name", "quota_pct", "dense_rank")
print("Top 2 Performers per Region (Preserving Ties):")
df_top2.show()"""
                }
            ],
            "key_takeaways": [
                "`row_number()` assigns strictly unique sequential integers regardless of ties.",
                "`rank()` produces duplicate ranks on ties and skips subsequent numbers (e.g. 1, 2, 2, 4).",
                "`dense_rank()` produces duplicate ranks on ties without skipping (e.g. 1, 2, 2, 3).",
                "Never call `.over(Window.orderBy())` without `partitionBy()` on large tables, as it moves the entire dataset into one partition."
            ],
            "interview_questions": [
                "What is the difference between `rank()` and `dense_rank()` when multiple rows have identical values?",
                "What happens to cluster resources if you define a Window without a `partitionBy` clause?",
                "How does `percent_rank()` calculate its value mathematically?"
            ]
        },
        {
            "nb_id": "027",
            "title": "Aggregate & Cumulative Window Functions: Running Totals and Moving Averages",
            "difficulty": "Intermediate",
            "business_scenario": "A fintech wealth management app displays account balance trajectories and 7-day smoothed moving averages of daily expenditures to help customers track spending habits.",
            "problem_statement": "Implement cumulative sums (running totals), moving averages, and frame boundary specifications: compare physical row-based frames (`rowsBetween`) vs logical value-based frames (`rangeBetween`). Calculate moving averages over preceding windows.",
            "architectural_notes": "The default window frame for an ordered window is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. This can cause subtle bugs when duplicate order keys exist, as Spark aggregates all rows with the identical key together. Using `ROWS BETWEEN` guarantees explicit physical row framing.",
            "sections": [
                {
                    "title": "Generating Daily Spending Feed",
                    "explanation": "Create daily customer transactions.",
                    "code": """spending = [
    ("ACC-1", "2023-11-01", 120.0),
    ("ACC-1", "2023-11-02", 45.0),
    ("ACC-1", "2023-11-03", 210.0),
    ("ACC-1", "2023-11-04", 80.0),
    ("ACC-1", "2023-11-05", 15.0),
    ("ACC-1", "2023-11-06", 350.0),
    ("ACC-1", "2023-11-07", 90.0)
]

schema = "account_id STRING, txn_date STRING, amount DOUBLE"
df_spend = spark.createDataFrame(spending, schema)
df_spend.show()"""
                },
                {
                    "title": "Cumulative Running Total & 3-Day Moving Average",
                    "explanation": "Apply rowsBetween frames for physical sliding windows.",
                    "code": """# Cumulative window from start to current row
cum_window = (
    Window.partitionBy("account_id")
    .orderBy("txn_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

# 3-day moving window: 2 preceding rows plus current row
moving_window = (
    Window.partitionBy("account_id")
    .orderBy("txn_date")
    .rowsBetween(-2, Window.currentRow)
)

df_analytics = df_spend.withColumn(
    "cumulative_spend", F.round(F.sum("amount").over(cum_window), 2)
).withColumn(
    "moving_avg_3day", F.round(F.avg("amount").over(moving_window), 2)
)

print("Cumulative Spending & Moving Average Analysis:")
df_analytics.show()"""
                },
                {
                    "title": "Comparing ROWS BETWEEN vs RANGE BETWEEN with Duplicate Keys",
                    "explanation": "Illustrate how duplicate keys affect RANGE frames vs ROWS frames.",
                    "code": """dup_data = [
    ("A", 1, 10.0),
    ("A", 2, 20.0),
    ("A", 2, 30.0),  # Duplicate order key
    ("A", 3, 40.0)
]
df_dup = spark.createDataFrame(dup_data, ["id", "val", "amt"])

# ROWS vs RANGE
w_rows = Window.partitionBy("id").orderBy("val").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_range = Window.partitionBy("id").orderBy("val").rangeBetween(Window.unboundedPreceding, Window.currentRow)

df_comp = df_dup.withColumn("sum_rows", F.sum("amt").over(w_rows))\
                .withColumn("sum_range", F.sum("amt").over(w_range))

print("Notice sum_range evaluates tied val=2 together into 60.0, while sum_rows evaluates row-by-row:")
df_comp.show()"""
                }
            ],
            "key_takeaways": [
                "`ROWS BETWEEN` defines physical row count boundaries (e.g. 2 preceding rows).",
                "`RANGE BETWEEN` defines logical value offsets based on the order column.",
                "Default window frame with `orderBy` is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, which aggregates identical order keys together."
            ],
            "interview_questions": [
                "What is the difference between `rowsBetween` and `rangeBetween` in Spark Window specifications?",
                "What is the default frame specification when an `orderBy` clause is present in a Window?",
                "How do you compute a rolling 30-day time-based window on irregular transaction dates?"
            ]
        },
        {
            "nb_id": "028",
            "title": "Offset Window Functions: lead, lag and Period-over-Period Churn",
            "difficulty": "Intermediate",
            "business_scenario": "A SaaS subscription billing service analyzes customer MRR (Monthly Recurring Revenue). Finance requires month-over-month revenue variance, detection of plan upgrades/downgrades, and churn identification when an active account drops subscription renewal.",
            "problem_statement": "Master offset window functions: implement `lag()` to retrieve previous month revenue and `lead()` to inspect subsequent renewal state. Calculate absolute and percentage period-over-period differences, and handle default fallback values on boundary rows.",
            "architectural_notes": "Offset functions fetch values from neighboring rows within the same executor partition buffer without self-joining the table. Self-joins require a full Cartesian shuffle, whereas `lag()` operates in single-pass linear time $O(N)$ inside the partition.",
            "sections": [
                {
                    "title": "Generating Monthly SaaS Subscription History",
                    "explanation": "Create monthly subscription charges per customer account.",
                    "code": """subscription_history = [
    ("SUB-1", "2023-01", 100.0),
    ("SUB-1", "2023-02", 100.0),
    ("SUB-1", "2023-03", 150.0), # Upgrade
    ("SUB-1", "2023-04", 120.0), # Downgrade
    ("SUB-2", "2023-01", 50.0),
    ("SUB-2", "2023-02", 50.0),
    ("SUB-2", "2023-03", 0.0)    # Churn
]

schema = "sub_id STRING, billing_month STRING, mrr DOUBLE"
df_subs = spark.createDataFrame(subscription_history, schema)
df_subs.show()"""
                },
                {
                    "title": "Applying lag() and lead() with Default Values",
                    "explanation": "Fetch previous month and next month MRR.",
                    "code": """window_sub = Window.partitionBy("sub_id").orderBy("billing_month")

df_lag_lead = df_subs.withColumn(
    "prev_mrr", F.lag("mrr", 1, default=0.0).over(window_sub)
).withColumn(
    "next_mrr", F.lead("mrr", 1, default=0.0).over(window_sub)
)

df_lag_lead.show()"""
                },
                {
                    "title": "Calculating Month-over-Month Growth & Change Classification",
                    "explanation": "Compute variance and classify subscription status.",
                    "code": """df_variance = df_lag_lead.withColumn(
    "mrr_change", F.round(F.col("mrr") - F.col("prev_mrr"), 2)
).withColumn(
    "pct_change",
    F.when(F.col("prev_mrr") == 0, None)
     .otherwise(F.round(((F.col("mrr") - F.col("prev_mrr")) / F.col("prev_mrr")) * 100, 1))
).withColumn(
    "status_tag",
    F.when(F.col("prev_mrr") == 0, "NEW")
     .when(F.col("mrr") > F.col("prev_mrr"), "EXPANSION")
     .when((F.col("mrr") < F.col("prev_mrr")) & (F.col("mrr") > 0), "CONTRACTION")
     .when(F.col("mrr") == 0, "CHURNED")
     .otherwise("RETAINED")
)

df_variance.select("sub_id", "billing_month", "prev_mrr", "mrr", "mrr_change", "pct_change", "status_tag").show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "`lag(col, offset, default)` accesses preceding rows without requiring self-joins.",
                "`lead(col, offset, default)` peeks forward into subsequent rows in the partition.",
                "Always provide sensible `default` values to avoid propagating accidental nulls on boundary rows."
            ],
            "interview_questions": [
                "Why is `lag()` significantly more efficient than joining a DataFrame to itself on `t.date = t_prev.date + 1`?",
                "How do you calculate year-over-year growth for missing months using window functions?",
                "What is the maximum offset value you can safely supply to `lag()` in PySpark?"
            ]
        },
        {
            "nb_id": "029",
            "title": "Sessionization and the Gaps & Islands Problem in PySpark",
            "difficulty": "Advanced",
            "business_scenario": "A digital analytics platform tracks web page views. An active browsing session is defined as consecutive clicks separated by no more than 30 minutes of inactivity. When a gap exceeds 30 minutes, a new unique session identifier must be generated.",
            "problem_statement": "Solve the classic Gaps and Islands problem in distributed PySpark: detect temporal gaps using `lag()`, mark island session boundaries with conditional flags, compute cumulative sums over flags to generate sequential session IDs, and aggregate session metrics.",
            "architectural_notes": "Solving Gaps & Islands in Spark uses a 2-step window pattern: (1) An ordered window with `lag()` to compute time delta between consecutive events and emit a 1/0 boundary indicator, and (2) A cumulative sum window over the boundary indicator to generate monotonic session IDs across the entire partition.",
            "sections": [
                {
                    "title": "Generating Clickstream Events with Inactivity Gaps",
                    "explanation": "Create web clickstream data with varying time intervals.",
                    "code": """clicks = [
    ("U-01", "2023-11-20 10:00:00", "/home"),
    ("U-01", "2023-11-20 10:12:00", "/catalog"),     # 12 min gap (same session)
    ("U-01", "2023-11-20 10:25:00", "/product/42"),  # 13 min gap (same session)
    ("U-01", "2023-11-20 11:30:00", "/home"),        # 65 min gap -> NEW SESSION!
    ("U-01", "2023-11-20 11:40:00", "/cart"),        # 10 min gap (same session)
    ("U-02", "2023-11-20 09:00:00", "/login"),
    ("U-02", "2023-11-20 09:15:00", "/dashboard")
]

schema = "user_id STRING, event_time STRING, page STRING"
df_raw_clicks = spark.createDataFrame(clicks, schema).withColumn("ts", F.to_timestamp("event_time"))
df_raw_clicks.show(truncate=False)"""
                },
                {
                    "title": "Step 1: Calculate Elapsed Inactivity & Identify New Session Boundaries",
                    "explanation": "Calculate difference in minutes from previous click.",
                    "code": """user_window = Window.partitionBy("user_id").orderBy("ts")

df_gaps = df_raw_clicks.withColumn(
    "prev_ts", F.lag("ts").over(user_window)
).withColumn(
    "inactivity_minutes",
    F.coalesce((F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 60.0, F.lit(0.0))
).withColumn(
    "is_new_session",
    F.when((F.col("inactivity_minutes") > 30.0) | (F.col("prev_ts").isNull()), 1).otherwise(0)
)

df_gaps.select("user_id", "event_time", "inactivity_minutes", "is_new_session").show()"""
                },
                {
                    "title": "Step 2: Cumulative Sum to Form Session IDs & Aggregate",
                    "explanation": "Cumulative sum of is_new_session produces a persistent session number.",
                    "code": """cum_session_window = Window.partitionBy("user_id").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_sessionized = df_gaps.withColumn(
    "session_num", F.sum("is_new_session").over(cum_session_window)
).withColumn(
    "session_id", F.concat(F.col("user_id"), F.lit("_SESS_"), F.col("session_num"))
)

print("Sessionized Clickstream:")
df_sessionized.select("session_id", "user_id", "event_time", "page", "inactivity_minutes").show(truncate=False)

# Aggregate session summaries
df_summary = df_sessionized.groupBy("session_id", "user_id").agg(
    F.min("ts").alias("session_start"),
    F.max("ts").alias("session_end"),
    F.count("page").alias("page_views"),
    F.round((F.max("ts").cast("long") - F.min("ts").cast("long")) / 60.0, 1).alias("session_duration_min")
)

print("Aggregated Session Profiles:")
df_summary.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "The Gaps and Islands problem is solved using a two-window approach: flag boundaries with `lag`, then group using cumulative `sum`.",
                "Sessionization enables accurate attribution modeling, user journey mapping, and conversion funnel analysis.",
                "Ensure timestamp columns are parsed to epoch seconds or timestamps to avoid incorrect string comparison ordering."
            ],
            "interview_questions": [
                "Explain the two-phase window pattern used to solve Gaps and Islands in PySpark.",
                "What is the difference between physical sessionization in batch vs stateful session windows in Structured Streaming?",
                "How do you handle clock drift across client devices when calculating session boundary gaps?"
            ]
        },
        {
            "nb_id": "030",
            "title": "Relational Joins Masterclass: Inner, Outer, Semi and Anti Joins",
            "difficulty": "Intermediate",
            "business_scenario": "An enterprise CRM data reconciler syncs accounts between Salesforce and a billing database. The reconciliation team needs to find: (1) Fully matched customer profiles, (2) Customers in CRM with zero billing records, and (3) Active billing records missing from CRM.",
            "problem_statement": "Implement and compare all relational join types in PySpark: `inner`, `left`, `right`, `full_outer`, `left_semi`, and `left_anti`. Explain why `left_semi` and `left_anti` are preferred over left joins with null checks.",
            "architectural_notes": "`left_semi` join filters rows from the left table that have at least one match in the right table without duplicating left rows or including right columns. `left_anti` returns rows from the left table that have NO match in the right table. Both are far more memory-efficient than standard joins.",
            "sections": [
                {
                    "title": "Generating CRM and Billing Datasets",
                    "explanation": "Create overlapping CRM and billing customer tables.",
                    "code": """crm_data = [
    ("C-01", "Acme Corp", "Enterprise"),
    ("C-02", "Beta Ltd", "Mid-Market"),
    ("C-03", "Cyber Dyne", "SMB"),
    ("C-04", "Delta Air", "Enterprise")
]

billing_data = [
    ("C-01", 15000.0, "ACTIVE"),
    ("C-02", 4500.0, "ACTIVE"),
    ("C-05", 2200.0, "OVERDUE") # In billing, but missing in CRM
]

df_crm = spark.createDataFrame(crm_data, ["cust_id", "company_name", "tier"])
df_bill = spark.createDataFrame(billing_data, ["cust_id", "mrr", "billing_status"])

print("CRM Dataset:")
df_crm.show()
print("Billing Dataset:")
df_bill.show()"""
                },
                {
                    "title": "Inner, Full Outer & Duplicate Key Avoidance",
                    "explanation": "Execute standard relational joins and resolve duplicate join column names.",
                    "code": """# Inner join: only customers present in both
df_inner = df_crm.join(df_bill, on="cust_id", how="inner")
print("Inner Join (Matched Customers):")
df_inner.show()

# Full Outer join: all records from both systems
df_outer = df_crm.join(df_bill, on="cust_id", how="full_outer")
print("Full Outer Join (Complete System Footprint):")
df_outer.show()"""
                },
                {
                    "title": "High-Efficiency Left Semi and Left Anti Joins",
                    "explanation": "Filter without duplicating left-side rows or loading right-side columns.",
                    "code": """# Left Semi: Keep CRM customers that have billing records (retains CRM columns only)
df_semi = df_crm.join(df_bill, on="cust_id", how="left_semi")
print("Left Semi Join (CRM Accounts with Active Billing):")
df_semi.show()

# Left Anti: Keep CRM customers that have NO billing records (unbilled prospects)
df_anti = df_crm.join(df_bill, on="cust_id", how="left_anti")
print("Left Anti Join (CRM Accounts Missing from Billing):")
df_anti.show()"""
                }
            ],
            "key_takeaways": [
                "Use `on='col_name'` instead of `df1['col'] == df2['col']` to avoid producing duplicate join column names.",
                "`left_semi` acts as an `EXISTS` filter: it does not duplicate left rows if multiple right rows match.",
                "`left_anti` acts as a `NOT EXISTS` filter: highly optimized for finding orphaned or missing records."
            ],
            "interview_questions": [
                "What is the difference between `left_semi` join and an `inner` join followed by `.select(df1['*']).distinct()`?",
                "Why does `left_anti` outperform `left` join followed by `.filter(df2['key'].isNull())`?",
                "How does Catalyst Optimizer choose between SortMergeJoin and ShuffleHashJoin?"
            ]
        },
        {
            "nb_id": "031",
            "title": "Broadcast Hash Joins, Threshold Tuning and Memory Limits",
            "difficulty": "Intermediate",
            "business_scenario": "A point-of-sale stream joins billions of transaction rows with a small 2MB store metadata lookup table. Default join configuration causes an unnecessary multi-gigabyte shuffle across all executors, degrading cluster performance.",
            "problem_statement": "Eliminate shuffle overhead using Broadcast Hash Joins: apply explicit `broadcast(df)` hint, inspect the Catalyst physical plan to verify `BroadcastHashJoinExec`, understand `spark.sql.autoBroadcastJoinThreshold`, and identify the risks of driver OutOfMemory (OOM) when broadcasting large tables.",
            "architectural_notes": "In a Broadcast Hash Join, the driver downloads the small DataFrame, builds an in-memory hash table, and broadcasts it to all worker executors via Spark's peer-to-peer torrent protocol. Workers join streaming partition data locally with zero network shuffle. The default auto-broadcast threshold is 10MB (10485760 bytes).",
            "sections": [
                {
                    "title": "Generating Fact and Small Dimension Tables",
                    "explanation": "Create large transaction table and small store dimension.",
                    "code": """# Simulate 20,000 transactions (Fact Table)
df_transactions = spark.range(1, 20001, 1, 4).select(
    F.col("id").alias("txn_id"),
    (F.col("id") % 10 + 1).alias("store_id"),
    F.round(F.rand(seed=42) * 200 + 5, 2).alias("amount")
)

# Small Store Dimension Table (10 rows)
stores = [
    (1, "Downtown Flagship", "New York"),
    (2, "Airport Kiosk", "Chicago"),
    (3, "Suburban Mall", "Dallas"),
    (4, "Tech Center", "San Francisco"),
    (5, "Metro Hub", "Atlanta"),
    (6, "Harbor Outlet", "Boston"),
    (7, "Plaza Central", "Seattle"),
    (8, "Galleria", "Houston"),
    (9, "Old Town", "Denver"),
    (10, "Express Mart", "Miami")
]
df_stores = spark.createDataFrame(stores, ["store_id", "store_name", "city"])

print(f"Transactions: {df_transactions.count()} | Stores: {df_stores.count()}")"""
                },
                {
                    "title": "Applying Explicit Broadcast Join Hint & Physical Plan Inspection",
                    "explanation": "Force broadcast join using F.broadcast() and verify execution plan.",
                    "code": """# Join with broadcast hint
df_joined = df_transactions.join(F.broadcast(df_stores), on="store_id", how="inner")

print("Joined Sample:")
df_joined.show(5)

print("=== PHYSICAL PLAN (Notice BroadcastHashJoinExec and absence of Exchange/Shuffle) ===")
df_joined.explain(mode="simple")"""
                },
                {
                    "title": "Understanding autoBroadcastJoinThreshold",
                    "explanation": "Inspect and tune the auto-broadcast threshold in Spark configurations.",
                    "code": """current_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"Current Auto Broadcast Threshold: {int(current_threshold) / (1024*1024):.1f} MB")

# To disable auto-broadcast: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# To increase threshold to 50MB: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))"""
                }
            ],
            "key_takeaways": [
                "Broadcast joins completely eliminate shuffle exchange, providing order-of-magnitude speedups for asymmetric table joins.",
                "The small broadcasted table is collected to the driver node first—broadcasting tables larger than driver memory triggers Driver OOM.",
                "Default auto-broadcast threshold is 10MB; setting to -1 disables auto-broadcasting."
            ],
            "interview_questions": [
                "How does the Driver node distribute a broadcasted table to worker executors?",
                "What error occurs if you broadcast a DataFrame that is larger than the Driver's available memory?",
                "Why can broadcast joins fail or hang on datasets with massive row counts even if disk size is small?"
            ]
        },
        {
            "nb_id": "032",
            "title": "Spark SQL Views, Temp Tables and Catalyst SQL Optimization",
            "difficulty": "Beginner",
            "business_scenario": "A hybrid analytics team consists of Python data engineers and SQL business analysts. The data engineering team prepares sanitized DataFrames that SQL analysts query using ANSI SQL queries and Common Table Expressions (CTEs).",
            "problem_statement": "Demonstrate seamless interoperability between PySpark DataFrames and Spark SQL: register temporary views (`createOrReplaceTempView`), global temporary views (`createGlobalTempView`), run complex SQL with CTEs via `spark.sql()`, pass dynamic parameters, and inspect Catalyst optimization.",
            "architectural_notes": "Temporary views exist purely as logical metadata pointers in the SparkSession catalog without materializing data to disk. Calling `spark.sql()` compiles SQL text through the same Catalyst parser and optimizer as DataFrame API code, producing identical physical plans.",
            "sections": [
                {
                    "title": "Creating DataFrame & Registering Temporary Views",
                    "explanation": "Register local temporary views and global temporary views.",
                    "code": """employees = [
    ("E-1", "Alice", "Engineering", 130000, "2020-03-15"),
    ("E-2", "Bob", "Engineering", 145000, "2019-06-01"),
    ("E-3", "Charlie", "Marketing", 95000, "2021-01-10"),
    ("E-4", "Diana", "Marketing", 105000, "2018-11-20"),
    ("E-5", "Evan", "Finance", 115000, "2022-04-05")
]

schema = "emp_id STRING, name STRING, dept STRING, salary DOUBLE, hire_date STRING"
df_emp = spark.createDataFrame(employees, schema)

# Register local temporary view (scoped to this SparkSession)
df_emp.createOrReplaceTempView("v_employees")
print("Temporary view 'v_employees' registered in SparkSession catalog.")"""
                },
                {
                    "title": "Executing Complex SQL with CTEs and Window Functions",
                    "explanation": "Run standard ANSI SQL queries over registered DataFrames.",
                    "code": """sql_query = \"\"\"
WITH dept_stats AS (
    SELECT 
        dept,
        AVG(salary) AS avg_dept_salary,
        MAX(salary) AS max_dept_salary
    FROM v_employees
    GROUP BY dept
)
SELECT 
    e.emp_id,
    e.name,
    e.dept,
    e.salary,
    ROUND(d.avg_dept_salary, 2) AS dept_avg,
    ROUND(e.salary - d.avg_dept_salary, 2) AS diff_from_avg
FROM v_employees e
JOIN dept_stats d ON e.dept = d.dept
ORDER BY diff_from_avg DESC
\"\"\"

df_sql_result = spark.sql(sql_query)
df_sql_result.show()"""
                },
                {
                    "title": "Parameterized SQL Queries via Python Formatting & Identical Plans",
                    "explanation": "Inject dynamic variables safely into SQL queries and verify Catalyst equivalence.",
                    "code": """min_salary = 100000
target_dept = "Engineering"

# Parameterized SQL execution
df_filtered_sql = spark.sql(
    "SELECT * FROM v_employees WHERE salary >= :min_sal AND dept = :dept",
    args={"min_sal": min_salary, "dept": target_dept}
)
df_filtered_sql.show()

# Verify that DataFrame API produces the exact same physical plan as Spark SQL
df_api = df_emp.filter((F.col("salary") >= min_salary) & (F.col("dept") == target_dept))

print("SQL and DataFrame API produce identical Catalyst plans:")
assert df_filtered_sql.collect() == df_api.collect(), "Plan execution result mismatch"
print("Verification passed!")"""
                }
            ],
            "key_takeaways": [
                "Temporary views are lightweight metadata aliases that require zero disk storage or computation until queried.",
                "`createOrReplaceTempView` is scoped to the current SparkSession; `createGlobalTempView` is accessible across sessions under the `global_temp` database.",
                "DataFrame API and Spark SQL share the exact same Catalyst compiler and Tungsten execution engine."
            ],
            "interview_questions": [
                "What is the difference between a Temporary View and a Managed Table in Spark?",
                "How does `createGlobalTempView` differ from `createOrReplaceTempView`?",
                "Does writing code in Spark SQL perform slower or faster than writing PySpark DataFrame API code?"
            ]
        }
    ]

    for nb in notebooks:
        nb_json = build_notebook(
            nb_id=nb["nb_id"],
            title=nb["title"],
            module_folder=MODULE_FOLDER,
            module_name=MODULE_NAME,
            difficulty=nb["difficulty"],
            yt_ref=nb["yt_ref"],
            business_scenario=nb["business_scenario"],
            problem_statement=nb["problem_statement"],
            architectural_notes=nb["architectural_notes"],
            sections=nb["sections"],
            key_takeaways=nb["key_takeaways"],
            interview_questions=nb["interview_questions"]
        )
        filename = sanitize_filename(nb["nb_id"], nb["title"])
        write_notebook(out_dir / filename, nb_json)
    print(f"Generated {len(notebooks)} notebooks for {MODULE_NAME}")

if __name__ == "__main__":
    generate(Path(__file__).parent.parent)
