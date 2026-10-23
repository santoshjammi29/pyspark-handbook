"""
Module 02: Complex Data Types & Nested Structures Handling (Notebooks 013 - 022)
"""

import re
from pathlib import Path
from notebook_builder import build_notebook, write_notebook

MODULE_FOLDER = "02_complex_data_types"
MODULE_NAME = "Module 02: Complex Data Types & Nested Structures Handling"

def sanitize_filename(nb_id: str, title: str) -> str:
    cleaned = title.lower().replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return f"{nb_id}_{cleaned}.ipynb"

def generate(base_dir: Path):
    out_dir = base_dir / "notebooks" / MODULE_FOLDER
    out_dir.mkdir(parents=True, exist_ok=True)

    notebooks = [
        {
            "nb_id": "013",
            "title": "Array Creation, Element Extraction and Array Search Functions",
            "difficulty": "Intermediate",
            "yt_ref": "Video 06: Master Databricks Notebooks & Video 23: Interview Questions",
            "business_scenario": "A digital streaming platform logs user engagement tags, genre preferences, and content playlists as arrays. Marketing analysts need to inspect array length, test for specific genre membership, and extract primary tags.",
            "problem_statement": "Master array operations: create arrays from multiple columns (`array()`), test element membership (`array_contains()`), retrieve element by index with 1-based and negative indexing (`element_at`), sort array elements (`array_sort`), and compute array sizes.",
            "architectural_notes": "Spark represents ArrayType as native columnar array buffers in Tungsten memory. `element_at` uses 1-based indexing for positive indices (1 = first element) and -1 for the last element, differing from standard Python 0-based indexing.",
            "sections": [
                {
                    "title": "Generating Streaming User Preferences",
                    "explanation": "Create mock streaming user records with tags and category arrays.",
                    "code": """user_media = [
    ("U-101", ["Sci-Fi", "Action", "Thriller"], ["HD", "Dolby Atmos", "4K"]),
    ("U-102", ["Romance", "Comedy"], ["SD"]),
    ("U-103", ["Documentary"], ["HD", "Subtitles"]),
    ("U-104", ["Action", "Anime", "Fantasy", "Adventure"], ["4K", "HDR10"])
]

schema = "user_id STRING, genres ARRAY<STRING>, stream_features ARRAY<STRING>"
df_users = spark.createDataFrame(user_media, schema)
df_users.show(truncate=False)"""
                },
                {
                    "title": "Array Membership, Length and Extraction",
                    "explanation": "Inspect arrays with array_contains, size, element_at, and array_sort.",
                    "code": """df_array_ops = df_users.withColumn(
    "genre_count", F.size(F.col("genres"))
).withColumn(
    "likes_action", F.array_contains(F.col("genres"), "Action")
).withColumn(
    "primary_genre", F.element_at(F.col("genres"), 1)  # 1-based index: first
).withColumn(
    "last_genre", F.element_at(F.col("genres"), -1)     # negative index: last
).withColumn(
    "sorted_genres", F.array_sort(F.col("genres"))
)

df_array_ops.select("user_id", "genre_count", "likes_action", "primary_genre", "last_genre", "sorted_genres").show(truncate=False)"""
                },
                {
                    "title": "Array Set Operations: Union, Intersection & Distinct",
                    "explanation": "Compute combined tags across multiple arrays using array_union, array_intersect, and array_distinct.",
                    "code": """# Simulate secondary recommendations array
df_with_recs = df_users.withColumn("recommended_genres", F.array(F.lit("Action"), F.lit("Mystery"), F.lit("Sci-Fi")))

df_sets = df_with_recs.withColumn(
    "all_genres_union", F.array_distinct(F.array_union(F.col("genres"), F.col("recommended_genres")))
).withColumn(
    "common_genres", F.array_intersect(F.col("genres"), F.col("recommended_genres"))
)

df_sets.select("user_id", "all_genres_union", "common_genres").show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "`element_at(col, 1)` uses 1-based indexing for positive numbers and -1 for the last element.",
                "`array_contains()` is optimized for fast binary scans across Tungsten array representations.",
                "Use `array_union` paired with `array_distinct` to combine distinct sets without exploding into rows."
            ],
            "interview_questions": [
                "How does `element_at` handle out-of-bounds indices in Spark SQL?",
                "What is the difference between `array_distinct` and exploding an array followed by `distinct()`?",
                "How does Spark's Tungsten memory format store nested ArrayType values internally?"
            ]
        },
        {
            "nb_id": "014",
            "title": "Spark Higher-Order Functions: Transform, Filter and Aggregate",
            "difficulty": "Intermediate",
            "yt_ref": "Video 12: Debugging Code in Databricks & Video 23: Interview Questions",
            "business_scenario": "A fintech loyalty platform tracks customer reward points earned across multiple merchant transactions in an array. The team needs to adjust point values, filter out expired promo points, and sum net balances without exploding rows.",
            "problem_statement": "Implement Spark 3+ Higher-Order Functions directly on arrays: `transform` (lambda mapping), `filter` (predicate filtering), `aggregate` (reduce/fold), `exists`, and `forall` without incurring expensive explode-and-groupBy shuffles.",
            "architectural_notes": "Higher-order functions run inline lambda expressions inside the JVM on array elements without flattening the DataFrame. Exploding an array multiplies the row count and requires a subsequent `groupBy()` shuffle to reconstitute the row, causing 10x-100x slower execution.",
            "sections": [
                {
                    "title": "Generating Loyalty Transactions Data",
                    "explanation": "Create accounts with arrays of point earnings.",
                    "code": """loyalty_data = [
    ("ACC-1", [150, 45, 800, 20, 310]),
    ("ACC-2", [50, 25, 10]),
    ("ACC-3", [1200, 450, 90, 850]),
    ("ACC-4", [5, 12, 18])
]

df_loyalty = spark.createDataFrame(loyalty_data, ["account_id", "point_events"])
df_loyalty.show(truncate=False)"""
                },
                {
                    "title": "Transform & Filter Higher-Order Functions",
                    "explanation": "Apply 10% bonus to points over 100 and filter out micro-transactions (< 30 points).",
                    "code": """# 1. Transform: add 10% bonus to all points: x -> cast(x * 1.10 as int)
df_transformed = df_loyalty.withColumn(
    "bonus_points",
    F.expr("transform(point_events, x -> cast(x * 1.10 AS INT))")
)

# 2. Filter: keep only significant transactions >= 50
df_filtered = df_transformed.withColumn(
    "significant_events",
    F.expr("filter(point_events, x -> x >= 50)")
)

df_filtered.show(truncate=False)"""
                },
                {
                    "title": "Aggregate (Reduce), Exists and Forall",
                    "explanation": "Compute running sum via aggregate and evaluate boolean array conditions.",
                    "code": """# Aggregate: sum all points starting from initial accumulator 0: (acc, x) -> acc + x
df_agg = df_filtered.withColumn(
    "total_points",
    F.expr("aggregate(point_events, 0, (acc, x) -> acc + x)")
).withColumn(
    "has_super_bonus",
    F.expr("exists(point_events, x -> x >= 500)")
).withColumn(
    "all_positive",
    F.expr("forall(point_events, x -> x > 0)")
)

df_agg.select("account_id", "point_events", "total_points", "has_super_bonus", "all_positive").show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Higher-order functions eliminate the need for `explode()` and `groupBy()` when manipulating array elements.",
                "`transform` maps every element to a new value via lambda expression.",
                "`aggregate(arr, zero, (acc, x) -> ...)` performs in-memory reduction without network shuffle."
            ],
            "interview_questions": [
                "Why are Higher-Order Functions significantly faster than `explode` followed by `groupBy`?",
                "How does `exists(arr, x -> cond)` differ from `array_contains(arr, val)`?",
                "Can you use custom Python functions inside Spark's `transform` higher-order function?"
            ]
        },
        {
            "nb_id": "015",
            "title": "Exploding and Flattening Arrays: explode vs explode_outer and posexplode",
            "difficulty": "Intermediate",
            "yt_ref": "Video 06: Master Databricks Notebooks & Video 23: Interview Questions",
            "business_scenario": "A flight booking aggregator normalizes JSON payloads of airline bookings. Bookings contain lists of passengers and stops. Some bookings have empty passenger lists (infant reservation hold) that must not be accidentally dropped during normalization.",
            "problem_statement": "Compare `explode()` vs `explode_outer()` to prevent data loss on empty/null arrays. Use `posexplode()` to capture element sequence positions (e.g. flight leg order 0, 1, 2). Flatten multi-dimensional nested arrays (`flatten()`).",
            "architectural_notes": "Standard `explode()` drops rows where the array is empty or NULL, leading to silent data loss. `explode_outer()` emits a row with NULL for the exploded element, preserving the parent record. `posexplode()` generates both position (zero-based index) and element.",
            "sections": [
                {
                    "title": "Mocking Multi-Leg Booking Records",
                    "explanation": "Create bookings with multiple flight legs and edge cases (empty passengers).",
                    "code": """bookings = [
    ("BKG-001", "Delta", ["ATL", "JFK", "LHR"], ["Alice", "Bob"]),
    ("BKG-002", "United", ["ORD", "FRA"], ["Charlie"]),
    ("BKG-003", "Lufthansa", ["MUC"], []),          # Empty passenger array
    ("BKG-004", "British Airways", ["LHR", "SIN"], None) # Null passenger array
]

schema = "booking_id STRING, airline STRING, flight_legs ARRAY<STRING>, passengers ARRAY<STRING>"
df_bookings = spark.createDataFrame(bookings, schema)
df_bookings.show(truncate=False)"""
                },
                {
                    "title": "Standard explode() vs explode_outer() Data Loss Comparison",
                    "explanation": "Demonstrate how standard explode drops records BKG-003 and BKG-004.",
                    "code": """# Standard explode: drops rows with empty or null arrays
df_standard = df_bookings.select("booking_id", "airline", F.explode("passengers").alias("passenger"))
print(f"Standard explode row count: {df_standard.count()} (BKG-003 and BKG-004 dropped!)")
df_standard.show()

# explode_outer: retains parent row with null
df_outer = df_bookings.select("booking_id", "airline", F.explode_outer("passengers").alias("passenger"))
print(f"explode_outer row count: {df_outer.count()} (Preserves all bookings!)")
df_outer.show()"""
                },
                {
                    "title": "posexplode() for Leg Sequencing & Array Flattening",
                    "explanation": "Extract zero-based stop sequences for flight itineraries.",
                    "code": """# posexplode generates leg_sequence and airport
df_legs = df_bookings.select(
    "booking_id",
    "airline",
    F.posexplode("flight_legs").alias("leg_seq", "airport")
)
df_legs.show()

# Flatten 2D nested arrays: [[A, B], [C, D]] -> [A, B, C, D]
df_nested_arr = spark.createDataFrame([("TEST", [["A", "B"], ["C", "D", "E"]])], ["id", "matrix"])
df_flat = df_nested_arr.withColumn("flat_matrix", F.flatten("matrix"))
df_flat.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Always use `explode_outer()` if the array column can contain nulls or empty lists to avoid losing parent rows.",
                "`posexplode()` provides the original ordinal index of each element within its array.",
                "`flatten()` combines an `ARRAY<ARRAY<T>>` into a single `ARRAY<T>` without expanding rows."
            ],
            "interview_questions": [
                "Under what condition does `explode()` drop rows from a DataFrame?",
                "What is the difference between `posexplode` and `posexplode_outer`?",
                "How does exploding large arrays impact partition skew and shuffle memory?"
            ]
        },
        {
            "nb_id": "016",
            "title": "Struct Creation, Nested Field Access and Updating Sub-Fields",
            "difficulty": "Intermediate",
            "yt_ref": "Video 14: Master Unity Catalog & Video 23: Interview Questions",
            "business_scenario": "A customer identity platform groups personal attributes, contact points, and address details into nested structs for clean schema organization. Downstream systems need to query specific sub-fields and update an address without reconstructing the entire object.",
            "problem_statement": "Create nested `StructType` columns using `struct()`. Access inner fields via dot notation and `col.getField()`. Demonstrate updating a single nested field inside a struct in PySpark 3.1+ without manually re-specifying all sibling fields.",
            "architectural_notes": "StructType is a compound type that stores multiple typed named fields together in memory. In earlier Spark versions, updating one nested field required re-creating the entire struct. Spark 3.1+ introduced `withField()` and `dropFields()` on Struct columns to manipulate nested fields cleanly.",
            "sections": [
                {
                    "title": "Creating Nested Struct Records",
                    "explanation": "Package flat customer columns into organized nested structs.",
                    "code": """customers_flat = [
    ("C-01", "Alice", "Smith", "123 Elm St", "Austin", "TX", "78701", 85000),
    ("C-02", "Bob", "Jones", "456 Oak Ave", "Seattle", "WA", "98101", 92000)
]

cols = ["id", "first_name", "last_name", "street", "city", "state", "zip", "salary"]
df_flat = spark.createDataFrame(customers_flat, cols)

# Bundle into nested structs
df_nested = df_flat.select(
    F.col("id"),
    F.struct(F.col("first_name"), F.col("last_name")).alias("name"),
    F.struct(F.col("street"), F.col("city"), F.col("state"), F.col("zip")).alias("address"),
    F.col("salary")
)

df_nested.printSchema()
df_nested.show(truncate=False)"""
                },
                {
                    "title": "Querying Nested Fields via Dot Notation & getField",
                    "explanation": "Select specific sub-fields directly.",
                    "code": """# Accessing nested fields
df_selected = df_nested.select(
    "id",
    "name.last_name",
    F.col("address.city").alias("home_city"),
    F.col("address").getField("state").alias("home_state")
)

df_selected.show()"""
                },
                {
                    "title": "Updating Single Nested Fields using withField (Spark 3.1+)",
                    "explanation": "Modify address.city without rewriting the entire struct definition.",
                    "code": """# Update city to uppercase and add a new nested field country
df_updated_struct = df_nested.withColumn(
    "address",
    F.col("address")
     .withField("city", F.upper(F.col("address.city")))
     .withField("country", F.lit("USA"))
)

df_updated_struct.printSchema()
df_updated_struct.select("id", "address").show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Use `struct(*cols)` to group related columns into a compound object without network shuffle.",
                "`col.withField('subfield', expr)` (Spark 3.1+) updates or adds a nested field inside an existing struct.",
                "`col.dropFields('subfield')` removes specific nested attributes from a struct."
            ],
            "interview_questions": [
                "How does `withField()` simplify nested struct manipulation compared to older Spark versions?",
                "What is the difference between dot notation `col('a.b')` and `col('a').getField('b')`?",
                "Can Parquet push down column projection into individual fields of a nested StructType?"
            ]
        },
        {
            "nb_id": "017",
            "title": "Recursive Schema Traversal: Flattening Deeply Nested JSON/Structs",
            "difficulty": "Advanced",
            "yt_ref": "Video 11: Auto-loader & Video 23: Interview Questions",
            "business_scenario": "An enterprise integration hub ingests complex payloads from ERP and CRM systems containing 5+ levels of nested structs and arrays. Analysts require a completely flattened tabular DataFrame where column names reflect their full dot-delimited path.",
            "problem_statement": "Develop a generic, production-ready recursive schema flattener in Python that traverses any arbitrarily deep Spark schema. The function must explode nested arrays and flatten all nested structs into top-level columns (`parent_child_field`).",
            "architectural_notes": "Deeply nested schemas can cause severe query compilation overhead. Traversing the schema programmatically inspects `df.schema.fields` and constructs a projection expression list dynamically without executing any JVM actions.",
            "sections": [
                {
                    "title": "Generating Arbitrarily Deep Nested Schema",
                    "explanation": "Create a multi-level nested customer record with structs inside structs.",
                    "code": """deep_data = [
    {
        "id": "ACC-99",
        "company": {
            "name": "Acme Corp",
            "headquarters": {
                "geo": {"lat": 37.7749, "lon": -122.4194},
                "city": "San Francisco"
            }
        },
        "metrics": {"financials": {"revenue": 5000000.0, "ebitda": 1200000.0}}
    }
]

import json
rdd_json = spark.sparkContext.parallelize([json.dumps(d) for d in deep_data])
df_deep = spark.read.json(rdd_json)
print("Original Deeply Nested Schema:")
df_deep.printSchema()"""
                },
                {
                    "title": "Recursive Flattening Algorithm",
                    "explanation": "Implement recursive schema inspection function.",
                    "code": """def flatten_dataframe(df):
    \"\"\"
    Recursively flattens all StructType columns in a DataFrame into top-level columns.
    \"\"\"
    flat_cols = []
    has_struct = False
    
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StructType):
            has_struct = True
            for child in field.dataType.fields:
                flat_cols.append(
                    F.col(f"{field.name}.{child.name}").alias(f"{field.name}_{child.name}")
                )
        else:
            flat_cols.append(F.col(field.name))
            
    df_result = df.select(*flat_cols)
    # If any struct columns were flattened, recurse until all structs are resolved
    if has_struct:
        return flatten_dataframe(df_result)
    return df_result

df_flattened = flatten_dataframe(df_deep)
print("Completely Flattened Schema:")
df_flattened.printSchema()
df_flattened.show(truncate=False)"""
                },
                {
                    "title": "Validation & Column Verification",
                    "explanation": "Verify all leaf columns are top-level primitives.",
                    "code": """expected_cols = [
    "id",
    "company_name",
    "company_headquarters_city",
    "company_headquarters_geo_lat",
    "company_headquarters_geo_lon",
    "metrics_financials_ebitda",
    "metrics_financials_revenue"
]

for c in expected_cols:
    assert c in df_flattened.columns, f"Expected column {c} missing from flattened DataFrame"

print(f"Successfully flattened deep schema into {len(df_flattened.columns)} scalar columns!")"""
                }
            ],
            "key_takeaways": [
                "Programmatic schema inspection via `df.schema.fields` enables dynamic metadata-driven pipelines.",
                "Recursive flattening eliminates manual column renaming and hardcoded nested selectors.",
                "Flattened schemas allow downstream BI tools (Power BI, Tableau) to query data without JSON parsing."
            ],
            "interview_questions": [
                "How do you handle nested `ArrayType(StructType)` columns during automated schema flattening?",
                "What are the performance implications of flattening a schema with 500+ leaf columns in Parquet?",
                "Why can recursive schema transformations exceed Java bytecode limits (64KB method size) in Spark?"
            ]
        },
        {
            "nb_id": "018",
            "title": "MapType Creation, Key Extraction and Dynamic Properties",
            "difficulty": "Intermediate",
            "yt_ref": "Video 14: Master Unity Catalog & Video 23: Interview Questions",
            "business_scenario": "An advertising technology platform captures custom user ad-tracking attributes (cookies, campaign IDs, UTM tags, experiment variants) as dynamic key-value pairs where keys vary across partners and cannot be fixed into a static schema.",
            "problem_statement": "Master MapType operations: construct maps from column pairs (`create_map()`), extract keys (`map_keys()`), extract values (`map_values()`), build maps from paired arrays (`map_from_arrays()`), and query dynamic attributes safely without null pointer exceptions.",
            "architectural_notes": "MapType represents key-value collections with arbitrary keys of identical data type. Unlike StructType (where field names are fixed in schema metadata), MapType allows dynamic runtime keys, making it ideal for sparse key-value attributes.",
            "sections": [
                {
                    "title": "Creating Ad-Tech Campaign Metadata with MapType",
                    "explanation": "Construct maps using create_map and map_from_arrays.",
                    "code": """# Build map using paired key-value lists
campaign_data = [
    ("EV-1", ["utm_source", "utm_campaign", "device"], ["google", "black_friday", "mobile"]),
    ("EV-2", ["utm_source", "experiment_group"], ["facebook", "variant_b"]),
    ("EV-3", ["utm_source", "utm_medium", "utm_campaign"], ["linkedin", "cpc", "q4_enterprise"])
]

df_raw_campaigns = spark.createDataFrame(campaign_data, ["event_id", "param_keys", "param_vals"])

# Convert parallel arrays into MapType
df_mapped = df_raw_campaigns.withColumn(
    "attributes",
    F.map_from_arrays(F.col("param_keys"), F.col("param_vals"))
)

df_mapped.printSchema()
df_mapped.select("event_id", "attributes").show(truncate=False)"""
                },
                {
                    "title": "Querying Map Keys, Values & Dynamic Lookup",
                    "explanation": "Access map values by key name using bracket notation and inspect key catalogs.",
                    "code": """# Querying specific keys
df_extracted = df_mapped.withColumn(
    "source", F.col("attributes")["utm_source"]
).withColumn(
    "campaign", F.coalesce(F.col("attributes")["utm_campaign"], F.lit("ORGANIC"))
).withColumn(
    "all_keys", F.map_keys(F.col("attributes"))
).withColumn(
    "all_values", F.map_values(F.col("attributes"))
)

df_extracted.select("event_id", "source", "campaign", "all_keys").show(truncate=False)"""
                },
                {
                    "title": "Exploding MapType into Relational Key-Value Rows",
                    "explanation": "Convert sparse key-value pairs into normalized relational rows.",
                    "code": """df_kv_pairs = df_mapped.select(
    "event_id",
    F.explode("attributes").alias("attr_key", "attr_value")
)

df_kv_pairs.show()"""
                }
            ],
            "key_takeaways": [
                "Use `MapType` for semi-structured data where keys are dynamic, variable, or unpredictable.",
                "`map_from_arrays(keys_col, values_col)` builds maps from two parallel array columns.",
                "Accessing non-existent map keys returns `NULL` safely rather than raising runtime errors."
            ],
            "interview_questions": [
                "What is the difference between `MapType` and `StructType` in PySpark?",
                "When should you choose `MapType` over a normalized child table in Lakehouse architecture?",
                "How does `explode(map_col)` generate key and value columns simultaneously?"
            ]
        },
        {
            "nb_id": "019",
            "title": "Parsing Raw JSON Payloads: from_json, to_json and schema_of_json",
            "difficulty": "Intermediate",
            "yt_ref": "Video 11: Auto-loader & Video 17: Lakeflow Declarative Pipelines",
            "business_scenario": "A payment gateway streams raw JSON transaction events via Apache Kafka or cloud storage. Each record arrives as a single raw string column containing nested transaction data that must be parsed into typed columns.",
            "problem_statement": "Master JSON string handling: dynamically infer schema using `schema_of_json()`, parse stringified JSON into structured structs using `from_json()`, serialize DataFrames back to JSON using `to_json()`, and handle schema evolution options.",
            "architectural_notes": "`from_json()` parses JSON strings inside JVM tasks using Jackson parser, avoiding Python serialization. Specifying an explicit schema avoids costly full-table scans to infer JSON structure.",
            "sections": [
                {
                    "title": "Generating Raw Kafka-like Message Strings",
                    "explanation": "Create sample DataFrame with raw JSON payload strings.",
                    "code": """kafka_stream = [
    ("K-001", '{"order_id": "ORD-1", "customer": {"id": "C-10", "email": "c10@mail.com"}, "amount": 149.99, "tags": ["sale", "prime"]}'),
    ("K-002", '{"order_id": "ORD-2", "customer": {"id": "C-20", "email": "c20@mail.com"}, "amount": 89.50, "tags": ["clearance"]}'),
    ("K-003", '{"order_id": "ORD-3", "customer": {"id": "C-30", "email": "c30@mail.com"}, "amount": 420.00, "tags": []}')
]

df_kafka = spark.createDataFrame(kafka_stream, ["kafka_offset", "payload"])
df_kafka.show(truncate=False)"""
                },
                {
                    "title": "Inferring Schema via schema_of_json & Explicit Parsing",
                    "explanation": "Extract schema from a sample row and parse payloads into typed structs.",
                    "code": """# 1. Infer sample DDL schema using schema_of_json
sample_json = df_kafka.select("payload").first()[0]
inferred_ddl = spark.range(1).select(F.schema_of_json(F.lit(sample_json))).first()[0]
print(f"Inferred Schema DDL: {inferred_ddl}")

# 2. Parse using explicit StructType schema for production safety
order_schema = T.StructType([
    T.StructField("order_id", T.StringType(), True),
    T.StructField("customer", T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("email", T.StringType(), True)
    ]), True),
    T.StructField("amount", T.DoubleType(), True),
    T.StructField("tags", T.ArrayType(T.StringType()), True)
])

df_parsed = df_kafka.withColumn("data", F.from_json(F.col("payload"), order_schema))
df_parsed.printSchema()
df_parsed.select("kafka_offset", "data.order_id", "data.customer.email", "data.amount", "data.tags").show(truncate=False)"""
                },
                {
                    "title": "Serializing Structs Back to JSON with to_json",
                    "explanation": "Serialize structured data back into JSON strings for downstream API dispatch.",
                    "code": """df_re_serialized = df_parsed.select(
    "kafka_offset",
    F.to_json("data").alias("exported_json")
)

df_re_serialized.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Always supply explicit schemas to `from_json()` to avoid driver-side inference bottlenecks.",
                "`schema_of_json()` is a helpful utility for deriving schema DDL strings from representative payloads.",
                "`to_json()` serializes complex structs and maps back into compact JSON strings."
            ],
            "interview_questions": [
                "What happens when a JSON payload contains keys that are not defined in the `from_json` schema?",
                "How does `from_json` performance compare to parsing JSON with Python's built-in `json.loads` UDF?",
                "How do you handle schema evolution when incoming JSON payloads gain new fields over time?"
            ]
        },
        {
            "nb_id": "020",
            "title": "Handling Corrupted & Malformed JSON: Permissive vs DropMalformed vs FailFast",
            "difficulty": "Intermediate",
            "yt_ref": "Video 11: Auto-loader & Video 12: Debugging Code in Databricks",
            "business_scenario": "A public data ingestion pipeline scrapes third-party web endpoints. Up to 5% of incoming payloads contain syntax errors (truncated braces, mismatched quotes, non-JSON tokens). The pipeline must never crash and must capture all corrupted lines into an audit column.",
            "problem_statement": "Compare Spark's JSON read modes: `PERMISSIVE`, `DROPMALFORMED`, and `FAILFAST`. Configure the `columnNameOfCorruptRecord` option to capture raw malformed strings into a `_corrupt_record` quarantine column for compliance review.",
            "architectural_notes": "`PERMISSIVE` is Spark's default mode: corrupted fields evaluate to NULL and the unparseable string is diverted to `_corrupt_record`. `FAILFAST` halts execution immediately upon encountering syntax errors. `DROPMALFORMED` silently drops damaged records.",
            "sections": [
                {
                    "title": "Creating Dataset with Intentionally Malformed JSON Strings",
                    "explanation": "Simulate mixed clean and corrupted JSON inputs.",
                    "code": """bad_json_lines = [
    '{"id": "EVT-1", "user": "alice", "score": 95}',
    '{"id": "EVT-2", "user": "bob", "score": INVALID_NUMBER}', # Malformed token
    '{"id": "EVT-3", "user": "charlie", "score": 82}',
    'NOT_EVEN_JSON_PAYLOAD',                                      # Completely unparseable
    '{"id": "EVT-4", "user": "diana", "score": 100}'
]

rdd_dirty = spark.sparkContext.parallelize(bad_json_lines)"""
                },
                {
                    "title": "Resilient Ingestion with PERMISSIVE Mode & _corrupt_record",
                    "explanation": "Define schema with _corrupt_record to quarantine broken payloads.",
                    "code": """# Define schema including special corrupt record column
quarantine_schema = T.StructType([
    T.StructField("id", T.StringType(), True),
    T.StructField("user", T.StringType(), True),
    T.StructField("score", T.IntegerType(), True),
    T.StructField("_corrupt_record", T.StringType(), True)
])

# Read with PERMISSIVE mode
df_permissive = (
    spark.read
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(quarantine_schema)
    .json(rdd_dirty)
)

print("Ingested Records with Quarantine Column:")
df_permissive.show(truncate=False)"""
                },
                {
                    "title": "Quarantine Routing: Splitting Clean Records from Corrupted Rows",
                    "explanation": "Filter clean rows for production and route corrupt payloads to a dead-letter sink.",
                    "code": """df_clean = df_permissive.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
df_dead_letter = df_permissive.filter(F.col("_corrupt_record").isNotNull()).select("_corrupt_record")

print(f"Clean Records: {df_clean.count()}")
df_clean.show()

print(f"Corrupted Dead-Letter Records: {df_dead_letter.count()}")
df_dead_letter.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "`PERMISSIVE` mode ensures zero pipeline downtime by capturing unparseable strings into `_corrupt_record`.",
                "`FAILFAST` is recommended for high-integrity financial reconciliation where no data corruption can be tolerated.",
                "`DROPMALFORMED` should be used with extreme caution because it silently discards records without audit trails."
            ],
            "interview_questions": [
                "What are the three JSON read modes supported by Apache Spark?",
                "How does Databricks Auto Loader's `_rescued_data` column differ from Spark's `_corrupt_record`?",
                "Why must `_corrupt_record` be explicitly declared in the StructType schema?"
            ]
        },
        {
            "nb_id": "021",
            "title": "XML Data Ingestion, Hierarchical Parsing and Tag Structuring",
            "difficulty": "Intermediate",
            "yt_ref": "Video 09: DBUtils & Video 16: Databricks LakeFlow Connect",
            "business_scenario": "A legacy healthcare exchange transmits insurance claims in XML format. Claims contain nested patient information, diagnosis codes, and billing line items that must be parsed into structured Spark tables.",
            "problem_statement": "Ingest hierarchical XML records in PySpark: parse nested XML tags into StructType and ArrayType hierarchies, configure row tags, and handle optional missing tags without schema mismatches.",
            "architectural_notes": "Native XML parsing in Spark utilizes the `spark-xml` library (or Databricks built-in XML reader in DBR 14+). In standard PySpark, XML can be processed via the XML data source format or parsed using Python/XPath helper functions for lightweight payloads.",
            "sections": [
                {
                    "title": "Generating Sample Healthcare XML Claims",
                    "explanation": "Create multi-level XML claim documents.",
                    "code": """xml_payloads = [
    (\"\"\"<claim id="CLM-001">
        <patient id="P-10">
            <name>John Doe</name>
            <dob>1980-05-12</dob>
        </patient>
        <items>
            <item code="99213" amount="150.00"/>
            <item code="80053" amount="75.50"/>
        </items>
    </claim>\"\"\",),
    (\"\"\"<claim id="CLM-002">
        <patient id="P-20">
            <name>Jane Smith</name>
            <dob>1992-11-23</dob>
        </patient>
        <items>
            <item code="99214" amount="220.00"/>
        </items>
    </claim>\"\"\",)
]

df_raw_xml = spark.createDataFrame(xml_payloads, ["raw_xml"])
df_raw_xml.show(truncate=False)"""
                },
                {
                    "title": "Parsing XML Payloads via Built-in Regex & XPath Expressions",
                    "explanation": "Extract attributes and tags using xpath_string and xpath_double expressions.",
                    "code": """df_parsed_claims = df_raw_xml.withColumn(
    "claim_id", F.expr("xpath_string(raw_xml, 'claim/@id')")
).withColumn(
    "patient_id", F.expr("xpath_string(raw_xml, 'claim/patient/@id')")
).withColumn(
    "patient_name", F.expr("xpath_string(raw_xml, 'claim/patient/name')")
).withColumn(
    "patient_dob", F.expr("xpath_string(raw_xml, 'claim/patient/dob')")
).withColumn(
    "primary_item_code", F.expr("xpath_string(raw_xml, 'claim/items/item[1]/@code')")
).withColumn(
    "primary_item_amount", F.expr("xpath_double(raw_xml, 'claim/items/item[1]/@amount')")
)

df_parsed_claims.select("claim_id", "patient_id", "patient_name", "primary_item_code", "primary_item_amount").show(truncate=False)"""
                },
                {
                    "title": "Validation & Structural Integrity Check",
                    "explanation": "Verify that all claims were parsed with valid IDs and positive amounts.",
                    "code": """assert df_parsed_claims.filter(F.col("claim_id").isNull()).count() == 0, "Missing claim IDs"
assert df_parsed_claims.filter(F.col("primary_item_amount") <= 0).count() == 0, "Invalid item amounts"

print("XML extraction and schema validation completed successfully!")"""
                }
            ],
            "key_takeaways": [
                "Spark SQL provides built-in XPath expressions (`xpath_string`, `xpath_double`, `xpath_int`) for lightweight XML parsing without external dependencies.",
                "For heavy multi-gigabyte XML datasets, use `spark.read.format('xml').option('rowTag', 'tag_name')`.",
                "Always validate attribute extraction to ensure mandatory business keys are populated."
            ],
            "interview_questions": [
                "What is the difference between parsing XML via XPath expressions versus using the `spark-xml` library?",
                "How does the `rowTag` option dictate partition splitting in large XML files?",
                "Why is XML typically less efficient than Parquet or Avro in distributed big data pipelines?"
            ]
        },
        {
            "nb_id": "022",
            "title": "Advanced Aggregations: collect_list vs collect_set and Sorted Collections",
            "difficulty": "Intermediate",
            "yt_ref": "Video 06: Master Databricks Notebooks & Video 23: Interview Questions",
            "business_scenario": "A digital customer journey pipeline aggregates all URLs visited by users within a session. Product teams need both: (1) The exact chronologically ordered sequence of pages visited (including duplicates), and (2) The unique set of pages visited without duplicates.",
            "problem_statement": "Compare `collect_list()` (preserves duplicates) vs `collect_set()` (drops duplicates). Demonstrate sorting collected arrays chronologically by sorting before aggregation or using `array_sort` on collected structs.",
            "architectural_notes": "`collect_list` and `collect_set` are wide aggregations that buffer array elements in executor memory per group. If a single key contains millions of entries (key skew), this can trigger severe Executor OutOfMemory (OOM) errors. Pre-sorting rows before `groupBy` is NOT guaranteed to preserve order across shuffles unless using sorted structs.",
            "sections": [
                {
                    "title": "Generating User Session Clickstream",
                    "explanation": "Create chronologically timestamped page views per user session.",
                    "code": """clickstream = [
    ("SESS-1", "2023-11-20 10:00:00", "/home"),
    ("SESS-1", "2023-11-20 10:02:00", "/products"),
    ("SESS-1", "2023-11-20 10:05:00", "/products"),  # Repeated visit
    ("SESS-1", "2023-11-20 10:10:00", "/checkout"),
    ("SESS-2", "2023-11-20 11:00:00", "/home"),
    ("SESS-2", "2023-11-20 11:04:00", "/pricing"),
    ("SESS-2", "2023-11-20 11:08:00", "/home")       # Repeated visit
]

schema = "session_id STRING, event_time STRING, page_url STRING"
df_clicks = spark.createDataFrame(clickstream, schema)
df_clicks.show()"""
                },
                {
                    "title": "collect_list vs collect_set Aggregation",
                    "explanation": "Observe difference in duplicate retention.",
                    "code": """df_aggregated = df_clicks.groupBy("session_id").agg(
    F.collect_list("page_url").alias("all_pages_list"),
    F.collect_set("page_url").alias("unique_pages_set")
)

df_aggregated.show(truncate=False)"""
                },
                {
                    "title": "Guaranteed Chronological Sorting of Collected Elements",
                    "explanation": "Pack timestamp and URL into a struct, collect, sort, and extract page URLs.",
                    "code": """# Pack timestamp and URL into struct: array_sort will sort by struct's first field (event_time)
df_sorted_journey = (
    df_clicks
    .groupBy("session_id")
    .agg(
        F.array_sort(F.collect_list(F.struct("event_time", "page_url"))).alias("sorted_struct_events")
    )
    .withColumn(
        "ordered_page_sequence",
        F.expr("transform(sorted_struct_events, x -> x.page_url)")
    )
    .select("session_id", "ordered_page_sequence")
)

print("Guaranteed Chronologically Ordered Customer Journey:")
df_sorted_journey.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "`collect_list()` retains duplicates and element count; `collect_set()` removes duplicates without guaranteed order.",
                "Shuffles in Spark do not guarantee row arrival order; to sort collected arrays, bundle values into structs with sort keys and apply `array_sort()`.",
                "Beware of key skew: collecting millions of items on a single key causes executor heap exhaustion."
            ],
            "interview_questions": [
                "Why is `df.sort('time').groupBy('id').agg(collect_list('val'))` NOT guaranteed to preserve order in distributed Spark?",
                "How do you safely sort elements collected by `collect_list()`?",
                "What risk does `collect_list()` pose when operating on skewed keys?"
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
