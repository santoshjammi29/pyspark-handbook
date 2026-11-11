# dbt Project Structure Guide

## Recommended Folder Layout

```
my_dbt_project/
├── models/
│   ├── staging/
│   │   ├── _sources.yml          # Source system declarations
│   │   ├── _staging.yml          # Staging model docs + tests
│   │   ├── crm/
│   │   │   ├── stg_crm__customers.sql
│   │   │   └── stg_crm__contacts.sql
│   │   └── erp/
│   │       ├── stg_erp__orders.sql
│   │       └── stg_erp__products.sql
│   ├── intermediate/
│   │   ├── _int.yml
│   │   ├── int_customer__unified.sql      # Cross-source join
│   │   └── int_orders__enriched.sql       # Business logic
│   └── marts/
│       ├── sales/
│       │   ├── _sales.yml
│       │   ├── fct_orders.sql
│       │   ├── fct_returns.sql
│       │   └── dim_customer.sql
│       └── finance/
│           ├── _finance.yml
│           ├── fct_revenue.sql
│           └── dim_account.sql
├── macros/
│   ├── generate_schema_name.sql     # Override schema naming
│   ├── incremental_merge.sql        # Reusable MERGE macro
│   ├── surrogate_key.sql            # SK generation
│   └── date_spine.sql               # Date dimension helper
├── tests/
│   ├── generic/
│   │   └── test_not_empty.sql
│   └── singular/
│       └── assert_revenue_matches_source.sql
├── seeds/
│   ├── ref_country.csv
│   └── ref_currency.csv
├── snapshots/
│   └── snap_dim_customer.sql        # SCD Type 2 via dbt snapshot
├── analyses/
│   └── ad_hoc_revenue_analysis.sql
├── packages.yml
├── dbt_project.yml
└── profiles/
    └── profiles.yml                 # Never commit credentials here
```

---

## dbt_project.yml

```yaml
name: 'my_dbt_project'
version: '1.0.0'
config-version: 2

profile: 'my_databricks_profile'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  my_dbt_project:
    staging:
      +materialized: view          # Staging = always views
      +tags: ['staging']
    intermediate:
      +materialized: ephemeral     # Intermediate = CTEs (no storage)
      +tags: ['intermediate']
    marts:
      +materialized: table         # Marts = persisted tables
      sales:
        +tags: ['gold', 'sales']
        fct_orders:
          +materialized: incremental
          +incremental_strategy: merge
          +unique_key: order_id
      finance:
        +tags: ['gold', 'finance']
        +post-hook:
          - "GRANT SELECT ON {{ this }} TO ROLE analyst"

vars:
  start_date: '2020-01-01'
  dbt_date:start_date: '2020-01-01'

seeds:
  my_dbt_project:
    +schema: reference
    ref_country:
      +column_types:
        country_code: varchar(3)
        country_name: varchar(100)
```

---

## Key Macros

### Surrogate Key Generation
```sql
-- macros/surrogate_key.sql
{% macro surrogate_key(columns) %}
    {{ dbt_utils.surrogate_key(columns) }}
{%- endmacro %}
```

### Incremental Merge Macro
```sql
-- macros/incremental_merge.sql
{% macro get_max_updated_at(target_table, updated_at_col='updated_at') %}
    {% if is_incremental() %}
        (SELECT COALESCE(MAX({{ updated_at_col }}), '1900-01-01') FROM {{ this }})
    {% else %}
        '1900-01-01'
    {% endif %}
{% endmacro %}
```

### Schema Naming (multi-env)
```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

---

## SCD Type 2 via dbt Snapshot

```sql
-- snapshots/snap_dim_customer.sql
{% snapshot snap_dim_customer %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['email', 'phone', 'address', 'country_code'],
    )
}}

SELECT
    customer_id,
    customer_name,
    email,
    phone,
    address,
    country_code,
    updated_at
FROM {{ source('crm', 'customers') }}

{% endsnapshot %}
```

dbt automatically adds: `dbt_scd_id`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`

---

## packages.yml

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<1.0.0"]
  - package: dbt-labs/dbt_date
    version: [">=0.10.0", "<1.0.0"]
  - package: Datavault-UK/dbtvault
    version: [">=0.9.0", "<1.0.0"]   # For Data Vault 2.0
```

---

## Environment Configuration (profiles.yml)

```yaml
# profiles/profiles.yml — use environment variables for credentials
my_databricks_profile:
  target: dev
  outputs:
    dev:
      type: databricks
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      catalog: dev_catalog
      schema: dbt_dev
      threads: 4

    prod:
      type: databricks
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH_PROD') }}"
      token: "{{ env_var('DATABRICKS_TOKEN_PROD') }}"
      catalog: prod_catalog
      schema: gold
      threads: 16
```

---

## dbt Test Standards

### Schema Tests (in YAML)
```yaml
models:
  - name: fct_orders
    tests:
      - dbt_utils.expression_is_true:
          expression: "revenue >= 0"
    columns:
      - name: order_id
        tests: [not_null, unique]
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_bk
      - name: order_date
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: "'2020-01-01'"
              max_value: "current_date()"
```

### Singular Test (custom assertion)
```sql
-- tests/singular/assert_revenue_matches_source.sql
-- This test passes if it returns 0 rows
SELECT
    s.order_date,
    ABS(s.source_revenue - t.dbt_revenue) AS discrepancy
FROM (
    SELECT order_date, SUM(total_amount) AS source_revenue
    FROM {{ source('erp', 'orders') }}
    GROUP BY order_date
) s
JOIN (
    SELECT order_date, SUM(revenue) AS dbt_revenue
    FROM {{ ref('fct_orders') }}
    GROUP BY order_date
) t ON s.order_date = t.order_date
WHERE ABS(s.source_revenue - t.dbt_revenue) > 0.01;
```

---

## Useful dbt Commands

```bash
# Compile and test all models
dbt build

# Run only modified models + downstream
dbt build --select state:modified+

# Run specific model and its upstream dependencies
dbt build --select +fct_orders

# Run tests only
dbt test --select tag:gold

# Generate and serve documentation
dbt docs generate && dbt docs serve

# Show model dependencies
dbt ls --select +fct_orders --output path

# Snapshot (SCD Type 2)
dbt snapshot

# Seed reference data
dbt seed

# Run with deferred state (CI optimization)
dbt build --defer --state prod-artifacts/ --select state:modified+
```
