# Semantic Layer Design Reference

## What is the Semantic Layer?

The semantic layer is an abstraction layer between the physical data model (Gold/DW) and business consumers (BI tools, APIs, analysts). It translates technical column names into business-friendly metrics with consistent business rules.

---

## Semantic Layer Options

| Tool | Best For | Integration |
|---|---|---|
| **dbt Metrics / MetricFlow** | dbt-native, multi-BI | Any SQL warehouse |
| **Microsoft Fabric Semantic Model** | Power BI + Azure stack | OneLake / DirectLake |
| **Azure Analysis Services (AAS)** | Enterprise SSAS tabular | Azure SQL, Synapse |
| **Power BI Dataset (XMLA)** | Power BI-centric | Power BI Premium |
| **Cube.dev** | API-first, headless BI | Any database |
| **AtScale** | Federated semantic layer | Multi-cloud |
| **Looker (LookML)** | GCP ecosystem | BigQuery, any DB |
| **Dremio Semantic Layer** | Lakehouse + multi-source | Any Iceberg/Delta |

---

## dbt Semantic Layer (MetricFlow)

### Project Structure
```
my_dbt_project/
├── models/
│   ├── staging/          # 1:1 with source tables, minimal transforms
│   ├── intermediate/     # Business logic, joins
│   ├── marts/
│   │   ├── finance/      # Finance domain Gold tables
│   │   └── sales/        # Sales domain Gold tables
├── metrics/              # MetricFlow metric definitions
├── semantic_models/      # Semantic model YAML definitions
├── tests/                # dbt tests
├── macros/               # Jinja macros
├── seeds/                # Reference data CSVs
└── dbt_project.yml
```

### Semantic Model Definition (YAML)
```yaml
# semantic_models/sem_orders.yml
semantic_models:
  - name: orders
    description: "Order transactions semantic model"
    model: ref('fct_orders')
    
    entities:
      - name: order
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id

    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
      - name: order_status
        type: categorical
      - name: sales_channel
        type: categorical

    measures:
      - name: order_total
        agg: sum
        expr: order_amount
      - name: order_count
        agg: count_distinct
        expr: order_id
      - name: avg_order_value
        agg: average
        expr: order_amount
```

### Metric Definition
```yaml
# metrics/revenue_metrics.yml
metrics:
  - name: total_revenue
    label: "Total Revenue"
    description: "Sum of all completed order amounts"
    type: simple
    type_params:
      measure: order_total
    filter: |
      {{ Dimension('order__order_status') }} = 'COMPLETED'

  - name: revenue_mom_growth
    label: "Revenue MoM Growth"
    type: derived
    type_params:
      expr: "(current_period_revenue - prior_period_revenue) / prior_period_revenue"
      metrics:
        - name: total_revenue
          alias: current_period_revenue
        - name: total_revenue
          alias: prior_period_revenue
          offset_window: 1 month
```

---

## Microsoft Fabric Semantic Model

### DirectLake Mode (Recommended)
- Reads directly from Delta tables in OneLake — no data import
- Power BI queries directly on Parquet files in storage
- Best performance for large datasets without duplication

### Key Design Principles

**Table Relationships**
```
dim_date ──── fact_sales ──── dim_customer
                  │
              dim_product
                  │
             dim_geography
```

**DAX Measures (Standards)**
```dax
-- Basic aggregation
Total Revenue = SUM(fact_sales[revenue])

-- Time intelligence
Revenue YTD = CALCULATE([Total Revenue], DATESYTD(dim_date[date_actual]))

Revenue LY = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(dim_date[date_actual]))

Revenue YoY% = 
DIVIDE(
    [Total Revenue] - [Revenue LY],
    [Revenue LY],
    BLANK()
)

-- Filtered measure
Online Revenue = 
CALCULATE([Total Revenue], dim_channel[channel_name] = "Online")

-- Row-level security
-- In RLS role filter expression:
[region_code] = USERPRINCIPALNAME()
```

**Row-Level Security (RLS)**
```dax
-- Dynamic RLS using security table
[email] = USERPRINCIPALNAME()
-- OR for group-based RLS:
[region_code] IN 
    VALUES(security_table[region_code])
```

---

## Headless BI / API-First Semantic Layer

### Cube.dev Schema Example
```javascript
// schema/Orders.js
cube(`Orders`, {
  sql: `SELECT * FROM gold.fact_orders`,
  
  measures: {
    total_revenue: {
      sql: `revenue`,
      type: `sum`,
      format: `currency`
    },
    order_count: {
      type: `count`,
      drillMembers: [id, order_date]
    }
  },
  
  dimensions: {
    id: {
      sql: `order_id`,
      type: `number`,
      primaryKey: true
    },
    order_date: {
      sql: `order_date`,
      type: `time`
    },
    status: {
      sql: `order_status`,
      type: `string`
    }
  },
  
  segments: {
    completed: {
      sql: `${CUBE}.order_status = 'COMPLETED'`
    }
  }
});
```

---

## Semantic Layer Best Practices

1. **Single definition** — Each metric has ONE definition. No duplicates across reports.
2. **Business naming** — Use `Total Revenue` not `sum_ord_amt`
3. **Certified datasets** — Mark approved semantic models as certified in Purview/catalog
4. **Versioning** — Semantic model changes go through code review (dbt in git, AAS in BISM Normalizer)
5. **Documentation** — Every measure has a description and owner
6. **Testing** — Validate metric values against known reference values after any schema change
7. **Lineage** — Maintain end-to-end lineage from source → Bronze → Silver → Gold → Semantic model
