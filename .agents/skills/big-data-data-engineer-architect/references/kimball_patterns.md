# Kimball Dimensional Modelling Patterns

## Core Concepts

### The Four-Step Dimensional Design Process
1. **Select the business process** — identify the measurable event (e.g., sales order, claim submission)
2. **Declare the grain** — the most atomic level a fact table row represents (e.g., one order line item)
3. **Identify the dimensions** — the who/what/when/where/why/how context of each fact
4. **Identify the facts** — the numeric, additive measures (quantity, amount, duration)

> **RULE**: Never mix grains in a single fact table. If you need different grains, create separate fact tables.

---

## Fact Table Types

| Type | Description | Example | Key Characteristic |
|---|---|---|---|
| **Transaction** | One row per event | Sales line item | Sparse, highest volume |
| **Periodic Snapshot** | One row per period per entity | Monthly account balance | Predictable, dense |
| **Accumulating Snapshot** | One row per instance of a process | Insurance claim lifecycle | Multiple date FKs updated |
| **Factless** | Records events with no measure | Student attendance | Junction table semantics |

### Fact Additivity Rules
- **Fully additive** — can sum across all dimensions (revenue, quantity)
- **Semi-additive** — can sum across some dimensions (balance — not across time)
- **Non-additive** — ratios, percentages, unit prices (use in calculations only)

---

## Dimension Design

### Slowly Changing Dimensions (SCD)

| Type | Behaviour | Implementation | Use Case |
|---|---|---|---|
| **SCD 0** | Never changes | Single row | Birth date, original contract date |
| **SCD 1** | Overwrite | UPDATE in place | Fix typos, non-historical attributes |
| **SCD 2** | Add new row | Surrogate key + effective_date + expiry_date + is_current | Historical tracking (standard) |
| **SCD 3** | Add column | prior_value column | Limited history (2 versions max) |
| **SCD 4** | Mini-dimension | Separate fast-changing attributes | Demographics, segment changes |
| **SCD 6** | Hybrid 1+2+3 | SCD2 rows + current value column | Both current and historical values |
| **SCD 7** | Dual foreign keys | FK to SCD2 + FK to current row | Flexible reporting |

### SCD Type 2 — Standard Implementation (Delta Lake)
```sql
MERGE INTO dim_customer AS target
USING (
    SELECT
        src.*,
        COALESCE(tgt.customer_sk, -1) AS existing_sk,
        CASE
            WHEN tgt.customer_sk IS NULL THEN 'INSERT'
            WHEN tgt.email != src.email OR tgt.address != src.address THEN 'UPDATE'
            ELSE 'NO_CHANGE'
        END AS change_type
    FROM stage_customer src
    LEFT JOIN dim_customer tgt
        ON src.customer_bk = tgt.customer_bk AND tgt.is_current = TRUE
) AS source
ON target.customer_sk = source.existing_sk AND source.change_type = 'UPDATE'
WHEN MATCHED AND source.change_type = 'UPDATE' THEN
    UPDATE SET is_current = FALSE, expiry_date = current_date() - 1
WHEN NOT MATCHED AND source.change_type IN ('INSERT', 'UPDATE') THEN
    INSERT (customer_bk, email, address, effective_date, expiry_date, is_current)
    VALUES (source.customer_bk, source.email, source.address, current_date(), '9999-12-31', TRUE);
```

---

## Dimension Patterns

### Conformed Dimensions
- Shared across multiple fact tables and business processes
- Same surrogate key, same attribute definitions
- Enables cross-process "drill-across" queries
- **Examples**: Date, Customer, Product, Geography, Employee

### Degenerate Dimension
- Dimension key stored directly on fact table with no corresponding dimension table
- **Examples**: Order number, invoice number, transaction ID

### Junk Dimension
- Group low-cardinality flags and indicators into a single dimension
- Reduces fact table width
- **Examples**: Order_flags_dim (is_online, is_returned, is_gift)

### Role-Playing Dimension
- Same dimension table used multiple times with different aliases
- **Example**: Date dim as order_date_dim, ship_date_dim, delivery_date_dim

### Bridge Table (Multi-valued Dimension)
- Resolves many-to-many relationships (e.g., patient-diagnosis, account-customer)
- Includes a weighting factor when applicable

---

## Bus Matrix

A grid showing which dimensions are shared across business processes:

| Business Process | Date | Customer | Product | Geography | Salesperson |
|---|:---:|:---:|:---:|:---:|:---:|
| Sales Orders | ✓ | ✓ | ✓ | ✓ | ✓ |
| Returns | ✓ | ✓ | ✓ | ✓ | |
| Inventory Snapshot | ✓ | | ✓ | ✓ | |
| Customer Service | ✓ | ✓ | | ✓ | ✓ |

> Build the bus matrix FIRST before designing any schemas.

---

## Surrogate Key Management

- Always use system-generated surrogate keys (integer or UUID) — never use business keys as PKs
- Store the original business/natural key as `customer_bk` or `customer_nk`
- Use a dedicated **key management** table or sequence for consistent generation
- In Spark/Delta Lake, use `monotonically_increasing_id()` with caution — prefer sequence tables

---

## Date Dimension — Standard Attributes

```sql
CREATE TABLE dim_date (
    date_sk         INT          NOT NULL,  -- YYYYMMDD integer
    date_actual     DATE         NOT NULL,
    year            SMALLINT,
    quarter         TINYINT,
    month           TINYINT,
    month_name      VARCHAR(10),
    week_of_year    TINYINT,
    day_of_week     TINYINT,
    day_name        VARCHAR(10),
    is_weekend      BOOLEAN,
    is_public_holiday BOOLEAN,
    fiscal_year     SMALLINT,
    fiscal_quarter  TINYINT,
    fiscal_month    TINYINT,
    PRIMARY KEY (date_sk)
);
```

---

## Data Vault 2.0 (Complementary to Kimball)

Use Data Vault when:
- Source systems are numerous and frequently changing
- Auditability and full history are mandatory
- Integration layer needs to decouple from business rules

| Component | Purpose | Example |
|---|---|---|
| **Hub** | Unique business keys | HUB_CUSTOMER (customer_bk) |
| **Link** | Relationships between hubs | LNK_ORDER_CUSTOMER |
| **Satellite** | Descriptive attributes + history | SAT_CUSTOMER_DETAILS |
| **PIT Table** | Point-in-time reconstruction | PIT_CUSTOMER |
| **Bridge Table** | Flatten multiple links for query performance | BRG_ORDER |

> Pattern: Ingest to Data Vault (raw vault) → Apply business rules → Mart (Kimball star schema for consumption)
