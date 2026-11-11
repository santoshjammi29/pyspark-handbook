# Databricks Unity Catalog Governance, Security & FinOps Reference

This reference covers the 3-tier namespace, Managed vs. External Tables, Managed & External Volumes, Dynamic Row Filters, Dynamic Column Masking, PII Encryption, Secret Scopes, and Billing Telemetry.

---

## 1. 3-Tier Namespace & Securable Object Hierarchy

Unity Catalog replaces legacy Hive metastores with a centralized 3-level namespace across workspaces:

```text
metastore (Unity Catalog Global Instance)
└── catalog (e.g., prod_catalog, dev_catalog)
    └── schema (e.g., raw, silver, gold)
        ├── tables (Managed or External Delta/Parquet)
        ├── views (Standard, Dynamic, Metric Views)
        ├── volumes (Managed or External unstructured files)
        ├── functions (User-Defined Functions, Masking functions)
        └── models (Registered MLflow / GenAI models)
```

### Table Architecture: Managed vs. External
| Property | Managed Tables | External Tables |
| :--- | :--- | :--- |
| **Storage Location** | Root metastore or catalog-managed storage path | Explicit cloud container / bucket path |
| **`DROP TABLE` Behavior** | Deletes **both** metadata AND underlying Parquet files | Deletes **only** metadata; Parquet files remain intact on cloud storage |
| **Use Case** | Internal curated lakehouse tables (Silver/Gold) | Legacy data, shared external systems, Bronze landing |

---

## 2. Unity Catalog Volumes (`/Volumes/`)

Volumes govern non-tabular files (CSVs, JSON, PDFs, images, model weights, audio) alongside tables:

```sql
-- Create managed volume
CREATE VOLUME prod_catalog.raw_landing.telemetry_files;

-- Inspect files via POSIX path or SQL
SELECT * FROM directory('/Volumes/prod_catalog/raw_landing/telemetry_files');
```

```python
# Read unstructured data with standard Python OS tools:
import os

volume_path = "/Volumes/prod_catalog/raw_landing/telemetry_files"
for root, dirs, files in os.walk(volume_path):
    for f in files:
        print(f"Discovered volume file: {f}")
```

---

## 3. Dynamic Row-Level Security (RLS) via Row Filters

Row filters evaluate conditions dynamically at query time based on user group membership:

```sql
-- Step 1: Create deterministic filter function
CREATE OR REPLACE FUNCTION prod_catalog.security.region_row_filter(country_code STRING)
RETURN IF(
    IS_ACCOUNT_GROUP_MEMBER('global_admins') OR 
    (IS_ACCOUNT_GROUP_MEMBER('emea_sales') AND country_code IN ('UK', 'DE', 'FR')) OR
    (IS_ACCOUNT_GROUP_MEMBER('apac_sales') AND country_code IN ('IN', 'SG', 'AU')),
    TRUE,
    FALSE
);

-- Step 2: Apply filter function to table
ALTER TABLE prod_catalog.gold.fact_sales 
SET ROW FILTER prod_catalog.security.region_row_filter ON (country_code);
```

---

## 4. Dynamic Column Masking for PII Protection

Mask sensitive columns (e.g., SSN, credit cards, emails) depending on caller authorization:

```sql
-- Step 1: Create masking function
CREATE OR REPLACE FUNCTION prod_catalog.security.mask_email(email STRING)
RETURN IF(
    IS_ACCOUNT_GROUP_MEMBER('pii_authorized_users'),
    email,
    CONCAT(LEFT(email, 2), '****@', SPLIT(email, '@')[1])
);

-- Step 2: Apply mask to column
ALTER TABLE prod_catalog.gold.dim_customer 
ALTER COLUMN email SET MASK prod_catalog.security.mask_email;
```

---

## 5. PII Encryption & Salted Hashing

```sql
-- AES Encryption with 256-bit key
SELECT 
    customer_id,
    base64(aes_encrypt(credit_card_number, secret('vault_scope', 'aes_encryption_key'))) as encrypted_pan,
    sha2(concat(email, secret('vault_scope', 'salt_secret')), 256) as salted_email_hash
FROM raw.customers;
```

---

## 6. Secret Scopes & Secure Credential Injection

Never hardcode credentials or connection strings in notebooks:

```python
# Fetch secrets via Databricks dbutils:
db_user = dbutils.secrets.get(scope="azure_keyvault", key="database-username")
db_password = dbutils.secrets.get(scope="azure_keyvault", key="database-password")

# Secure JDBC connection:
jdbc_df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://db.corp.net:5432/finance")
    .option("dbtable", "public.ledger")
    .option("user", db_user)
    .option("password", db_password)
    .option("partitionColumn", "ledger_id")
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .option("numPartitions", "16")
    .load()
)
```

---

## 7. FinOps DBU Billing Queries (`system.billing.usage`)

```sql
-- Real-time DBU consumption aggregated by Workspace & SKU
SELECT 
    workspace_id,
    sku_name,
    SUM(usage_quantity) as total_dbus,
    ROUND(SUM(usage_quantity * 0.40), 2) as estimated_cost_usd
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE() - 30
GROUP BY workspace_id, sku_name
ORDER BY total_dbus DESC;
```
