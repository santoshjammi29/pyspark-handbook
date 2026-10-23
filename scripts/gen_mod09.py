"""
Module 09: Databricks Platform, Unity Catalog, Security & FinOps (Notebooks 086 - 095)
"""

import re
from pathlib import Path
from notebook_builder import build_notebook, write_notebook

MODULE_FOLDER = "09_unity_catalog_and_security"
MODULE_NAME = "Module 09: Databricks Platform, Unity Catalog, Security & FinOps"

def sanitize_filename(nb_id: str, title: str) -> str:
    cleaned = title.lower().replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return f"{nb_id}_{cleaned}.py"

def generate(base_dir: Path):
    out_dir = base_dir / "notebooks" / MODULE_FOLDER
    out_dir.mkdir(parents=True, exist_ok=True)

    notebooks = [
        {
            "nb_id": "086",
            "title": "Unity Catalog Three-Tier Namespace: Catalogs, Schemas and Managed vs External Tables",
            "difficulty": "Intermediate",
            "business_scenario": "An enterprise migrates from the legacy Hive Metastore (`hive_metastore.default`) to Databricks Unity Catalog. Platform administrators need to establish a secure 3-tier namespace (`catalog.schema.table`) separating dev, staging, and prod environments, and differentiate between Managed and External tables.",
            "problem_statement": "Master Unity Catalog architecture: create 3-tier namespaces (`catalog.schema.table`), configure Managed tables (storage lifecycle managed by Databricks) vs External tables (pointing to existing cloud object storage paths), grant access permissions, and query table metadata.",
            "architectural_notes": "Unity Catalog introduces a centralized governance model across workspaces using a 3-tier namespace: `catalog` -> `schema` -> `table/view/volume`. Dropping a Managed table deletes both metadata and underlying Delta files; dropping an External table removes ONLY metadata, preserving underlying cloud storage data files.",
            "sections": [
                {
                    "title": "Configuring 3-Tier Namespace Hierarchy in SQL",
                    "explanation": "Create catalogs and schemas representing development and production environments.",
                    "code": """# SQL Blueprint for Unity Catalog 3-Tier Hierarchy:
uc_setup_sql = \"\"\"
-- 1. Create Catalogs for environment isolation
CREATE CATALOG IF NOT EXISTS enterprise_dev COMMENT 'Development Sandbox Catalog';
CREATE CATALOG IF NOT EXISTS enterprise_prod COMMENT 'Production Analytical Catalog';

-- 2. Create Schemas within Catalog
CREATE SCHEMA IF NOT EXISTS enterprise_dev.finance COMMENT 'Finance development schema';
CREATE SCHEMA IF NOT EXISTS enterprise_dev.marketing COMMENT 'Marketing development schema';

-- 3. Set current active context
USE CATALOG enterprise_dev;
USE SCHEMA finance;
\"\"\"

print("Unity Catalog 3-Tier DDL Hierarchy defined:")
print(uc_setup_sql)"""
                },
                {
                    "title": "Comparing Managed vs External Tables",
                    "explanation": "Illustrate differences in storage lifecycle and DDL syntax.",
                    "code": """# Managed Table: Storage location managed by Unity Catalog metastore root
# CREATE TABLE enterprise_dev.finance.t_managed_invoices (
#     invoice_id STRING,
#     amount DOUBLE
# ) USING DELTA;

# External Table: References explicit cloud storage location (S3 / ADLS / GCS)
# CREATE TABLE enterprise_dev.finance.t_external_invoices (
#     invoice_id STRING,
#     amount DOUBLE
# ) USING DELTA
# LOCATION 's3://my-enterprise-data-bucket/finance/invoices/';

table_comparison = [
    ("Managed Table", "Managed by Unity Catalog root", "Both metadata AND physical data files are DELETED"),
    ("External Table", "Custom user-supplied S3/ADLS/GCS URL", "Metadata deleted; physical Parquet files are PRESERVED")
]

schema = "table_type STRING, storage_lifecycle STRING, drop_table_behavior STRING"
df_comp = spark.createDataFrame(table_comparison, schema)
df_comp.show(truncate=False)"""
                },
                {
                    "title": "Granting Access Control Privileges",
                    "explanation": "Apply role-based access control grants in Unity Catalog.",
                    "code": """# Production Grant Examples:
# GRANT USAGE ON CATALOG enterprise_dev TO `data_engineers`;
# GRANT SELECT, MODIFY ON SCHEMA enterprise_dev.finance TO `data_engineers`;
# GRANT SELECT ON TABLE enterprise_dev.finance.t_managed_invoices TO `finance_analysts`;

print("Unity Catalog RBAC privilege model mapped.")"""
                }
            ],
            "key_takeaways": [
                "Unity Catalog replaces the legacy 2-tier Hive Metastore with a structured 3-tier namespace (`catalog.schema.table`).",
                "Managed tables manage both file storage and metadata; dropping them purges physical data files.",
                "External tables decouple metadata from underlying cloud storage, preserving files upon DROP."
            ],
            "interview_questions": [
                "What is the difference between Managed Tables and External Tables in Databricks Unity Catalog?",
                "What happens to underlying data files when an External Table is dropped versus a Managed Table?",
                "Why is a 3-tier namespace superior to the legacy 2-tier Hive Metastore for enterprise multi-tenancy?"
            ]
        },
        {
            "nb_id": "087",
            "title": "Unity Catalog Volumes for Unstructured and Semi-Structured Data Governance",
            "difficulty": "Intermediate",
            "business_scenario": "A machine learning team manages non-tabular files (PDF documents, medical images, audio files, and model weights) in cloud storage. Legacy DBFS mounts lack row/file-level access control. The platform requires Unity Catalog Volumes for secure governance.",
            "problem_statement": "Master Unity Catalog Volumes: compare Managed Volumes vs External Volumes, manage files inside volumes using `dbutils.fs` and standard POSIX `/Volumes/catalog/schema/volume` paths, apply governance privileges, and ingest raw documents into Spark.",
            "architectural_notes": "Unity Catalog Volumes govern non-tabular files (CSVs, images, PDFs, models) alongside tabular tables. Unlike legacy DBFS mounts that grant broad workspace-wide access, Volumes enforce fine-grained access control using the standard 3-tier namespace (`/Volumes/catalog/schema/volume_name/`).",
            "sections": [
                {
                    "title": "Unity Catalog Volumes DDL Specification",
                    "explanation": "Create Managed and External Volumes within schemas.",
                    "code": """# Volume DDL Syntax:
volume_sql = \"\"\"
-- Managed Volume: Databricks manages underlying storage path
CREATE VOLUME IF NOT EXISTS enterprise_dev.finance.raw_receipts_managed
COMMENT 'Managed volume storing customer invoice PDFs and scans';

-- External Volume: Governs existing cloud bucket directory
CREATE EXTERNAL VOLUME IF NOT EXISTS enterprise_dev.finance.partner_drops_external
LOCATION 's3://vendor-inbound-bucket/claims/'
COMMENT 'External volume pointing to vendor claims inbox';
\"\"\"

print("Volume DDL defined:")
print(volume_sql)"""
                },
                {
                    "title": "Interacting with Volumes via POSIX Paths and DBUtils",
                    "explanation": "Read and write files directly using the unified /Volumes path.",
                    "code": """# Databricks supports standard POSIX path access in Python:
# volume_file_path = "/Volumes/enterprise_dev/finance/raw_receipts_managed/invoice_1001.pdf"
# with open(volume_file_path, "rb") as f:
#     pdf_bytes = f.read()

# Using dbutils.fs on Volumes:
# dbutils.fs.ls("/Volumes/enterprise_dev/finance/raw_receipts_managed")

print("Volumes provide direct POSIX file path access for Python libraries (OpenCV, PIL, PyPDF2)!")"""
                },
                {
                    "title": "Governing File Access with Unity Catalog Permissions",
                    "explanation": "Grant read and write privileges on Volumes.",
                    "code": """# Access Control Commands for Volumes:
# GRANT READ VOLUME ON VOLUME enterprise_dev.finance.raw_receipts_managed TO `finance_auditors`;
# GRANT WRITE VOLUME ON VOLUME enterprise_dev.finance.raw_receipts_managed TO `data_engineers`;

print("Fine-grained volume access control mapped.")"""
                }
            ],
            "key_takeaways": [
                "Unity Catalog Volumes extend data governance to non-tabular files (PDFs, images, CSVs, models).",
                "Volumes provide standard POSIX path syntax (`/Volumes/catalog/schema/volume/file.ext`) compatible with Python libraries.",
                "Volumes deprecate insecure legacy DBFS mounts by bringing unified access controls and audit logs."
            ],
            "interview_questions": [
                "What is a Unity Catalog Volume and why was it introduced to replace legacy DBFS mounts?",
                "What is the difference between a Managed Volume and an External Volume?",
                "How do data scientists access files inside a Volume using standard Python `open()`?"
            ]
        },
        {
            "nb_id": "088",
            "title": "Dynamic Row-Level Security (RLS) Using Row Filters and User Functions",
            "difficulty": "Advanced",
            "business_scenario": "A multinational sales organization maintains a single centralized `sales_pipeline` table. Regional sales reps must only see deals within their assigned territory (US reps only see US deals; EU reps only see EU deals), while executive managers see all global records.",
            "problem_statement": "Implement dynamic Row-Level Security (RLS) in Databricks Unity Catalog: create a SQL user-defined filter function using `is_account_group_member()` and `current_user()`, apply the filter to a table using `ALTER TABLE ... SET ROW FILTER`, and verify secure row filtering.",
            "architectural_notes": "Unity Catalog Row Filters evaluate dynamically at query compilation time. Catalyst injects the boolean filter function into the query plan, ensuring unauthorized rows are filtered out at the storage scan level before reaching memory.",
            "sections": [
                {
                    "title": "Defining SQL Row Filter User-Defined Function",
                    "explanation": "Create function evaluating user group membership against row region.",
                    "code": """# Row Filter Function Blueprint:
rls_sql = \"\"\"
CREATE OR REPLACE FUNCTION enterprise_prod.sales.region_row_filter(region STRING)
RETURN IF(
    is_account_group_member('global_sales_executives'),  -- Execs see all rows
    true,
    IF(is_account_group_member('us_sales_reps') AND region = 'US', true,
    IF(is_account_group_member('eu_sales_reps') AND region = 'EU', true, false))
);
\"\"\"

print("Row Filter SQL Function:")
print(rls_sql)"""
                },
                {
                    "title": "Applying Row Filter to Production Table",
                    "explanation": "Attach filter function to table using SET ROW FILTER DDL.",
                    "code": """# Attach filter function to target table:
# ALTER TABLE enterprise_prod.sales.t_deals
# SET ROW FILTER enterprise_prod.sales.region_row_filter ON (deal_region);

print("Table successfully protected with dynamic Row Filter.")"""
                },
                {
                    "title": "Simulating Dynamic Row Filtering Locally in PySpark",
                    "explanation": "Simulate the Catalyst row filtering evaluation logic.",
                    "code": """# Simulating current user context and dynamic row filter
deals = [
    ("DEAL-1", "US", 150000.0, "Acme Corp"),
    ("DEAL-2", "EU", 220000.0, "Bavaria Ltd"),
    ("DEAL-3", "APAC", 95000.0, "Tokyo Corp")
]
df_deals = spark.createDataFrame(deals, ["deal_id", "deal_region", "value", "account"])

# Simulate user who belongs to 'us_sales_reps'
user_group = "us_sales_reps"

def simulate_rls_filter(df, active_group):
    if active_group == "global_sales_executives":
        return df
    elif active_group == "us_sales_reps":
        return df.filter(F.col("deal_region") == "US")
    elif active_group == "eu_sales_reps":
        return df.filter(F.col("deal_region") == "EU")
    return df.filter("1 = 0") # Deny all

df_filtered_user = simulate_rls_filter(df_deals, user_group)
print(f"Query Result for user in group '{user_group}':")
df_filtered_user.show()"""
                }
            ],
            "key_takeaways": [
                "Unity Catalog Row Filters enforce data isolation on single conformed tables without creating fragmented regional views.",
                "Row filters are defined via SQL UDFs using `is_account_group_member()` and `current_user()`.",
                "Filters are injected into the Catalyst optimizer plan, ensuring performance optimization and predicate pushdown."
            ],
            "interview_questions": [
                "How does Unity Catalog Row-Level Security differ from creating separate regional SQL views?",
                "What happens if an unauthorized user attempts to query a table protected by a Row Filter?",
                "How does Catalyst Optimizer optimize queries with Row Filters applied?"
            ]
        },
        {
            "nb_id": "089",
            "title": "Dynamic Column Masking for PII Protection and Compliance",
            "difficulty": "Intermediate",
            "business_scenario": "A medical insurance database stores member Social Security Numbers (SSN), full names, and email addresses. Customer service representatives must only see masked SSNs (`***-**-1234`), while compliance officers see cleartext.",
            "problem_statement": "Implement dynamic Column Masking in Databricks Unity Catalog: create a SQL masking function using `is_account_group_member()`, apply the mask to sensitive columns using `ALTER TABLE ... ALTER COLUMN ... SET MASK`, and verify dynamic redaction.",
            "architectural_notes": "Column Masks evaluate dynamically at query execution. If a user is not in the authorized administrative group, the masking function replaces sensitive column data with a masked pattern (e.g. SHA256 hash or asterisks) before returning results.",
            "sections": [
                {
                    "title": "Defining SQL Column Masking Function",
                    "explanation": "Create SQL function that returns cleartext for compliance officers and redacted strings for others.",
                    "code": """# SQL Mask Function Blueprint:
mask_sql = \"\"\"
CREATE OR REPLACE FUNCTION enterprise_prod.governance.ssn_mask(ssn STRING)
RETURN IF(
    is_account_group_member('compliance_officers'),
    ssn,  -- Cleartext for compliance officers
    CONCAT('***-**-', SUBSTRING(ssn, 8, 4)) -- Redacted for general analysts
);
\"\"\"

print("Column Masking SQL Function:")
print(mask_sql)"""
                },
                {
                    "title": "Attaching Column Mask to Table Column",
                    "explanation": "Attach mask function using SET MASK DDL.",
                    "code": """# Apply mask to table:
# ALTER TABLE enterprise_prod.hr.t_members
# ALTER COLUMN ssn SET MASK enterprise_prod.governance.ssn_mask;

print("SSN column masked with dynamic security policy.")"""
                },
                {
                    "title": "Simulating Dynamic Column Masking in PySpark",
                    "explanation": "Demonstrate masking transformations in standard PySpark.",
                    "code": """members = [
    ("M-101", "Alice Smith", "123-45-6789", "alice@corp.com"),
    ("M-102", "Bob Jones", "987-65-4321", "bob@corp.com")
]
df_members = spark.createDataFrame(members, ["member_id", "name", "ssn", "email"])

# Simulate non-compliance user session
is_compliance = False

df_masked = df_members.withColumn(
    "ssn",
    F.when(F.lit(is_compliance), F.col("ssn"))
     .otherwise(F.concat(F.lit("***-**-"), F.substring(F.col("ssn"), 8, 4)))
).withColumn(
    "email",
    F.when(F.lit(is_compliance), F.col("email"))
     .otherwise(F.regexp_replace(F.col("email"), r"(^.).*(@.*$)", "$1***$2"))
)

print(f"Masked Output for Standard Analyst (is_compliance={is_compliance}):")
df_masked.show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "Column Masking anonymizes sensitive attributes dynamically without mutating underlying storage files.",
                "Single table serves both authorized compliance officers (cleartext) and general analysts (masked).",
                "Column masks cannot be bypassed by copying or selecting into another table without proper privileges."
            ],
            "interview_questions": [
                "What is the difference between dynamic Column Masking and permanent hashing at write time?",
                "Can a user bypass a Column Mask by executing `SELECT * INTO new_table FROM masked_table`?",
                "How do Column Masks interact with downstream aggregations (e.g. `COUNT(DISTINCT ssn)`)?"
            ]
        },
        {
            "nb_id": "090",
            "title": "PII Encryption, Salted Hashing and AES Decryption (aes_encrypt, sha2)",
            "difficulty": "Advanced",
            "business_scenario": "A payment processing platform stores credit card Primary Account Numbers (PANs). PCI-DSS regulatory standards mandate that card numbers must be cryptographically encrypted at rest using AES-256 with managed secret keys and salted hashes for tokenization.",
            "problem_statement": "Master cryptographic security in PySpark: encrypt sensitive columns using `aes_encrypt()`, decrypt using `aes_decrypt()`, implement irreversible salted hashing with `sha2()`, and manage cryptographic keys securely.",
            "architectural_notes": "Spark SQL built-in `aes_encrypt(expr, key, mode, padding)` provides hardware-accelerated AES encryption. Using a secret cryptographic salt prevents rainbow table attacks on hashed columns.",
            "sections": [
                {
                    "title": "Generating Credit Card Transactions",
                    "explanation": "Create sensitive payment records requiring PCI-DSS encryption.",
                    "code": """cards = [
    ("TXN-1", "4111111111111234", 150.00),
    ("TXN-2", "5500000000005678", 320.50)
]

schema = "txn_id STRING, card_number STRING, amount DOUBLE"
df_cards = spark.createDataFrame(cards, schema)
df_cards.show()"""
                },
                {
                    "title": "AES Encryption and Salted Hashing",
                    "explanation": "Encrypt card numbers with AES-128/256 and generate tokenized hash.",
                    "code": """# 16-byte key for AES-128 (in production, retrieved via dbutils.secrets.get)
aes_key = "1234567890123456"
salt_token = "SECURE_ENTERPRISE_SALT_2023"

# 1. AES Encrypt (produces binary ciphertext)
# 2. Salted SHA-256 Tokenization (irreversible pseudonymization)
df_secured = (
    df_cards
    .withColumn(
        "encrypted_card_bin",
        F.expr(f"aes_encrypt(card_number, '{aes_key}')")
    )
    .withColumn(
        "tokenized_card_hash",
        F.sha2(F.concat(F.col("card_number"), F.lit(salt_token)), 256)
    )
    .drop("card_number") # Drop cleartext immediately!
)

print("Secured Payment Table (Cleartext PAN eliminated):")
df_secured.show(truncate=False)"""
                },
                {
                    "title": "Decryption for Authorized Settlement Workflow",
                    "explanation": "Decrypt ciphertext back to cleartext using aes_decrypt in authorized settlement service.",
                    "code": """# Decrypt binary ciphertext back to string
df_decrypted = df_secured.withColumn(
    "decrypted_card_number",
    F.expr(f"cast(aes_decrypt(encrypted_card_bin, '{aes_key}') AS STRING)")
)

print("Decrypted Payment Records (Authorized Session):")
df_decrypted.select("txn_id", "decrypted_card_number", "amount", "tokenized_card_hash").show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "`aes_encrypt` and `aes_decrypt` provide native, high-speed cryptographic protection directly in Spark SQL.",
                "Salted hashing (`sha2(concat(col, salt))`) enables irreversible tokenization resistant to rainbow table attacks.",
                "Never store encryption keys in notebook code—always inject them dynamically via secret scopes."
            ],
            "interview_questions": [
                "What is the difference between reversible encryption (AES) and irreversible tokenization (SHA-256)?",
                "Why is salting mandatory when hashing identifiers like emails or credit card numbers?",
                "How do you rotate AES encryption keys on multi-terabyte historical Delta tables?"
            ]
        },
        {
            "nb_id": "091",
            "title": "Databricks Secret Scopes and Secure Credential Injection via dbutils.secrets",
            "difficulty": "Intermediate",
            "business_scenario": "A data integration pipeline connects to cloud databases, external APIs (Stripe, Salesforce), and message queues. Developers must never hardcode API keys or passwords in notebooks, Git repositories, or logs.",
            "problem_statement": "Master secret management in Databricks: create Azure Key Vault / AWS Secrets Manager backed secret scopes, retrieve secrets safely via `dbutils.secrets.get()`, understand automatic secret redaction in driver stdout, and pass credentials into JDBC and REST APIs securely.",
            "architectural_notes": "Databricks Secret Scopes store encrypted key-value pairs backed by Databricks internal storage or cloud key vaults (Azure Key Vault / AWS KMS). When `dbutils.secrets.get()` is called, Databricks replaces the secret with `[REDACTED]` in all notebook output cells and Spark logs, preventing credential leakage.",
            "sections": [
                {
                    "title": "Secret Scope Creation Blueprint (CLI & Cloud Key Vault)",
                    "explanation": "Review commands to configure Databricks Secret Scopes.",
                    "code": """# Databricks CLI Secret Scope Creation:
# 1. Create Databricks-backed scope:
#    databricks secrets create-scope finance_scope
# 2. Store a secret:
#    databricks secrets put-secret finance_scope stripe_api_key
# 3. List available scopes:
#    databricks secrets list-scopes

print("Databricks secret scope management commands reviewed.")"""
                },
                {
                    "title": "Demonstrating Secure Secret Retrieval and Output Redaction",
                    "explanation": "Retrieve secrets via dbutils.secrets.get and observe automatic redaction.",
                    "code": """# Safe wrapper for dbutils.secrets across Databricks and local dev
import os

class SafeSecretProvider:
    @staticmethod
    def get_secret(scope: str, key: str) -> str:
        try:
            # Native Databricks environment
            import dbutils
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            # Local / CI fallback using environment variables
            env_var = f"{scope.upper()}_{key.upper()}"
            return os.environ.get(env_var, f"dummy_secret_for_{key}")

api_token = SafeSecretProvider.get_secret("finance_scope", "stripe_api_key")
print(f"Retrieved token value: {api_token[:4]}**** (Never print complete secrets to logs!)")"""
                },
                {
                    "title": "Injecting Credentials into JDBC Connections Safely",
                    "explanation": "Use retrieved credentials in connection configurations without persisting to disk.",
                    "code": """db_pass = SafeSecretProvider.get_secret("dw_scope", "postgres_password")

connection_properties = {
    "user": "etl_service_account",
    "password": db_pass,
    "driver": "org.postgresql.Driver"
}

print("Database connection properties assembled securely.")"""
                }
            ],
            "key_takeaways": [
                "Never hardcode passwords or API keys in notebooks or Git repositories.",
                "Databricks automatically intercepts and redacts secrets retrieved via `dbutils.secrets.get()` in notebook outputs.",
                "Back secret scopes with Azure Key Vault or AWS Secrets Manager for enterprise audit compliance."
            ],
            "interview_questions": [
                "How does Databricks ensure that secrets retrieved via `dbutils.secrets.get()` are not exposed in stdout logs?",
                "What is the difference between a Databricks-backed secret scope and an Azure Key Vault-backed secret scope?",
                "How do you manage secret scope access control using Databricks Access Control Lists (ACLs)?"
            ]
        },
        {
            "nb_id": "092",
            "title": "FinOps and DBU Cost Calculation Using system.billing.usage System Tables",
            "difficulty": "Advanced",
            "business_scenario": "A Director of Data Engineering needs to track monthly Databricks DBU (Databricks Unit) consumption across business units (Finance, Marketing, ML). The FinOps team must identify expensive rogue jobs, untagged clusters, and cost spikes.",
            "problem_statement": "Query Databricks Unity Catalog system billing tables: analyze `system.billing.usage`, compute DBU consumption by job, cluster, and user, calculate estimated dollar expenditures, and build an automated FinOps cost anomaly detection query in PySpark SQL.",
            "architectural_notes": "Databricks system tables (`system.billing.usage`, `system.compute.clusters`) provide historical operational telemetry. The `system.billing.usage` table records DBU consumption per SKU, account, workspace, cluster, and custom billing tags every hour.",
            "sections": [
                {
                    "title": "Mocking system.billing.usage Telemetry Data",
                    "explanation": "Create representative billing usage records.",
                    "code": """usage_records = [
    ("WS-1", "JOB-101", "Nightly Ingestion", "ALL_PURPOSE", 45.2, "Finance", "2023-11-20"),
    ("WS-1", "JOB-102", "Customer 360", "JOBS", 12.8, "Marketing", "2023-11-20"),
    ("WS-1", "JOB-103", "Fraud Model Training", "DLT", 65.0, "Fraud", "2023-11-20"),
    ("WS-1", None, "Ad-Hoc Data Science", "ALL_PURPOSE", 110.5, "DataScience", "2023-11-20"),
    ("WS-1", "JOB-101", "Nightly Ingestion", "ALL_PURPOSE", 48.0, "Finance", "2023-11-21")
]

schema = "workspace_id STRING, job_id STRING, workload_name STRING, sku_name STRING, dbus DOUBLE, department STRING, usage_date STRING"
df_billing = spark.createDataFrame(usage_records, schema)
df_billing.createOrReplaceTempView("system_billing_usage")
df_billing.show()"""
                },
                {
                    "title": "FinOps DBU and Dollar Cost Calculation by Department",
                    "explanation": "Aggregate DBU consumption and apply average contract DBU rate ($0.30/DBU).",
                    "code": """finops_query = \"\"\"
SELECT 
    department,
    sku_name,
    COUNT(*) AS run_events,
    ROUND(SUM(dbus), 2) AS total_dbus,
    ROUND(SUM(dbus) * 0.30, 2) AS estimated_cost_usd
FROM system_billing_usage
GROUP BY department, sku_name
ORDER BY total_dbus DESC
\"\"\"

df_finops = spark.sql(finops_query)
print("Departmental FinOps Cost Report:")
df_finops.show(truncate=False)"""
                },
                {
                    "title": "Detecting Cost Anomalies and Untagged Workloads",
                    "explanation": "Identify interactive all-purpose cluster usage that should run as scheduled jobs.",
                    "code": """anomaly_query = \"\"\"
SELECT 
    workload_name,
    SUM(dbus) AS total_dbus,
    ROUND(SUM(dbus) * 0.30, 2) AS cost_usd,
    'WARNING: High All-Purpose Compute! Convert to Automated Jobs Cluster to save 60%' AS recommendation
FROM system_billing_usage
WHERE sku_name = 'ALL_PURPOSE' AND job_id IS NULL
GROUP BY workload_name
\"\"\"

spark.sql(anomaly_query).show(truncate=False)"""
                }
            ],
            "key_takeaways": [
                "`system.billing.usage` provides granular operational visibility into DBU spend across workspaces.",
                "Automated Jobs compute is ~60% cheaper than interactive All-Purpose compute for scheduled pipelines.",
                "Enforce custom cluster tags (e.g. `Department`, `CostCenter`, `Environment`) to attribute costs accurately."
            ],
            "interview_questions": [
                "What is a DBU (Databricks Unit) and how is it calculated?",
                "What is the cost difference between All-Purpose Compute and Jobs Compute in Databricks?",
                "How do you query `system.billing.usage` to identify untagged compute resources?"
            ]
        },
        {
            "nb_id": "093",
            "title": "Lakeflow Declarative Pipelines and Delta Live Tables (DLT) Expectations",
            "difficulty": "Advanced",
            "business_scenario": "An enterprise financial lakehouse migrates manual notebook pipelines to Databricks Lakeflow Declarative Pipelines (Delta Live Tables). The platform requires declarative Medallion pipelines with embedded Data Quality Expectations that log, drop, or fail on bad data.",
            "problem_statement": "Master Delta Live Tables (DLT) & Lakeflow Declarative Pipelines: understand the `@dlt.table` and `@dlt.view` decorators, compare the 3 expectation types (`expect`, `expect_or_drop`, `expect_or_fail`), configure streaming tables vs materialized views, and inspect pipeline event logs.",
            "architectural_notes": "Lakeflow Declarative Pipelines (DLT) allow data engineers to define *what* tables to produce rather than *how* to orchestrate them. DLT automatically constructs the DAG, provisions clusters, manages checkpoints, and captures data quality metrics per expectation rule.",
            "sections": [
                {
                    "title": "Lakeflow / DLT Declarative Pipeline Blueprint",
                    "explanation": "Display the production PySpark syntax for Delta Live Tables with expectations.",
                    "code": """# Production Delta Live Tables / Lakeflow Pipeline:
dlt_pipeline_code = \"\"\"
import dlt
from pyspark.sql import functions as F

# 1. Bronze Streaming Table (Raw Ingestion)
@dlt.table(
    comment="Raw streaming invoices ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
def invoices_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/enterprise/raw/invoices/")
    )

# 2. Silver Table with Data Quality Expectations
@dlt.table(
    comment="Cleansed invoices with data quality rules"
)
@dlt.expect("valid_invoice_no", "invoice_no IS NOT NULL")
@dlt.expect_or_drop("valid_positive_amount", "amount > 0")
@dlt.expect_or_fail("valid_status", "status IN ('APPROVED', 'PENDING', 'REJECTED')")
def invoices_silver():
    return dlt.read_stream("invoices_raw").select("invoice_no", "amount", "status")

# 3. Gold Materialized View (Business Summary)
@dlt.table(
    comment="Daily aggregated sales summary"
)
def invoices_gold():
    return (
        dlt.read("invoices_silver")
        .groupBy("status")
        .agg(F.sum("amount").alias("total_amount"))
    )
\"\"\"

print("Lakeflow / DLT Declarative Pipeline Blueprint defined:")
print(dlt_pipeline_code)"""
                },
                {
                    "title": "Simulating the 3 DLT Expectation Behaviors in Standard PySpark",
                    "explanation": "Demonstrate expect (log only), expect_or_drop, and expect_or_fail.",
                    "code": """# Mock incoming data with varying levels of quality issues
raw_batch = [
    ("INV-01", 100.0, "APPROVED"),   # Valid
    ("INV-02", -50.0, "APPROVED"),   # Violates positive amount -> DROP
    (None,     250.0, "PENDING"),    # Violates non-null -> LOG (retain row)
    ("INV-03", 500.0, "INVALID_STATE")# Violates valid_status -> FAIL PIPELINE
]

schema = "invoice_no STRING, amount DOUBLE, status STRING"
df_test = spark.createDataFrame(raw_batch, schema)

# Behavior 1: expect_or_drop (Filters out violating rows)
df_clean = df_test.filter(F.col("amount") > 0)
print(f"After expect_or_drop (amount > 0): {df_clean.count()} rows retained.")
df_clean.show()

# Behavior 2: expect_or_fail (Throws exception if any row violates rule)
has_fatal_error = df_clean.filter(~F.col("status").isin(["APPROVED", "PENDING", "REJECTED"])).count() > 0
if has_fatal_error:
    print("ALERT: expect_or_fail triggered! Pipeline run aborted due to invalid status values.")"""
                },
                {
                    "title": "DLT Expectations Cheatsheet",
                    "explanation": "Review the operational behavior of DLT expectation modes.",
                    "code": """exp_modes = [
    ("@dlt.expect('rule', 'condition')", "Logs violation count to event log; retains row in table."),
    ("@dlt.expect_or_drop('rule', 'condition')", "Logs violation; automatically drops invalid rows from target table."),
    ("@dlt.expect_or_fail('rule', 'condition')", "Logs violation; immediately aborts pipeline execution with error.")
]

for mode, behavior in exp_modes:
    print(f" {mode}\\n   -> {behavior}")"""
                }
            ],
            "key_takeaways": [
                "Delta Live Tables (DLT) provides a declarative framework for building reliable Medallion lakehouses.",
                "Use `@dlt.expect` to log metrics, `@dlt.expect_or_drop` to clean data, and `@dlt.expect_or_fail` for strict compliance.",
                "DLT automatically manages streaming checkpoints, schema evolution, and optimal cluster sizing."
            ],
            "interview_questions": [
                "What is the difference between a Streaming Table and a Materialized View in Delta Live Tables?",
                "Explain the 3 types of DLT Data Quality Expectations and when to use each.",
                "How does DLT handle pipeline failure recovery compared to standard Databricks Jobs?"
            ]
        },
        {
            "nb_id": "094",
            "title": "Databricks Asset Bundles (DABs) for Multi-Environment CI/CD Automation",
            "difficulty": "Advanced",
            "business_scenario": "An enterprise development team deploys 50 Databricks workflows and pipelines across development, staging, and production workspaces. Manual UI deployments cause configuration drift, broken paths, and deployment bugs.",
            "problem_statement": "Master Databricks Asset Bundles (DABs): configure `databricks.yml`, define multi-environment deployment targets (dev, staging, prod), configure parameterized workflows, and automate CI/CD validation (`bundle validate`, `bundle deploy`, `bundle run`).",
            "architectural_notes": "Databricks Asset Bundles (DABs) provide Infrastructure-as-Code (IaC) for Databricks projects. A YAML bundle definition (`databricks.yml`) manages notebooks, pipelines, jobs, and permissions, allowing automated deployments via GitHub Actions or GitLab CI.",
            "sections": [
                {
                    "title": "Designing databricks.yml Bundle Configuration",
                    "explanation": "Create production-ready YAML specification for multi-environment deployment.",
                    "code": """bundle_yaml_content = \"\"\"
bundle:
  name: enterprise_spark_masterclass

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-dev.azuredatabricks.net
      root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}

  staging:
    mode: production
    workspace:
      host: https://adb-staging.azuredatabricks.net
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}

  prod:
    mode: production
    workspace:
      host: https://adb-prod.azuredatabricks.net
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
\"\"\"

print("databricks.yml configuration structure:")
print(bundle_yaml_content)"""
                },
                {
                    "title": "Defining Workflow Job Resource in Bundle",
                    "explanation": "Declare jobs, schedules, and cluster settings in resources/jobs.yml.",
                    "code": """job_resource_yaml = \"\"\"
resources:
  jobs:
    nightly_etl_pipeline:
      name: "[${bundle.target}] Nightly Financial ETL"
      tasks:
        - task_key: run_ingestion
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ../notebooks/01_ingest.py
        - task_key: run_transformation
          depends_on:
            - task_key: run_ingestion
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ../notebooks/02_transform.py
      job_clusters:
        - job_cluster_key: etl_cluster
          new_cluster:
            spark_version: 14.3.x-scala2.12
            node_type_id: Standard_D4ds_v5
            num_workers: 2
\"\"\"

print("resources/jobs.yml definition structure:")
print(job_resource_yaml)"""
                },
                {
                    "title": "DABs CLI Command Lifecycle Cheatsheet",
                    "explanation": "Review developer CLI workflows for DABs.",
                    "code": """commands = [
    ("databricks bundle init", "Initializes a new bundle template project."),
    ("databricks bundle validate", "Validates YAML syntax, resource references, and paths."),
    ("databricks bundle deploy -t dev", "Deploys resources to target environment (dev sandbox)."),
    ("databricks bundle run -t dev nightly_etl_pipeline", "Triggers immediate run of the deployed workflow in dev.")
]

for cmd, desc in commands:
    print(f" $ {cmd}\\n   -> {desc}")"""
                }
            ],
            "key_takeaways": [
                "Databricks Asset Bundles (DABs) implement Infrastructure-as-Code (IaC) for Databricks projects.",
                "Target environments (dev, staging, prod) enable seamless promotion via GitHub Actions CI/CD.",
                "DABs eliminate manual workspace UI changes, preventing configuration drift."
            ],
            "interview_questions": [
                "What is a Databricks Asset Bundle (DAB) and what problem does it solve in enterprise MLOps/DataOps?",
                "What is the difference between `mode: development` and `mode: production` in `databricks.yml`?",
                "How do you parameterize cluster configurations across dev and prod targets in DABs?"
            ]
        },
        {
            "nb_id": "095",
            "title": "Semantic Layer Modeling and Databricks Metric Views for BI and AI Agents",
            "difficulty": "Advanced",
            "business_scenario": "Different business teams calculate 'Customer Churn' and 'Net Revenue' using conflicting SQL formulas, producing discrepancies between executive reports and Databricks Genie AI natural language answers.",
            "problem_statement": "Build an enterprise Semantic Layer using Databricks Metric Views: define centralized business metrics, dimensions, hierarchies, and time grains, ensuring identical calculations across Power BI, Tableau, and Databricks Genie AI agents.",
            "architectural_notes": "A Semantic Layer translates physical Delta tables into business concepts (Measures and Dimensions). Databricks Metric Views provide a standardized semantic definition where metrics (e.g. ARR, Churn Rate) are defined once and computed dynamically at any aggregation grain.",
            "sections": [
                {
                    "title": "Creating Physical Sales Fact and Dimension Tables",
                    "explanation": "Create underlying analytical data for semantic modeling.",
                    "code": """sales_records = [
    ("S-01", "2023-11-01", "US", "Electronics", 1200.0, 100.0),
    ("S-02", "2023-11-02", "US", "Home", 450.0, 50.0),
    ("S-03", "2023-11-03", "EU", "Electronics", 800.0, 80.0),
    ("S-04", "2023-11-04", "EU", "Electronics", 950.0, 95.0)
]

schema = "sale_id STRING, sale_date STRING, country STRING, category STRING, gross_revenue DOUBLE, discount DOUBLE"
df_sales = spark.createDataFrame(sales_records, schema)
df_sales.createOrReplaceTempView("fact_sales_semantic")
df_sales.show()"""
                },
                {
                    "title": "Defining Semantic Metric View with Standard Measures",
                    "explanation": "Define conformed measures: Gross Revenue, Total Discounts, and Net Revenue.",
                    "code": """semantic_view_sql = \"\"\"
CREATE OR REPLACE VIEW v_semantic_sales_metrics AS
SELECT 
    sale_date,
    country,
    category,
    -- Standard Measures:
    gross_revenue,
    discount,
    (gross_revenue - discount) AS net_revenue,
    ROUND(((discount) / gross_revenue) * 100, 1) AS discount_pct
FROM fact_sales_semantic
\"\"\"

spark.sql(semantic_view_sql)
print("Semantic View 'v_semantic_sales_metrics' created.")"""
                },
                {
                    "title": "Querying Metrics Consistently Across Any Dimensional Grain",
                    "explanation": "Demonstrate that Net Revenue computes identically across country or category.",
                    "code": """# Query 1: By Country
print("Metric Aggregation by Country:")
spark.sql(\"\"\"
SELECT 
    country,
    ROUND(SUM(gross_revenue), 2) AS total_gross,
    ROUND(SUM(net_revenue), 2) AS total_net
FROM v_semantic_sales_metrics
GROUP BY country
\"\"\").show()

# Query 2: By Category
print("Metric Aggregation by Category:")
spark.sql(\"\"\"
SELECT 
    category,
    ROUND(SUM(gross_revenue), 2) AS total_gross,
    ROUND(SUM(net_revenue), 2) AS total_net
FROM v_semantic_sales_metrics
GROUP BY category
\"\"\").show()"""
                }
            ],
            "key_takeaways": [
                "A Semantic Layer standardizes business metrics (Net Revenue, Churn, Margin) across the entire company.",
                "Metric Views allow BI tools and Databricks Genie AI agents to query certified metrics without SQL discrepancies.",
                "Defining measures once ensures compliance and trust across executive reporting."
            ],
            "interview_questions": [
                "What is a Semantic Layer and why is it critical when deploying AI agents like Databricks Genie?",
                "How does a Metric View differ from a standard SQL view?",
                "How do you prevent 'metric drift' across multiple self-service BI tools?"
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
