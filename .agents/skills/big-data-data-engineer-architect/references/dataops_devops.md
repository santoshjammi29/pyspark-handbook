# DataOps & DevOps for Data Engineering

## Core Principles

1. **Everything as Code** — Pipelines, schemas, infra, tests, governance rules
2. **Automated Testing** — Unit, integration, data quality, regression
3. **Environment Parity** — Dev, Test, Prod use same code with different config
4. **Automated Deployment** — No manual releases; every merge triggers CI/CD
5. **Observability** — Metrics, logs, lineage, alerts for every pipeline
6. **Trunk-Based Development** — Short-lived feature branches, frequent integration

---

## dbt Project Structure (Production-Ready)

```
my_dbt_project/
├── models/
│   ├── staging/              # 1:1 source replicas, lightweight transforms
│   │   ├── _staging.yml      # source definitions
│   │   ├── stg_orders.sql
│   │   └── stg_customers.sql
│   ├── intermediate/         # Business logic, complex joins
│   │   ├── int_orders_enriched.sql
│   │   └── int_customer_lifetime_value.sql
│   └── marts/
│       ├── sales/
│       │   ├── _sales.yml    # model documentation + tests
│       │   ├── fct_orders.sql
│       │   └── dim_customer.sql
│       └── finance/
├── macros/
│   ├── generate_schema_name.sql   # custom schema naming
│   ├── incremental_merge.sql      # reusable merge macro
│   └── date_spine.sql
├── tests/
│   ├── generic/              # Custom generic tests
│   └── singular/             # One-off data assertions
├── seeds/
│   └── ref_country.csv
├── snapshots/                # SCD Type 2 via dbt snapshots
│   └── snap_dim_customer.sql
├── analyses/                 # Ad-hoc analyses (not deployed)
├── docs/
├── profiles/
│   └── profiles.yml          # Connection profiles (DO NOT commit credentials)
├── dbt_project.yml
└── packages.yml              # dbt packages (dbt-utils, dbt-expectations)
```

### dbt Model Configuration Standards
```sql
-- models/marts/sales/fct_orders.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        cluster_by=['order_date'],
        tags=['gold', 'sales', 'daily']
    )
}}

SELECT
    o.order_id,
    o.customer_id,
    p.product_sk,
    d.date_sk                    AS order_date_sk,
    o.order_amount               AS revenue,
    o.quantity,
    CURRENT_TIMESTAMP()          AS dbt_updated_at
FROM {{ ref('int_orders_enriched') }} o
LEFT JOIN {{ ref('dim_product') }} p ON o.product_id = p.product_bk
LEFT JOIN {{ ref('dim_date') }} d ON CAST(o.order_date AS DATE) = d.date_actual

{% if is_incremental() %}
WHERE o.updated_at > (SELECT MAX(dbt_updated_at) FROM {{ this }})
{% endif %}
```

### dbt Schema Tests (YAML)
```yaml
# models/marts/sales/_sales.yml
models:
  - name: fct_orders
    description: "Fact table for all order transactions at order-line grain"
    columns:
      - name: order_id
        description: "Unique order identifier (business key)"
        tests:
          - not_null
          - unique
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_bk
      - name: revenue
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000
      - name: order_date_sk
        tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_sk
```

---

## CI/CD Pipeline for Data Projects

### GitHub Actions — dbt CI/CD
```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  dbt-test:
    runs-on: ubuntu-latest
    environment: ${{ github.ref == 'refs/heads/main' && 'production' || 'development' }}
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dbt
        run: pip install dbt-databricks dbt-synapse
      
      - name: dbt debug (connectivity check)
        run: dbt debug
        env:
          DBT_TARGET: ${{ vars.DBT_TARGET }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      
      - name: dbt deps
        run: dbt deps
      
      - name: dbt build (compile + test modified models)
        run: |
          dbt build \
            --select state:modified+ \
            --defer \
            --state prod-artifacts/
        
      - name: Upload dbt artifacts
        uses: actions/upload-artifact@v4
        with:
          name: dbt-artifacts
          path: target/
```

### Azure DevOps — ADF CI/CD
```yaml
# azure-pipelines.yml
trigger:
  branches:
    include: [main, release/*]

stages:
  - stage: Validate
    jobs:
      - job: ValidateADF
        steps:
          - task: AzureDataFactory@2
            inputs:
              action: 'validateArm'
              resourceGroupName: $(ResourceGroup)
              dataFactoryName: $(DataFactoryName)

  - stage: Deploy_Dev
    dependsOn: Validate
    jobs:
      - deployment: DeployADF
        environment: development
        strategy:
          runOnce:
            deploy:
              steps:
                - task: AzureDataFactory@2
                  inputs:
                    action: 'deploy'
                    resourceGroupName: $(DevResourceGroup)
                    dataFactoryName: $(DevDataFactoryName)
                    armTemplateFile: '$(Pipeline.Workspace)/adf_arm/ARMTemplateForFactory.json'

  - stage: Deploy_Prod
    dependsOn: Deploy_Dev
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: DeployProd
        environment: production  # requires manual approval
```

---

## Terraform — Azure Data Infrastructure

```hcl
# main.tf — Azure Databricks + ADLS Gen2 + ADF
terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.0" }
    databricks = { source = "databricks/databricks" }
  }
  backend "azurerm" {
    resource_group_name  = "tfstate-rg"
    storage_account_name = "tfstatestorage"
    container_name       = "tfstate"
    key                  = "data-platform.tfstate"
  }
}

# ADLS Gen2
resource "azurerm_storage_account" "datalake" {
  name                     = "datalake${var.env}"
  resource_group_name      = var.resource_group
  location                 = var.location
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = "ZRS"
  is_hns_enabled           = true  # Hierarchical namespace = ADLS Gen2
}

# Bronze / Silver / Gold containers
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# Azure Databricks Workspace
resource "azurerm_databricks_workspace" "main" {
  name                = "databricks-${var.env}"
  resource_group_name = var.resource_group
  location            = var.location
  sku                 = "premium"
  
  custom_parameters {
    virtual_network_id                                   = var.vnet_id
    private_subnet_name                                  = var.private_subnet
    public_subnet_name                                   = var.public_subnet
    no_public_ip                                         = true
  }
}

# Unity Catalog Metastore
resource "databricks_metastore" "main" {
  name          = "main-metastore"
  storage_root  = "abfss://unity-catalog@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
  region        = var.location
}

# Azure Data Factory
resource "azurerm_data_factory" "main" {
  name                = "adf-${var.env}"
  resource_group_name = var.resource_group
  location            = var.location
  
  identity {
    type = "SystemAssigned"
  }
}
```

---

## Pipeline Monitoring & Observability

### Key Metrics to Monitor
| Metric | Alert Threshold |
|---|---|
| Pipeline duration | > 2× baseline |
| Row count vs expected | < 95% or > 105% |
| Failed pipeline runs | Any failure |
| Data freshness | Older than SLA |
| Data quality pass rate | < 99% |
| Cost per run | > 120% of budget |

### Azure Monitor Alert (Bicep)
```bicep
resource adfPipelineFailureAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: 'adf-pipeline-failure-alert'
  location: 'global'
  properties: {
    description: 'Alert on ADF pipeline failure'
    severity: 1
    enabled: true
    scopes: [adf.id]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [{
        metricName: 'PipelineFailedRuns'
        metricNamespace: 'Microsoft.DataFactory/factories'
        operator: 'GreaterThan'
        threshold: 0
        timeAggregation: 'Total'
      }]
    }
    actions: [{ actionGroupId: alertActionGroup.id }]
  }
}
```

### OpenLineage Integration (dbt + Airflow)
```python
# airflow/dags/data_pipeline.py
from openlineage.airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

with DAG('data_pipeline', schedule_interval='@daily', ...) as dag:
    run_silver = DatabricksSubmitRunOperator(
        task_id='run_silver_transform',
        databricks_conn_id='databricks_default',
        json={
            'run_name': 'silver_transform',
            'existing_cluster_id': '{{ var.value.cluster_id }}',
            'notebook_task': {
                'notebook_path': '/Shared/pipelines/silver_transform'
            }
        }
    )
# OpenLineage automatically captures lineage via Marquez
```

---

## Declarative Automation Bundles (DABs) for Multi-Environment CI/CD

Databricks Declarative Automation Bundles (formerly Asset Bundles) provide software-engineering rigor for deploying Databricks jobs, pipelines, and notebooks across environments.

### `databricks.yml` Specification
```yaml
bundle:
  name: enterprise_lakehouse_orchestration

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-dev-workspace.azuredatabricks.net

  staging:
    mode: production
    workspace:
      host: https://adb-staging-workspace.azuredatabricks.net
    run_as:
      service_principal_name: 00000000-0000-0000-0000-000000000000

  prod:
    mode: production
    workspace:
      host: https://adb-prod-workspace.azuredatabricks.net
    run_as:
      service_principal_name: 11111111-1111-1111-1111-111111111111
```

### Keyless OIDC GitHub Actions Workflow (`deploy_bundles.yml`)
```yaml
name: Deploy Databricks Bundles

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

permissions:
  id-token: write  # Required for Azure OIDC Federated Credential
  contents: read

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@v0.220.0
      - name: Validate Bundle Configuration
        run: databricks bundle validate -t dev

  deploy_prod:
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4
      - uses: azure/login@v2
        with:
          client-id: ${{ secrets.AZURE_CLIENT_ID }}
          tenant-id: ${{ secrets.AZURE_TENANT_ID }}
          subscription-id: ${{ secrets.AZURE_SUBSCRIPTION_ID }}
      - uses: databricks/setup-cli@v0.220.0
      - name: Deploy to Production
        run: databricks bundle deploy -t prod
```

---

## Azure Data Factory (ADF) Release Automation Lifecycle

Deploying ADF via CI/CD requires special handling for stateful triggers to prevent schema locks or running pipelines during release:

```
[PR Merged to Main] ──> [Build ARM Template]
                               │
                               ▼
                   [Pre-Deploy: Stop Active Triggers]
                               │
                               ▼
                   [Deploy ARM Template to Target Factory]
                   (Override KeyVault, ADLS & DB URLs)
                               │
                               ▼
                   [Post-Deploy: Restart Triggers]
                               │
                               ▼
                   [Smoke Test Pipeline Validation]
```
