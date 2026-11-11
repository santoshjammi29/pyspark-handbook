# Databricks Asset Bundles (DABs) Syntax & Architecture Reference

Databricks Asset Bundles (officially known as Declarative Automation Bundles / DABs) provide declarative Infrastructure-as-Code (IaC) for authoring, testing, deploying, and managing Databricks Lakehouse assets across development, staging, and production workspaces.

---

## 1. Top-Level `databricks.yml` Specification

```yaml
bundle:
  name: enterprise-lakehouse-pipeline

include:
  - resources/*.yml

variables:
  catalog:
    description: "Target Unity Catalog name"
    default: "main"
  schema:
    description: "Target Unity Catalog schema name"
    default: "sales_pipeline"
  notification_email:
    description: "Alerts distribution list"
    default: "data-eng-alerts@corp.com"
  compute_spark_version:
    description: "Databricks Runtime version for job clusters"
    default: "15.4.x-scala2.12"
  node_type_id:
    description: "Cloud VM instance size"
    default: "Standard_D4ds_v5"

targets:
  # --------------------------------------------------------------------------
  # 1. Developer Sandbox
  # --------------------------------------------------------------------------
  dev:
    mode: development
    default: true
    workspace:
      root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: "dev_catalog"
      schema: "dev_${workspace.current_user.short_name}"
    sync:
      include:
        - notebooks/**
        - resources/**

  # --------------------------------------------------------------------------
  # 2. Staging / Pre-Production
  # --------------------------------------------------------------------------
  staging:
    mode: production
    workspace:
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: "staging_catalog"
      schema: "sales_pipeline"

  # --------------------------------------------------------------------------
  # 3. Production
  # --------------------------------------------------------------------------
  prod:
    mode: production
    workspace:
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: "prod_catalog"
      schema: "sales_pipeline"
    run_as:
      service_principal_name: "11111111-2222-3333-4444-555555555555"
```

---

## 2. Resource Definition: Workflows & Jobs (`resources/jobs.yml`)

```yaml
resources:
  jobs:
    daily_medallion_etl:
      name: "[${bundle.target}] Daily Medallion ETL & Quality Gates"
      
      schedule:
        quartz_cron_expression: "0 0 2 * * ?" # 02:00 UTC daily
        timezone_id: "UTC"
        pause_status: ${bundle.target == "prod" ? "UNPAUSED" : "PAUSED"}

      email_notifications:
        on_failure:
          - ${var.notification_email}

      job_clusters:
        - job_cluster_key: etl_cluster
          new_cluster:
            spark_version: ${var.compute_spark_version}
            node_type_id: ${var.node_type_id}
            num_workers: 4
            spark_conf:
              spark.sql.adaptive.enabled: "true"
              spark.sql.shuffle.partitions: "auto"

      tasks:
        - task_key: ingest_bronze
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ../notebooks/01_ingest_bronze.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}

        - task_key: transform_silver
          depends_on:
            - task_key: ingest_bronze
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ../notebooks/02_transform_silver.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}

        - task_key: curate_gold
          depends_on:
            - task_key: transform_silver
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ../notebooks/03_curate_gold.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}
```

---

## 3. Resource Definition: Delta Live Tables (`resources/dlt_pipelines.yml`)

```yaml
resources:
  pipelines:
    streaming_orders_pipeline:
      name: "[${bundle.target}] Streaming Orders DLT Pipeline"
      target: ${var.schema}
      continuous: false # Set true for 24/7 streaming or false for triggered batch
      channel: "preview"
      edition: "advanced"
      photon: true
      
      libraries:
        - notebook:
            path: ../notebooks/streaming_orders_dlt.py

      configuration:
        bundle.source: ${workspace.file_path}
        catalog: ${var.catalog}
        schema: ${var.schema}
```

---

## 4. CLI Workflow & Key Commands

```bash
# 1. Initialize a new bundle from template
databricks bundle init

# 2. Validate bundle configuration, variable interpolation, and asset syntax
databricks bundle validate -t dev

# 3. Deploy bundle to developer target
databricks bundle deploy -t dev

# 4. Trigger ad-hoc execution of a deployed job
databricks bundle run -t dev daily_medallion_etl

# 5. Destroy/clean up ephemeral developer resources
databricks bundle destroy -t dev
```
