---
name: databricks-asset-bundles-dev
description: |
  Specialized skill for authoring, configuring, linting, validating, and deploying Databricks Asset Bundles (DABs) Infrastructure-as-Code.
  Activate when working on:
  - Authoring or modifying databricks.yml or resource definitions (resources/jobs.yml, resources/dlt_pipelines.yml, resources/unity_catalog.yml, etc.)
  - Setting up multi-environment CI/CD automation for Databricks Lakehouse pipelines across dev, staging, and production workspaces
  - Configuring target isolation (mode: development vs mode: production), variable interpolation, and run_as service principals
  - Validating and linting bundles via `databricks bundle validate -t <target>` or local syntax linters
  - Deploying workflows, Delta Live Tables (DLT) pipelines, Model Serving endpoints, Dashboards, and Databricks Apps via `databricks bundle deploy`
  - Integrating GitHub Actions or Azure DevOps pipelines with Databricks CLI and keyless OIDC authentication

  DO NOT trigger for:
  - Pure Python data manipulation inside notebooks without bundle configuration
  - Non-Databricks cloud infrastructure (use Terraform directly for general cloud infra)
---

# Databricks Asset Bundles (DABs) Development Skill

You are an **Enterprise Databricks DevOps & Platform Architect**. Use this skill to design, author, validate, and automate **Databricks Asset Bundles** (officially known as Declarative Automation Bundles / DABs) — the modern standard for Databricks Infrastructure-as-Code.

---

## 1. When to Activate

Activate immediately when the user mentions:
- Configuring or editing `databricks.yml` or files under `resources/*.yml`.
- Deploying Databricks workflows or pipelines to `dev`, `staging`, or `prod`.
- Running `databricks bundle validate`, `databricks bundle deploy`, or `databricks bundle run`.
- Setting up GitHub Actions or Azure DevOps CI/CD for Databricks projects.
- Migrating legacy UI-managed jobs or notebooks to declarative version-controlled bundles.

---

## 2. Core Architecture & Bundle Structure

A production Databricks Asset Bundle is organized as follows:

```text
my-databricks-project/
├── databricks.yml                  # Root bundle configuration, variables & targets
├── resources/                      # Modular YAML resource declarations
│   ├── jobs.yml                    # Multi-task Workflows & job clusters
│   ├── dlt_pipelines.yml           # Lakeflow / Delta Live Tables declarations
│   ├── unity_catalog.yml           # Schemas, volumes, and grants
│   ├── model_serving.yml           # MLflow model serving endpoints
│   └── apps.yml                    # Custom Databricks Apps
├── notebooks/                      # Databricks Python source notebooks (.py)
├── src/                            # Wheel / Python package source code
└── .github/workflows/
    └── deploy_bundles.yml          # Automated CI/CD validation & deployment
```

---

## 3. Workflow — Authoring to Production Deployment

### Step 1: Define Variables & Targets in `databricks.yml`
- Always define explicit **variables** with clear defaults (`catalog`, `schema`, `compute_spark_version`, `notification_email`).
- Separate environments cleanly:
  - **`dev` target**: Must have `mode: development` (creates isolated user workspace folders, prepends `[dev <username>]` to job names, pauses schedules).
  - **`staging` target**: `mode: production` pointing to staging Unity Catalog schemas.
  - **`prod` target**: `mode: production` running under an enterprise **Service Principal** (`run_as.service_principal_name`), unpausing production cron schedules.

### Step 2: Define Modular Resources in `resources/*.yml`
- Modularize resources into separate YAML files under `resources/` rather than bloating `databricks.yml`.
- Reference variables cleanly using `${var.catalog}` and `${bundle.target}`.
- Use `job_clusters` within jobs to ensure ephemeral, cost-efficient compute rather than sharing all-purpose clusters.

### Step 3: Local Validation & Linting
Run pre-deployment validation before committing:
```bash
# 1. Local structure linting (no credentials required)
python .agents/skills/databricks-asset-bundles-dev/scripts/lint_bundle.py databricks.yml

# 2. Remote bundle validation against Databricks API
databricks bundle validate -t dev
```

### Step 4: Developer Sandbox Deployment
Deploy and test in the personal developer sandbox:
```bash
# Deploy assets to personal development folder
databricks bundle deploy -t dev

# Trigger ad-hoc job execution
databricks bundle run -t dev my_job_key
```

### Step 5: Multi-Environment CI/CD Automation
- Automate pull request validation: Every PR triggers `databricks bundle validate -t dev`.
- Automate staging release: Merges to `main` trigger `databricks bundle deploy -t staging`.
- Manual approval gate: Production deployment via `workflow_dispatch` triggering `databricks bundle deploy -t prod`.

---

## 4. Invariants & Best Practices

1. **Never Commit Hardcoded Secrets or Personal Access Tokens (PATs)**: Always use GitHub Secrets with Service Principal OAuth M2M credentials (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`) or Keyless OIDC.
2. **Always Use `mode: development` for Dev Targets**: Prevents accidental overwrites of shared staging/production jobs and schedules.
3. **Use Ephemeral Job Clusters for Workflows**: Never attach automated production batch jobs to high-cost interactive all-purpose clusters.
4. **Parameterize Unity Catalog Namespaces**: Always inject catalog and schema via bundle variables so assets deploy cleanly across catalog boundaries (`dev_catalog` -> `staging_catalog` -> `prod_catalog`).

---

## 5. Reference Manuals & Templates

- 📘 **[Bundle Syntax & Specification Guide](./references/dabs_bundle_syntax_reference.md)**: Full reference for `databricks.yml`, jobs, and DLT pipelines.
- 🚀 **[GitHub Actions CI/CD Template](./references/dabs_cicd_github_actions.md)**: Production workflow with OIDC and Service Principal authentication.
- 🔍 **[Bundle Linter Script](./scripts/lint_bundle.py)**: Fast offline YAML and bundle structure validator.
- 📦 **Sample Templates**: See `resources/databricks.yml`, `resources/resources/jobs.yml`, `resources/resources/dlt_pipelines.yml`, and `resources/resources/unity_catalog.yml`.
