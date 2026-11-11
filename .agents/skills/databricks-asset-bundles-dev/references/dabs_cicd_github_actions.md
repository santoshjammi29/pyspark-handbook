# Databricks Asset Bundles (DABs) Multi-Environment CI/CD Automation

This guide covers automating bundle validation, linting, testing, and multi-environment promotion across `dev`, `staging`, and `production` using GitHub Actions and keyless authentication.

---

## 1. Authentication Best Practices

### Recommended: Keyless OIDC (Workload Identity Federation)
Avoid long-lived personal access tokens (PATs) or static client secrets. Use OpenID Connect (OIDC) between GitHub Actions and your cloud provider (Azure AD / AWS IAM / GCP Workload Identity) to dynamically mint short-lived tokens.

### Service Principal (OAuth M2M)
For automated CI/CD jobs, configure an enterprise Service Principal:
- `DATABRICKS_HOST`: e.g. `https://adb-1234567890123456.7.azuredatabricks.net`
- `DATABRICKS_CLIENT_ID`: Service Principal Application UUID
- `DATABRICKS_CLIENT_SECRET`: Secure Service Principal Secret

---

## 2. Production GitHub Actions Pipeline (`.github/workflows/deploy_bundles.yml`)

```yaml
name: Databricks Asset Bundle CI/CD

on:
  pull_request:
    branches: [ main ]
    paths:
      - 'databricks.yml'
      - 'resources/**'
      - 'notebooks/**'

  push:
    branches: [ main ]
    paths:
      - 'databricks.yml'
      - 'resources/**'
      - 'notebooks/**'

  workflow_dispatch:
    inputs:
      target:
        description: 'Deployment Target Environment'
        required: true
        default: 'staging'
        type: choice
        options:
          - staging
          - prod

jobs:
  # --------------------------------------------------------------------------
  # 1. PR Check: Syntax Validation & Asset Linting
  # --------------------------------------------------------------------------
  validate:
    name: Validate Bundle Syntax
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main

      - name: Validate Dev Bundle Target
        run: databricks bundle validate -t dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_DEV_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_DEV_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_DEV_CLIENT_SECRET }}

  # --------------------------------------------------------------------------
  # 2. Automated Staging Deployment on Merge to Main
  # --------------------------------------------------------------------------
  deploy_staging:
    name: Deploy to Staging
    needs: [validate]
    if: github.ref == 'refs/heads/main' && (github.event_name == 'push' || (github.event_name == 'workflow_dispatch' && inputs.target == 'staging'))
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main

      - name: Deploy Bundle to Staging
        run: databricks bundle deploy -t staging
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_STAGING_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_STAGING_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_STAGING_CLIENT_SECRET }}

  # --------------------------------------------------------------------------
  # 3. Controlled Production Deployment with Environment Gate
  # --------------------------------------------------------------------------
  deploy_prod:
    name: Deploy to Production
    needs: [validate]
    if: github.event_name == 'workflow_dispatch' && inputs.target == 'prod'
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main

      - name: Deploy Bundle to Production
        run: databricks bundle deploy -t prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_PROD_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_PROD_CLIENT_SECRET }}
```

---

## 3. Pre-Deployment Integrity Checks

Always enforce pre-commit and PR checks:
1. **YAML Schema Validation**: Verify `databricks.yml` and all `resources/*.yml` files match Databricks DAB schema specifications.
2. **Notebook AST Compilation**: Assert all `.py` notebooks compile without syntax errors.
3. **Environment Target Isolation**: Guarantee developer variables never bleed into staging or production configurations.
