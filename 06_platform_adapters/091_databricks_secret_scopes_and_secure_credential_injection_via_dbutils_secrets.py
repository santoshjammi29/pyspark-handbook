"""
091 — Databricks Secret Scopes & Secure Credential Injection via SecretProvider.

Scenario & Scale:
    Enterprise pipelines connect to PostgreSQL, Kafka, and external REST APIs
    across dev/staging/prod workspaces. Credentials must never be committed to
    Git, printed in driver stdout, or leaked into the Spark UI Environment tab.
"""

from __future__ import annotations

import os


# ==============================================================================
# 2. Anti-Pattern: Hardcoded Secrets & Leaking Credentials via SparkConf/Logs
# ==============================================================================
# ❌ Bad Practice:
#   spark.conf.set("spark.jdbc.password", "SuperSecret123!")
#   print(f"Connecting with password={db_pass}")
#
# Otherwise:
#   Static strings leak in Git history. Putting secrets in `spark.conf` exposes
#   them in plaintext on the Spark UI `Environment` tab and event logs.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: SecretProvider Boundary & Transient Options
# ==============================================================================
# ✅ Good Practice: Resolve secrets dynamically from Databricks Secret Scopes
#                   (backed by Azure Key Vault / AWS Secrets Manager) or env vars,
#                   and inject them only into transient reader/client options.
class SecretProvider:
    """Platform adapter for Databricks Secret Scopes with local CI env-var fallback."""

    def __init__(self, dbutils_obj: object | None = None) -> None:
        self._dbutils = dbutils_obj

    def get_secret(self, scope: str, key: str) -> str:
        if self._dbutils is not None:
            return str(self._dbutils.secrets.get(scope=scope, key=key))  # type: ignore[attr-defined]

        env_name = f"{scope.upper()}_{key.upper()}"
        value = os.environ.get(env_name)
        if value is None:
            raise KeyError(f"Secret '{scope}/{key}' not found; set {env_name} in CI/local env.")
        return value


def build_jdbc_options(
    jdbc_url: str,
    table: str,
    username: str,
    provider: SecretProvider,
    scope: str = "dw_scope",
    secret_key: str = "postgres_password",
) -> dict[str, str]:
    """Assemble transient JDBC DataFrameReader options without mutating global SparkConf."""
    return {
        "url": jdbc_url,
        "dbtable": table,
        "user": username,
        "password": provider.get_secret(scope, secret_key),
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }


def redact_preview(secret_value: str) -> str:
    """Return a log-safe masked fingerprint without exposing secret material."""
    return f"<redacted:len={len(secret_value)}>"


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `df.explain("formatted")` for `JDBCRelation`, verify `PushedFilters` appear
#   without credential strings in SQL predicates; never embed passwords in `url`.
# - Audit Spark UI -> `Environment` tab to confirm zero `password`, `token`, or
#   `secret` entries exist in Spark Properties or System Properties.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: `dbutils.secrets.get()` registers the string for `[REDACTED]` scrubbing
#   in Databricks notebook stdout, but sending the value to an external webhook or
#   writing it to a Delta table bypasses redaction completely.
# - Gotcha 2: Prefer cloud-backed secret scopes (Azure Key Vault / AWS Secrets Manager)
#   over Databricks-backed scopes so rotation, RBAC, and audit logs stay centralized.
# - Gotcha 3: Better yet, replace static passwords with keyless OIDC / Managed Identity
#   / Unity Catalog Lakehouse Federation connections wherever supported.


if __name__ == "__main__":
    os.environ.setdefault("DW_SCOPE_POSTGRES_PASSWORD", "ephemeral_ci_only_token")
    provider = SecretProvider(dbutils_obj=None)
    opts = build_jdbc_options(
        jdbc_url="jdbc:postgresql://warehouse.internal:5432/analytics",
        table="public.fact_orders",
        username="etl_service_principal",
        provider=provider,
    )
    print("JDBC target:", opts["url"], "| password:", redact_preview(opts["password"]))
