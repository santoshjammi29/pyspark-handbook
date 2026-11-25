"""
096 — Microsoft Fabric OneLake ABFSS, Shortcuts, notebookutils & Direct Lake V-Order Adapter.

Scenario & Scale:
    An enterprise runs Microsoft Fabric Lakehouse pipelines processing multi-TB
    Delta tables consumed by Power BI Direct Lake semantic models, while reading
    zero-copy OneLake Shortcuts pointing to ADLS Gen2 / S3 without vendor lock-in.
"""

from __future__ import annotations

import os
import re
from typing import Protocol


# ==============================================================================
# 2. Anti-Pattern: Hardcoded mssparkutils, dbfs:/ Paths & Missing V-Order
# ==============================================================================
# ❌ Bad Practice:
#   secret = mssparkutils.credentials.getSecret("https://kv.vault.azure.net/", "api-key")
#   df.write.format("delta").save("dbfs:/mnt/gold/fact_sales")
#   # Plus writing uncompacted Delta files with V-Order disabled
#
# Otherwise:
#   `mssparkutils` is deprecated in favor of `notebookutils` and fails in local CI.
#   `dbfs:/` does not exist in Fabric OneLake. Disabling V-Order or accumulating
#   tiny files forces Power BI Direct Lake to fall back to slow DirectQuery mode.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: OneLake ABFSS + notebookutils + V-Order Adapter
# ==============================================================================
# ✅ Good Practice: Abstract `notebookutils` behind a runtime boundary, construct
#                   canonical OneLake ABFSS URIs, and enable V-Order for Direct Lake.
_FABRIC_IDENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


class FabricContext(Protocol):
    def get_secret(self, key_vault_url: str, secret_name: str) -> str: ...
    def run_notebook(self, path: str, timeout_seconds: int = 300, args: dict[str, str] | None = None) -> str: ...


class NotebookUtilsAdapter:
    """Adapter wrapping Fabric `notebookutils` (or legacy `mssparkutils`) with CI fallback."""

    def __init__(self, runtime_utils: object | None = None) -> None:
        self._utils = runtime_utils

    @classmethod
    def auto_detect(cls) -> NotebookUtilsAdapter:
        for mod_name in ("notebookutils", "mssparkutils"):
            try:
                mod = __import__(mod_name)
                return cls(mod)
            except ImportError:
                continue
        return cls(None)

    def get_secret(self, key_vault_url: str, secret_name: str) -> str:
        if self._utils is not None:
            return str(self._utils.credentials.getSecret(key_vault_url, secret_name))  # type: ignore[attr-defined]
        env_key = f"FABRIC_SECRET_{secret_name.upper().replace('-', '_')}"
        return os.environ.get(env_key, f"local-placeholder-{secret_name}")

    def run_notebook(self, path: str, timeout_seconds: int = 300, args: dict[str, str] | None = None) -> str:
        if self._utils is not None:
            return str(self._utils.notebook.run(path, timeout_seconds, args or {}))  # type: ignore[attr-defined]
        return f"SIMULATED_RUN:{path}"


def build_onelake_abfss_uri(
    workspace: str,
    lakehouse: str,
    area: str,
    object_path: str,
) -> str:
    """Build a canonical OneLake ABFSS URI for Tables, Files, or Shortcuts."""
    if area not in {"Tables", "Files"}:
        raise ValueError("OneLake area must be 'Tables' or 'Files'.")
    for label, val in (("workspace", workspace), ("lakehouse", lakehouse)):
        if not _FABRIC_IDENT_RE.match(val):
            raise ValueError(f"Invalid Fabric {label}: {val!r}")
    clean_obj = object_path.strip("/")
    return f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/{area}/{clean_obj}"


def get_direct_lake_vorder_configs() -> dict[str, str]:
    """Return Spark configurations optimizing Delta writes for Fabric Direct Lake."""
    return {
        "spark.sql.ansi.enabled": "true",
        "spark.sql.parquet.vorder.enabled": "true",
        "spark.microsoft.delta.optimizeWrite.enabled": "true",
        "spark.microsoft.delta.optimizeWrite.binSize": "1073741824",
    }


def prepare_direct_lake_fact(df):
    """Apply ANSI-safe casting and clean column types compatible with Direct Lake."""
    from pyspark.sql import functions as F

    return (
        df.withColumn("net_amount", F.expr("try_cast(raw_amount AS DECIMAL(18, 2))"))
        .withColumn("event_date", F.to_date("event_ts"))
        .filter(F.col("net_amount").isNotNull())
    )


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `df.explain("formatted")`, verify `Location` uses `abfss://<ws>@onelake.dfs.fabric.microsoft.com/...`
#   and `OptimizeWrite` coalesces small partitions before writing V-Order Parquet row groups.
# - In Spark UI -> SQL/DataFrame tab, confirm `try_cast` avoids driver errors and
#   output file sizes target 128 MB–1 GB so Direct Lake loads columns into VertiPaq cleanly.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: Fabric deprecated `mssparkutils` in favor of `notebookutils`; wrapping it
#   behind an adapter lets pipelines run seamlessly across Fabric runtimes and local CI.
# - Gotcha 2: Power BI Direct Lake silently falls back to DirectQuery if the Delta table
#   exceeds SKU row/parquet-file guardrails, uses unsupported SQL views, or lacks V-Order.
# - Gotcha 3: OneLake Shortcuts expose S3, ADLS Gen2, or GCS directories inside `Files/`
#   or `Tables/` with zero data duplication—enforce RBAC on the Shortcut target boundary.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_096_Fabric_OneLake").getOrCreate()
    for k, v in get_direct_lake_vorder_configs().items():
        spark.conf.set(k, v)

    ctx = NotebookUtilsAdapter.auto_detect()
    table_uri = build_onelake_abfss_uri("prod_analytics", "enterprise_gold", "Tables", "fact_sales")
    shortcut_uri = build_onelake_abfss_uri("prod_analytics", "enterprise_bronze", "Files", "shortcuts/aws_orders")
    print("OneLake Table URI:", table_uri)
    print("OneLake Shortcut URI:", shortcut_uri)
    print("Vault token preview:", ctx.get_secret("https://kv.vault.azure.net/", "api-key")[:10])

    sample = [("ORD-1", "199.95", "2026-01-15T12:00:00Z"), ("ORD-2", "BAD", "2026-01-15T12:05:00Z")]
    df_raw = spark.createDataFrame(sample, "order_id STRING, raw_amount STRING, event_ts STRING")
    prepare_direct_lake_fact(df_raw).show(truncate=False)
