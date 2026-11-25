"""
012 — Emulating Databricks dbutils (FS, Widgets, Secrets) Locally & Across Clouds.

Scenario & Scale:
    An enterprise data team maintains 50+ parameterized pipelines that run in
    Databricks Jobs in production but must execute in local pytest suites,
    GitHub Actions CI, and Spark Connect sessions without Databricks Runtime.
    Direct calls to the injected `dbutils` Java/Scala bridge raise `NameError`
    outside Databricks and couple business transforms to a single vendor runtime.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


# ==============================================================================
# 2. Anti-Pattern: Hardcoded dbutils & Ephemeral Local Paths in Transform Logic
# ==============================================================================
# ❌ Bad Practice:
#   def load_orders(spark):
#       env = dbutils.widgets.get("env")
#       token = dbutils.secrets.get("finance", "api_key")
#       dbutils.fs.put("file:/local/staging/flag.json", f'{{"env": "{env}"}}', True)
#       return spark.read.json(f"dbfs:/mnt/raw/{env}/orders")
#
# Otherwise:
#   Any unit test, CI runner, or Spark Connect client outside Databricks fails
#   with `NameError: name 'dbutils' is not defined`. Hardcoded local scratch writes leak
#   stale state across runs and break on serverless/multi-tenant drivers.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Portable Platform Context Adapter
# ==============================================================================
# ✅ Good Practice: Isolate vendor bridges behind a thin dependency-injected adapter.
@dataclass(frozen=True)
class FileEntry:
    path: str
    name: str
    size: int


class PlatformContext(Protocol):
    def get_param(self, name: str, default: str = "") -> str: ...
    def get_secret(self, scope: str, key: str) -> str: ...
    def read_text(self, path: str, max_bytes: int = 4096) -> str: ...
    def write_text(self, path: str, content: str) -> None: ...
    def list_dir(self, path: str) -> list[FileEntry]: ...
    def remove_path(self, path: str, recurse: bool = False) -> None: ...


class LocalDBUtilsAdapter:
    """Drop-in local/CI adapter for dbutils.fs, dbutils.widgets, and dbutils.secrets."""

    def __init__(self, root_dir: Path, params: dict[str, str] | None = None) -> None:
        self._root = root_dir
        self._params = dict(params or {})

    def register_param(self, name: str, default: str) -> None:
        self._params.setdefault(name, default)

    def get_param(self, name: str, default: str = "") -> str:
        env_key = f"PIPELINE_PARAM_{name.upper()}"
        return os.environ.get(env_key, self._params.get(name, default))

    def get_secret(self, scope: str, key: str) -> str:
        env_key = f"{scope.upper()}_{key.upper()}"
        secret = os.environ.get(env_key)
        if secret is None:
            return f"local-dev-placeholder-{key}"
        return secret

    def _resolve(self, uri: str) -> Path:
        clean = uri.replace("dbfs:", "").replace("file:", "").lstrip("/")
        resolved = (self._root / clean).resolve()
        if not str(resolved).startswith(str(self._root.resolve())):
            raise ValueError(f"Path escapes sandbox root: {uri}")
        return resolved

    def write_text(self, path: str, content: str) -> None:
        target = self._resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")

    def read_text(self, path: str, max_bytes: int = 4096) -> str:
        target = self._resolve(path)
        with target.open("r", encoding="utf-8") as handle:
            return handle.read(max_bytes)

    def list_dir(self, path: str) -> list[FileEntry]:
        target = self._resolve(path)
        if not target.exists():
            return []
        return [
            FileEntry(path=str(item), name=item.name, size=item.stat().st_size)
            for item in sorted(target.iterdir())
        ]

    def remove_path(self, path: str, recurse: bool = False) -> None:
        target = self._resolve(path)
        if target.is_dir():
            if not recurse:
                raise IsADirectoryError(f"Set recurse=True to delete directory: {path}")
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()


class DatabricksDBUtilsAdapter:
    """Thin production wrapper around Databricks runtime dbutils."""

    def __init__(self, dbutils_obj: object) -> None:
        self._dbutils = dbutils_obj

    def get_param(self, name: str, default: str = "") -> str:
        try:
            return str(self._dbutils.widgets.get(name))  # type: ignore[attr-defined]
        except Exception:
            return default

    def get_secret(self, scope: str, key: str) -> str:
        return str(self._dbutils.secrets.get(scope=scope, key=key))  # type: ignore[attr-defined]

    def read_text(self, path: str, max_bytes: int = 4096) -> str:
        return str(self._dbutils.fs.head(path, max_bytes))  # type: ignore[attr-defined]

    def write_text(self, path: str, content: str) -> None:
        self._dbutils.fs.put(path, content, True)  # type: ignore[attr-defined]

    def list_dir(self, path: str) -> list[FileEntry]:
        items = self._dbutils.fs.ls(path)  # type: ignore[attr-defined]
        return [FileEntry(path=i.path, name=i.name, size=i.size) for i in items]

    def remove_path(self, path: str, recurse: bool = False) -> None:
        self._dbutils.fs.rm(path, recurse)  # type: ignore[attr-defined]


def build_platform_context(sandbox_root: Path, native_dbutils: object | None = None) -> PlatformContext:
    if native_dbutils is not None:
        return DatabricksDBUtilsAdapter(native_dbutils)
    return LocalDBUtilsAdapter(root_dir=sandbox_root)


def run_parameterized_query(spark, ctx: PlatformContext):
    """Keep Spark 4.0+ SQL execution pure and ANSI-safe using parameterized args."""
    from pyspark.sql import functions as F

    spark.conf.set("spark.sql.ansi.enabled", "true")
    target_env = ctx.get_param("env", "staging")
    batch_date = ctx.get_param("processing_date", "2026-01-15")

    df = spark.createDataFrame(
        [("ORD-1", "staging", "2026-01-15", "100.50"), ("ORD-2", "prod", "2026-01-15", "bad_amt")],
        ["order_id", "env", "order_date", "raw_amount"],
    )
    return (
        df.filter((F.col("env") == F.lit(target_env)) & (F.col("order_date") == F.lit(batch_date)))
        .withColumn("amount", F.expr("try_cast(raw_amount AS DECIMAL(12, 2))"))
    )


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - Parameter injection happens on the driver before Catalyst compiles the plan;
#   in `df.explain("formatted")`, `target_env` and `batch_date` appear as pushed
#   literal predicates inside `Filter` / `FileScan` (`PushedFilters`).
# - Because business logic receives plain strings instead of `dbutils` calls,
#   Spark Connect thin clients serialize only the DataFrame plan without JVM bridges.
# - In Spark UI -> SQL/DataFrame tab, verify no `BatchEvalPython` nodes appear.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: `dbutils` is unavailable in pure Spark Connect clients and local unit
#   tests; always pass parameters via job arguments or an injected adapter interface.
# - Gotcha 2: Databricks redacts `dbutils.secrets.get()` only in notebook stdout/logs;
#   writing a secret into a DataFrame column or JSON file bypasses redaction.
# - Gotcha 3: Prefer Unity Catalog Volumes (`/Volumes/<cat>/<schema>/<vol>`) over
#   `dbfs:/mnt/...` mounts so standard Python libraries can use POSIX I/O safely.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    with tempfile.TemporaryDirectory() as tmp_dir:
        ctx = build_platform_context(Path(tmp_dir))
        ctx.write_text("config/run.json", '{"env": "staging", "batch_size": 5000}')
        assert "staging" in ctx.read_text("config/run.json")
        assert len(ctx.list_dir("config")) == 1

        spark = SparkSession.builder.appName("Adapter_012_DBUtils_Boundary").getOrCreate()
        result_df = run_parameterized_query(spark, ctx)
        result_df.show(truncate=False)
        ctx.remove_path("config", recurse=True)
