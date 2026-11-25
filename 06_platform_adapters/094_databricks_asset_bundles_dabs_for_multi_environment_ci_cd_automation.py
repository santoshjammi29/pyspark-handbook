"""
094 — Declarative Automation Bundles (DABs / Asset Bundles) for Multi-Env CI/CD.

Scenario & Scale:
    An enterprise manages 50+ Databricks Jobs, Lakeflow DLT pipelines, and Unity
    Catalog schemas across dev, staging, and prod workspaces. Manual UI edits cause
    configuration drift, broken relative notebook paths, and cyclic task failures.
"""

from __future__ import annotations

from collections import defaultdict, deque


# ==============================================================================
# 2. Anti-Pattern: Manual UI Edits, Hardcoded Env Names & Unchecked DAG Cycles
# ==============================================================================
# ❌ Bad Practice:
#   # Cloning jobs manually in the Databricks UI and hardcoding `prod_catalog`
#   # or pointing `notebook_path` to stale `../notebooks/...` directories.
#
# Otherwise:
#   Dev runs accidentally overwrite production tables, deployments are non-reproducible,
#   and broken `depends_on` references or missing notebook paths only fail at runtime.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Parameterized DABs YAML + Offline DAG Validator
# ==============================================================================
# ✅ Good Practice: Parameterize targets (`mode: development` vs `production`),
#                   enforce `run_as` Service Principals in prod, and validate DAGs in CI.
SAMPLE_BUNDLE_CONFIG = {
    "bundle": {"name": "pyspark-handbook"},
    "targets": {
        "dev": {"mode": "development", "default": True},
        "staging": {"mode": "production"},
        "prod": {"mode": "production", "run_as": {"service_principal_name": "00000000-0000-0000-0000-000000000000"}},
    },
}

SAMPLE_TASKS = [
    {"task_key": "01_ingest_autoloader", "notebook_path": "../../048_databricks_auto_loader_directory_mode_cloudfiles_schema_inference_and_checkpointing.py"},
    {"task_key": "02_quarantine_json", "depends_on": [{"task_key": "01_ingest_autoloader"}], "notebook_path": "../../../01_foundations/020_handling_corrupted_and_malformed_json_permissive_vs_dropmalformed_vs_failfast.py"},
    {"task_key": "03_upsert_scd2", "depends_on": [{"task_key": "02_quarantine_json"}], "notebook_path": "../../../02_production_patterns/069_slowly_changing_dimension_type_2_scd_type_2_historical_tracking_with_effective_dates.py"},
]


def validate_bundle_and_dag(bundle_cfg: dict, tasks: list[dict]) -> list[str]:
    """Validate bundle target invariants and verify task dependencies form an acyclic DAG."""
    if "bundle" not in bundle_cfg or "targets" not in bundle_cfg:
        raise ValueError("Bundle config must define 'bundle' and 'targets'.")
    for env in ("dev", "staging", "prod"):
        if env not in bundle_cfg["targets"]:
            raise ValueError(f"Missing required target environment: {env}")
    if "run_as" not in bundle_cfg["targets"]["prod"]:
        raise ValueError("Production target must specify a 'run_as' service principal.")

    adj: dict[str, list[str]] = defaultdict(list)
    in_degree: dict[str, int] = {t["task_key"]: 0 for t in tasks}
    all_keys = set(in_degree)

    for task in tasks:
        key = task["task_key"]
        for dep in task.get("depends_on", []):
            parent = dep["task_key"]
            if parent not in all_keys:
                raise ValueError(f"Task '{key}' depends on unknown task '{parent}'.")
            adj[parent].append(key)
            in_degree[key] += 1

    queue = deque([k for k, deg in in_degree.items() if deg == 0])
    topo_order: list[str] = []
    while queue:
        node = queue.popleft()
        topo_order.append(node)
        for nxt in adj[node]:
            in_degree[nxt] -= 1
            if in_degree[nxt] == 0:
                queue.append(nxt)

    if len(topo_order) != len(all_keys):
        raise ValueError("Circular dependency detected in DABs task graph.")
    return topo_order


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - DABs controls control-plane orchestration (`databricks bundle validate/deploy`),
#   while each task's `job_clusters[].new_cluster.spark_conf` injects runtime flags
#   (`spark.sql.adaptive.enabled=true`, `spark.sql.shuffle.partitions=auto`).
# - Verify those injected configs in Spark UI -> `Environment` tab and confirm AQE
#   `AdaptiveSparkPlan` appears in task `explain("formatted")` outputs.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: `mode: development` automatically prefixes job names with `[dev <user>]`,
#   pauses cron schedules, and isolates bundle state under `/Users/<user>/.bundle/...`.
# - Gotcha 2: Relative `notebook_path` entries inside `resources/*.yml` resolve relative
#   to the resource YAML file directory (e.g. `resources/`), not the repo root.
# - Gotcha 3: Always authenticate GitHub Actions DABs deploys with keyless OIDC
#   (`ARM_USE_OIDC` / AWS IAM OIDC) rather than long-lived PAT tokens.


if __name__ == "__main__":
    order = validate_bundle_and_dag(SAMPLE_BUNDLE_CONFIG, SAMPLE_TASKS)
    print("Validated DABs DAG execution order:", " -> ".join(order))
