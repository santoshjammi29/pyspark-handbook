"""
099 — Kubernetes Spark Operator, Volcano Gang Scheduling & Decommissioning Adapter.

Scenario & Scale:
    A multi-tenant Spark 4.0+ platform runs on Kubernetes (Kubeflow Spark Operator)
    using Spot/preemptible worker nodes, handling terabyte-scale shuffles without
    external shuffle services, pod evictions, or driver-executor quota deadlocks.
"""

from __future__ import annotations


# ==============================================================================
# 2. Anti-Pattern: Dynamic Allocation Without Shuffle Tracking & Root emptyDir Spill
# ==============================================================================
# ❌ Bad Practice:
#   spark.conf.set("spark.dynamicAllocation.enabled", "true")
#   # Missing shuffleTracking.enabled, missing PVC/local.dir sizing, no decommissioning
#
# Otherwise:
#   Spark on K8s has NO YARN External Shuffle Service, so dynamic allocation either
#   refuses to start or loses shuffle blocks on scale-down. Shuffle spills into the
#   container's root `emptyDir` trigger Kubernetes `Evicted` (`ephemeral-storage`) kills.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Shuffle Tracking, PVC Storage & Decommissioning
# ==============================================================================
# ✅ Good Practice: Enable shuffle tracking, mount PVC/NVMe `spark.local.dir`,
#                   use Volcano gang scheduling, and enable graceful decommissioning.
def get_k8s_spark_configs(
    fallback_storage_uri: str,
    min_executors: int = 2,
    max_executors: int = 32,
) -> dict[str, str]:
    """Return resilient Spark-on-Kubernetes configs for dynamic allocation & Spot nodes."""
    return {
        "spark.sql.ansi.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        # 1. Dynamic allocation without an External Shuffle Service
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.shuffleTracking.enabled": "true",
        "spark.dynamicAllocation.minExecutors": str(min_executors),
        "spark.dynamicAllocation.maxExecutors": str(max_executors),
        # 2. Graceful node decommissioning for Spot / Karpenter scale-in
        "spark.decommission.enabled": "true",
        "spark.storage.decommission.enabled": "true",
        "spark.storage.decommission.shuffleBlocks.enabled": "true",
        "spark.storage.decommission.rddBlocks.enabled": "true",
        "spark.storage.decommission.fallbackStorage.path": fallback_storage_uri,
        # 3. PVC-backed shuffle scratch space (mount at /mnt/spark-shuffle)
        "spark.local.dir": "/mnt/spark-shuffle",
        "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName": "OnDemand",
        "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass": "gp3-nvme",
        "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit": "100Gi",
        "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path": "/mnt/spark-shuffle",
        # 4. Volcano gang scheduling to prevent driver/executor deadlock
        "spark.kubernetes.scheduler.name": "volcano",
    }


def build_spark_application_manifest(
    app_name: str,
    namespace: str,
    image: str,
    main_app_file: str,
    fallback_storage_uri: str,
) -> dict:
    """Build a minimal Kubeflow Spark Operator `SparkApplication` CRD payload."""
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": app_name, "namespace": namespace},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": image,
            "mainApplicationFile": main_app_file,
            "sparkVersion": "4.0.0",
            "batchScheduler": "volcano",
            "sparkConf": get_k8s_spark_configs(fallback_storage_uri),
            "driver": {"cores": 2, "memory": "4g", "serviceAccount": "spark-driver-sa"},
            "executor": {
                "cores": 4,
                "memory": "16g",
                "memoryOverhead": "4g",
                "terminationGracePeriodSeconds": 120,
            },
        },
    }


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In Spark UI -> `Stages` tab, watch `Shuffle Write` and `Shuffle Spill (Disk)`:
#   spills land on `/mnt/spark-shuffle` (PVC) instead of container root `emptyDir`.
# - In Spark UI -> `Executors` tab during Spot preemption, verify executors transition
#   to `Decommissioned` while migrating active shuffle blocks to peers/fallback storage.
# - In `df.explain("formatted")`, confirm `AdaptiveSparkPlan` coalesces post-shuffle
#   partitions so idle executors can scale down cleanly once shuffle blocks expire.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: `spark.dynamicAllocation.shuffleTracking.enabled=true` keeps executors
#   alive as long as they hold active shuffle blocks; pair it with `shuffleTracking.timeout`
#   and AQE coalescing so idle executors do not linger indefinitely.
# - Gotcha 2: Without Volcano (or YuniKorn) gang scheduling, 10 concurrent drivers can
#   consume all cluster CPU/RAM and deadlock waiting for executor pods to schedule.
# - Gotcha 3: Set `terminationGracePeriodSeconds` (>= 90–120s) on executor pods and
#   configure `fallbackStorage.path` (S3/GCS/ADLS) so Spot SIGTERM migrations succeed.


if __name__ == "__main__":
    manifest = build_spark_application_manifest(
        app_name="lakehouse-daily-etl",
        namespace="data-prod",
        image="ghcr.io/example/spark-py:4.0.0",
        main_app_file="local:///opt/spark/work-dir/etl_job.py",
        fallback_storage_uri="s3a://prod-spark-shuffle-fallback/decommission/",
    )
    print("SparkApplication CRD generated:", manifest["metadata"], "| scheduler:", manifest["spec"]["batchScheduler"])
