"""
run_pipelines.py — Orchestrator
─────────────────────────────────────────────────────────────────
Runs all enabled pipelines in order.
To add a new topic: edit pipeline_config.py only.
This file never changes.

Layer types:
  landing    → Kafka → ADLS files
  autoloader → ADLS files → Bronze (via Auto Loader)
  quarantine → failed deserialization capture
  bronze     → kept for backward compatibility (POC Delta mode)
─────────────────────────────────────────────────────────────────
"""

import sys
from pyspark.sql import SparkSession

from config.pipeline_config import (
    KAFKA_CONFIG,
    SCHEMA_REGISTRY_CONFIG,
    ALERT_CONFIG,
    CATALOG,
    DB_SCHEMA,
    PIPELINE_CONFIGS,
)
from utils.logger                    import PipelineLogger
from utils.alert_manager             import AlertManager
from pipelines.landing_pipeline      import LandingPipeline
from pipelines.bronze_pipeline       import BronzePipeline
from pipelines.auto_loader_pipeline  import AutoLoaderPipeline
from pipelines.quarantine_pipeline   import QuarantinePipeline

spark   = SparkSession.builder.getOrCreate()
logger  = PipelineLogger(spark, CATALOG, DB_SCHEMA)
alerter = AlertManager(ALERT_CONFIG)

PIPELINE_REGISTRY = {
    "landing":    LandingPipeline,
    "autoloader": AutoLoaderPipeline,   # ← new
    "bronze":     BronzePipeline,       # ← kept for POC compatibility
    "quarantine": QuarantinePipeline,
}

print("=" * 60)
print("STREAMING FRAMEWORK — STARTING")
print(f"Catalog : {CATALOG}")
print(f"Schema  : {DB_SCHEMA}")
enabled = [c for c in PIPELINE_CONFIGS if c.get("enabled", True)]
print(f"Enabled : {len(enabled)} pipelines")
print("=" * 60)

failed  = []
success = []

for cfg in PIPELINE_CONFIGS:

    if not cfg.get("enabled", True):
        print(f"\n[SKIPPED] {cfg['pipeline_name']} (disabled)")
        continue

    layer        = cfg["layer"]
    pipeline_cls = PIPELINE_REGISTRY.get(layer)

    if not pipeline_cls:
        print(f"\n[ERROR] No class for layer: '{layer}'")
        failed.append(cfg["pipeline_name"])
        continue

    if layer in ("bronze", "autoloader", "quarantine"):
        cfg["schema_registry_config"] = SCHEMA_REGISTRY_CONFIG

    pipeline = pipeline_cls(
        spark         = spark,
        config        = cfg,
        kafka_config  = KAFKA_CONFIG,
        logger        = logger,
        alert_manager = alerter,
    )

    try:
        pipeline.run()
        success.append(cfg["pipeline_name"])
    except Exception as e:
        failed.append(cfg["pipeline_name"])
        print(f"\n[FAILED] {cfg['pipeline_name']}: {e}")

print("\n" + "=" * 60)
print("STREAMING FRAMEWORK — COMPLETE")
print(f"  Succeeded : {success}")
print(f"  Failed    : {failed}")
print("=" * 60)

if failed:
    sys.exit(1)