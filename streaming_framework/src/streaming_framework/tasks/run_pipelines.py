"""
run_pipelines.py — Bundle task: run pipelines for a job type
─────────────────────────────────────────────────────────────────
Replaces both run_pipelines.py and CELL 0 / CELL 3 of run_notebook.py.

Two ways to select pipelines:
  1. Per-topic (what the bundle jobs use) — pass --topic + friends:
       --source-system KDP --topic life_policy_overview_sync_
       --schema-subject life_policy_overview_sync_-value
       --key-columns application_id:int,policy_number:long
     → build_pipeline_configs() creates landing/autoloader/quarantine
  2. Static — no --topic → PIPELINE_TEMPLATES in pipeline_config.py

--job-type picks the layers:
  landing    → Job A: Kafka → ADLS files
  autoloader → Job B: ADLS files → Bronze + quarantine
  all        → everything (manual / dev)
--layer narrows further to one layer (lets Job B run autoloader and
quarantine as separate, parallel tasks).

Layer types:
  landing    → Kafka → ADLS files
  autoloader → ADLS files → Bronze (via Auto Loader)
  quarantine → failed deserialization capture
  bronze     → kept for backward compatibility (POC Delta mode)
─────────────────────────────────────────────────────────────────
"""

from typing import List, Optional

from streaming_framework.config.pipeline_config import (
    JOB_TYPE_LAYERS,
    RuntimeSettings,
    build_pipeline_configs,
    build_runtime_settings,
    build_static_pipeline_configs,
)
from streaming_framework.tasks.common import add_connection_args, base_parser, get_spark


def _registry():
    from streaming_framework.pipelines.landing_pipeline     import LandingPipeline
    from streaming_framework.pipelines.bronze_pipeline      import BronzePipeline
    from streaming_framework.pipelines.auto_loader_pipeline import AutoLoaderPipeline
    from streaming_framework.pipelines.quarantine_pipeline  import QuarantinePipeline
    return {
        "landing":    LandingPipeline,
        "autoloader": AutoLoaderPipeline,
        "bronze":     BronzePipeline,       # kept for POC compatibility
        "quarantine": QuarantinePipeline,
    }


def select_configs(configs: List[dict], job_type: str = "all",
                   layer: Optional[str] = None) -> List[dict]:
    layers = JOB_TYPE_LAYERS[job_type]
    if layer:
        if layer not in layers:
            raise ValueError(f"layer '{layer}' is not part of job_type '{job_type}' ({layers})")
        layers = [layer]
    return [c for c in configs if c["layer"] in layers]


def run(spark, settings: RuntimeSettings, pipeline_configs: List[dict], label: str = "") -> dict:
    from streaming_framework.utils.logger        import PipelineLogger
    from streaming_framework.utils.alert_manager import AlertManager

    registry = _registry()
    logger   = PipelineLogger(spark, settings.catalog, settings.schema)
    alerter  = AlertManager(settings.alert_config)

    print(f"\n{'=' * 60}")
    print(f"STREAMING FRAMEWORK — {label.upper() or 'RUN'}")
    print(f"Target : {settings.catalog}.{settings.schema}")
    print(f"Running: {[c['pipeline_name'] for c in pipeline_configs]}")
    print(f"{'=' * 60}")

    failed, success = [], []

    for cfg in pipeline_configs:
        if not cfg.get("enabled", True):
            print(f"\n[SKIPPED] {cfg['pipeline_name']} (disabled)")
            continue

        layer        = cfg["layer"]
        pipeline_cls = registry.get(layer)
        if not pipeline_cls:
            print(f"\n[ERROR] No class for layer: '{layer}'")
            failed.append(cfg["pipeline_name"])
            continue

        if layer in ("autoloader", "bronze", "quarantine"):
            cfg["schema_registry_config"] = settings.schema_registry_config

        pipeline = pipeline_cls(
            spark         = spark,
            config        = cfg,
            kafka_config  = settings.kafka_config,
            logger        = logger,
            alert_manager = alerter,
        )
        try:
            pipeline.run()
            success.append(cfg["pipeline_name"])
        except Exception as e:
            failed.append(cfg["pipeline_name"])
            print(f"\n[FAILED] {cfg['pipeline_name']}: {e}")

    print(f"\nSucceeded : {success}")
    print(f"Failed    : {failed}")
    return {"success": success, "failed": failed}


def main(argv=None):
    p = add_connection_args(base_parser("Run streaming pipelines"))
    p.add_argument("--job-type", default="all", choices=sorted(JOB_TYPE_LAYERS))
    p.add_argument("--layer",    default="",
                   help="Optional: run a single layer of the job type")
    # per-topic parameters (Job A / Job B)
    p.add_argument("--source-system",    default="")
    p.add_argument("--topic",            default="")
    p.add_argument("--schema-subject",   default="")
    p.add_argument("--key-columns",      default="",
                   help="Comma-separated, e.g. application_id:int,policy_number:long")
    p.add_argument("--encoding",         default="avro", choices=["avro", "json"])
    p.add_argument("--starting-offset",  default="latest")
    p.add_argument("--trigger-interval", default="availableNow")
    args = p.parse_args(argv)

    if args.topic:
        missing = [n for n in ("source_system", "schema_subject", "key_columns")
                   if not getattr(args, n)]
        if missing:
            p.error(f"--topic also requires: {', '.join('--' + m.replace('_', '-') for m in missing)}")
        configs = build_pipeline_configs(
            source_system    = args.source_system,
            topic            = args.topic,
            schema_subject   = args.schema_subject,
            key_columns      = [k.strip() for k in args.key_columns.split(",") if k.strip()],
            encoding         = args.encoding,
            starting_offset  = args.starting_offset,
            trigger_interval = args.trigger_interval,
            catalog          = args.catalog,
            schema           = args.schema,
            landing_volume   = args.landing_volume,
        )
        label = f"{args.job_type} | {args.source_system} | {args.topic}"
    else:
        configs = build_static_pipeline_configs(args.catalog, args.schema, args.landing_volume)
        label   = f"{args.job_type} | static config"

    selected = select_configs(configs, args.job_type, args.layer or None)

    spark    = get_spark()
    settings = build_runtime_settings(
        catalog                 = args.catalog,
        schema                  = args.schema,
        landing_volume          = args.landing_volume,
        secret_scope            = args.secret_scope,
        kafka_bootstrap_servers = args.kafka_bootstrap_servers,
        schema_registry_url     = args.schema_registry_url,
        spark                   = spark,
    )
    result = run(spark, settings, selected, label)
    if result["failed"]:
        # non-zero exit → the job task is marked FAILED
        raise RuntimeError(
            f"❌ {len(result['failed'])} pipeline(s) failed: {result['failed']}. Check logs above."
        )


if __name__ == "__main__":
    main()
