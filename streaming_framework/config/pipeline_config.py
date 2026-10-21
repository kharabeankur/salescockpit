"""
pipeline_config.py — Single Source of Truth
─────────────────────────────────────────────────────────────────
FULLY DYNAMIC — no manual schema mapping needed.

DEV changes from POC:
  1. landing writes files to ADLS Volume (not Delta table)
  2. bronze reads via Auto Loader (not Delta readStream)
  3. two separate jobs per topic (Job A landing, Job B autoloader)

landing_path : Unity Catalog Volume path for raw files
               /Volumes/catalog/schema/volume/source/topic/

To change file format: change "encoding" only.
  "avro" → writes/reads .avro files
  "json" → writes/reads .json files
  Zero code changes needed anywhere else.
─────────────────────────────────────────────────────────────────
"""

import os

# ── Infrastructure ─────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap_servers": "pkc-619z3.us-east1.gcp.confluent.cloud:9092",
    "api_key":           "SYR5NDEIHCTTPZQU",
    "api_secret":        "cfltbIK0mdZ4BDHT8eKNQfv/tyuQWyLjGUeBfs8wkgH7iHc5gFcrC5hzIbzKvotA",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism":    "PLAIN",
    "jaas_class":        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule",
}

SCHEMA_REGISTRY_CONFIG = {
    "url":        "https://psrc-176j5l8.us-east1.gcp.confluent.cloud",
    "api_key":    "HEIKTXYCHIJYQLFZ",
    "api_secret": "cflt2Fao/Ek/fY7GlWbP6tFjZlwgnuLOOSaEZxyxgqcv2CIBCAf6w7iPJdlW4vDg",
}

CATALOG      = "abd_easywithdata_poc"
DB_SCHEMA    = "streaming_poc"
LANDING_BASE = f"/Volumes/{CATALOG}/{DB_SCHEMA}/Landing"

ALERT_CONFIG = {
    "enabled":    False,
    "smtp_host":  "smtp.office365.com",
    "smtp_port":  587,
    "sender":     "your_email@outlook.com",
    "password":   os.environ.get("SMTP_PASSWORD"),
    "recipients": ["your_email@outlook.com"],
}

# ── Pipeline configs ───────────────────────────────────────────
# Structure per topic:
#   1. landing    → Job A: Kafka → ADLS files
#   2. autoloader → Job B: ADLS files → Bronze (via Auto Loader)
#   3. quarantine → Job B: runs alongside autoloader
# ──────────────────────────────────────────────────────────────

PIPELINE_CONFIGS = [

    # ══════════════════════════════════════════════════════════
    # KDP — life_policy_overview_sync_event
    # Job A: Kafka → Landing files
    # ══════════════════════════════════════════════════════════
    {
        "pipeline_name":    "kdp_life_policy_landing",
        "layer":            "landing",
        "enabled":          True,
        "topic":            "life_policy_overview_sync_",
        "starting_offset":  "latest",
        "encoding":         "avro",

        # ADLS path via Unity Catalog Volume
        # change "avro" → "json" here and landing + autoloader
        # both switch format automatically — zero other changes
        "landing_path":     f"{LANDING_BASE}/kdp/life_policy_sync/",

        "checkpoint_path":  f"/Volumes/{CATALOG}/{DB_SCHEMA}/checkpoints/kdp_life_policy_landing",
        "trigger_interval": "availableNow",
        "max_retries":      3,
        "retry_wait_secs":  10,
    },

    # ══════════════════════════════════════════════════════════
    # KDP — life_policy_overview_sync_event
    # Job B: Landing files → Bronze (Auto Loader)
    # ══════════════════════════════════════════════════════════
    {
        "pipeline_name":    "kdp_life_policy_autoloader",
        "layer":            "autoloader",              # ← new layer type
        "enabled":          True,
        "encoding":         "avro",
        "key_columns":      ["application_id:int", "policy_number:long"],
        "source_system":    "KDP",
        "topic":            "life_policy_overview_sync_",

        # same landing_path as above — Auto Loader reads from here
        "landing_path":     f"{LANDING_BASE}/kdp/life_policy_sync/",

        "target_table":     f"{CATALOG}.{DB_SCHEMA}.life_policy_overview_sync_event",
        "checkpoint_path":  f"/Volumes/{CATALOG}/{DB_SCHEMA}/checkpoints/kdp_life_policy_autoloader",
        "schema_subject":   "life_policy_overview_sync_-value",
        "trigger_interval": "availableNow",
        "max_retries":      3,
        "retry_wait_secs":  10,
        # schema auto-generated from Schema Registry at runtime
    },

    # ══════════════════════════════════════════════════════════
    # KDP — Quarantine (runs with Job B)
    # ══════════════════════════════════════════════════════════
    # In build_pipeline_configs() — quarantine block
    {
        "pipeline_name":    "kdp_life_policy_quarantine",
        "layer":            "quarantine",
        "enabled":          True,
        "encoding":         "avro",
        "source_system":    "KDP",
        "topic":            "life_policy_overview_sync_",
        "landing_path":     f"{LANDING_BASE}/kdp/life_policy_sync/",
        "source_table":     f"{CATALOG}.{DB_SCHEMA}.landing_life_policy",
        "target_table":     f"{CATALOG}.{DB_SCHEMA}.kafka_message_quarantine_kdp",
        "checkpoint_path":  f"/Volumes/{CATALOG}/{DB_SCHEMA}/checkpoints/kdp_life_policy_quarantine",
        "schema_subject":   "life_policy_overview_sync_-value",
        "trigger_interval": "availableNow",
        "max_retries":      3,
        "retry_wait_secs":  10,
},
    # # ══════════════════════════════════════════════════════════
    # # TIA — nonlife_policy_overview_sync_event (disabled — enable when ready)
    # # Same pattern — just change encoding, source_system, topic, key_columns
    # # ══════════════════════════════════════════════════════════
    # {
    #     "pipeline_name":    "tia_nonlife_policy_landing",
    #     "layer":            "landing",
    #     "enabled":          False,
    #     "topic":            "nonlife_policy_overview_sync",
    #     "starting_offset":  "latest",
    #     "encoding":         "avro",
    #     "landing_path":     f"{LANDING_BASE}/tia/nonlife_policy_sync/",
    #     "checkpoint_path":  f"/Volumes/{CATALOG}/{DB_SCHEMA}/checkpoints/tia_nonlife_policy_landing",
    #     "trigger_interval": "availableNow",
    #     "max_retries":      3,
    #     "retry_wait_secs":  10,
    # },
    # {
    #     "pipeline_name":    "tia_nonlife_policy_autoloader",
    #     "layer":            "autoloader",
    #     "enabled":          False,
    #     "encoding":         "avro",
    #     "key_columns":      ["application_id:int", "policy_number:long"],
    #     "source_system":    "TIA",
    #     "topic":            "nonlife_policy_overview_sync",
    #     "landing_path":     f"{LANDING_BASE}/tia/nonlife_policy_sync/",
    #     "target_table":     f"{CATALOG}.{DB_SCHEMA}.nonlife_policy_overview_sync_event",
    #     "checkpoint_path":  f"/Volumes/{CATALOG}/{DB_SCHEMA}/checkpoints/tia_nonlife_policy_autoloader",
    #     "schema_subject":   "nonlife_policy_overview_sync-value",
    #     "trigger_interval": "availableNow",
    #     "max_retries":      3,
    #     "retry_wait_secs":  10,
    # },
]


def get_pipeline_config(pipeline_name: str) -> dict:
    for cfg in PIPELINE_CONFIGS:
        if cfg["pipeline_name"] == pipeline_name:
            return cfg
    raise ValueError(f"No config for: {pipeline_name}")


def get_configs_by_layer(layer: str) -> list:
    return [
        c for c in PIPELINE_CONFIGS
        if c["layer"] == layer and c.get("enabled", True)
    ]


def get_enabled_pipelines() -> list:
    return [c for c in PIPELINE_CONFIGS if c.get("enabled", True)]


def build_pipeline_configs(
    source_system:    str,
    topic:            str,
    schema_subject:   str,
    key_columns:      list,
    encoding:         str = "avro",
    starting_offset:  str = "latest",
    trigger_interval: str = "availableNow",
) -> list:
    """
    Build pipeline configs dynamically from Job parameters.
    Used by parameterised Databricks Jobs.
    """
    src   = source_system.lower()
    tname = topic.rstrip("_").lower()
    base  = f"/Volumes/{CATALOG}/{DB_SCHEMA}/checkpoints"
    lpath = f"{LANDING_BASE}/{src}/{tname}/"

    return [
        {
            "pipeline_name":    f"{src}_{tname}_landing",
            "layer":            "landing",
            "enabled":          True,
            "topic":            topic,
            "starting_offset":  starting_offset,
            "encoding":         encoding,
            "landing_path":     lpath,
            "checkpoint_path":  f"{base}/{src}_{tname}_landing",
            "trigger_interval": trigger_interval,
            "max_retries":      3,
            "retry_wait_secs":  10,
        },
        {
            "pipeline_name":    f"{src}_{tname}_autoloader",
            "layer":            "autoloader",
            "enabled":          True,
            "encoding":         encoding,
            "key_columns":      key_columns,
            "source_system":    source_system,
            "topic":            topic,
            "landing_path":     lpath,
            "target_table":     f"{CATALOG}.{DB_SCHEMA}.{tname}",
            "checkpoint_path":  f"{base}/{src}_{tname}_autoloader",
            "schema_subject":   schema_subject,
            "trigger_interval": trigger_interval,
            "max_retries":      3,
            "retry_wait_secs":  10,
        },
        {
            "pipeline_name":    f"{src}_{tname}_quarantine",
            "layer":            "quarantine",
            "enabled":          True,
            "encoding":         encoding,
            "source_system":    source_system,
            "topic":            topic,
            "landing_path":     lpath,
            "target_table":     f"{CATALOG}.{DB_SCHEMA}.kafka_message_quarantine_{src}",
            "checkpoint_path":  f"{base}/{src}_{tname}_quarantine",
            "schema_subject":   schema_subject,
            "trigger_interval": trigger_interval,
            "max_retries":      3,
            "retry_wait_secs":  10,
        },
    ]