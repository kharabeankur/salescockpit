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

Asset Bundle (stage 1) changes
  - Catalog / schema / landing volume are no longer hard-coded.
    They come from the bundle target and are passed to the builder
    functions below. Static configs use {catalog} / {schema} /
    {landing_base} placeholders.
  - Kafka, Schema Registry and SMTP credentials are read from a
    Databricks secret scope (build_runtime_settings()).
  - Endpoints (bootstrap servers, SR URL) are bundle variables.
─────────────────────────────────────────────────────────────────
"""

import copy
from dataclasses import dataclass
from typing import Dict, List, Optional

from streaming_framework.utils.secrets import get_secret

# ── Defaults (overridden by bundle variables) ─────────────────
DEFAULT_CATALOG                 = "abd_easywithdata_poc"
DEFAULT_SCHEMA                  = "streaming_poc"
DEFAULT_LANDING_VOLUME          = "Landing"
DEFAULT_SECRET_SCOPE            = "streaming_framework"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
DEFAULT_SCHEMA_REGISTRY_URL     = "https://psrc-176j5l8.us-east1.gcp.confluent.cloud"

# ── Secret keys inside the scope (and env-var fallbacks) ──────
SECRET_KEYS = {
    "kafka_api_key":    ("kafka-api-key",    "KAFKA_API_KEY"),
    "kafka_api_secret": ("kafka-api-secret", "KAFKA_API_SECRET"),
    "sr_api_key":       ("sr-api-key",       "SR_API_KEY"),
    "sr_api_secret":    ("sr-api-secret",    "SR_API_SECRET"),
    "smtp_password":    ("smtp-password",    "SMTP_PASSWORD"),
}

# ── Static (non-secret) settings ──────────────────────────────
KAFKA_STATIC = {
    "security_protocol": "SASL_SSL",
    "sasl_mechanism":    "PLAIN",
    "jaas_class":        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule",
}

ALERT_STATIC = {
    "enabled":    False,
    "smtp_host":  "smtp.office365.com",
    "smtp_port":  587,
    "sender":     "your_email@outlook.com",
    "recipients": ["your_email@outlook.com"],
}

# Which layers each job type runs
#   Job A (landing)    → Kafka → ADLS files
#   Job B (autoloader) → ADLS files → Bronze + quarantine
JOB_TYPE_LAYERS = {
    "landing":    ["landing"],
    "autoloader": ["autoloader", "quarantine"],
    "all":        ["landing", "autoloader", "quarantine", "bronze"],
}


def landing_base(catalog: str, schema: str, landing_volume: str = DEFAULT_LANDING_VOLUME) -> str:
    return f"/Volumes/{catalog}/{schema}/{landing_volume}"


# ══════════════════════════════════════════════════════════════
# Mode 1 — static pipeline list (used by run_pipelines without --topic)
# ══════════════════════════════════════════════════════════════
PIPELINE_TEMPLATES = [

    # KDP — life_policy_overview_sync_event — Job A: Kafka → Landing files
    {
        "pipeline_name":    "kdp_life_policy_landing",
        "layer":            "landing",
        "enabled":          True,
        "topic":            "life_policy_overview_sync_",
        "starting_offset":  "latest",
        "encoding":         "avro",
        "landing_path":     "{landing_base}/kdp/life_policy_sync/",
        "checkpoint_path":  "/Volumes/{catalog}/{schema}/checkpoints/kdp_life_policy_landing",
        "trigger_interval": "availableNow",
        "max_retries":      3,
        "retry_wait_secs":  10,
    },

    # KDP — Job B: Landing files → Bronze (Auto Loader)
    {
        "pipeline_name":    "kdp_life_policy_autoloader",
        "layer":            "autoloader",
        "enabled":          True,
        "encoding":         "avro",
        "key_columns":      ["application_id:int", "policy_number:long"],
        "source_system":    "KDP",
        "topic":            "life_policy_overview_sync_",
        "landing_path":     "{landing_base}/kdp/life_policy_sync/",
        "target_table":     "{catalog}.{schema}.life_policy_overview_sync_event",
        "checkpoint_path":  "/Volumes/{catalog}/{schema}/checkpoints/kdp_life_policy_autoloader",
        "schema_subject":   "life_policy_overview_sync_-value",
        "trigger_interval": "availableNow",
        "max_retries":      3,
        "retry_wait_secs":  10,
    },

    # KDP — Quarantine (runs with Job B)
    {
        "pipeline_name":    "kdp_life_policy_quarantine",
        "layer":            "quarantine",
        "enabled":          True,
        "encoding":         "avro",
        "source_system":    "KDP",
        "topic":            "life_policy_overview_sync_",
        "landing_path":     "{landing_base}/kdp/life_policy_sync/",
        "source_table":     "{catalog}.{schema}.landing_life_policy",
        "target_table":     "{catalog}.{schema}.kafka_message_quarantine_kdp",
        "checkpoint_path":  "/Volumes/{catalog}/{schema}/checkpoints/kdp_life_policy_quarantine",
        "schema_subject":   "life_policy_overview_sync_-value",
        "trigger_interval": "availableNow",
        "max_retries":      3,
        "retry_wait_secs":  10,
    },

    # TIA — nonlife_policy_overview_sync_event (disabled — enable when ready)
    # Same pattern — just change encoding, source_system, topic, key_columns.
    # (see the original file history for the commented-out blocks)
]

_TEMPLATED_KEYS = ("target_table", "source_table", "checkpoint_path", "landing_path")


def build_static_pipeline_configs(catalog: str = DEFAULT_CATALOG,
                                  schema: str = DEFAULT_SCHEMA,
                                  landing_volume: str = DEFAULT_LANDING_VOLUME) -> List[dict]:
    """Fresh copy of PIPELINE_TEMPLATES for one catalog / schema."""
    lbase   = landing_base(catalog, schema, landing_volume)
    configs = copy.deepcopy(PIPELINE_TEMPLATES)
    for cfg in configs:
        for key in _TEMPLATED_KEYS:
            if key in cfg:
                cfg[key] = (cfg[key]
                            .replace("{landing_base}", lbase)
                            .replace("{catalog}", catalog)
                            .replace("{schema}", schema))
    return configs


# ══════════════════════════════════════════════════════════════
# Mode 2 — parameterised per-topic builder (used by the bundle jobs)
# ══════════════════════════════════════════════════════════════
def build_pipeline_configs(
    source_system:    str,
    topic:            str,
    schema_subject:   str,
    key_columns:      list,
    encoding:         str = "avro",
    starting_offset:  str = "latest",
    trigger_interval: str = "availableNow",
    catalog:          str = DEFAULT_CATALOG,
    schema:           str = DEFAULT_SCHEMA,
    landing_volume:   str = DEFAULT_LANDING_VOLUME,
) -> list:
    """
    Build landing + autoloader + quarantine configs from job parameters.
    Used by the parameterised Databricks Jobs (Job A / Job B).
    """
    src   = source_system.lower()
    tname = topic.rstrip("_").lower()
    base  = f"/Volumes/{catalog}/{schema}/checkpoints"
    lpath = f"{landing_base(catalog, schema, landing_volume)}/{src}/{tname}/"

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
            "target_table":     f"{catalog}.{schema}.{tname}",
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
            "target_table":     f"{catalog}.{schema}.kafka_message_quarantine_{src}",
            "checkpoint_path":  f"{base}/{src}_{tname}_quarantine",
            "schema_subject":   schema_subject,
            "trigger_interval": trigger_interval,
            "max_retries":      3,
            "retry_wait_secs":  10,
        },
    ]


# ── Helpers (kept for backward compatibility) ─────────────────
def get_pipeline_config(pipeline_name: str, configs: Optional[List[dict]] = None) -> dict:
    for cfg in configs or build_static_pipeline_configs():
        if cfg["pipeline_name"] == pipeline_name:
            return cfg
    raise ValueError(f"No config for: {pipeline_name}")


def get_configs_by_layer(layer: str, configs: Optional[List[dict]] = None) -> list:
    return [
        c for c in (configs or build_static_pipeline_configs())
        if c["layer"] == layer and c.get("enabled", True)
    ]


def get_enabled_pipelines(configs: Optional[List[dict]] = None) -> list:
    return [c for c in (configs or build_static_pipeline_configs()) if c.get("enabled", True)]


# ── Runtime settings (resolved inside the Databricks job) ─────
@dataclass
class RuntimeSettings:
    catalog:                str
    schema:                 str
    landing_volume:         str
    kafka_config:           Dict
    schema_registry_config: Dict
    alert_config:           Dict


def build_runtime_settings(catalog: str = DEFAULT_CATALOG,
                           schema: str = DEFAULT_SCHEMA,
                           landing_volume: str = DEFAULT_LANDING_VOLUME,
                           secret_scope: str = DEFAULT_SECRET_SCOPE,
                           kafka_bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
                           schema_registry_url: str = DEFAULT_SCHEMA_REGISTRY_URL,
                           spark=None) -> RuntimeSettings:
    """Resolve secrets and build the connection / alert config dicts."""

    def secret(name, required=True):
        key, env_var = SECRET_KEYS[name]
        return get_secret(key, scope=secret_scope, env_var=env_var,
                          spark=spark, required=required)

    return RuntimeSettings(
        catalog        = catalog,
        schema         = schema,
        landing_volume = landing_volume,
        kafka_config   = {
            "bootstrap_servers": kafka_bootstrap_servers,
            "api_key":           secret("kafka_api_key"),
            "api_secret":        secret("kafka_api_secret"),
            **KAFKA_STATIC,
        },
        schema_registry_config = {
            "url":        schema_registry_url,
            "api_key":    secret("sr_api_key"),
            "api_secret": secret("sr_api_secret"),
        },
        alert_config = {
            **ALERT_STATIC,
            "password": secret("smtp_password", required=ALERT_STATIC["enabled"]),
        },
    )
