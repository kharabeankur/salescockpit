# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming Framework — interactive runner
# MAGIC Replaces the old `run_notebook.py`. Same parameters as the bundle jobs, but imports
# MAGIC the framework from the bundle's synced `src/` folder so you can iterate without
# MAGIC rebuilding the wheel. Scheduled runs: `databricks bundle run kdp_life_policy_landing_job`.

# COMMAND ----------

# MAGIC %pip install "confluent-kafka[avro,schemaregistry]>=2.6" "fastavro>=1.9"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# ── Parameters (same names as the job parameters) ──────────────
import os, sys

for name, default in {
    "job_type":         "all",          # landing | autoloader | all
    "source_system":    "KDP",
    "topic":            "life_policy_overview_sync_",
    "schema_subject":   "life_policy_overview_sync_-value",
    "key_columns":      "application_id:int,policy_number:long",
    "encoding":         "avro",
    "starting_offset":  "latest",
    "trigger_interval": "availableNow",
    "catalog":          "abd_easywithdata_poc",
    "schema":           "streaming_poc_dev",
    "landing_volume":   "Landing",
    "secret_scope":     "streaming_framework",
}.items():
    dbutils.widgets.text(name, default)
get = dbutils.widgets.get

# ── Import framework from the bundle's src/ folder ────────────
SRC_PATH = os.path.abspath("../src")
for m in [k for k in sys.modules if k == "streaming_framework" or k.startswith("streaming_framework.")]:
    sys.modules.pop(m, None)
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
print(f"✅ Framework : {SRC_PATH}")
print(f"✅ Target    : {get('catalog')}.{get('schema')}  | job_type={get('job_type')}")

# COMMAND ----------

# ── Schema + volumes + audit table (idempotent) ────────────────
from streaming_framework.tasks.setup_tables import setup
setup(spark, get("catalog"), get("schema"), get("landing_volume"), apply_ddl=False)

# COMMAND ----------

# ── Run pipelines filtered by job_type ─────────────────────────
from streaming_framework.config.pipeline_config import build_pipeline_configs, build_runtime_settings
from streaming_framework.tasks.run_pipelines import run, select_configs

all_configs = build_pipeline_configs(
    source_system    = get("source_system"),
    topic            = get("topic"),
    schema_subject   = get("schema_subject"),
    key_columns      = get("key_columns").split(","),
    encoding         = get("encoding"),
    starting_offset  = get("starting_offset"),
    trigger_interval = get("trigger_interval"),
    catalog          = get("catalog"),
    schema           = get("schema"),
    landing_volume   = get("landing_volume"),
)
settings = build_runtime_settings(
    catalog=get("catalog"), schema=get("schema"), landing_volume=get("landing_volume"),
    secret_scope=get("secret_scope"), spark=spark,
)
result = run(spark, settings, select_configs(all_configs, get("job_type")), get("job_type"))
if result["failed"]:
    raise Exception(f"❌ {len(result['failed'])} pipeline(s) failed: {result['failed']}")

# COMMAND ----------

# ── Monitoring dashboard ───────────────────────────────────────
from streaming_framework.utils.monitor import PipelineMonitor
PipelineMonitor(spark, get("catalog"), get("schema")).run_all()

# COMMAND ----------

# ── Reset (uncomment only when needed) ─────────────────────────
# for c in all_configs:
#     dbutils.fs.rm(c["checkpoint_path"], True)
# dbutils.fs.rm(all_configs[0]["landing_path"], True)
# for c in all_configs:
#     if "target_table" in c:
#         spark.sql(f"DROP TABLE IF EXISTS {c['target_table']}")
# print("✅ Reset done")
