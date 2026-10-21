# ── CELL 1: Install libraries ────────────────────────────────

# ============================================================
# run_notebook.py — Framework Runner
# Copy each cell into a Databricks notebook.
# ============================================================
# ── CELL 0: Read parameters ───────────────────────────────────
import sys
import subprocess, sys
subprocess.check_call([
    sys.executable, "-m", "pip", "install",
    "confluent-kafka", "fastavro", "authlib", "requests", "faker",
    "--quiet"
])
print("✅ Libraries installed")
def get_param(name, default):
    """Read from widgets (notebook) or sys.argv (job script)."""
    try:
        return dbutils.widgets.get(name)
    except:
        # running as Python script via job — read from sys.argv
        args = sys.argv
        flag = f"--{name}"
        if flag in args:
            return args[args.index(flag) + 1]
        return default

SOURCE_SYSTEM    = get_param("source_system",    "KDP")
TOPIC            = get_param("topic",            "life_policy_overview_sync_")
SCHEMA_SUBJECT   = get_param("schema_subject",   "life_policy_overview_sync_-value")
KEY_COLUMNS      = get_param("key_columns",      "application_id:int,policy_number:long").split(",")
ENCODING         = get_param("encoding",         "avro")
STARTING_OFFSET  = get_param("starting_offset",  "latest")
TRIGGER_INTERVAL = get_param("trigger_interval", "availableNow")
JOB_TYPE         = get_param("job_type",         "landing")

print(f"✅ job_type      : {JOB_TYPE}")
print(f"✅ source_system : {SOURCE_SYSTEM}")
print(f"✅ topic         : {TOPIC}")
print(f"✅ key_columns   : {KEY_COLUMNS}")
print(f"✅ encoding      : {ENCODING}")

# ── CELL 1: Setup — run every session ────────────────────────
import shutil, os, sys
# %pip install confluent-kafka authlib requests fastavro

# FRAMEWORK_PATH = "/Workspace/Users/lanister20@gmail.com/streaming_framework"
# LOCAL_PATH     = "/tmp/streaming_framework_final"


FRAMEWORK_PATH = "/Workspace/Users/lanister20@gmail.com/streaming_framework"

# Clear old imports
mods = [k for k in sys.modules if any(
    k == m or k.startswith(m + '.')
    for m in ['config', 'utils', 'pipelines', 'producers']
)]
for m in list(mods):
    sys.modules.pop(m, None)

sys.path = [p for p in sys.path if "streaming_framework" not in p]
sys.path.insert(0, FRAMEWORK_PATH)

LOCAL_PATH = FRAMEWORK_PATH  # use workspace directly — no copy needed
print(f"✅ Framework : {LOCAL_PATH}")
print(f"✅ Cleared   : {len(mods)} modules")
print(f"✅ Ready!")
# if os.path.exists(LOCAL_PATH):
#     shutil.rmtree(LOCAL_PATH)
# shutil.copytree(FRAMEWORK_PATH, LOCAL_PATH)

# mods = [k for k in sys.modules if any(
#     k == m or k.startswith(m + '.')
#     for m in ['config', 'utils', 'pipelines', 'producers']
# )]
# for m in list(mods):
#     sys.modules.pop(m, None)

# sys.path = [p for p in sys.path if "streaming_framework" not in p]
# sys.path.insert(0, LOCAL_PATH)

print(f"✅ Framework : {LOCAL_PATH}")
print(f"✅ Cleared   : {len(mods)} modules")
print(f"✅ Ready!")

# ── CELL 2: Create tables (run ONCE before first pipeline run) ─
# Reads and executes DDL — creates tables if not exist
ddl_path = f"{LOCAL_PATH}/ddl/create_tables_kdp.sql"
with open(ddl_path) as f:
    ddl = f.read()

# Execute each statement separately (skip comments and blank lines)
statements = [
    s.strip() for s in ddl.split(";")
    if s.strip() and not s.strip().startswith("--")
]
for stmt in statements:
    if stmt.upper().startswith(("CREATE", "USE", "SHOW")):
        try:
            spark.sql(stmt)
            first_line = stmt.split('\n')[0][:60]
            print(f"✅ {first_line}")
        except Exception as e:
            print(f"⚠️  {str(e)[:100]}")

print("\n✅ Tables ready!")

# ── CELL 3: Run all pipelines ─────────────────────────────────
# ── CELL 3: Run pipelines filtered by job_type ────────────────
from config.pipeline_config import (
    KAFKA_CONFIG, SCHEMA_REGISTRY_CONFIG,
    ALERT_CONFIG, CATALOG, DB_SCHEMA,
    build_pipeline_configs,
)
from utils.logger        import PipelineLogger
from utils.alert_manager import AlertManager
from pipelines.landing_pipeline      import LandingPipeline
from pipelines.auto_loader_pipeline  import AutoLoaderPipeline
from pipelines.bronze_pipeline       import BronzePipeline
from pipelines.quarantine_pipeline   import QuarantinePipeline

logger  = PipelineLogger(spark, CATALOG, DB_SCHEMA)
alerter = AlertManager(ALERT_CONFIG)

PIPELINE_REGISTRY = {
    "landing":    LandingPipeline,
    "autoloader": AutoLoaderPipeline,
    "bronze":     BronzePipeline,
    "quarantine": QuarantinePipeline,
}

# build all 3 configs (landing + autoloader + quarantine)
all_configs = build_pipeline_configs(
    source_system    = SOURCE_SYSTEM,
    topic            = TOPIC,
    schema_subject   = SCHEMA_SUBJECT,
    key_columns      = KEY_COLUMNS,
    encoding         = ENCODING,
    starting_offset  = STARTING_OFFSET,
    trigger_interval = TRIGGER_INTERVAL,
)

# ── filter by job_type ────────────────────────────────────────
# Job A (job_type=landing)    → runs only landing pipeline
# Job B (job_type=autoloader) → runs autoloader + quarantine
if JOB_TYPE == "landing":
    pipeline_configs = [c for c in all_configs if c["layer"] == "landing"]
elif JOB_TYPE == "autoloader":
    pipeline_configs = [c for c in all_configs if c["layer"] in ("autoloader", "quarantine")]
else:
    pipeline_configs = all_configs  # run all (manual/dev mode)

print(f"\n{'='*60}")
print(f"STREAMING FRAMEWORK — {JOB_TYPE.upper()}")
print(f"Source : {SOURCE_SYSTEM} | Topic: {TOPIC}")
print(f"Running: {[c['pipeline_name'] for c in pipeline_configs]}")
print(f"{'='*60}")

failed = []
success = []

for cfg in pipeline_configs:
    layer        = cfg["layer"]
    pipeline_cls = PIPELINE_REGISTRY[layer]

    if layer in ("autoloader", "bronze", "quarantine"):
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
        print(f"[FAILED] {cfg['pipeline_name']}: {e}")

print(f"\nSucceeded : {success}")
print(f"Failed    : {failed}")

# ← ADD THESE 3 LINES
if failed:
    raise Exception(
        f"❌ {len(failed)} pipeline(s) failed: {failed}. Check logs above."
    )
# ── CELL 7: Reset (run only when needed) ─────────────────────
# dbutils.fs.rm("/Volumes/abd_easywithdata_poc/streaming_poc/checkpoints/kdp_life_policy_landing",    True)
# dbutils.fs.rm("/Volumes/abd_easywithdata_poc/streaming_poc/checkpoints/kdp_life_policy_bronze",     True)
# dbutils.fs.rm("/Volumes/abd_easywithdata_poc/streaming_poc/checkpoints/kdp_life_policy_quarantine", True)
# spark.sql("DROP TABLE IF EXISTS abd_easywithdata_poc.streaming_poc.landing_life_policy")
# spark.sql("DROP TABLE IF EXISTS abd_easywithdata_poc.streaming_poc.life_policy_overview_sync_event")
# spark.sql("DROP TABLE IF EXISTS abd_easywithdata_poc.streaming_poc.kafka_message_quarantine_kdp")
# print("✅ Reset done")
