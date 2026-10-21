# ============================================================
# produce_notebook.py
# Copy each cell into a Databricks notebook called
# "produce_fake_data" (outside streaming_framework_final folder)
#
# This is POC only — in production your real backend produces data
# ============================================================

# ── CELL 1: Install libraries ─────────────────────────────────
%pip install faker authlib

# ── CELL 2: Setup path ────────────────────────────────────────
import sys, os

FRAMEWORK_PATH = "/Workspace/Users/lanister20@gmail.com/streaming_framework_final"
LOCAL_PATH     = "/tmp/streaming_framework_final"

if LOCAL_PATH not in sys.path:
    sys.path.insert(0, LOCAL_PATH)

print(f"✅ Path set: {LOCAL_PATH}")

# ── CELL 3: Config (fill in your credentials) ─────────────────
KAFKA_CONFIG = {
    "bootstrap_servers": "pkc-619z3.us-east1.gcp.confluent.cloud:9092",
    "api_key":           os.environ.get("KAFKA_API_KEY"),
    "api_secret":        os.environ.get("KAFKA_API_SECRET"),
}

SR_CONFIG = {
    "url":      "https://psrc-176j5l8.us-east1.gcp.confluent.cloud",
    "api_key":  os.environ.get("SR_API_KEY"),
    "api_secret": os.environ.get("SR_API_SECRET"),
}

print(f"KAFKA_API_KEY : {'✅' if KAFKA_CONFIG['api_key']    else '❌ MISSING'}")
print(f"SR_API_KEY    : {'✅' if SR_CONFIG['api_key']       else '❌ MISSING'}")

# ── CELL 4: Produce to life_policy topic ─────────────────────
from producers.avro_producer import produce_to_topic

produce_to_topic(
    topic       = "life_policy_overview_sync_",
    num_records = 100,
    kafka_config = KAFKA_CONFIG,
    sr_config    = SR_CONFIG,
)

# ── CELL 5: Produce to nonlife_policy topic ───────────────────
# Uncomment after creating topic + schema in Confluent
# produce_to_topic(
#     topic       = "nonlife_policy_overview_sync",
#     num_records = 100,
#     kafka_config = KAFKA_CONFIG,
#     sr_config    = SR_CONFIG,
# )

# ── CELL 6: Produce to nonlife_sales_cockpit topic ────────────
# Uncomment after creating topic + schema in Confluent
# produce_to_topic(
#     topic       = "nonlife_sales_cockpit_sync",
#     num_records = 100,
#     kafka_config = KAFKA_CONFIG,
#     sr_config    = SR_CONFIG,
# )
