# Databricks notebook source
# MAGIC %md
# MAGIC # Produce fake Avro data (POC only)
# MAGIC In production your real backend produces data. Run via the bundle:
# MAGIC `databricks bundle run produce_fake_data_job --params num_records=500`

# COMMAND ----------

# MAGIC %pip install faker "confluent-kafka[avro,schemaregistry]>=2.6" "fastavro>=1.9"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys
SRC_PATH = os.path.abspath("../src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# Job parameters arrive as widgets; defaults are for interactive use.
dbutils.widgets.text("topic", "life_policy_overview_sync_")
dbutils.widgets.text("num_records", "100")
dbutils.widgets.text("secret_scope", "streaming_framework")
dbutils.widgets.text("kafka_bootstrap_servers", "pkc-619z3.us-east1.gcp.confluent.cloud:9092")
dbutils.widgets.text("schema_registry_url", "https://psrc-176j5l8.us-east1.gcp.confluent.cloud")

SCOPE = dbutils.widgets.get("secret_scope")

KAFKA_CONFIG = {
    "bootstrap_servers": dbutils.widgets.get("kafka_bootstrap_servers"),
    "api_key":           dbutils.secrets.get(SCOPE, "kafka-api-key"),
    "api_secret":        dbutils.secrets.get(SCOPE, "kafka-api-secret"),
}
SR_CONFIG = {
    "url":        dbutils.widgets.get("schema_registry_url"),
    "api_key":    dbutils.secrets.get(SCOPE, "sr-api-key"),
    "api_secret": dbutils.secrets.get(SCOPE, "sr-api-secret"),
}
print("✅ Credentials loaded from secret scope:", SCOPE)

# COMMAND ----------

from streaming_framework.producers.avro_producer import produce_to_topic

produce_to_topic(
    topic        = dbutils.widgets.get("topic"),
    num_records  = int(dbutils.widgets.get("num_records")),
    kafka_config = KAFKA_CONFIG,
    sr_config    = SR_CONFIG,
)

# Other topics (after creating topic + schema in Confluent):
#   databricks bundle run produce_fake_data_job --params topic=nonlife_policy_overview_sync
#   databricks bundle run produce_fake_data_job --params topic=nonlife_sales_cockpit_sync
