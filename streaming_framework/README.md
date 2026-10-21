# Streaming Framework — Final
## Confluent Kafka → Databricks Delta Lake

### Overview
Config-driven, scalable streaming pipeline framework.
Supports any number of topics with zero code changes.
Two schema modes: dynamic (Schema Registry) and static (config).

---

### Folder Structure
```
streaming_framework_final/
├── config/
│   └── pipeline_config.py     ← ALL configs here (topics, tables, schemas)
├── utils/
│   ├── logger.py              ← audit log writer
│   └── alert_manager.py       ← email on failure
├── pipelines/
│   ├── base_pipeline.py       ← parent class (retry + logging)
│   ├── landing_pipeline.py    ← Kafka → raw Delta
│   └── bronze_pipeline.py     ← parse JSON → typed Delta (static + dynamic)
├── producers/
│   ├── avro_producer.py       ← generic fake data producer (POC only)
│   └── produce_notebook.py    ← notebook cells to produce data
├── run_pipelines.py           ← orchestrator
└── run_notebook.py            ← notebook cells to run framework
```

---

### Topics Configured

| Topic | Schema Mode | Status |
|-------|-------------|--------|
| sample_data_orders | Static (json_schema) | ✅ Enabled |
| life_policy_overview_sync_ | Dynamic (Schema Registry) | ✅ Enabled |
| nonlife_policy_overview_sync | Dynamic (Schema Registry) | ⏸ Disabled |
| nonlife_sales_cockpit_sync | Dynamic (Schema Registry) | ⏸ Disabled |

---

### To Enable a New Topic
1. Create topic in Confluent
2. Register Avro schema in Schema Registry
3. Produce data (run produce_notebook.py cells)
4. In pipeline_config.py: set `"enabled": True`
5. Run framework — done!

---

### Environment Variables (set on Databricks cluster)
```
KAFKA_API_KEY     → Confluent Cluster → API Keys
KAFKA_API_SECRET  → same
SR_API_KEY        → Confluent Environment → Schema Registry → API Keys
SR_API_SECRET     → same
SMTP_PASSWORD     → email password (optional)
```

---

### How to Run
1. Upload this folder to Databricks Workspace
2. Create notebook → copy cells from run_notebook.py
3. Run Cell 1 (setup) → Cell 2 (pipelines)
4. Check Cell 3 (audit log) for status

---

### Schema Modes Explained

**Static** (json_schema in config):
- Define field names and types manually
- Good for simple flat schemas
- Change schema = edit pipeline_config.py

**Dynamic** (schema_subject in config):
- Fetch schema from Confluent Schema Registry at runtime
- No schema defined anywhere in code
- Change schema in Confluent = picked up automatically next run
- Works for complex nested Avro schemas

---

### Adding Another Topic (example: nonlife_policy)
In pipeline_config.py, change:
```python
"enabled": False  →  "enabled": True
```
That's it. Zero other changes needed.
```
