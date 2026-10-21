# Databricks notebook source
# MAGIC %md
# MAGIC Great — let me give you a complete demo guide covering everything you built and how to handle technical questions.
# MAGIC
# MAGIC Here is your complete demo guide:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ![POC Demo Architecture](../docs/poc_demo_architecture.png)
# MAGIC
# MAGIC ## 1. Business case
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Problem** — "Generali has 6 source systems (KDP, TIA, HUGO, LISA, REDy, CCD) each sending real-time policy events over Kafka. Previously each team built their own ingestion — no standards, no reusability, no schema governance."
# MAGIC
# MAGIC **Solution** — "We built a config-driven streaming framework on Databricks that handles any Kafka topic with zero code changes. Add a new topic in 2 minutes by editing one config file."
# MAGIC
# MAGIC **Proof** — show the running pipeline with real data in Delta tables.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Key areas to cover in the demo
# MAGIC
# MAGIC **Area 1 — Architecture (show the diagram)**
# MAGIC - Confluent Cloud → Kafka topics → Databricks Structured Streaming → Landing → Bronze → Quarantine → Unity Catalog Delta Lake
# MAGIC - Highlight: Schema Registry handles Avro schema automatically — no manual typing
# MAGIC
# MAGIC **Area 2 — Framework design (open `pipeline_config.py`)**
# MAGIC - Show how small the config is — 3 blocks per topic, no schema mapping
# MAGIC - Say: "The framework auto-fetches the Avro schema from Schema Registry at runtime using `SchemaConverter`. Adding TIA, HUGO, LISA topics means editing this file only — zero code changes anywhere else"
# MAGIC
# MAGIC **Area 3 — Bronze model alignment (show the table)**
# MAGIC ```sql
# MAGIC DESCRIBE abd_easywithdata_poc.streaming_poc.life_policy_overview_sync_event
# MAGIC ```
# MAGIC - Show all 42 columns matching the bronze physical model v2
# MAGIC - Key columns, struct/array columns, metadata columns — all present
# MAGIC
# MAGIC **Area 4 — Live data (query the table)**
# MAGIC ```sql
# MAGIC SELECT application_id, policy_number,
# MAGIC        audit_information_user,
# MAGIC        life_policy_life_product_instance,
# MAGIC        life_policy_status,
# MAGIC        __message_valid_flag,
# MAGIC        __schema_version_id,
# MAGIC        __source_system,
# MAGIC        __load_timestamp
# MAGIC FROM streaming_poc.life_policy_overview_sync_event
# MAGIC LIMIT 5
# MAGIC ```
# MAGIC
# MAGIC **Area 5 — Monitoring dashboard (run Cell 4)**
# MAGIC - Show audit log, row counts, pipeline performance, quarantine summary
# MAGIC - Say: "Every pipeline run is logged. We can see success rate, duration, and any failures instantly"
# MAGIC
# MAGIC **Area 6 — Test results (run test notebook)**
# MAGIC - Show all green ticks
# MAGIC - Say: "30 automated tests validate data quality — key columns not null, correct format, no duplicates, all metadata populated"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Questions you will get — and how to answer them
# MAGIC
# MAGIC **Q: How does schema evolution work if the Avro schema changes?**
# MAGIC "Schema Registry manages versions. The framework fetches the latest version on every run. If a new field is added to the Avro schema, `SchemaConverter` automatically picks it up — the Spark StructType is rebuilt from scratch each run. The DDL table uses `CLUSTER BY` and Delta's schema evolution handles new columns via `mergeSchema`."
# MAGIC
# MAGIC **Q: What about late-arriving data or replay?**
# MAGIC "Landing table stores all raw Kafka messages as binary. `starting_offset: earliest` replays from the beginning. Checkpoints track exactly where each pipeline is — if bronze fails, we replay only the missing data from landing without re-reading Kafka."
# MAGIC
# MAGIC **Q: How do you handle message failures?**
# MAGIC "Three-layer approach: `__message_valid_flag` marks messages that deserialized but had data issues. `__rescued_data` captures partial data. Quarantine table catches messages that couldn't be deserialized at all — raw bytes preserved for replay once the schema issue is fixed."
# MAGIC
# MAGIC **Q: How does this scale to all 14 topics?**
# MAGIC "Config-only. Each topic is 3 config blocks — landing, bronze, quarantine. One Databricks Job per source system. TIA, HUGO, LISA, REDy, CCD topics are already stubbed in config as `enabled: False` — flip to `True` when topics are ready in Confluent."
# MAGIC
# MAGIC **Q: What about the real project catalog and schemas?**
# MAGIC "POC uses `streaming_poc` schema. Real project uses `src_kdp`, `src_tia` etc. in the production catalog. One config change — `CATALOG` and `DB_SCHEMA` variables at the top of `pipeline_config.py`. Everything else stays identical."
# MAGIC
# MAGIC **Q: How does this connect to Silver/Gold?**
# MAGIC "Bronze stores typed structs — `life_policy_life_product_instance` as a full STRUCT with `signedOn`, `status`, `yearlyPremium` etc. Silver/Gold (dbt) flattens these to typed columns for analytics. The bronze design deliberately keeps full nested structure so Silver can access any field without going back to Kafka."
# MAGIC
# MAGIC **Q: What is the latency?**
# MAGIC "With `trigger_interval: availableNow` it processes all available data in one batch — typically under 30 seconds end to end from Kafka produce to Delta table. For continuous mode change to `processingTime: 30 seconds` for near-real-time."
# MAGIC
# MAGIC **Q: Why Confluent and not Azure Event Hubs?**
# MAGIC "Confluent provides Schema Registry natively — central Avro schema management is essential for this pattern. Event Hubs has Kafka protocol compatibility but no Schema Registry. The real project already uses Confluent."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. What makes this POC strong
# MAGIC
# MAGIC ```
# MAGIC ✅ Matches real bronze model v2 exactly (42 typed columns)
# MAGIC ✅ Fully config-driven — zero code for new topics
# MAGIC ✅ Auto schema from Schema Registry — no manual StructType
# MAGIC ✅ Three-layer error handling (valid flag, rescued data, quarantine)
# MAGIC ✅ Full audit log of every pipeline run
# MAGIC ✅ 30 automated data quality tests
# MAGIC ✅ DDL pre-creates tables — pipelines just write
# MAGIC ✅ Monitoring dashboard built in
# MAGIC ✅ Same framework scales to all 14 Kafka topics
# MAGIC ✅ Same framework works for any future topics
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. What to say about moving to development
# MAGIC
# MAGIC "The POC proves the framework pattern works end to end. Moving to development means: point `KAFKA_CONFIG` at the production Confluent cluster, update `CATALOG` and `DB_SCHEMA` to the production Unity Catalog, run the DDL scripts to create tables in `src_kdp`, enable the remaining topic configs, and set up Databricks Jobs with one job per source system. The framework code itself does not change."