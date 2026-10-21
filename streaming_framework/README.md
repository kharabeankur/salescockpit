# Streaming Framework — Databricks Asset Bundle (Stage 1)
## Confluent Kafka → ADLS landing files → Auto Loader → Bronze Delta

Config-driven streaming framework packaged and deployed as a
[Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/).

* **Stage 1 (this version):** Asset Bundle — packaging, environments, jobs, secrets.
* **Stage 2 (next):** metadata-driven architecture — topics defined as data, not as code / YAML.

---

### Folder Structure
```
streaming_framework/
├── databricks.yml                    ← bundle: variables, dev / prod targets, wheel artifact
├── pyproject.toml                    ← python package + job entry points
├── resources/
│   ├── kdp_life_policy.jobs.yml      ← Job A (landing) + Job B (autoloader + quarantine)
│   └── producer.job.yml              ← POC fake-data producer (manual)
├── src/streaming_framework/
│   ├── config/pipeline_config.py     ← per-topic builder + static configs (no hard-coded env)
│   ├── pipelines/                    ← base / landing / auto_loader / bronze / quarantine
│   ├── utils/                        ← logger, alerts, monitor, schema converter, secrets
│   ├── producers/avro_producer.py    ← fake data producer (POC only)
│   ├── ddl/create_tables_kdp.sql     ← typed DDL (opt-in, see below)
│   └── tasks/                        ← entry points: setup_tables, run_pipelines, monitor
├── notebooks/
│   ├── interactive_runner.py         ← replaces run_notebook.py (dev iteration)
│   ├── pipeline_health_checks.py      ← failed runs, audit log, latest data
│   ├── produce_fake_data.py / Producer_notebook.py
│   ├── framework_tests.py            ← was Frameworktesting.py
│   └── Solution_Pointers.py
├── docs/poc_demo_architecture.png
└── tests/test_bundle.py              ← local unit tests (no Spark needed)
```

---

### Job design (per topic)
```
Job A  kdp_life_policy_landing      (scheduled)
       setup ──► landing ──► trigger_autoloader ─┐
                (Kafka → /Volumes/…/Landing/…)    │
                                                  ▼
Job B  kdp_life_policy_autoloader   (triggered by Job A)
       autoloader ─┬──► monitor (runs even if a layer failed)
       quarantine ─┘
```
* Same job parameters as the old notebook: `source_system`, `topic`, `schema_subject`,
  `key_columns`, `encoding`, `starting_offset`, `trigger_interval`
  (plus `catalog`, `schema`, `landing_volume`, `secret_scope`, …).
* Job B starts only after landing has finished writing files.
* `max_concurrent_runs: 1` on both — a checkpoint is never shared by two runs.
* Serverless compute by default (comment in the job file shows the classic-compute switch).

---

### Prerequisites
* Databricks CLI ≥ 0.250, `databricks auth login --host <workspace-url>`
* [`uv`](https://docs.astral.sh/uv/) (builds the wheel)
* An existing catalog (default `abd_easywithdata_poc`). The schema and the `checkpoints` and
  `Landing` volumes are created by the `setup` task if missing. An existing **external** `Landing`
  volume on ADLS is left untouched; in a fresh dev schema a managed volume is created.

### One-time: secret scope
```bash
databricks secrets create-scope streaming_framework
databricks secrets put-secret streaming_framework kafka-api-key
databricks secrets put-secret streaming_framework kafka-api-secret
databricks secrets put-secret streaming_framework sr-api-key
databricks secrets put-secret streaming_framework sr-api-secret
databricks secrets put-secret streaming_framework smtp-password   # only if email alerts enabled
```
Missing keys fall back to env vars `KAFKA_API_KEY`, `KAFKA_API_SECRET`, `SR_API_KEY`,
`SR_API_SECRET`, `SMTP_PASSWORD`.

---

### Deploy & Run
```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run    -t dev produce_fake_data_job           # POC test data
databricks bundle run    -t dev kdp_life_policy_landing_job     # Job A → Job B
databricks bundle run    -t dev kdp_life_policy_autoloader_job  # Job B on its own

databricks bundle deploy -t prod
```
Override for one run: `databricks bundle run kdp_life_policy_landing_job --params starting_offset=earliest`

| Target | Mode | Schema | Job A schedule |
|--------|------|--------|----------------|
| `dev` (default) | development, jobs prefixed `[dev <user>]` | `streaming_poc_dev` | always paused |
| `prod` | production | `streaming_poc` (existing files, tables, checkpoints reused) | every 15 min |

### Adding a topic (stage 1)
Copy `resources/kdp_life_policy.jobs.yml`, rename the two resource keys and the job names, change
the topic parameter defaults, then `databricks bundle deploy`. No Python changes.
(Stage 2 removes this copy step.)

### Typed DDL (`apply_ddl`)
Off by default. The old notebook cell never actually executed it (every `CREATE TABLE` chunk started
with a `--` comment and was skipped), so tables are currently created by the streams. Its quarantine
table schema also differs from what `QuarantinePipeline` writes. Align the DDL first, then enable with
`--var="apply_ddl=true"`.

### Local tests
```bash
uv run --extra dev pytest
```
