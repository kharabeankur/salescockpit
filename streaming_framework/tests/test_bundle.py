"""
Local tests — no Spark / Databricks needed.
Run:  uv run --extra dev pytest
"""

from pathlib import Path

import pytest
import yaml

from streaming_framework.config.pipeline_config import (
    PIPELINE_TEMPLATES,
    build_pipeline_configs,
    build_runtime_settings,
    build_static_pipeline_configs,
)
from streaming_framework.tasks.run_pipelines import select_configs
from streaming_framework.tasks.setup_tables import load_ddl, render_ddl

ROOT = Path(__file__).resolve().parents[1]


def _topic_configs(**kw):
    args = dict(source_system="KDP", topic="life_policy_overview_sync_",
                schema_subject="life_policy_overview_sync_-value",
                key_columns=["application_id:int", "policy_number:long"],
                catalog="cat", schema="dev", landing_volume="Landing")
    args.update(kw)
    return build_pipeline_configs(**args)


def test_topic_builder_uses_target_catalog_schema():
    for cfg in _topic_configs():
        assert cfg["checkpoint_path"].startswith("/Volumes/cat/dev/checkpoints/")
        assert cfg["landing_path"] == "/Volumes/cat/dev/Landing/kdp/life_policy_overview_sync/"
        if "target_table" in cfg:
            assert cfg["target_table"].startswith("cat.dev.")


def test_landing_and_autoloader_share_the_landing_path():
    paths = {c["landing_path"] for c in _topic_configs()}
    assert len(paths) == 1


def test_static_templates_are_rendered_and_not_mutated():
    for cfg in build_static_pipeline_configs("cat", "dev", "Landing"):
        for key in ("target_table", "source_table", "checkpoint_path", "landing_path"):
            if key in cfg:
                assert "{" not in cfg[key]
    assert "{landing_base}" in PIPELINE_TEMPLATES[0]["landing_path"]


@pytest.mark.parametrize("job_type,layer,expected", [
    ("landing",    None,         ["landing"]),
    ("autoloader", None,         ["autoloader", "quarantine"]),
    ("autoloader", "quarantine", ["quarantine"]),
    ("all",        None,         ["landing", "autoloader", "quarantine"]),
])
def test_job_type_selection(job_type, layer, expected):
    assert [c["layer"] for c in select_configs(_topic_configs(), job_type, layer)] == expected


def test_layer_outside_job_type_is_rejected():
    with pytest.raises(ValueError):
        select_configs(_topic_configs(), "landing", "autoloader")


def test_runtime_settings_env_fallback(monkeypatch):
    for k in ("KAFKA_API_KEY", "KAFKA_API_SECRET", "SR_API_KEY", "SR_API_SECRET"):
        monkeypatch.setenv(k, f"val-{k}")
    s = build_runtime_settings("c", "s", secret_scope=None)
    assert s.kafka_config["api_key"] == "val-KAFKA_API_KEY"
    assert s.schema_registry_config["api_secret"] == "val-SR_API_SECRET"


def test_no_credentials_in_source():
    for f in (ROOT / "src").rglob("*.py"):
        text = f.read_text()
        assert "cflt" not in text, f"looks like a Confluent secret in {f}"


def test_ddl_renders_every_create_statement():
    stmts = render_ddl(load_ddl("create_tables_kdp.sql"), "my-cat", "my_schema")
    assert stmts[0] == "USE CATALOG `my-cat`"
    assert len([s for s in stmts if s.upper().startswith("CREATE TABLE")]) == 4


def test_job_entry_points_exist_in_pyproject():
    import tomllib
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    jobs = yaml.safe_load((ROOT / "resources/kdp_life_policy.jobs.yml").read_text())["resources"]["jobs"]
    for job in jobs.values():
        for t in job["tasks"]:
            if "python_wheel_task" in t:
                assert t["python_wheel_task"]["entry_point"] in scripts


def test_job_b_is_triggered_by_job_a():
    jobs = yaml.safe_load((ROOT / "resources/kdp_life_policy.jobs.yml").read_text())["resources"]["jobs"]
    trigger = [t for t in jobs["kdp_life_policy_landing_job"]["tasks"] if "run_job_task" in t][0]
    assert "kdp_life_policy_autoloader_job" in trigger["run_job_task"]["job_id"]
    assert trigger["depends_on"] == [{"task_key": "landing"}]
