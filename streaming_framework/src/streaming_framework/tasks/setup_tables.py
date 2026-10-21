"""
setup_tables.py — Bundle task: schema, volumes, audit log, (optional) DDL
─────────────────────────────────────────────────────────────────
Replaces CELL 2 of the old run_notebook.py. Always (idempotent):
  - CREATE SCHEMA IF NOT EXISTS
  - CREATE VOLUME IF NOT EXISTS checkpoints
  - CREATE VOLUME IF NOT EXISTS <landing volume>   (managed; an existing
    external ADLS volume with that name is left untouched)
  - pipeline_audit_log table

The typed DDL (ddl/create_tables_kdp.sql) only runs with --apply-ddl=true.
Default is false because the old notebook cell never actually ran it
(it skipped every chunk starting with "--", i.e. every CREATE TABLE),
so today the tables are created by the streams themselves — and the
DDL's quarantine table schema does not match QuarantinePipeline's output.
─────────────────────────────────────────────────────────────────
"""

from importlib import resources
from typing import List

from streaming_framework.tasks.common import base_parser, get_spark, str2bool

DDL_FILES = ["create_tables_kdp.sql"]


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def render_ddl(sql: str, catalog: str, schema: str) -> List[str]:
    """Substitute ${catalog}/${schema} and split into executable statements."""
    lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
    body  = "\n".join(lines)
    body  = body.replace("${catalog}", _quote(catalog)).replace("${schema}", _quote(schema))
    return [s.strip() for s in body.split(";") if s.strip()]


def load_ddl(name: str) -> str:
    return resources.files("streaming_framework.ddl").joinpath(name).read_text()


def setup(spark, catalog: str, schema: str, landing_volume: str, apply_ddl: bool = False) -> None:
    from streaming_framework.utils.logger import PipelineLogger

    fq = f"{_quote(catalog)}.{_quote(schema)}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {fq}.checkpoints")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {fq}.{_quote(landing_volume)}")
    print(f"✅ schema {catalog}.{schema} + volumes: checkpoints, {landing_volume}")

    PipelineLogger(spark, catalog, schema)   # ensures pipeline_audit_log
    print("✅ pipeline_audit_log ready")

    if apply_ddl:
        for ddl_file in DDL_FILES:
            for stmt in render_ddl(load_ddl(ddl_file), catalog, schema):
                spark.sql(stmt)
                print(f"✅ {stmt.splitlines()[0][:80]}")
    else:
        print("ℹ️  typed DDL skipped (--apply-ddl=false); tables are created by the streams")


def main(argv=None):
    p = base_parser("Create schema, volumes and framework tables")
    p.add_argument("--apply-ddl", default="false")
    args = p.parse_args(argv)
    setup(get_spark(), args.catalog, args.schema, args.landing_volume, str2bool(args.apply_ddl))


if __name__ == "__main__":
    main()
