"""
auto_loader_pipeline.py — Auto Loader Pipeline (Landing → Bronze)
─────────────────────────────────────────────────────────────────
Reads new files from ADLS using Databricks Auto Loader.
Inherits ALL bronze logic from BronzePipeline — only
read_stream() changes.

What Auto Loader does:
  - Watches landing_path for new files
  - Detects new files automatically (directory listing or
    file notifications via Event Grid)
  - Tracks which files have been processed via checkpoint
  - Never re-processes the same file twice
  - Handles schema inference and evolution

File format is config-driven — same as landing:
  "encoding": "avro" → reads .avro files
  "encoding": "json" → reads .json files

Everything after reading (Avro deserialize, typed columns,
schema auto-generation, metadata, quarantine) is IDENTICAL
to BronzePipeline — inherited with zero code duplication.
─────────────────────────────────────────────────────────────────
"""

from pyspark.sql import DataFrame
from pipelines.bronze_pipeline import BronzePipeline


class AutoLoaderPipeline(BronzePipeline):
    """
    Inherits from BronzePipeline.

    Only read_stream() is different — uses cloudFiles format
    to read from ADLS instead of Delta landing table.

    All of these are inherited unchanged:
      _fetch_schema_from_registry()
      _build_avro_udf()
      _col_path()
      _add_metadata()
      _final_select()
      transform() → _transform_avro() / _transform_json()
      SchemaConverter auto schema generation
      write_stream()
      run() with retry logic and audit logging
    """

    def read_stream(self) -> DataFrame:
        """
        Read new files from ADLS using Auto Loader.

        POC:  readStream.format("delta").table(landing_table)
        DEV:  readStream.format("cloudFiles").load(landing_path)

        Auto Loader modes (controlled by volume):
          directory listing — default, works up to ~10k files/hr
          file notifications — needs Event Grid, scales to millions
        """
        encoding     = self.config.get("encoding", "json")
        landing_path = self.config["landing_path"]
        checkpoint   = self.config["checkpoint_path"]

        # file format matches what landing_pipeline wrote
        file_format  = "avro" if encoding == "avro" else "json"

        print(f"[{self.pipeline_name}] Auto Loader reading {file_format} from: {landing_path}")

        return (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",         file_format)
            .option("cloudFiles.schemaLocation",  checkpoint + "/autoloader_schema")
            .option("cloudFiles.inferColumnTypes","false")
            # ── optional: enable file notifications (needs Event Grid) ──
            # .option("cloudFiles.useNotifications", "true")
            # .option("cloudFiles.subscriptionId",   os.environ.get("AZURE_SUBSCRIPTION_ID"))
            # .option("cloudFiles.tenantId",         os.environ.get("AZURE_TENANT_ID"))
            # .option("cloudFiles.clientId",         os.environ.get("AZURE_CLIENT_ID"))
            # .option("cloudFiles.clientSecret",     os.environ.get("AZURE_CLIENT_SECRET"))
            .load(landing_path)
        )