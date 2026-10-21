"""
quarantine_pipeline.py — Kafka Message Quarantine
─────────────────────────────────────────────────────────────────
Dead letter table for messages that CANNOT be deserialized at all.

From the Bronze physical model:
  "Dead letter table for messages of this source that could not be
   deserialised at all, so they could not be landed in their bronze
   table. The raw bytes are kept, which is the only form that
   survives a failed decode and the only form a replay can use.
   Nothing is ever discarded."

How it works:
  Landing reads ALL messages → stores raw_bytes
  Bronze tries to deserialize → success → bronze table
  Bronze fails completely    → quarantine table

Quarantine table schema (21 columns per Excel):
  quarantine_id         ← auto-generated UUID
  raw_bytes             ← original binary from Kafka
  __deserialization_error ← what went wrong
  __schema_version_id   ← schema ID from header (best effort)
  __kafka_partition     ← Kafka partition
  __kafka_offset        ← Kafka offset
  __kafka_timestamp     ← Kafka timestamp
  __source_system       ← source system (KDP, TIA etc.)
  __topic               ← topic name
  __load_timestamp      ← when quarantined
─────────────────────────────────────────────────────────────────
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, udf, expr
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    BooleanType, TimestampType, BinaryType
)
from pipelines.base_pipeline import BasePipeline


class QuarantinePipeline(BasePipeline):
    """
    Reads from Landing Delta table.
    Attempts deserialization of each message.
    Routes ONLY completely failed messages to quarantine table.
    Successfully deserialized messages are ignored here
    (they go to bronze via BronzePipeline).
    """

    def _try_deserialize(self, schema_str: str):
        """
        Returns a UDF that attempts deserialization.
        Returns True if successful, False + error if failed completely.
        """
        _schema_str = schema_str

        result_schema = StructType([
            StructField("can_deserialize",   BooleanType(), False),
            StructField("error_message",     StringType(),  True),
            StructField("schema_version_id", IntegerType(), True),
        ])

        def attempt_deserialize(value_bytes):
            try:
                if value_bytes is None:
                    return False, "null value — no bytes received", 0

                import fastavro, io, json as json_lib
                from decimal import Decimal

                raw       = bytes(value_bytes)
                schema_id = int.from_bytes(raw[1:5], 'big')
                avro_data = raw[5:]

                schema = fastavro.parse_schema(json_lib.loads(_schema_str))
                fastavro.schemaless_reader(io.BytesIO(avro_data), schema)

                return True, None, schema_id  # ← success, not quarantined

            except Exception as e:
                # extract schema_id even from failed messages
                try:
                    schema_id = int.from_bytes(bytes(value_bytes)[1:5], 'big')
                except Exception:
                    schema_id = 0
                return False, str(e)[:500], schema_id

        return udf(attempt_deserialize, result_schema)

    def _fetch_schema_from_registry(self) -> str:
        """Fetch Avro schema from Schema Registry."""
        from confluent_kafka.schema_registry import SchemaRegistryClient

        sr_cfg    = self.config.get("schema_registry_config", {})
        sr_client = SchemaRegistryClient({
            "url": sr_cfg.get("url"),
            "basic.auth.user.info": (
                f"{sr_cfg.get('api_key')}:{sr_cfg.get('api_secret')}"
            ),
        })
        subject    = self.config["schema_subject"]
        latest     = sr_client.get_latest_version(subject)
        print(f"[{self.pipeline_name}] Quarantine schema: {subject}")
        return latest.schema.schema_str

    def read_stream(self) -> DataFrame:
        """
        Read from ADLS files via Auto Loader.
        DEV: reads from landing_path files (not Delta landing table)
        """
        encoding     = self.config.get("encoding", "avro")
        landing_path = self.config.get("landing_path")
        checkpoint   = self.config["checkpoint_path"]

        # fall back to Delta table if landing_path not set (POC backward compat)
        if not landing_path:
            return (
                self.spark.readStream
                .format("delta")
                .table(self.config["source_table"])
            )

        file_format = "avro" if encoding == "avro" else "json"

        print(f"[{self.pipeline_name}] Quarantine reading {file_format} from: {landing_path}")

        return (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",        file_format)
            .option("cloudFiles.schemaLocation", checkpoint + "/schema")
            .option("cloudFiles.inferColumnTypes", "false")
            .load(landing_path)
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Attempt deserialization of every message.
        Keep ONLY messages that fail completely.
        These go to the quarantine table.
        """
        encoding      = self.config.get("encoding", "avro")
        source_system = self.config.get("source_system", "UNKNOWN")
        topic         = self.config.get("topic", "unknown")

        if encoding != "avro":
            # JSON topics — quarantine not applicable
            # Return empty dataframe with correct schema
            print(f"[{self.pipeline_name}] JSON encoding — no quarantine needed")
            return df.filter(lit(False)).select(
                lit(None).cast("string").alias("quarantine_id"),
                col("raw_value").cast("binary").alias("raw_bytes"),
                lit(None).cast("string").alias("__deserialization_error"),
                lit(None).cast("int").alias("__schema_version_id"),
                col("partition").cast("int").alias("__kafka_partition"),
                col("offset").cast("long").alias("__kafka_offset"),
                col("kafka_timestamp").alias("__kafka_timestamp"),
                lit(source_system).alias("__source_system"),
                lit(topic).alias("__topic"),
                current_timestamp().alias("__load_timestamp"),
            )

        # Avro — attempt deserialization
        schema_str    = self._fetch_schema_from_registry()
        deserialize   = self._try_deserialize(schema_str)

        print(f"[{self.pipeline_name}] Checking for undeserializable messages...")

        return (
            df
            .withColumn("check", deserialize(col("raw_bytes")))
            # keep ONLY messages that cannot be deserialized
            .filter(col("check.can_deserialize") == False)
            .withColumn(
                "quarantine_id",
                expr("uuid()")                          # unique ID per quarantined row
            )
            .select(
                col("quarantine_id"),
                col("raw_bytes"),
                col("check.error_message").alias("__deserialization_error"),
                col("check.schema_version_id").alias("__schema_version_id"),
                col("partition").cast("int").alias("__kafka_partition"),
                col("offset").cast("long").alias("__kafka_offset"),
                col("kafka_timestamp").alias("__kafka_timestamp"),
                lit(source_system).alias("__source_system"),
                lit(topic).alias("__topic"),
                current_timestamp().alias("__load_timestamp"),
            )
        )
