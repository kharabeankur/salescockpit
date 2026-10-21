"""
landing_pipeline.py — Landing Layer Pipeline
─────────────────────────────────────────────────────────────────
Reads raw messages from Confluent Kafka.
Writes raw files to ADLS via Unity Catalog Volume.

File format is config-driven:
  "encoding": "avro" → writes .avro files (binary preserved)
  "encoding": "json" → writes .json files

Files land at:
  {landing_path}/year=YYYY/month=MM/day=DD/hour=HH/

Auto Loader (auto_loader_pipeline.py) picks up these files.
─────────────────────────────────────────────────────────────────
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp,
    year, month, dayofmonth, hour
)
from streaming_framework.pipelines.base_pipeline import BasePipeline


class LandingPipeline(BasePipeline):

    # ── Read — unchanged ──────────────────────────────────────
    def read_stream(self) -> DataFrame:
        return (
            self.spark.readStream
            .format("kafka")
            .options(**self._kafka_options())
            .load()
        )

    # ── Transform — adds partition columns for file layout ────
    def transform(self, df: DataFrame) -> DataFrame:
        encoding = self.config.get("encoding", "json")

        if encoding == "avro":
            print(f"[{self.pipeline_name}] Mode: Avro binary → files on ADLS")
            return df.select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
                col("key").cast("string").alias("kafka_key"),
                col("value").alias("raw_bytes"),
                current_timestamp().alias("ingested_at"),
                year(col("timestamp")).alias("year"),
                month(col("timestamp")).alias("month"),
                dayofmonth(col("timestamp")).alias("day"),
                hour(col("timestamp")).alias("hour"),
            )
        else:
            print(f"[{self.pipeline_name}] Mode: JSON → files on ADLS")
            return df.select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("raw_value"),
                current_timestamp().alias("ingested_at"),
                year(col("timestamp")).alias("year"),
                month(col("timestamp")).alias("month"),
                dayofmonth(col("timestamp")).alias("day"),
                hour(col("timestamp")).alias("hour"),
            )

    # ── Write — files to ADLS Volume ──────────────────────────
    def write_stream(self, df: DataFrame):
        encoding     = self.config.get("encoding", "json")
        landing_path = self.config["landing_path"]
        checkpoint   = self.config["checkpoint_path"]
        trigger      = self.config.get("trigger_interval", "availableNow")

        # file format follows encoding — one config value controls both
        file_format  = "avro" if encoding == "avro" else "json"

        print(f"[{self.pipeline_name}] Writing {file_format} files → {landing_path}")

        trigger_opts = (
            {"availableNow": True}
            if trigger == "availableNow"
            else {"processingTime": trigger}
        )

        query = (
            df.writeStream
            .format(file_format)
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("path", landing_path)
            .partitionBy("year", "month", "day", "hour")
            .trigger(**trigger_opts)
            .start()
        )
        # Block until the availableNow batch is done (same as BasePipeline).
        # Without this, run() logs SUCCESS immediately and a job task can exit
        # — and Job B can start — while files are still being written.
        query.awaitTermination()
        return query
