"""
base_pipeline.py — Parent Pipeline Class
─────────────────────────────────────────────────────────────────
Contains all shared logic:
- Kafka connection options builder
- Retry loop (configurable max_retries + retry_wait_secs)
- Audit logging (STARTED / SUCCESS / FAILED)
- Email alert on final failure
- write_stream() with configurable trigger

Each child class implements only:
- read_stream() → where data comes from
- transform()   → what to do with it
─────────────────────────────────────────────────────────────────
"""

import time
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame


class BasePipeline:

    def __init__(self,
                 spark:         SparkSession,
                 config:        dict,
                 kafka_config:  dict,
                 logger,
                 alert_manager):
        self.spark         = spark
        self.config        = config
        self.kafka_config  = kafka_config
        self.logger        = logger
        self.alerter       = alert_manager
        self.pipeline_name = config["pipeline_name"]
        self.layer         = config["layer"]

    def _kafka_options(self) -> dict:
        """Build Kafka connection options from KAFKA_CONFIG."""
        kc   = self.kafka_config
        jaas = (
            f'{kc["jaas_class"]} required '
            f'username="{kc["api_key"]}" '
            f'password="{kc["api_secret"]}";'
        )
        return {
            "kafka.bootstrap.servers": kc["bootstrap_servers"],
            "kafka.security.protocol": kc["security_protocol"],
            "kafka.sasl.mechanism":    kc["sasl_mechanism"],
            "kafka.sasl.jaas.config":  jaas,
            "subscribe":               self.config.get("topic", ""),
            "startingOffsets":         self.config.get("starting_offset", "latest"),
            "failOnDataLoss":          "false",
        }

    def read_stream(self) -> DataFrame:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement read_stream()"
        )

    def transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement transform()"
        )

    def write_stream(self, df: DataFrame):
        """Write streaming DataFrame to Delta table."""
        trigger = self.config.get("trigger_interval", "availableNow")

        writer = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.config["checkpoint_path"])
            .option("mergeSchema", "true")
        )

        if trigger == "availableNow":
            query = writer.trigger(availableNow=True).toTable(
                self.config["target_table"]
            )
        else:
            query = writer.trigger(processingTime=trigger).toTable(
                self.config["target_table"]
            )

        query.awaitTermination()
        return query

    def run(self):
        """
        Run the pipeline with retry logic.

        Flow:
        1. Log STARTED
        2. Try: read_stream → transform → write_stream
        3. On success → log SUCCESS → return
        4. On failure → wait → retry
        5. After max_retries → log FAILED → alert → raise
        """
        max_retries     = self.config.get("max_retries", 3)
        retry_wait_secs = self.config.get("retry_wait_secs", 10)
        attempt         = 0
        start_time      = datetime.now()

        self.logger.log(
            pipeline_name = self.pipeline_name,
            layer         = self.layer,
            status        = "STARTED",
            start_time    = start_time,
        )

        while attempt < max_retries:
            try:
                attempt += 1
                print(f"\n[{self.pipeline_name}] attempt {attempt}/{max_retries}")

                df       = self.read_stream()
                df_final = self.transform(df)
                self.write_stream(df_final)

                end_time = datetime.now()
                self.logger.log(
                    pipeline_name = self.pipeline_name,
                    layer         = self.layer,
                    status        = "SUCCESS",
                    start_time    = start_time,
                    end_time      = end_time,
                )
                print(f"[{self.pipeline_name}] completed successfully!")
                return

            except Exception as e:
                error_msg = str(e)
                print(f"[{self.pipeline_name}] attempt {attempt} failed: {error_msg}")

                if attempt < max_retries:
                    print(f"[{self.pipeline_name}] retrying in {retry_wait_secs}s...")
                    time.sleep(retry_wait_secs)
                else:
                    end_time = datetime.now()
                    self.logger.log(
                        pipeline_name = self.pipeline_name,
                        layer         = self.layer,
                        status        = "FAILED",
                        start_time    = start_time,
                        end_time      = end_time,
                        error_message = error_msg,
                    )
                    self.alerter.send_failure_alert(
                        pipeline_name = self.pipeline_name,
                        layer         = self.layer,
                        error_message = error_msg,
                        retry_count   = attempt,
                    )
                    raise
