"""
logger.py — Centralized Audit Logger
─────────────────────────────────────────────────────────────────
Writes every pipeline event to pipeline_audit_log Delta table.
Uses explicit Spark schema to avoid type inference errors with nulls.
─────────────────────────────────────────────────────────────────
"""

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DoubleType, TimestampType
)


class PipelineLogger:

    SCHEMA = StructType([
        StructField("pipeline_name",  StringType(),    True),
        StructField("layer",          StringType(),    True),
        StructField("status",         StringType(),    True),
        StructField("rows_processed", LongType(),      True),
        StructField("start_time",     TimestampType(), True),
        StructField("end_time",       TimestampType(), True),
        StructField("duration_secs",  DoubleType(),    True),
        StructField("error_message",  StringType(),    True),
        StructField("created_at",     TimestampType(), True),
    ])

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark       = spark
        self.catalog     = catalog
        self.schema      = schema
        self.audit_table = f"{catalog}.{schema}.pipeline_audit_log"
        self._ensure_audit_table()

    def _ensure_audit_table(self):
        """Create audit table if it does not exist."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.audit_table} (
                pipeline_name   STRING,
                layer           STRING,
                status          STRING,
                rows_processed  LONG,
                start_time      TIMESTAMP,
                end_time        TIMESTAMP,
                duration_secs   DOUBLE,
                error_message   STRING,
                created_at      TIMESTAMP
            )
            USING DELTA
        """)

    def log(self,
            pipeline_name:  str,
            layer:          str,
            status:         str,
            rows_processed: int      = 0,
            start_time:     datetime = None,
            end_time:       datetime = None,
            error_message:  str      = None):
        """
        Log a pipeline event.
        Status values: STARTED | SUCCESS | FAILED
        """
        duration = (
            (end_time - start_time).total_seconds()
            if start_time and end_time else 0.0
        )

        msg = (
            f"[{status}] {pipeline_name} | layer={layer} "
            f"| rows={rows_processed} | duration={duration:.1f}s"
        )
        if error_message:
            msg += f" | error={error_message}"
        print(msg)

        row = [(
            pipeline_name,
            layer,
            status,
            rows_processed,
            start_time,
            end_time,
            float(duration),
            error_message,
            datetime.now(),
        )]

        df = self.spark.createDataFrame(row, schema=self.SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(self.audit_table)
