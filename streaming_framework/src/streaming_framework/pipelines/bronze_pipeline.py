"""
bronze_pipeline.py — Bronze Layer Pipeline
─────────────────────────────────────────────────────────────────
Reads from Landing Delta table.
Deserializes Avro binary → typed columns per bronze physical model v2.

DESIGN CHANGE (v2 bronze model):
  OLD: payload stored as JSON string (VARIANT equivalent)
  NEW: payload fully parsed into typed struct/array columns
       matching exact DDL from bronze model Excel v2

Tables are PRE-CREATED via ddl/create_tables_kdp.sql.
Pipeline writes into existing tables using mergeSchema=false.

Column structure per new model:
  key columns        → from kafka_key split by ":"
  structure columns  → parsed from Avro record (typed STRUCTs/ARRAYs)
  __message_valid_flag / __message_error_desc  ← renamed from payload_valid_flag
  __rescued_data, __schema_version_id
  metadata columns   → kafka partition/offset/timestamp, source_system etc.

KEY FIX: col()["field"] bracket notation used instead of expr() dot notation
  for camelCase struct field access — avoids Spark field resolution errors.
─────────────────────────────────────────────────────────────────
"""

import json
import io
from decimal import Decimal
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, expr,
    udf, lit
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType,
    DoubleType, FloatType, BooleanType,
    ArrayType, DecimalType, TimestampType,
    DateType, BinaryType
)
from streaming_framework.pipelines.base_pipeline import BasePipeline


class BronzePipeline(BasePipeline):

    # ── Schema Registry fetch ──────────────────────────────────
    def _fetch_schema_from_registry(self) -> str:
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
        schema_str = latest.schema.schema_str
        print(f"[{self.pipeline_name}] Schema fetched:")
        print(f"  Subject   : {subject}")
        print(f"  Schema ID : {latest.schema_id}")
        print(f"  Version   : {latest.version}")
        return schema_str

    # ── Avro → Python dict UDF ─────────────────────────────────
    def _build_avro_udf(self, schema_str: str, result_schema: StructType):
        """
        Returns a UDF that deserializes Confluent Avro binary
        into a Python dict matching result_schema.
        Fastavro returns camelCase keys — preserved as-is in the struct.
        """
        _schema_str = schema_str

        def deserialize(value_bytes):
            try:
                if value_bytes is None:
                    return None, False, "null value", 0, None

                import fastavro, io as _io, json as _json
                from decimal import Decimal as D
                import datetime

                raw       = bytes(value_bytes)
                schema_id = int.from_bytes(raw[1:5], 'big')
                avro_data = raw[5:]

                schema = fastavro.parse_schema(_json.loads(_schema_str))
                record = fastavro.schemaless_reader(_io.BytesIO(avro_data), schema)

                def clean(obj):
                    if obj is None:
                        return None
                    if isinstance(obj, dict):
                        return {k: clean(v) for k, v in obj.items()}
                    if isinstance(obj, list):
                        return [clean(i) for i in obj]
                    if isinstance(obj, D):
                        return float(obj)
                    if isinstance(obj, bytes):
                        try:
                            int_val = int.from_bytes(obj, 'big', signed=True)
                            return float(int_val) / 100.0  # scale=2
                        except Exception:
                            return obj.hex()
                    if isinstance(obj, datetime.date) and not isinstance(obj, datetime.datetime):
                        return obj
                    if isinstance(obj, datetime.datetime):
                        return obj
                    return obj

                cleaned = clean(record)
                return cleaned, True, None, schema_id, None

            except Exception as e:
                try:
                    schema_id = int.from_bytes(bytes(value_bytes)[1:5], 'big')
                except Exception:
                    schema_id = 0
                return None, False, str(e)[:500], schema_id, None

        outer_schema = StructType([
            StructField("record",       result_schema,  True),
            StructField("valid_flag",   BooleanType(),  False),
            StructField("error_desc",   StringType(),   True),
            StructField("schema_id",    IntegerType(),  True),
            StructField("rescued_data", StringType(),   True),
        ])
        return udf(deserialize, outer_schema)

    # ── Safe struct access using col()["field"] ────────────────
    @staticmethod
    def _col_path(*parts):
        """
        Build a column accessor using bracket notation.
        col_path("result", "record", "auditInformation", "user")
        → col("result")["record"]["auditInformation"]["user"]

        This handles camelCase field names correctly without
        triggering Spark's case-insensitive field resolution.
        """
        c = col(parts[0])
        for part in parts[1:]:
            c = c[part]
        return c

    # ── Add standard metadata columns ─────────────────────────
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        key_columns   = self.config.get("key_columns", [])
        source_system = self.config.get("source_system", "UNKNOWN")

        # key columns from kafka_key split — use expr() which is fine here
        # (kafka_key is a plain string column, no camelCase struct involved)
        for i, key_def in enumerate(key_columns):
            parts    = key_def.split(":")
            col_name = parts[0]
            col_type = parts[1] if len(parts) > 1 else "string"
            if col_type == "int":
                df = df.withColumn(col_name,
                    expr(f"try_cast(get(split(kafka_key, ':'), {i}) AS INT)"))
            elif col_type == "long":
                df = df.withColumn(col_name,
                    expr(f"try_cast(get(split(kafka_key, ':'), {i}) AS BIGINT)"))
            else:
                df = df.withColumn(col_name,
                    expr(f"get(split(kafka_key, ':'), {i})"))

        # __user_domain_login — use bracket notation for camelCase path
        has_result = "result" in df.columns
        user_domain_col = (
            self._col_path("result", "record", "auditInformation", "user", "domainLogin")
            .cast("string")
            if has_result
            else lit(None).cast("string")
        )

        df = (
            df
            .withColumn("__kafka_partition",   col("partition").cast("int"))
            .withColumn("__kafka_offset",      col("offset").cast("long"))
            .withColumn("__kafka_timestamp",   col("kafka_timestamp"))
            .withColumn("__source_system",     lit(source_system))
            .withColumn("__load_timestamp",    current_timestamp())
            .withColumn("__file_name",         lit(None).cast("string"))
            .withColumn("__file_path",         lit(None).cast("string"))
            .withColumn("__trace_id",          lit(None).cast("string"))
            .withColumn("__msg_id",            lit(None).cast("string"))
            .withColumn("__source_eap_id",     lit(None).cast("string"))
            .withColumn("__user_domain_login", user_domain_col)
        )
        return df

    # ── Build final column select ──────────────────────────────
    def _final_select(self, df: DataFrame) -> DataFrame:
        """Select only columns that exist — matches pre-created table schema."""
        key_cols        = [c.split(":")[0] for c in self.config.get("key_columns", [])]
        structure_cols  = self.config.get("structure_columns", [])
        validation_cols = [
            "__message_valid_flag",
            "__message_error_desc",
            "__rescued_data",
            "__schema_version_id",
        ]
        metadata_cols = [
            "__trace_id", "__msg_id", "__source_eap_id",
            "__user_domain_login", "__kafka_partition",
            "__kafka_offset", "__kafka_timestamp",
            "__source_system", "__load_timestamp",
            "__file_name", "__file_path",
        ]
        all_cols = key_cols + structure_cols + validation_cols + metadata_cols
        existing = set(df.columns)
        final    = [c for c in all_cols if c in existing]
        return df.select(*final)

    # ── Read ───────────────────────────────────────────────────
    def read_stream(self) -> DataFrame:
        return (
            self.spark.readStream
            .format("delta")
            .table(self.config["source_table"])
        )

    # ── Transform dispatcher ───────────────────────────────────
    def transform(self, df: DataFrame) -> DataFrame:
        encoding = self.config.get("encoding", "avro")
        print(f"[{self.pipeline_name}] Encoding: {encoding}")
        if encoding == "avro":
            return self._transform_avro(df)
        else:
            return self._transform_json(df)

    # ── Avro transform — typed columns ─────────────────────────
    def _transform_avro(self, df: DataFrame) -> DataFrame:
        from streaming_framework.utils.schema_converter import SchemaConverter

        print(f"[{self.pipeline_name}] Mode: Avro → typed columns (auto schema)")

        sr_cfg  = self.config.get("schema_registry_config", {})
        subject = self.config["schema_subject"]

        converter           = SchemaConverter(sr_cfg, subject)
        schema_str          = converter.schema_str
        record_schema       = converter.record_schema
        structure_columns   = self.config.get("structure_columns") \
                              or converter.structure_columns
        column_to_avro_path = self.config.get("column_to_avro_path") \
                              or converter.column_to_avro_path

        print(f"[{self.pipeline_name}] Auto-generated {len(structure_columns)} structure columns")

        deserialize_udf = self._build_avro_udf(schema_str, record_schema)

        df = (
            df
            .withColumn("result",               deserialize_udf(col("raw_bytes")))
            .withColumn("__message_valid_flag", col("result")["valid_flag"])
            .withColumn("__message_error_desc", col("result")["error_desc"])
            .withColumn("__schema_version_id",  col("result")["schema_id"])
            .withColumn("__rescued_data",       col("result")["rescued_data"])
        )

        for struct_col in structure_columns:
            avro_path = column_to_avro_path.get(struct_col)
            if avro_path:
                path_parts = avro_path.split(".")
                c = self._col_path("result", "record", *path_parts)
                df = df.withColumn(struct_col, c)
            else:
                df = df.withColumn(struct_col, lit(None).cast("string"))

        self.config["structure_columns"]   = structure_columns
        self.config["column_to_avro_path"] = column_to_avro_path

        df = self._add_metadata(df)
        return self._final_select(df)