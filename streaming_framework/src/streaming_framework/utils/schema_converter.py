"""
schema_converter.py — Avro Schema → Spark Types (Auto)
─────────────────────────────────────────────────────────────────
Fetches Avro schema from Schema Registry and auto-generates:
  1. Spark StructType  (record_schema)
  2. structure_columns (list of snake_case column names)
  3. column_to_avro_path (snake_case → camelCase Avro path)

This means ZERO manual mapping for any new topic.
The framework reads the Avro schema and figures everything out.

Usage:
  converter = SchemaConverter(sr_config, schema_subject)
  record_schema      = converter.record_schema
  structure_columns  = converter.structure_columns
  column_to_avro_path= converter.column_to_avro_path
─────────────────────────────────────────────────────────────────
"""

import re
import json
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType,
    BooleanType, TimestampType, DateType,
    ArrayType, DecimalType, FloatType, DoubleType,
    BinaryType, MapType
)


def _camel_to_snake(name: str) -> str:
    """Convert camelCase → snake_case."""
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s)
    return s.lower()


def _avro_to_spark(avro_type, named_types: dict = None) -> object:
    """
    Recursively convert Avro type definition → Spark DataType.
    named_types: cache of named record types (for $ref resolution)
    """
    if named_types is None:
        named_types = {}

    # Union type — ["null", "actual_type"] → take non-null
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        if not non_null:
            return StringType()
        return _avro_to_spark(non_null[0], named_types)

    # Simple string type
    if isinstance(avro_type, str):
        if avro_type in named_types:
            return named_types[avro_type]
        return _PRIMITIVE_MAP.get(avro_type, StringType())

    # Complex type dict
    if isinstance(avro_type, dict):
        avro_kind = avro_type.get("type")
        logical   = avro_type.get("logicalType", "")

        # Logical types
        if logical in ("date",):
            return DateType()
        if logical in ("timestamp-millis", "timestamp-micros",
                       "local-timestamp-millis", "local-timestamp-micros"):
            return TimestampType()
        if logical == "decimal":
            precision = avro_type.get("precision", 38)
            scale     = avro_type.get("scale", 2)
            return DecimalType(precision, scale)
        if logical in ("time-millis", "time-micros"):
            return LongType()

        # Record
        if avro_kind == "record":
            name   = avro_type.get("name", "unknown")
            fields = []
            for f in avro_type.get("fields", []):
                spark_type = _avro_to_spark(f["type"], named_types)
                fields.append(StructField(f["name"], spark_type, True))
            struct = StructType(fields)
            named_types[name] = struct
            # also register with namespace if present
            ns = avro_type.get("namespace")
            if ns:
                named_types[f"{ns}.{name}"] = struct
            return struct

        # Array
        if avro_kind == "array":
            item_type = _avro_to_spark(avro_type.get("items", "string"), named_types)
            return ArrayType(item_type, True)

        # Map
        if avro_kind == "map":
            val_type = _avro_to_spark(avro_type.get("values", "string"), named_types)
            return MapType(StringType(), val_type, True)

        # Enum → string
        if avro_kind == "enum":
            return StringType()

        # Fixed → binary
        if avro_kind == "fixed":
            return BinaryType()

        # Bytes with logical type handled above; plain bytes → binary
        if avro_kind == "bytes":
            return BinaryType()

        # Fallback: try to resolve as primitive
        return _PRIMITIVE_MAP.get(avro_kind, StringType())

    return StringType()


_PRIMITIVE_MAP = {
    "string":  StringType(),
    "int":     IntegerType(),
    "long":    LongType(),
    "float":   FloatType(),
    "double":  DoubleType(),
    "boolean": BooleanType(),
    "bytes":   BinaryType(),
    "null":    StringType(),
}


def _extract_structure_info(
    spark_type,
    avro_fields: list,
    prefix_snake: str = "",
    prefix_avro:  str = "",
) -> tuple:
    """
    Walk the top-level fields of the Avro record and build:
      structure_columns  : ["audit_information_application", ...]
      column_to_avro_path: {"audit_information_application": "auditInformation.application"}

    Only top-level and second-level fields become separate columns
    (matching the bronze model pattern: one column per top-level
    field, or one column per nested record field one level deep).
    """
    structure_columns   = []
    column_to_avro_path = {}

    for avro_field in avro_fields:
        field_name  = avro_field["name"]           # camelCase Avro name
        snake_name  = _camel_to_snake(field_name)  # snake_case

        col_name  = f"{prefix_snake}{snake_name}" if prefix_snake else snake_name
        avro_path = f"{prefix_avro}.{field_name}" if prefix_avro else field_name

        # Resolve actual type (strip union null)
        raw_type = avro_field["type"]
        if isinstance(raw_type, list):
            raw_type = next((t for t in raw_type if t != "null"), raw_type[0])

        is_record = (
            isinstance(raw_type, dict) and raw_type.get("type") == "record"
        )

        if is_record and not prefix_snake:
            # Top-level record field — expand one level deep
            nested_fields = raw_type.get("fields", [])
            for nf in nested_fields:
                nf_name  = nf["name"]
                nf_snake = _camel_to_snake(nf_name)
                nc_name  = f"{snake_name}_{nf_snake}"
                na_path  = f"{field_name}.{nf_name}"
                structure_columns.append(nc_name)
                column_to_avro_path[nc_name] = na_path
        else:
            structure_columns.append(col_name)
            column_to_avro_path[col_name] = avro_path

    return structure_columns, column_to_avro_path


class SchemaConverter:
    """
    Fetches Avro schema from Schema Registry and auto-generates
    everything the bronze pipeline needs — no manual mapping.

    Parameters
    ----------
    sr_config      : dict with url, api_key, api_secret
    schema_subject : Confluent Schema Registry subject name
    """

    def __init__(self, sr_config: dict, schema_subject: str):
        self.sr_config      = sr_config
        self.schema_subject = schema_subject
        self._schema_str    = None
        self._avro_schema   = None
        self._record_schema = None
        self._struct_cols   = None
        self._avro_paths    = None
        self._load()

    def _load(self):
        from confluent_kafka.schema_registry import SchemaRegistryClient
        sr = SchemaRegistryClient({
            "url": self.sr_config["url"],
            "basic.auth.user.info": (
                f"{self.sr_config['api_key']}:{self.sr_config['api_secret']}"
            ),
        })
        latest            = sr.get_latest_version(self.schema_subject)
        self._schema_str  = latest.schema.schema_str
        self._avro_schema = json.loads(self._schema_str)
        self._build()

    def _build(self):
        named_types = {}
        # Build full Spark StructType
        self._record_schema = _avro_to_spark(self._avro_schema, named_types)

        # Build column list and path mapping from top-level Avro fields
        top_fields = self._avro_schema.get("fields", [])
        self._struct_cols, self._avro_paths = _extract_structure_info(
            self._record_schema, top_fields
        )

    @property
    def record_schema(self) -> StructType:
        """Full Spark StructType matching the Avro record."""
        return self._record_schema

    @property
    def structure_columns(self) -> list:
        """List of snake_case column names for the bronze table."""
        return self._struct_cols

    @property
    def column_to_avro_path(self) -> dict:
        """Maps snake_case column name → camelCase Avro field path."""
        return self._avro_paths

    @property
    def schema_str(self) -> str:
        """Raw Avro schema JSON string."""
        return self._schema_str

    def print_summary(self):
        print(f"Schema subject : {self.schema_subject}")
        print(f"Columns        : {len(self._struct_cols)}")
        for col, path in self._avro_paths.items():
            print(f"  {col:55s} ← {path}")
