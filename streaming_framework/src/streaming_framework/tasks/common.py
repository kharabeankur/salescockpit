"""Shared CLI arguments for the bundle job entry points."""

import argparse

from streaming_framework.config.pipeline_config import (
    DEFAULT_CATALOG,
    DEFAULT_SCHEMA,
    DEFAULT_LANDING_VOLUME,
    DEFAULT_SECRET_SCOPE,
    DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    DEFAULT_SCHEMA_REGISTRY_URL,
)


def str2bool(value: str) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def base_parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--catalog",        default=DEFAULT_CATALOG)
    p.add_argument("--schema",         default=DEFAULT_SCHEMA)
    p.add_argument("--landing-volume", default=DEFAULT_LANDING_VOLUME)
    return p


def add_connection_args(p: argparse.ArgumentParser) -> argparse.ArgumentParser:
    p.add_argument("--secret-scope",            default=DEFAULT_SECRET_SCOPE)
    p.add_argument("--kafka-bootstrap-servers", default=DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
    p.add_argument("--schema-registry-url",     default=DEFAULT_SCHEMA_REGISTRY_URL)
    return p


def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()
