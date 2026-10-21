"""
secrets.py — Credential resolution
─────────────────────────────────────────────────────────────────
Credentials are never stored in code or in the bundle.

Resolution order for each secret:
  1. Databricks secret scope   (dbutils.secrets.get(scope, key))
  2. Environment variable      (fallback for local runs / legacy clusters)

The secret scope name is passed in by the bundle (var.secret_scope),
so each target (dev / prod) can point at a different scope.
─────────────────────────────────────────────────────────────────
"""

import os
from typing import Optional


def get_dbutils(spark=None):
    """Return a dbutils handle in notebooks, wheel tasks and serverless."""
    try:
        from pyspark.dbutils import DBUtils  # classic + serverless runtimes
        if spark is None:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except Exception:
        pass
    try:
        from databricks.sdk.runtime import dbutils  # notebook / SDK runtime
        return dbutils
    except Exception:
        return None


def get_secret(key: str,
               scope: Optional[str] = None,
               env_var: Optional[str] = None,
               spark=None,
               required: bool = True) -> Optional[str]:
    """Fetch a secret from the scope, falling back to an env var."""
    scope_error = None
    if scope:
        dbutils = get_dbutils(spark)
        if dbutils is not None:
            try:
                return dbutils.secrets.get(scope=scope, key=key)
            except Exception as e:  # missing key / no permission
                scope_error = e

    if env_var and os.environ.get(env_var):
        return os.environ[env_var]

    if required:
        raise RuntimeError(
            f"Secret not found: key='{key}' scope='{scope}' env_var='{env_var}'"
            + (f" (scope error: {scope_error})" if scope_error else "")
        )
    return None
