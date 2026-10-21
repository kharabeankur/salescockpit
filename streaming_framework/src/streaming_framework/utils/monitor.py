"""
monitor.py — Pipeline Monitoring & Health Dashboard
─────────────────────────────────────────────────────────────────
All monitoring in one place. Run from Databricks notebook.
─────────────────────────────────────────────────────────────────
"""

from pyspark.sql import SparkSession


class PipelineMonitor:

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark   = spark
        self.catalog = catalog
        self.schema  = schema
        self.db      = f"{catalog}.{schema}"

    def _table_exists(self, table: str) -> bool:
        try:
            self.spark.sql(f"SELECT 1 FROM {table} LIMIT 1").collect()
            return True
        except Exception:
            return False

    def audit_summary(self, limit: int = 20):
        """Latest pipeline runs — status, duration, errors."""
        print("\n" + "="*60)
        print("AUDIT LOG — Latest Runs")
        print("="*60)
        return self.spark.sql(f"""
            SELECT
                pipeline_name,
                layer,
                status,
                round(duration_secs, 2)                    AS secs,
                error_message,
                date_format(created_at,'yyyy-MM-dd HH:mm:ss') AS run_time
            FROM {self.db}.pipeline_audit_log
            ORDER BY created_at DESC
            LIMIT {limit}
        """)

    def row_counts(self):
        """Row counts across all tables."""
        print("\n" + "="*60)
        print("TABLE ROW COUNTS")
        print("="*60)
        tables = [
            "landing_life_policy",
            "life_policy_overview_sync_event",
            "kafka_message_quarantine_tia",
            "kafka_message_quarantine_hugo",
            "pipeline_audit_log",
        ]
        queries = [
            f"SELECT '{t}' AS table_name, COUNT(*) AS rows FROM {self.db}.{t}"
            for t in tables if self._table_exists(f"{self.db}.{t}")
        ]
        if not queries:
            print("No tables found.")
            return None
        return self.spark.sql(" UNION ALL ".join(queries))

    def pipeline_performance(self):
        """Success rate and average duration per pipeline."""
        print("\n" + "="*60)
        print("PIPELINE PERFORMANCE")
        print("="*60)
        return self.spark.sql(f"""
            SELECT
                pipeline_name,
                layer,
                COUNT(*)                                             AS total_runs,
                SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END)  AS ok,
                SUM(CASE WHEN status='FAILED'  THEN 1 ELSE 0 END)  AS failed,
                ROUND(SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END)
                      * 100.0 / COUNT(*), 1)                        AS ok_pct,
                ROUND(AVG(CASE WHEN status='SUCCESS'
                               THEN duration_secs END), 2)          AS avg_secs,
                ROUND(MAX(duration_secs), 2)                        AS max_secs
            FROM {self.db}.pipeline_audit_log
            WHERE status IN ('SUCCESS','FAILED')
            GROUP BY pipeline_name, layer
            ORDER BY pipeline_name
        """)

    def validation_summary(self):
        """Valid vs invalid payloads per bronze table."""
        print("\n" + "="*60)
        print("PAYLOAD VALIDATION")
        print("="*60)
        tables = {
            "life_policy_overview_sync_event":
                f"{self.db}.life_policy_overview_sync_event",
        }
        results = []
        for name, table in tables.items():
            if self._table_exists(table):
                results.append(f"""
                    SELECT '{name}' AS table_name,
                        COUNT(*)   AS total,
                        SUM(CASE WHEN __message_valid_flag=true  THEN 1 ELSE 0 END) AS valid,
                        SUM(CASE WHEN __message_valid_flag=false THEN 1 ELSE 0 END) AS invalid,
                        ROUND(SUM(CASE WHEN __message_valid_flag=true THEN 1 ELSE 0 END)
                              *100.0/COUNT(*),1) AS valid_pct
                    FROM {table}
                """)
                                        
        if not results:
            print("No bronze tables found.")
            return None
        return self.spark.sql(" UNION ALL ".join(results))

    def quarantine_summary(self):
        """
        Messages that could not be deserialized at all.
        These need manual investigation or schema fix.
        """
        print("\n" + "="*60)
        print("QUARANTINE SUMMARY — Undeserializable Messages")
        print("="*60)
        tables = {
            "kdp":  f"{self.db}.kafka_message_quarantine_kdp",
            "tia":  f"{self.db}.kafka_message_quarantine_tia",
            "hugo": f"{self.db}.kafka_message_quarantine_hugo",
            "lisa": f"{self.db}.kafka_message_quarantine_lisa",
            "redy": f"{self.db}.kafka_message_quarantine_redy",
            "ccd":  f"{self.db}.kafka_message_quarantine_ccd",
        }
        results = []
        for source, table in tables.items():
            if self._table_exists(table):
                results.append(f"""
                    SELECT
                        '{source}'              AS source_system,
                        COUNT(*)                AS quarantined_messages,
                        MIN(__kafka_timestamp)  AS first_message,
                        MAX(__kafka_timestamp)  AS last_message,
                        COUNT(DISTINCT __schema_version_id) AS schema_versions_seen
                    FROM {table}
                """)
        if not results:
            print("No quarantine tables found — all messages deserialized successfully!")
            return None
        return self.spark.sql(" UNION ALL ".join(results))

    def quarantine_details(self, source_system: str, limit: int = 10):
        """
        Show individual quarantined messages for investigation.
        source_system: kdp, tia, hugo, lisa, redy, ccd
        """
        table = f"{self.db}.kafka_message_quarantine_{source_system.lower()}"
        if not self._table_exists(table):
            print(f"No quarantine table for {source_system}")
            return None
        print(f"\nQUARANTINE DETAILS — {source_system.upper()}")
        return self.spark.sql(f"""
            SELECT
                quarantine_id,
                __kafka_partition,
                __kafka_offset,
                __kafka_timestamp,
                __schema_version_id,
                __deserialization_error,
                __load_timestamp
            FROM {table}
            ORDER BY __load_timestamp DESC
            LIMIT {limit}
        """)

    def schema_versions(self):
        """Schema version IDs seen across bronze tables."""
        print("\n" + "="*60)
        print("SCHEMA VERSIONS IN BRONZE")
        print("="*60)
        tables = {
            "life_policy_overview_sync_event":
                f"{self.db}.life_policy_overview_sync_event",
            "nonlife_policy_overview_sync_event":
                f"{self.db}.nonlife_policy_overview_sync_event",
        }
        results = []
        for name, table in tables.items():
            if self._table_exists(table):
                results.append(f"""
                    SELECT '{name}' AS table_name,
                        __schema_version_id,
                        COUNT(*) AS messages,
                        MIN(__kafka_timestamp) AS first_seen,
                        MAX(__kafka_timestamp) AS last_seen
                    FROM {table}
                    GROUP BY __schema_version_id
                """)
        if not results:
            print("No bronze tables found.")
            return None
        return self.spark.sql(" UNION ALL ".join(results))

    def failed_runs(self, hours: int = 24):
        """All failed pipeline runs in last N hours."""
        print(f"\nFAILED RUNS — Last {hours} hours")
        return self.spark.sql(f"""
            SELECT pipeline_name, layer,
                   round(duration_secs,2) AS secs,
                   error_message,
                   date_format(created_at,'yyyy-MM-dd HH:mm:ss') AS run_time
            FROM {self.db}.pipeline_audit_log
            WHERE status = 'FAILED' AND created_at >= current_timestamp() - INTERVAL {hours} HOURS
            ORDER BY created_at DESC
        """)

    def run_all(self, render=None):
        """
        Run every monitoring check.

        render: optional callable(df). Defaults to df.display() (notebooks).
                The bundle's monitor task passes df.show() because
                display() only exists inside notebooks.
        """
        def _display(df):
            if df is None:
                return
            if render is not None:
                render(df)
            else:
                df.display()

        _display(self.audit_summary())
        _display(self.row_counts())
        _display(self.pipeline_performance())
        _display(self.validation_summary())
        _display(self.quarantine_summary())
        _display(self.schema_versions())