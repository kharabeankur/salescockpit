# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline health checks — failed runs, audit log, latest bronze rows and landing files

# COMMAND ----------

dbutils.widgets.text("catalog", "abd_easywithdata_poc")
dbutils.widgets.text("schema", "streaming_poc_dev")
DB = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

# COMMAND ----------

# Check audit log for exact error messages
spark.sql(f"""
    SELECT pipeline_name, status, error_message, created_at
    FROM {DB}.pipeline_audit_log
    WHERE status = 'FAILED'
    ORDER BY created_at DESC
    LIMIT 5
""").display()

# COMMAND ----------

spark.sql(f"SELECT * FROM {DB}.pipeline_audit_log ORDER BY start_time DESC").display()

# COMMAND ----------

# Bronze table written by the per-topic jobs is named after the topic
spark.sql(f"SELECT * FROM {DB}.life_policy_overview_sync ORDER BY `__kafka_timestamp` DESC LIMIT 10").display()

# COMMAND ----------

# Landing is now files, not a table
LANDING = f"/Volumes/{DB.replace('.', '/')}/Landing/kdp/life_policy_overview_sync/"
display(dbutils.fs.ls(LANDING))
