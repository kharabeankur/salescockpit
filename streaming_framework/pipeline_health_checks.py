# Databricks notebook source
# Check audit log for exact error messages
spark.sql("""
    SELECT pipeline_name, status, error_message, created_at
    FROM abd_easywithdata_poc.streaming_poc.pipeline_audit_log
    WHERE status = 'FAILED'
    ORDER BY created_at DESC
    LIMIT 5
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from abd_easywithdata_poc.streaming_poc.pipeline_audit_log order by start_time desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from abd_easywithdata_poc.streaming_poc.life_policy_overview_sync_event order by `__kafka_timestamp` desc limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from abd_easywithdata_poc.streaming_poc.landing_life_policy order by kafka_timestamp desc limit 10 

# COMMAND ----------

