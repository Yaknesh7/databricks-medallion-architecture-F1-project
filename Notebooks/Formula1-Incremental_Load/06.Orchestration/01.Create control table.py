# Databricks notebook source
# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

spark.sql(f"""
create schema if not exists {catalog_name}.{control_schema}
""")

# COMMAND ----------

spark.sql(f""" create table if not exists {catalog_name}.{control_schema}.batch_control
     (
         batch_id string,
         batch_status string,
         created_timestamp timestamp,
         updated_timestamp timestamp
     )""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1_incr.control.batch_control

# COMMAND ----------

tables = spark.catalog.listTables("formula1_incr.gold")

for table in tables:
    
    spark.sql(f"""
        DROP TABLE IF EXISTS formula1_incr.gold.{table.name}
    """)