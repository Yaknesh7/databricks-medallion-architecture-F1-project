# Databricks notebook source
# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Row

# COMMAND ----------

control_table=f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")


# COMMAND ----------

if v_batch_id:
    df=(spark
    .createDataFrame([Row(batch_id=v_batch_id, batch_status="in-progress")])
    .withColumn("created_timestamp",F.current_timestamp())
    .withColumn("updated_timestamp",F.current_timestamp()))
    (df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(control_table))
    print(f"batch id {v_batch_id} is in progress")

else:
    raise Exception("batch id is invalid")