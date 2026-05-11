# Databricks notebook source
# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Row
from delta.tables import DeltaTable as D

# COMMAND ----------

control_table=f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")


# COMMAND ----------

if v_batch_id:
    df=(spark
    .createDataFrame([Row(batch_id=v_batch_id, batch_status="Completed")])
    .withColumn("updated_timestamp",F.current_timestamp()))
    table=D.forName(spark,control_table)
    (
        table.alias('t').merge(df.alias('s'),'t.batch_id = s.batch_id AND t.batch_status = "in-progress"')
        .whenMatchedUpdate(
            set={
                "batch_status": "s.batch_status",
                "updated_timestamp": "s.updated_timestamp"
                })
        .execute()
    )
    print(f"batch id {v_batch_id} is completed")
else:
    raise Exception("batch id is invalid")
