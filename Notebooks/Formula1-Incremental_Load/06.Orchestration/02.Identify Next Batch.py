# Databricks notebook source
# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

control_table=f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

Batchid_in_landing=sorted(
    [ _.name.rstrip("/") for _ in dbutils.fs.ls(landing_folder_path) if _.isDir()]
    )

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable as D

# COMMAND ----------

if spark.catalog.tableExists(control_table):
    batchid_in_control_table = (
        spark.table(f'{control_table}')
        .filter(F.col('batch_status').isin("in-progress","completed"))
        .select('batch_id')
        .distinct()
        .collect())
    batchid_in_table=[_.batch_id for _ in batchid_in_control_table]
else:
    batchid_in_table=[]

# COMMAND ----------

batchids=sorted(list(set(Batchid_in_landing)-set(batchid_in_table)))

# COMMAND ----------

if batchids:
    next_batchid = batchids[0]
else:
    next_batchid = None
if next_batchid is None:
    dbutils.jobs.taskValues.set("p_bactch_id","")
    dbutils.jobs.taskValues.set("has_batch_id","false")
else:
    dbutils.jobs.taskValues.set("p_bactch_id",next_batchid)
    dbutils.jobs.taskValues.set("has_batch_id","true")

# COMMAND ----------

print(next_batchid)
print(batchids)
print(Batchid_in_landing)