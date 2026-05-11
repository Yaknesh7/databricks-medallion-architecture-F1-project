# Databricks notebook source
from delta.tables import DeltaTable as D

# COMMAND ----------

def write_to_gold(df,target_table,merge_on,columns):
    df=(df
    .withColumn('created_timestamp',F.current_timestamp())
    .withColumn('updated_timestamp',F.current_timestamp()))

    if spark.catalog.tableExists(target_table):
        delta=D.forName(spark,target_table)
        allcolumn={column:f"s.{column}"for column in columns}
        allcolumn["updated_timestamp"]='s.updated_timestamp'
        (delta.alias('t').merge(df.alias('s'),merge_on)
        .whenMatchedUpdate(
            set=allcolumn)
        .whenNotMatchedInsertAll()
        .execute())
    else:
        (df.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(target_table))
