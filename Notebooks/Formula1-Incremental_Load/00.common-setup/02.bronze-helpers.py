# Databricks notebook source

from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("Source_file", F.col("_metadata.file_path"))
    )


# COMMAND ----------

def write_to_bronze(df,batch_id,target_table):
    df=df.withColumn('batch_id',F.lit(batch_id))
    (df
     .write
     .format('delta')
     .mode('overwrite').partitionBy('batch_id')
     .option('replaceWhere',f'batch_id="{batch_id}"')
     .saveAsTable(target_table))