# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructors.json File
# MAGIC 1. Read the file using spark dataframe API
# MAGIC 2. Define and Enforce(preseve nested schema)
# MAGIC 3. Add metadata columns
# MAGIC - source file
# MAGIC - ingestion timestamp
# MAGIC 4. Write to bronze delta table

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/02.bronze-helpers

# COMMAND ----------

source_file=f"{landing_folder_path}/drivers.json"
table_name=f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 :Read the JSON file using spark dataframe API

# COMMAND ----------

# define the schema struct
from pyspark.sql.types import StructType, StructField, StringType, DateType

name_schema= StructType([
    StructField("givenName", StringType()),
    StructField("familyName",StringType())
    ])

driver_schema = StructType([
    StructField("driverId", StringType()),
    StructField("name", name_schema),
    StructField("dateOfBirth", DateType()),
    StructField("nationality", StringType()),
    StructField("url", StringType())
    ])
# define the schema DDL
# driver_schema = """
# driverId STRING,
# name STRUCT<givenName: STRING, familyName: STRING>,
# dateOfBirth DATE,
# nationality STRING,
# url STRING
# """

# COMMAND ----------

drivers_df=(
  spark.read
  .format('json')
  .option('header', 'true')
  # .option('inferSchema', 'true')
  .option('mode','FailFast')
  .schema(driver_schema)
  .load(source_file)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 : add metadatacolumn
# MAGIC - sourcefile
# MAGIC - timestamp

# COMMAND ----------

final_drivers_df=add_ingestion_metadata(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 3 : write into bronze delta table

# COMMAND ----------

(
  final_drivers_df
  .write
  .mode("overwrite")
  .format("delta")
  .saveAsTable(table_name))