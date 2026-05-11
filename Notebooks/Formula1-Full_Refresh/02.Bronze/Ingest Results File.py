# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructors.json File
# MAGIC 1. Read all the files from the folder using spark dataframe API
# MAGIC 2. Define and Enforce schema
# MAGIC 3. Add metadata columns
# MAGIC - source file
# MAGIC - ingestion timestamp
# MAGIC 4. Write to bronze delta table

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/02.bronze-helpers

# COMMAND ----------

source_file=f"{landing_folder_path}/results/"
table_name=f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 :Read the JSON file using spark dataframe API

# COMMAND ----------

# define the schema DDL
result_schema="""date DATE,raceName STRING,round INT,season INT, url STRING,constructorId STRING,driverId STRING,grid INT,laps INT,number INT,points FLOAT,position INT,positionText STRING,status STRING"""


# COMMAND ----------

result_df=(
  spark.read
  .format('json')
  # .option('inferSchema', 'true')
  .option('mode','FailFast')
  .schema(result_schema)
  .load(source_file)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 : add metadatacolumn
# MAGIC - sourcefile
# MAGIC - timestamp

# COMMAND ----------

final_results_df=add_ingestion_metadata(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 3 : write into bronze delta table

# COMMAND ----------

(
  final_results_df
  .write
  .mode("overwrite")
  .format("delta")
  .saveAsTable(table_name))