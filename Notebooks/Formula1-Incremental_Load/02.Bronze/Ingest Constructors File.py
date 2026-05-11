# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructors.json File
# MAGIC 1. Read the file using spark dataframe API
# MAGIC 2. Add metadata columns
# MAGIC - source file
# MAGIC - ingestion timestamp
# MAGIC 3. Write to bronze delta table

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/02.bronze-helpers

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id=dbutils.widgets.get("p_batch_id")

# COMMAND ----------

source_file=f"{landing_folder_path}/{v_batch_id}/constructors.json"
table_name=f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 :Read the JSON file using spark dataframe API

# COMMAND ----------

# define the schema DDL
constructors_schema="""constructorId STRING,name STRING,nationality STRING,url STRING"""

# COMMAND ----------

constructors_df=(
  spark.read
  .format('json')
  .option('header', 'true')
  # .option('inferSchema', 'true')
  .option('mode','FailFast')
  .schema(constructors_schema)
  .load(source_file)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 : add metadatacolumn
# MAGIC - sourcefile
# MAGIC - timestamp

# COMMAND ----------

final_constructors_df=add_ingestion_metadata(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 3 : write into bronze delta table

# COMMAND ----------

write_to_bronze(final_constructors_df,v_batch_id,table_name)

# COMMAND ----------

display(spark.read.table(table_name))