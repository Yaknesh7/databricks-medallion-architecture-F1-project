# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Races.csv File
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


source_file=f"{landing_folder_path}/races.csv"
table_name=f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
races_schema=StructType([StructField('season', IntegerType()),
            StructField('round', IntegerType()),
            StructField('url', StringType()),
            StructField('raceName', StringType()),
            StructField('date', DateType()),
            StructField('circuitid', StringType()),])

# COMMAND ----------


races_df=(
  spark.read
  .format('csv')
  .option('header', 'true')
# .option('inferSchema', 'true')
  .option('mode','FailFast')
  .schema(races_schema)
  .load(source_file)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : add metadatacolumn
# MAGIC - sourcefile
# MAGIC - timestamp

# COMMAND ----------


final_races_df=add_ingestion_metadata(races_df)
  

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 3 : write into bronze delta table

# COMMAND ----------

(
final_races_df
.write
.format('delta')
.mode('overwrite')
.saveAsTable(table_name))