# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Circuit.csv File
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

source_file=f"{landing_folder_path}/{v_batch_id}/circuits.csv"
table_name=f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, DoubleType
circuits_schema=StructType([StructField('circuitId', StringType()),
            StructField('url', StringType()),
            StructField('circuitname', StringType()),
            StructField('lat', DoubleType()),
            StructField('long', DoubleType()),
            StructField('locality', StringType()),
            StructField('country', StringType())])

# COMMAND ----------


circuit_df=(
  spark.read
  .format('csv')
  .option('header', 'true')
  # .option('inferSchema', 'true')
  .option('mode','FailFast')
  .schema(circuits_schema)
  .load(source_file)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : add metadatacolumn
# MAGIC - sourcefile
# MAGIC - timestamp

# COMMAND ----------


final_circuits_df=add_ingestion_metadata(circuit_df)

# COMMAND ----------

final_circuits_df=final_circuits_df.withColumn('batch_id',F.lit(v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 3 : write into bronze delta table

# COMMAND ----------

write_to_bronze(final_circuits_df,v_batch_id,table_name)

# COMMAND ----------


display(spark.table('formula1_incr.bronze.circuits'))