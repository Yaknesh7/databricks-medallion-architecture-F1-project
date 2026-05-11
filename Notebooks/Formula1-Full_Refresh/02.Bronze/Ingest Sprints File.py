# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructors.json File
# MAGIC 1. Read all the files from the folder using spark dataframe API
# MAGIC 2. Define and Enforce schema for multiLine
# MAGIC 3. Add metadata columns
# MAGIC - source file
# MAGIC - ingestion timestamp
# MAGIC 4. Write to bronze delta table

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/02.bronze-helpers

# COMMAND ----------

source_file=f"{landing_folder_path}/sprints/"
table_name=f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 1 :Read the JSON file using spark dataframe API

# COMMAND ----------

# define the schema struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

sprint_schema = StructType([
    StructField("date", DateType()),
    StructField("raceName", StringType()),
    StructField("round", IntegerType()),
    StructField("season", IntegerType()),
    StructField("url", StringType()),
    StructField("constructorId", StringType()),
    StructField("driverId", StringType()),
    StructField("grid", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("number", IntegerType()),
    StructField("points", FloatType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("status", StringType())
])


# COMMAND ----------

sprint_df=(
  spark.read
  .format('json')
  # .option('inferSchema', 'true')
  .option('mode','FailFast')
  .option('multiLine','true')
  .schema(sprint_schema)
  .load(source_file)
)

# COMMAND ----------

display(sprint_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 : add metadatacolumn
# MAGIC - sourcefile
# MAGIC - timestamp

# COMMAND ----------

final_sprints_df=add_ingestion_metadata(sprint_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 3 : write into bronze delta table

# COMMAND ----------

(
  final_sprints_df
  .write
  .mode("overwrite")
  .format("delta")
  .saveAsTable(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT season, count(*) from formula1.bronze.sprints group by season 