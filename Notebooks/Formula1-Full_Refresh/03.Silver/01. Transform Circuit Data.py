# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Circuits Data
# MAGIC
# MAGIC 1. Read bronze `circuits` table
# MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 1. Standardise column names using snake_case (`circuitId` → `circuit_id`, `circuitName` → `circuit_name`)
# MAGIC 1. Rename columns to make them more meaningful (`lat` → `latitude`, `long` → `longitude`)
# MAGIC 1. Filter out rows where `circuit_id` is null (business key validation)
# MAGIC 1. Remove duplicate records
# MAGIC 1. Transform values of columns `circuit_name` and `locality` to Title Case
# MAGIC 1. Write the transformed data to silver `circuits` table

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze `circuits` table

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.circuits"
silver_table=f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# circuits_df=spark.read.option('versionasof',0).table(bronze_table)
circuits_df=spark.table(bronze_table)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.keep only the columns required for analytics (drop url)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_selected_df=circuits_df.select(
  F.col("circuitid"),
  F.col("circuitname"),
  F.col("lat"),
  F.col("long"),
  F.col("locality"),
  F.col("country"),
  F.col("ingestion_timestamp"),
  F.col("Source_file")
  )

# COMMAND ----------

# MAGIC %md
# MAGIC another method

# COMMAND ----------

# circuit_selected_df.=spark.select("circuitid","circuitname","lat","long","locality","country","ingestion_timestamp","Source_file")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 & 4 standardize column name 

# COMMAND ----------

# circuits_renamed_df = (circuits_selected_df
#                        .withColumnRenamed("circuitid","circuit_id").
# withcolumnrenamed("circuitname","circuit_name").
# withcolumnrenamed("lat","latitude").
# withcolumnrenamed("long","longitude")
#                        )

# COMMAND ----------

circuits_renamed_df = (circuits_selected_df
                       .withColumnsRenamed({
                         "circuitid":"circuit_id",
                         "circuitname":"circuit_name",
                         "lat":"latitude",
                         "long":"longitude"
                         })
                       )

# COMMAND ----------

# MAGIC %md
# MAGIC ###  5. filter out the column where circuit_id is null (business key validation)
# MAGIC

# COMMAND ----------

# circuit_valid_ df = circuits_renamed_df.filter("circuit_id is not null")

# COMMAND ----------

circuits_valid_df= (circuits_renamed_df
                   .filter(F.col("circuit_id").isNotNull())
                   )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. remove the duplicate records

# COMMAND ----------

# circuits_distinct_df=circuits_valid_df.distinct()

# COMMAND ----------

circuits_distinct_df=circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.transform values of the column circuit_name and locality to titlecase
# MAGIC ### 8. write the transformed data to silver table

# COMMAND ----------

circuits_final_df= (circuits_distinct_df
                    .withColumn('circuit_name',F.initcap(F.col("circuit_name")))
                    .withColumn("locality",F.initcap(F.col("locality")))
)
display(circuits_final_df)

# COMMAND ----------

(
    circuits_final_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(silver_table)
)