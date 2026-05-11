# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Races Data
# MAGIC
# MAGIC 1. Read bronze `races` table
# MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 1. Standardise column names using snake_case (`raceName` → `race_name`, `circuitId` → `circuit_id`)
# MAGIC 1. Rename columns to make them more meaningful (`date` → `race_date`)
# MAGIC 1. Remove duplicate records
# MAGIC 1. Transform values of column `race_name` to Title Case
# MAGIC 1. Write the transformed data to silver `races` table
# MAGIC

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.races"
silver_table=f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze `races` table

# COMMAND ----------

races_df=spark.table(bronze_table)
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Keep only the columns required for analytics (Drop url column)
# MAGIC

# COMMAND ----------

races_selected_df=races_df.drop("url")

# COMMAND ----------

display(races_selected_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 & 4 - Standardise Column Names
# MAGIC - Standardise column names using snake_case (`circuitId` → `circuit_id`, `raceName` → `race_name`)
# MAGIC - Rename columns to make them more meaningful (`date` → `race_date`)

# COMMAND ----------

races_renamed_df=races_selected_df.withColumnsRenamed({"raceName" : "race_name", "circuitid" : "circuit_id"})

# COMMAND ----------

races_renamed_df=races_renamed_df.withColumnRenamed("date","race_date")

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Remove duplicate records

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_distinct_df = (
    races_renamed_df.filter((F.col("season").isNotNull()) &(F.col("round").isNotNull()))
    .dropDuplicates(["season", "round"])
)

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Transform values of column `race_name` to Title Case

# COMMAND ----------

races_final_df=races_distinct_df.withColumn("race_name",F.initcap(F.col("race_name")))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Write the transformed data to silver `races` table

# COMMAND ----------

(
    races_final_df.write.mode("overwrite").format("delta").saveAsTable(silver_table)
)