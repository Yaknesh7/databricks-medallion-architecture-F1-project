# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Constructors Data
# MAGIC
# MAGIC 1. Read bronze `constructors` table
# MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 1. Standardise column names using snake_case (`constructorId` → `constructor_id`)
# MAGIC 1. Rename columns to make them more meaningful (`name` → `constructor_name`)
# MAGIC 1. Remove duplicate records
# MAGIC 1. Transform values of column `nationality` to Title Case
# MAGIC 1. Write the transformed data to silver `constructors` table

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.constructors"
silver_table=f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze `constructors` table

# COMMAND ----------

constructors_df=spark.table(bronze_table)
display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Keep only the columns required for analytics (Drop url column)
# MAGIC

# COMMAND ----------

constructors_selected_df=constructors_df.drop("url")

# COMMAND ----------

display(constructors_selected_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 & 4 - Standardise Column Names
# MAGIC - Standardise column names using snake_case (`constructorId` → `constructor_id`)
# MAGIC - Rename columns to make them more meaningful (`name` → `constructor_name`)
# MAGIC

# COMMAND ----------

constructors_renamed_df=constructors_selected_df.withColumnsRenamed({"constructorId" :"constructor_id","name":"constructor_name"})

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Remove duplicate records

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

constructors_distinct_df = (
    constructors_renamed_df.filter(F.col("constructor_id").isNotNull()).dropDuplicates(["constructor_id"])
)

# COMMAND ----------

display(constructors_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Transform values of column `nationality` to Title Case

# COMMAND ----------

constructors_final_df=constructors_distinct_df.withColumn("nationality",F.initcap(F.col("nationality")))

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Write the transformed data to silver `constructors` table

# COMMAND ----------

(
    constructors_final_df.write.mode("overwrite").format("delta").saveAsTable(silver_table)
)