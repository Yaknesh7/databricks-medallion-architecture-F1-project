# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Drivers Data
# MAGIC
# MAGIC 1. Read bronze `drivers` table
# MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 1. Standardise column names using snake_case (`driverId` → `driver_id`, `dateOfbirth` → `date_of_birth`)
# MAGIC 1. Concatenate `name.givenName` and `name.familyName` to create a new column called `driver_name` and transform the value to Title Case
# MAGIC 1. Remove duplicate records
# MAGIC 1. Transform values of column `nationality` to Title Case
# MAGIC 1. Write the transformed data to silver `drivers` table
# MAGIC

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.drivers"
silver_table=f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze `drivers` table

# COMMAND ----------

drivers_df=spark.table(bronze_table)
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Keep only the columns required for analytics (Drop url column)

# COMMAND ----------

drivers_selected_df=drivers_df.drop("url")

# COMMAND ----------

display(drivers_selected_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Standardise Column Names

# COMMAND ----------

drivers_renamed_df=drivers_selected_df.withColumnsRenamed({"driverId":"driver_id","dateOfBirth":"date_of_birth"})

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Remove duplicate records

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

drivers_distinct_df = (
    drivers_renamed_df.filter(F.col("driver_id").isNotNull()).dropDuplicates(["driver_id"])
)

# COMMAND ----------

display(drivers_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Concatenate name.givenName and name.familyName to create a new column called driver_name

# COMMAND ----------

drivers_concate_df=(drivers_distinct_df.
                    withColumn(
                        "driver_name",F.initcap(F.concat_ws(" ",F.col("name.givenName"),F.col("name.familyName"))
        )).drop("name"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Transform values of column `nationality` to Title Case

# COMMAND ----------

drivers_final_df=drivers_concate_df.withColumn("nationality",F.initcap(F.col("nationality")))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Step 7 - Write the transformed data to silver `drivers` table

# COMMAND ----------

(
    drivers_final_df.write.mode("overwrite").format("delta").saveAsTable(silver_table)
)