# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Results Data
# MAGIC 1. Read bronze `results` table
# MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 1. Standardise column names using snake_case (`constructorId` → `constructor_id`, `driverId` → `driver_id`, `raceName` → `race_name`, `positionText` → `finish_position_text`)
# MAGIC 1. Rename columns to make them more meaningful (`date` → `race_date`, `grid` → `grid_position`, `laps` → `completed_laps`, `number` → `car_number`, `position` → `finish_position`)
# MAGIC 1. Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)
# MAGIC 1. Remove duplicate records
# MAGIC 1. Transform values of column `race_name` to Title Case
# MAGIC 1. Write the transformed data to silver `results` table

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.results"
silver_table=f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze `results` table

# COMMAND ----------

results_df=spark.table(bronze_table)
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Keep only the columns required for analytics (Drop url column)

# COMMAND ----------

results_selected_df=results_df.drop("url")

# COMMAND ----------

display(results_selected_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 & 4 - Standardise Column Names
# MAGIC - Standardise column names using snake_case (`constructorId` → `constructor_id`, `driverId` → `driver_id`, `raceName` → `race_name`, `positionText` → `finish_position_text`)
# MAGIC - Rename columns to make them more meaningful (`date` → `race_date`, `grid` → `grid_position`, `laps` → `completed_laps`, `number` → `car_number`, `position` → `finish_position`)

# COMMAND ----------

results_renamed_df=(
    results_selected_df.withColumnsRenamed(
                        {"driverId":"driver_id",
                        "constructorId":"constructor_id",
                        "raceName":"race_name","positionText":
                        "finished_position_text"})
                        .withColumnRenamed("date","race_date")
                        .withColumnRenamed("grid","race_grid")
                        .withColumnRenamed("laps","completed_laps")
                        .withColumnRenamed("number","race_number")
                        .withColumnRenamed("position","finished_position"))
    


# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)%md
# MAGIC #### Step 6 - Remove duplicate records

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

results_distinct_df = (
    results_renamed_df
    .filter(
        F.col("driver_id").isNotNull() &
        F.col("constructor_id").isNotNull()&
        F.col("season").isNotNull()&
        F.col("round").isNotNull())
    .dropDuplicates(["driver_id","constructor_id","season","round"])
)

# COMMAND ----------

display(results_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Transform values of column `race_name` to Title Case

# COMMAND ----------

results_final_df=results_distinct_df.withColumn("race_name",F.initcap(F.col("race_name")))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 8 - Write the transformed data to silver `results` table

# COMMAND ----------

# (
#     drivers_final_df.write.mode("overwrite").format("delta").saveAsTable(silver_table)
# )