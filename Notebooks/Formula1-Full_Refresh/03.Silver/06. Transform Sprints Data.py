# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Sprints Data
# MAGIC 1. Read bronze `sprints` table
# MAGIC 1. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 1. Standardise column names using snake_case (`constructorId` → `constructor_id`, `driverId` → `driver_id`, `raceName` → `race_name`, `positionText` → `finish_position_text`)
# MAGIC 1. Rename columns to make them more meaningful (`date` → `race_date`, `grid` → `grid_position`, `laps` → `completed_laps`, `number` → `car_number`, `position` → `finish_position`)
# MAGIC 1. Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)
# MAGIC 1. Remove duplicate records
# MAGIC 1. Transform values of column `race_name` to Title Case
# MAGIC 1. Write the transformed data to silver `sprints` table

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

bronze_table=f"{catalog_name}.{bronze_schema}.sprints"
silver_table=f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 to 4 Read Source Data, Select required columns & Standardise column names

# COMMAND ----------

sprints_df=(
    spark.table(bronze_table)
    .drop("url")
    .withColumnsRenamed(
                        {"driverId":"driver_id",
                        "constructorId":"constructor_id",
                        "raceName":"race_name",
                        "positionText":"finished_position_text"})
    .withColumnsRenamed({"date":"race_date",
                         "grid":"race_grid",
                        "laps":"completed_laps",
                        "number":"race_number",
                        "position":"finished_position"})
)



# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 & 6 Data Quality Checks
# MAGIC - Filter out rows where `season`, `round`, `custructor_id` or `driver_id` is null (business key validation)
# MAGIC - Remove duplicate records

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

sprints_distinct_df = (
    sprints_df
    .filter(
        F.col("driver_id").isNotNull() &
        F.col("constructor_id").isNotNull()&
        F.col("season").isNotNull()&
        F.col("round").isNotNull())
    .dropDuplicates(["driver_id","constructor_id","season","round"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Transform values of column `nationality` to Title Case

# COMMAND ----------

sprints_final_df=sprints_distinct_df.withColumn("race_name",F.initcap(F.col("race_name")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 8 - Write the transformed data to silver `sprints` table

# COMMAND ----------

(
    sprints_final_df.write.mode("overwrite").format("delta").saveAsTable(silver_table)
)