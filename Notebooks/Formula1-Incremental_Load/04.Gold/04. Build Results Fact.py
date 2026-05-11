# Databricks notebook source
# MAGIC %md
# MAGIC # Build Results Fact
# MAGIC
# MAGIC 1. Read silver `results` table
# MAGIC 1. Read silver `sprints` table
# MAGIC 1. Add new column `session_type` with values `RACE` or `SPRINT`
# MAGIC 1. UNION `results` and `sprints`
# MAGIC 1. Derive additional columns
# MAGIC     - is_win -> Indicates that the driver own the race
# MAGIC     - is_podium -> Indicates that the driver scored a podium result (1, 2, 3)
# MAGIC     - has_points -> Indicates that the driver has scored points
# MAGIC 1. Write the transformed data to gold `fact_session_results` table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text('p_batch_id',"")
v_batch_id=dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00.common/04.gold-helpers

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read source tables
# MAGIC - `silver.results`
# MAGIC - `silver.sprints`

# COMMAND ----------

result_df=(spark.table(f"{catalog_name}.{silver_schema}.results").filter(F.col('batch_id')==v_batch_id)
           .withColumn("session_type",F.lit("RACE"))
           .drop("race_name", "race_date", "ingestion_timestamp", "source_file","batch_id","created_timestamp","updated_timestamp"))

# COMMAND ----------

sprint_df=(spark.table(f"{catalog_name}.{silver_schema}.sprints").filter(F.col('batch_id')==v_batch_id)
           .withColumn("session_type",F.lit("SPRINT"))
           .drop("race_name", "race_date", "ingestion_timestamp", "source_file","batch_id","created_timestamp","updated_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - UNION `results` and `sprints`

# COMMAND ----------

result_sprint_df=result_df.unionByName(sprint_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Add dervied columns
# MAGIC 1. is_win -> Indicates that the driver own the race
# MAGIC 1. is_podium -> Indicates that the driver scored a podium result (1, 2, 3)
# MAGIC 1. has_points -> Indicates that the driver has scored points

# COMMAND ----------

fact_session_results_df = (result_sprint_df
 .withColumn('is_win',F.col("finished_position")==1)
 .withColumn("is_podium",F.col("finished_position").between(1,3))
 .withColumn("has_points", F.col("points") > 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the transformed data to the `gold` `fact_session_results` table

# COMMAND ----------

write_to_gold(fact_session_results_df,target_table,"t.round=s.round AND t.season=s.season AND t.driver_id=s.driver_id AND t.constructor_id=s.constructor_id AND t.session_type=s.session_type",fact_session_results_df.columns)