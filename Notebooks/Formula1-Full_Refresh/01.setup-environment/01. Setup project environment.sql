-- Databricks notebook source
-- MAGIC %md # Project Setup
-- MAGIC - 1.create external location for yogif1dlext_formula1
-- MAGIC - 2.create catalog-formula1
-- MAGIC - 3.create schema(DB)-landing,bronze,silver and gold
-- MAGIC - 4.create volumne/files in the landing schema

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@yogif1dlext.dfs.core.windows.net/landing'

-- COMMAND ----------

create external location if not exists yogif1dlext_formula1
url 'abfss://formula1@yogif1dlext.dfs.core.windows.net/'
with (storage credential `f1-ext-sc`)
comment 'external location for formula1 container'

-- COMMAND ----------

-- MAGIC %md #create catalog formula1

-- COMMAND ----------

show catalogs;

-- COMMAND ----------

create catalog if not exists formula1
managed location 'abfss://formula1@yogif1dlext.dfs.core.windows.net/'
comment 'This is the main catalog for formula1 project'

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md ##create schema landing,bronze,silver,gold

-- COMMAND ----------

create schema if not exists formula1.landing;
create schema if not exists formula1.bronze
managed location 'abfss://formula1@yogif1dlext.dfs.core.windows.net/bronze';
create schema if not exists formula1.silver
managed location 'abfss://formula1@yogif1dlext.dfs.core.windows.net/silver';
create schema if not exists formula1.gold
managed location 'abfss://formula1@yogif1dlext.dfs.core.windows.net/gold';

-- COMMAND ----------

show schemas

-- COMMAND ----------

use catalog formula1

-- COMMAND ----------

show schemas

-- COMMAND ----------

-- MAGIC %md ### create volumn files

-- COMMAND ----------

create external volume if not exists formula1.landing.files
location 'abfss://formula1@yogif1dlext.dfs.core.windows.net/landing'


-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files